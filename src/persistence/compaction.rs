//! Generation-based compaction for crash-safe state management.
//!
//! # Why Generation-Based Compaction?
//!
//! The naive approach of "write snapshot, then truncate log" is **not crash-safe**:
//! - Crash after snapshot write, before log truncation → replays duplicates
//! - Crash during truncation → log corruption
//!
//! Instead, we use generation-based compaction:
//! 1. Increment generation number N → N+1
//! 2. Write new snapshot to `snapshot.<N+1>.json` with `log_generation = N+1`, `log_position = 0`
//! 3. fsync snapshot, then fsync directory
//! 4. Update `generation` file to N+1, fsync, fsync directory
//! 5. **Only after** the new generation is durable, delete old files
//!
//! At any crash point, either the old or new generation is complete and usable.
//!
//! # The settlement invariant
//!
//! The live process never deletes a snapshot whose generation might be
//! committed; only startup recovery (`cleanup_stale_generations`, under the
//! state-directory lock) settles ambiguous states. Concretely: compaction's
//! rollback path runs only on provably-uncommitted failures
//! ([`WriteGenerationError::NotCommitted`] or a failed snapshot write, both
//! of which leave the generation file pointing at the old generation), while
//! a possibly-committed failure ([`WriteGenerationError::Ambiguous`]) deletes
//! nothing, is fatal for the handle, and is resolved by the next recovery —
//! which loses nothing in either world.
//!
//! # Recovery
//!
//! On startup (via `cleanup_stale_generations`):
//! 1. Scan for the highest existing snapshot generation (handles crash during compaction)
//! 2. Semantically validate that snapshot — including against its event log,
//!    with the same checks recovery applies — and refuse (deleting nothing)
//!    any state showing a committed generation's snapshot was lost
//! 3. If the generation file is stale, missing, or corrupt, restore it to match the
//!    highest snapshot
//! 4. Load the snapshot for the current generation
//! 5. Replay events for the current generation from the snapshot's `log_position`
//! 6. Clean up files from other generations

use std::io;
use std::path::{Path, PathBuf};

use thiserror::Error;

use super::event::StateEvent;
use super::fsync::{fsync_dir, remove_file};
use super::generation::{
    GenerationFileKind, WriteGenerationError, delete_old_generation, events_path,
    find_current_snapshot, list_generation_files, read_generation, snapshot_path, write_generation,
};
use super::log::{EventLog, EventLogError};
use super::snapshot::{PersistedRepoSnapshot, load_snapshot, save_snapshot_atomic};

/// Errors that can occur during compaction.
#[derive(Debug, Error)]
pub enum CompactionError {
    /// IO error during file operations.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Generation file error.
    #[error("generation error: {0}")]
    Generation(#[from] super::generation::GenerationError),

    /// Generation file write error, carrying its position relative to the
    /// commit point; see [`WriteGenerationError`].
    #[error("generation write error: {0}")]
    WriteGeneration(#[from] WriteGenerationError),

    /// Snapshot error.
    #[error("snapshot error: {0}")]
    Snapshot(#[from] super::snapshot::SnapshotError),

    /// Event log error while validating the snapshot against the log.
    #[error("event log error: {0}")]
    EventLog(#[from] EventLogError),

    /// A snapshot exists at a higher generation than the generation file
    /// records: `cleanup_stale_generations` was not called first.
    #[error(
        "snapshot exists at generation {max_snapshot_gen} but generation file says \
         {file_gen}; call cleanup_stale_generations first"
    )]
    StaleGenerations {
        max_snapshot_gen: u64,
        file_gen: u64,
    },

    /// The caller's in-memory snapshot does not describe the on-disk state:
    /// its `log_generation` disagrees with the generation file.
    #[error(
        "snapshot claims log_generation {snapshot_generation} but the generation file \
         says {disk_generation}; the snapshot does not describe the on-disk state"
    )]
    SnapshotGenerationMismatch {
        snapshot_generation: u64,
        disk_generation: u64,
    },

    /// The caller's in-memory snapshot has not folded in every event in the
    /// current log: compacting would delete events that exist nowhere else.
    #[error(
        "snapshot next_seq is {snapshot_next_seq} but the event log {} implies next_seq \
         {log_next_seq}; compacting would delete events never folded into the snapshot",
        path.display()
    )]
    NextSeqInconsistent {
        path: PathBuf,
        snapshot_next_seq: u64,
        log_next_seq: u64,
    },

    /// The generation file records a generation with no snapshot file (and
    /// the directory is not fresh). Compaction writes `snapshot.<N>` before
    /// advancing the generation file to N, so the committed generation's
    /// snapshot has been lost. "Repairing" the generation file to point at
    /// an older snapshot would silently roll back committed state, and the
    /// cleanup loop would then delete the committed generation's remaining
    /// files.
    #[error(
        "generation file says {file_gen} but the highest snapshot on disk is {}; \
         generation {file_gen} was committed and its snapshot has been lost; \
         refusing to repair the generation file or delete anything; manual \
         intervention required",
        describe_max_snapshot_gen(max_snapshot_gen)
    )]
    MissingSnapshot {
        file_gen: u64,
        max_snapshot_gen: Option<u64>,
    },

    /// An event log exists at a generation higher than every snapshot. Event
    /// logs are only created once their generation is committed (snapshot
    /// written, generation file advanced), so that generation's snapshot and
    /// generation-file record have both been lost; deleting the log would
    /// destroy the only remaining trace of its events.
    #[error(
        "event log {} is from generation {log_generation} but the highest snapshot \
         on disk is {}; generation {log_generation} was committed and its snapshot \
         has been lost; refusing to delete anything; manual intervention required",
        path.display(),
        describe_max_snapshot_gen(max_snapshot_gen)
    )]
    EventLogAheadOfSnapshots {
        path: PathBuf,
        log_generation: u64,
        max_snapshot_gen: Option<u64>,
    },

    /// The candidate (highest-generation) snapshot deserializes but its
    /// embedded `log_generation` disagrees with the generation in its
    /// filename: it does not describe the generation it is stored as.
    /// Promoting it would delete the other generations and recovery would
    /// then fail its `GenerationMismatch` hard check, after the last good
    /// generation was already gone.
    #[error(
        "snapshot {} says log_generation {snapshot_generation} but its filename \
         says generation {file_generation}; refusing to promote it or delete \
         anything; manual intervention required",
        path.display()
    )]
    CandidateGenerationMismatch {
        path: PathBuf,
        snapshot_generation: u64,
        file_generation: u64,
    },

    /// The event log is shorter than the snapshot's recorded `log_position`:
    /// bytes the snapshot has seen as durable are gone.
    #[error(
        "event log {} is {file_len} bytes but the snapshot's log_position is {log_position}; \
         the log has lost data; manual intervention required",
        path.display()
    )]
    LogTruncated {
        path: PathBuf,
        log_position: u64,
        file_len: u64,
    },

    /// The snapshot's `next_seq` does not line up with the events found in
    /// the log: events are missing or duplicated across snapshot and log.
    #[error(
        "sequence mismatch in {}: snapshot next_seq is {snapshot_next_seq} and {replayed} \
         events were replayed after it, but the log implies next_seq {log_next_seq}; \
         manual intervention required",
        path.display()
    )]
    SequenceMismatch {
        path: PathBuf,
        snapshot_next_seq: u64,
        replayed: usize,
        log_next_seq: u64,
    },

    /// FATAL: a step of compaction failed in a way that may have left an
    /// orphaned `snapshot.<N+1>` on disk (the snapshot write failed, or the
    /// generation file update failed *before its rename* — a post-rename
    /// failure is [`AmbiguousCommit`](Self::AmbiguousCommit) instead, and
    /// never attempts a rollback) AND rolling the orphan back also
    /// failed. On the next startup `cleanup_stale_generations` will promote
    /// the surviving orphan to generation N+1 and delete `events.<N>.log`,
    /// so any event appended to generation N after this error would be
    /// silently destroyed. [`super::recovery::PersistenceHandle::compact`]
    /// therefore poisons the handle on this error; the process must restart
    /// and recover. Restarting is safe: the orphan folds in every event
    /// appended before the failed compaction (guaranteed by the
    /// folded-everything guards), so whichever generation recovery settles
    /// on, no event is lost.
    #[error(
        "FATAL: compaction failed ({original}) and rolling back the orphaned \
         snapshot {} also failed ({rollback_error}); the orphan may survive on \
         disk, and the next recovery would promote it and delete the current \
         generation's event log; no further events may be appended; restart \
         and recover",
        snapshot_path.display()
    )]
    RollbackFailed {
        snapshot_path: PathBuf,
        original: Box<CompactionError>,
        rollback_error: io::Error,
    },

    /// FATAL: the generation-file update failed after its rename: the new
    /// generation is already live in the filesystem namespace, but whether
    /// it survives a crash is unknown. No running-process reaction is safe
    /// in both worlds — in particular, deleting `snapshot.<N+1>` as
    /// "rollback" would, in the world where the rename is durable, leave
    /// the generation file pointing at a missing snapshot, which recovery
    /// refuses to repair ([`MissingSnapshot`](Self::MissingSnapshot)).
    /// Nothing is deleted on this error, and
    /// [`super::recovery::PersistenceHandle::compact`] poisons the handle;
    /// the process must restart and let recovery settle the directory.
    /// Restarting is safe in both worlds: if the rename survived, recovery
    /// loads `snapshot.<N+1>` (which folds in every event appended before
    /// the compaction — the folded-everything guards); if it did not,
    /// recovery promotes the surviving orphan `snapshot.<N+1>` — the same
    /// data either way, and poisoning ensures no event was appended in
    /// between.
    #[error(
        "FATAL: generation update to {new_gen} is ambiguous (renamed, then: \
         {source}); the new generation is live in the namespace but its \
         crash durability is unknown; snapshot {} was NOT deleted; no \
         further events may be appended; restart and recover",
        snapshot_path.display()
    )]
    AmbiguousCommit {
        new_gen: u64,
        snapshot_path: PathBuf,
        source: io::Error,
    },
}

/// Renders the "highest snapshot on disk" part of cleanup error messages.
fn describe_max_snapshot_gen(max: &Option<u64>) -> String {
    match max {
        Some(g) => format!("generation {g}"),
        None => "no snapshot at all".to_string(),
    }
}

/// Result type for compaction operations.
pub type Result<T> = std::result::Result<T, CompactionError>;

/// Outcome of a compaction whose commit succeeded.
///
/// Post-commit cleanup failure is deliberately distinguishable from a failed
/// compaction: once the generation file points at the new generation, the
/// compaction has happened (the in-memory snapshot is updated and appends
/// must go to the new generation's log), regardless of whether the old
/// generation's files could be removed.
#[must_use]
#[derive(Debug)]
pub enum CompactionOutcome {
    /// The new generation is committed and the old generation's files were
    /// removed.
    Clean,
    /// The new generation is committed, but removing the old generation's
    /// files failed. The stale files are harmless and will be removed by
    /// `cleanup_stale_generations` on the next startup.
    CleanupPending(CompactionError),
}

/// Performs generation-based compaction.
///
/// This creates a new generation with a fresh snapshot and empty event log,
/// then cleans up old generation files.
///
/// Crate-private on purpose: committing a compaction deletes the old
/// generation's log file, so any live [`EventLog`] on it must be switched to
/// the new generation at the same time. The public entry point is
/// [`super::recovery::PersistenceHandle::compact`], which does exactly that.
///
/// # Arguments
///
/// * `state_dir` - The state directory containing snapshot and event files
/// * `snapshot` - The current state to persist as the new generation's snapshot
///
/// # Prerequisites
///
/// `cleanup_stale_generations` MUST be called before `compact` to ensure
/// the generation file is consistent with existing snapshots. Failure to do
/// so may result in creating a new generation that conflicts with existing
/// (higher-numbered) snapshots.
///
/// # Crash Safety
///
/// At any crash point during compaction:
/// - The old generation remains complete and usable
/// - The new generation is either complete or can be detected as incomplete
///
/// Recovery will use whichever generation is complete.
///
/// # Error Handling
///
/// If validation, the snapshot write, or the generation file update fails,
/// the in-memory `snapshot` is left unchanged. What happens on disk depends
/// on which side of the commit point (the generation file's rename) the
/// failure fell:
///
/// - **Provably uncommitted** (the snapshot write failed, or the generation
///   update failed before its rename): the possibly-present orphan
///   `snapshot.<N+1>` is rolled back, so that on a
///   non-[`RollbackFailed`](CompactionError::RollbackFailed) error the old
///   generation really is authoritative and the caller can safely retry or
///   continue with it. Rolling back is safe precisely because the
///   generation file provably still points at the old generation. If the
///   rollback itself fails, the error is the fatal
///   [`CompactionError::RollbackFailed`]: the surviving orphan would cause
///   `events.<N>.log` to be deleted on the next startup, so no further
///   event may be appended to it.
///
/// - **Ambiguously committed** (the generation update failed after its
///   rename): nothing is deleted, and the error is the fatal
///   [`CompactionError::AmbiguousCommit`]. The new generation is already
///   live in the filesystem namespace and may or may not survive a crash;
///   only the next recovery can settle which world this is, and it loses
///   nothing in either (see the variant's documentation).
///
/// On both fatal errors [`super::recovery::PersistenceHandle::compact`]
/// poisons the handle, and the process must restart and recover.
///
/// # Ordering invariant
///
/// `snapshot.<N+1>` is made fully durable (its own directory fsync
/// included) strictly **before** the generation-file update begins. The
/// crash-safety of [`AmbiguousCommit`](CompactionError::AmbiguousCommit)
/// depends on this: in the world where the generation rename survives, the
/// snapshot it points to must already be on disk. Do not reorder these
/// steps.
///
/// If cleanup of old generation files fails (after the new generation is
/// committed), the in-memory `snapshot` already reflects the new generation
/// and [`CompactionOutcome::CleanupPending`] is returned: once the generation
/// file is updated, events must be written to the new generation's log. The
/// old files are cleaned up on the next startup via
/// `cleanup_stale_generations`.
pub(crate) fn compact(
    state_dir: &Path,
    snapshot: &mut PersistedRepoSnapshot,
) -> Result<CompactionOutcome> {
    // 1. Read current generation
    let old_gen = read_generation(state_dir)?;

    // Guard: verify no snapshots exist at higher generations.
    // This indicates cleanup_stale_generations wasn't called first.
    if let Some((max_gen, _)) = find_current_snapshot(state_dir)?
        && max_gen > old_gen
    {
        return Err(CompactionError::StaleGenerations {
            max_snapshot_gen: max_gen,
            file_gen: old_gen,
        });
    }

    // Guard: the caller's snapshot must describe the on-disk state.
    if snapshot.log_generation != old_gen {
        return Err(CompactionError::SnapshotGenerationMismatch {
            snapshot_generation: snapshot.log_generation,
            disk_generation: old_gen,
        });
    }

    // Guard: the snapshot must have folded in every event in the current log
    // (snapshot.next_seq == max seq in log + 1), otherwise deleting the log
    // below would destroy events that exist nowhere else. An empty or missing
    // log carries no evidence either way (a freshly compacted generation's
    // log is empty while next_seq carries over), so only a non-empty log is
    // checked.
    let log_path = events_path(state_dir, old_gen);
    let (_, log_next_seq) = EventLog::replay_from(&log_path, u64::MAX)?;
    if log_next_seq != 0 && log_next_seq != snapshot.next_seq {
        return Err(CompactionError::NextSeqInconsistent {
            path: log_path,
            snapshot_next_seq: snapshot.next_seq,
            log_next_seq,
        });
    }

    let new_gen = old_gen + 1;

    // 2. Stage changes in a clone (don't mutate original until disk writes succeed)
    let mut staged = snapshot.clone();
    staged.log_generation = new_gen;
    staged.log_position = 0; // Start of new event log
    staged.touch(); // Update snapshot_at timestamp

    // 3. Write new snapshot (atomic)
    // "Atomic" covers the rename, not the whole call: a failure of the
    // directory fsync AFTER the rename leaves snapshot.<N+1> on disk, the
    // same orphan hazard as a pre-rename generation-file update failure
    // below. Roll the orphan back on any failure (a no-op when the rename
    // never happened). Unlike the generation update, rolling back is safe
    // wherever this step failed: the generation file is untouched at this
    // point, so the old generation is provably still authoritative.
    //
    // Ordering invariant: this step (including its directory fsync) must
    // complete before write_generation below begins — in the world where a
    // failed write_generation's rename nonetheless survives a crash, the
    // snapshot it points at must already be durable.
    let new_snapshot_path = snapshot_path(state_dir, new_gen);
    if let Err(e) = save_snapshot_atomic(&new_snapshot_path, &staged) {
        return Err(rollback_orphaned_snapshot(
            state_dir,
            &new_snapshot_path,
            e.into(),
        ));
    }

    // 4. Update generation file (atomic)
    // This is the commit point - once this completes, the new generation is active.
    //
    // A failure BEFORE the commit point (NotCommitted) is a clean abort: the
    // old generation is still authoritative, but the orphaned snapshot.<N+1>
    // must be removed to prevent data loss — without cleanup, the process
    // could continue appending to events.<N>.log, then on restart
    // cleanup_stale_generations() would promote to N+1 and delete those
    // events.
    //
    // A failure AFTER the commit point (Ambiguous) is the opposite: the new
    // generation may already be committed, so snapshot.<N+1> must NOT be
    // removed — in the world where the rename survives, deleting it would
    // leave the generation file pointing at a missing snapshot, which
    // recovery refuses to repair (MissingSnapshot). Nothing may be deleted
    // here; only the next recovery can settle which world this is.
    match write_generation(state_dir, new_gen) {
        Ok(()) => {}
        Err(e @ WriteGenerationError::NotCommitted(_)) => {
            return Err(rollback_orphaned_snapshot(
                state_dir,
                &new_snapshot_path,
                e.into(),
            ));
        }
        Err(WriteGenerationError::Ambiguous(source)) => {
            return Err(CompactionError::AmbiguousCommit {
                new_gen,
                snapshot_path: new_snapshot_path,
                source,
            });
        }
    }

    // 5. Commit to in-memory state BEFORE deleting old files.
    // This ensures snapshot.log_generation points to a valid generation if
    // events are written between here and the deletion below.
    *snapshot = staged;

    // 6. Clean up old generation files.
    // Safe to delete now - any new events will go to the new generation.
    // Failure here is NOT a failed compaction (the commit is durable); it is
    // reported as CleanupPending and repaired on the next startup.
    match delete_old_generation(state_dir, old_gen) {
        Ok(()) => Ok(CompactionOutcome::Clean),
        Err(e) => Ok(CompactionOutcome::CleanupPending(e.into())),
    }
}

/// Removes the (possibly existing) orphaned `snapshot.<N+1>` after a failed
/// snapshot write or a *pre-rename*
/// ([`NotCommitted`](WriteGenerationError::NotCommitted)) generation-file
/// update failure, returning the error `compact` should report.
///
/// Must only be called while the generation file provably still points at
/// the old generation: removing `snapshot.<N+1>` when the generation file
/// may already point at N+1 manufactures the unrecoverable
/// [`MissingSnapshot`](CompactionError::MissingSnapshot) state. A
/// post-rename failure is [`CompactionError::AmbiguousCommit`] and never
/// reaches this function.
///
/// On rollback success (including the orphan never having reached disk) the
/// original error is returned: the compaction failed cleanly and the old
/// generation remains authoritative. On rollback failure the fatal
/// [`CompactionError::RollbackFailed`] is returned instead: the orphan left
/// on disk would cause `cleanup_stale_generations` to promote to the new
/// generation on the next startup and delete the current event log.
fn rollback_orphaned_snapshot(
    state_dir: &Path,
    orphaned_snapshot: &Path,
    original: CompactionError,
) -> CompactionError {
    let removed = match remove_file(orphaned_snapshot) {
        Ok(()) => Ok(()),
        // Already gone: the rollback is trivially complete.
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    };
    // The unlink must be durable: if the directory entry survives a crash,
    // the orphan is back.
    match removed.and_then(|()| fsync_dir(state_dir)) {
        Ok(()) => original,
        Err(rollback_error) => CompactionError::RollbackFailed {
            snapshot_path: orphaned_snapshot.to_path_buf(),
            original: Box::new(original),
            rollback_error,
        },
    }
}

/// Validates a snapshot against its generation's on-disk event log, returning
/// the events recorded after the snapshot's `log_position` and the next
/// sequence number to append with.
///
/// These are the log-consistency checks recovery applies before trusting a
/// generation: the log must be at least `log_position` bytes long (before and
/// after replay's torn-tail repair), replay must not report uncharacterizable
/// corruption, and the replayed events must carry exactly the sequence
/// numbers the snapshot expects next. `cleanup_stale_generations` applies
/// them to the candidate snapshot before committing to it, so a candidate
/// that recovery would reject can never cause the older generations to be
/// deleted; `recover` then applies them to the settled generation as the
/// backstop (and to consume the replayed events).
///
/// The only file mutation this can perform is `EventLog::replay_from`'s
/// torn-tail truncation of the log.
pub(super) fn validate_snapshot_against_log(
    state_dir: &Path,
    snapshot: &PersistedRepoSnapshot,
) -> Result<(Vec<StateEvent>, u64)> {
    let log_path = events_path(state_dir, snapshot.log_generation);
    let file_len = match std::fs::metadata(&log_path) {
        Ok(m) => m.len(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
        Err(e) => return Err(e.into()),
    };
    if snapshot.log_position > file_len {
        return Err(CompactionError::LogTruncated {
            path: log_path,
            log_position: snapshot.log_position,
            file_len,
        });
    }
    let (events, log_next_seq) = EventLog::replay_from(&log_path, snapshot.log_position)?;

    // Replay may truncate a torn tail write, but it must never cut below the
    // snapshot's position: those bytes were durable, complete events when the
    // snapshot recorded them.
    let file_len_after = match std::fs::metadata(&log_path) {
        Ok(m) => m.len(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
        Err(e) => return Err(e.into()),
    };
    if snapshot.log_position > file_len_after {
        return Err(CompactionError::LogTruncated {
            path: log_path,
            log_position: snapshot.log_position,
            file_len: file_len_after,
        });
    }

    // Cross-check: events in the log after the snapshot's position must carry
    // exactly the sequence numbers the snapshot expects next. A log with no
    // events at all carries no evidence either way (a fresh generation's log
    // starts empty with next_seq carried over from the previous generation),
    // so only a non-empty log is checked.
    if log_next_seq == 0 {
        Ok((events, snapshot.next_seq))
    } else if log_next_seq == snapshot.next_seq + events.len() as u64 {
        Ok((events, log_next_seq))
    } else {
        Err(CompactionError::SequenceMismatch {
            path: log_path,
            snapshot_next_seq: snapshot.next_seq,
            replayed: events.len(),
            log_next_seq,
        })
    }
}

/// Cleans up stale generation files that may remain from interrupted compaction.
///
/// Called by [`super::recovery::recover`] during startup, under the
/// state-directory lock. Crate-private on purpose: it mutates the state
/// directory and must never run concurrently with a live
/// [`super::recovery::PersistenceHandle`].
///
/// # Logic
///
/// 1. Scan for existing snapshot files to find the highest generation
/// 2. Read the generation file value
/// 3. Use the highest snapshot generation (if any exist), updating the generation
///    file if it's stale or missing
/// 4. Delete files from other generations
/// 5. Delete any orphaned .tmp files
///
/// # Recovery from stale/missing/corrupt generation file
///
/// If the generation file is missing, corrupt, or points to a lower generation
/// than the highest existing snapshot, this function uses the snapshot's generation
/// and restores the generation file. This prevents data loss when the generation
/// file is corrupted or lost.
///
/// # Candidate snapshot validation
///
/// The highest-generation snapshot is semantically validated **before** the
/// generation file is touched or anything is deleted: it must deserialize,
/// its embedded `log_generation` must agree with the generation it is stored
/// as, and it must be consistent with its own event log (`log_position`
/// within the log, `next_seq` matching replay) — the same hard checks
/// `recover` applies afterwards. A candidate that fails validation means the
/// state is uncharacterizable: this function fails loudly and deletes
/// nothing, preserving the last good generation. (The one file mutation a
/// failed cleanup may perform is replay's torn-tail truncation of the
/// candidate's log, the same repair recovery itself performs.)
///
/// # Lost committed generations
///
/// Evidence that a generation was committed but its snapshot is gone is also
/// a loud failure that deletes nothing:
/// - the generation file points above every snapshot on disk, or
/// - an event log exists above every snapshot on disk (event logs are only
///   created once their generation is committed).
///
/// Silently falling back to an older snapshot in these states would roll
/// back committed state, and the cleanup loop would delete the committed
/// generation's remaining files.
pub(crate) fn cleanup_stale_generations(state_dir: &Path) -> Result<()> {
    let (file_gen, gen_file_corrupt) = match read_generation(state_dir) {
        Ok(g) => (g, false),
        Err(super::generation::GenerationError::InvalidNumber(_)) => {
            // Treat corrupt generation file like missing - scan snapshots to recover
            (0, true)
        }
        Err(e) => return Err(e.into()),
    };
    let files = list_generation_files(state_dir)?;

    // Find the highest generation among existing snapshot files (the shared
    // definition of "current snapshot", see `find_current_snapshot`).
    let max_snapshot_gen = find_current_snapshot(state_dir)?;

    // The best candidate for "current generation": the highest snapshot, or
    // generation 0 (fresh directory) if there are no snapshots.
    let candidate_gen = max_snapshot_gen.as_ref().map_or(0, |(g, _)| *g);

    // Guard: a non-corrupt generation file ahead of every snapshot means a
    // committed generation's snapshot has been lost (compaction writes the
    // snapshot before advancing the generation file). "Repairing" the
    // generation file downward would silently roll back committed state, and
    // the deletion loop below would remove the committed generation's event
    // log. Generation 0 needs no snapshot (fresh directory), so it is exempt.
    if !gen_file_corrupt && file_gen > candidate_gen {
        return Err(CompactionError::MissingSnapshot {
            file_gen,
            max_snapshot_gen: max_snapshot_gen.map(|(g, _)| g),
        });
    }

    // Guard: an event log ahead of every snapshot is the same loss with the
    // generation-file evidence also gone: event logs are only created once
    // their generation is committed.
    if let Some(max_event_gen) = files
        .iter()
        .filter(|(_, kind)| *kind == GenerationFileKind::Events)
        .map(|(g, _)| *g)
        .max()
        && max_event_gen > candidate_gen
    {
        return Err(CompactionError::EventLogAheadOfSnapshots {
            path: events_path(state_dir, max_event_gen),
            log_generation: max_event_gen,
            max_snapshot_gen: max_snapshot_gen.map(|(g, _)| g),
        });
    }

    // Determine the actual current generation:
    // - If snapshots exist, use the highest snapshot generation. This handles:
    //   - Stale generation file (points to older gen than exists)
    //   - Missing generation file (returns 0, but snapshots exist)
    //   - Crash after snapshot write but before gen file update
    // - If no snapshots exist, trust the generation file (fresh state)
    let current_gen = match max_snapshot_gen {
        Some((max_gen, snap_path)) => {
            // Validate the candidate snapshot BEFORE committing to it
            // (generation file update) or deleting anything based on it,
            // with the same semantic checks recovery applies afterwards: it
            // must deserialize, and its embedded log_generation must agree
            // with the generation it is stored as. An invalid candidate is a
            // loud failure that leaves every file untouched.
            let candidate = load_snapshot(&snap_path)?;
            if candidate.log_generation != max_gen {
                return Err(CompactionError::CandidateGenerationMismatch {
                    path: snap_path,
                    snapshot_generation: candidate.log_generation,
                    file_generation: max_gen,
                });
            }
            // The candidate must also be consistent with its own event log:
            // recovery failing LogTruncated/SequenceMismatch after the
            // deletion loop below has run would leave nothing to fall back
            // to. The replayed events are discarded here; recovery re-derives
            // them from the settled generation.
            let _ = validate_snapshot_against_log(state_dir, &candidate)?;
            // If there's a mismatch or corruption, update the generation file to be consistent
            if max_gen != file_gen || gen_file_corrupt {
                write_generation(state_dir, max_gen)?;
            }
            max_gen
        }
        None => {
            // No snapshots - use generation 0, but fix corrupt file if needed
            if gen_file_corrupt {
                write_generation(state_dir, 0)?;
            }
            file_gen
        }
    };

    // Delete files from generations other than current
    for (generation, _file_type) in files {
        if generation != current_gen {
            delete_old_generation(state_dir, generation)?;
        }
    }

    // Also clean up any temp files
    cleanup_temp_files(state_dir)?;

    Ok(())
}

/// Removes any orphaned .tmp files in the state directory.
fn cleanup_temp_files(state_dir: &Path) -> io::Result<()> {
    if !state_dir.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(state_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if name.ends_with(".tmp") {
            let _ = std::fs::remove_file(entry.path());
        }
    }

    Ok(())
}

/// Determines if compaction is needed based on event log size.
///
/// Compaction is triggered when the event log exceeds the threshold.
/// This prevents unbounded log growth while avoiding frequent compaction.
pub fn should_compact(state_dir: &Path, threshold_bytes: u64) -> io::Result<bool> {
    let current_gen = read_generation(state_dir).map_err(|e| match e {
        super::generation::GenerationError::Io(e) => e,
        super::generation::GenerationError::InvalidNumber(s) => {
            io::Error::new(io::ErrorKind::InvalidData, s)
        }
    })?;

    let events_file = events_path(state_dir, current_gen);

    match std::fs::metadata(&events_file) {
        Ok(metadata) => Ok(metadata.len() >= threshold_bytes),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::snapshot::PersistedRepoSnapshot;
    use crate::test_utils::test_timestamp;
    use crate::types::{CachedPr, MergeStateStatus, PrNumber, PrState, Sha, TrainRecord};
    use proptest::prelude::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    // ─── Test helpers ───

    fn create_test_snapshot() -> PersistedRepoSnapshot {
        PersistedRepoSnapshot::new("main")
    }

    /// Compacts and asserts both the commit and the post-commit cleanup succeeded.
    fn compact_clean(state_dir: &Path, snapshot: &mut PersistedRepoSnapshot) {
        match compact(state_dir, snapshot).unwrap() {
            CompactionOutcome::Clean => {}
            CompactionOutcome::CleanupPending(e) => panic!("post-commit cleanup failed: {}", e),
        }
    }

    /// Writes a loadable snapshot at the given generation (cleanup validates
    /// that the candidate "current" snapshot deserializes before deleting
    /// anything, so tests must not use empty placeholder files for it).
    fn write_valid_snapshot(state_dir: &Path, generation: u64) {
        let mut snapshot = create_test_snapshot();
        snapshot.log_generation = generation;
        save_snapshot_atomic(&snapshot_path(state_dir, generation), &snapshot).unwrap();
    }

    fn create_snapshot_with_train(pr_num: u64) -> PersistedRepoSnapshot {
        let mut snapshot = create_test_snapshot();
        let train = TrainRecord::new(PrNumber(pr_num), test_timestamp());
        snapshot.active_trains.insert(PrNumber(pr_num), train);
        snapshot
    }

    fn create_snapshot_with_pr(pr_num: u64) -> PersistedRepoSnapshot {
        let mut snapshot = create_test_snapshot();
        let pr = CachedPr::new(
            PrNumber(pr_num),
            Sha::parse("a".repeat(40)).unwrap(),
            format!("branch-{}", pr_num),
            "main".to_string(),
            None,
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        );
        snapshot.prs.insert(PrNumber(pr_num), pr);
        snapshot
    }

    /// State of the `generation` file in a generated state-directory layout.
    #[derive(Debug, Clone, Copy)]
    enum GenFileState {
        Missing,
        Corrupt,
        Value(u64),
    }

    /// State of `snapshot.<gen>.json` in a generated state-directory layout.
    #[derive(Debug, Clone, Copy)]
    enum SnapshotState {
        Absent,
        /// Loadable; embedded `log_generation` matches the filename.
        Valid,
        /// Loadable; embedded `log_generation` is this (possibly wrong) value.
        ClaimsGeneration(u64),
        Corrupt,
    }

    fn arb_gen_file_state() -> impl Strategy<Value = GenFileState> {
        prop_oneof![
            Just(GenFileState::Missing),
            Just(GenFileState::Corrupt),
            (0u64..8).prop_map(GenFileState::Value),
        ]
    }

    fn arb_snapshot_state() -> impl Strategy<Value = SnapshotState> {
        prop_oneof![
            Just(SnapshotState::Absent),
            Just(SnapshotState::Valid),
            (0u64..8).prop_map(SnapshotState::ClaimsGeneration),
            Just(SnapshotState::Corrupt),
        ]
    }

    /// Ways the candidate generation's snapshot/log pair can be damaged (or
    /// not) after being built through the real `EventLog` API.
    #[derive(Debug, Clone, Copy)]
    enum LogTamper {
        /// Leave the pair exactly as written: consistent.
        Intact,
        /// Delete the log file outright.
        DeleteLog,
        /// Truncate the log to half the snapshot's `log_position`.
        TruncateBelowPosition,
        /// Overstate the snapshot's `next_seq` by one.
        OverstateNextSeq,
        /// Append a torn tail write (partial JSON, no trailing newline).
        TornTail,
    }

    fn arb_log_tamper() -> impl Strategy<Value = LogTamper> {
        prop_oneof![
            Just(LogTamper::Intact),
            Just(LogTamper::DeleteLog),
            Just(LogTamper::TruncateBelowPosition),
            Just(LogTamper::OverstateNextSeq),
            Just(LogTamper::TornTail),
        ]
    }

    /// Sorted file names in the state directory.
    fn dir_listing(path: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(path)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    // ─── Unit tests ───

    #[test]
    fn compact_creates_new_generation() {
        let dir = tempdir().unwrap();
        let mut snapshot = create_test_snapshot();

        // Initial state: no generation file
        assert_eq!(read_generation(dir.path()).unwrap(), 0);

        // First compaction creates generation 1
        compact_clean(dir.path(), &mut snapshot);

        assert_eq!(read_generation(dir.path()).unwrap(), 1);
        assert!(snapshot_path(dir.path(), 1).exists());
        assert_eq!(snapshot.log_generation, 1);
        assert_eq!(snapshot.log_position, 0);
    }

    #[test]
    fn compact_increments_generation() {
        let dir = tempdir().unwrap();
        let mut snapshot = create_test_snapshot();

        // Set up initial generation
        compact_clean(dir.path(), &mut snapshot);
        assert_eq!(read_generation(dir.path()).unwrap(), 1);

        // Second compaction
        compact_clean(dir.path(), &mut snapshot);
        assert_eq!(read_generation(dir.path()).unwrap(), 2);
        assert!(snapshot_path(dir.path(), 2).exists());
        assert_eq!(snapshot.log_generation, 2);
    }

    #[test]
    fn compact_deletes_old_generation() {
        let dir = tempdir().unwrap();
        let mut snapshot = create_test_snapshot();

        // First compaction
        compact_clean(dir.path(), &mut snapshot);
        let old_snapshot = snapshot_path(dir.path(), 1);
        assert!(old_snapshot.exists());

        // Second compaction
        compact_clean(dir.path(), &mut snapshot);

        // Old generation should be deleted
        assert!(!old_snapshot.exists());
        assert!(snapshot_path(dir.path(), 2).exists());
    }

    #[test]
    fn compact_preserves_active_trains() {
        let dir = tempdir().unwrap();
        let mut snapshot = create_snapshot_with_train(123);

        compact_clean(dir.path(), &mut snapshot);

        // Reload and verify
        let loaded =
            crate::persistence::snapshot::load_snapshot(&snapshot_path(dir.path(), 1)).unwrap();
        assert!(loaded.active_trains.contains_key(&PrNumber(123)));
    }

    #[test]
    fn compact_preserves_prs() {
        let dir = tempdir().unwrap();
        let mut snapshot = create_snapshot_with_pr(456);

        compact_clean(dir.path(), &mut snapshot);

        // Reload and verify
        let loaded =
            crate::persistence::snapshot::load_snapshot(&snapshot_path(dir.path(), 1)).unwrap();
        assert!(loaded.prs.contains_key(&PrNumber(456)));
    }

    #[test]
    fn cleanup_removes_old_generations() {
        let dir = tempdir().unwrap();

        // Create files for multiple generations
        for generation in 0..3 {
            write_valid_snapshot(dir.path(), generation);
            File::create(events_path(dir.path(), generation)).unwrap();
        }

        // Set current generation to 2
        write_generation(dir.path(), 2).unwrap();

        // Cleanup
        cleanup_stale_generations(dir.path()).unwrap();

        // Only generation 2 files should remain
        assert!(!snapshot_path(dir.path(), 0).exists());
        assert!(!snapshot_path(dir.path(), 1).exists());
        assert!(snapshot_path(dir.path(), 2).exists());
        assert!(!events_path(dir.path(), 0).exists());
        assert!(!events_path(dir.path(), 1).exists());
        assert!(events_path(dir.path(), 2).exists());
    }

    #[test]
    fn cleanup_removes_temp_files() {
        let dir = tempdir().unwrap();

        // Create temp files
        File::create(dir.path().join("snapshot.1.json.tmp")).unwrap();
        File::create(dir.path().join("generation.tmp")).unwrap();

        cleanup_stale_generations(dir.path()).unwrap();

        assert!(!dir.path().join("snapshot.1.json.tmp").exists());
        assert!(!dir.path().join("generation.tmp").exists());
    }

    #[test]
    fn should_compact_returns_false_for_small_log() {
        let dir = tempdir().unwrap();

        // Create generation file
        write_generation(dir.path(), 0).unwrap();

        // Create small event log
        let events = events_path(dir.path(), 0);
        std::fs::write(&events, "small").unwrap();

        assert!(!should_compact(dir.path(), 1000).unwrap());
    }

    #[test]
    fn should_compact_returns_true_for_large_log() {
        let dir = tempdir().unwrap();

        // Create generation file
        write_generation(dir.path(), 0).unwrap();

        // Create large event log
        let events = events_path(dir.path(), 0);
        let large_content = "x".repeat(2000);
        std::fs::write(&events, large_content).unwrap();

        assert!(should_compact(dir.path(), 1000).unwrap());
    }

    #[test]
    fn should_compact_returns_false_for_missing_log() {
        let dir = tempdir().unwrap();

        // Create generation file but no event log
        write_generation(dir.path(), 0).unwrap();

        assert!(!should_compact(dir.path(), 1000).unwrap());
    }

    // ─── Property tests ───

    use crate::test_utils::arb_sha;

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        (1u64..10000).prop_map(PrNumber)
    }

    fn arb_cached_pr() -> impl Strategy<Value = CachedPr> {
        (arb_pr_number(), arb_sha(), "[a-z]{1,10}", "[a-z]{1,10}").prop_map(
            |(number, sha, head_ref, base_ref)| {
                CachedPr::new(
                    number,
                    sha,
                    head_ref,
                    base_ref,
                    None,
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                )
            },
        )
    }

    proptest! {
        /// Compaction preserves all active trains.
        #[test]
        fn compaction_preserves_trains(train_prs in prop::collection::vec(arb_pr_number(), 1..5)) {
            let dir = tempdir().unwrap();
            let mut snapshot = create_test_snapshot();

            for pr in &train_prs {
                snapshot.active_trains.insert(*pr, TrainRecord::new(*pr, test_timestamp()));
            }

            compact_clean(dir.path(), &mut snapshot);

            let loaded = crate::persistence::snapshot::load_snapshot(
                &snapshot_path(dir.path(), 1)
            ).unwrap();

            for pr in &train_prs {
                prop_assert!(
                    loaded.active_trains.contains_key(pr),
                    "Train for {} should be preserved", pr
                );
            }
        }

        /// Compaction preserves all PRs.
        #[test]
        fn compaction_preserves_prs(prs in prop::collection::vec(arb_cached_pr(), 1..10)) {
            let dir = tempdir().unwrap();
            let mut snapshot = create_test_snapshot();

            for pr in &prs {
                snapshot.prs.insert(pr.number, pr.clone());
            }

            compact_clean(dir.path(), &mut snapshot);

            let loaded = crate::persistence::snapshot::load_snapshot(
                &snapshot_path(dir.path(), 1)
            ).unwrap();

            for pr in &prs {
                prop_assert!(
                    loaded.prs.contains_key(&pr.number),
                    "PR {} should be preserved", pr.number
                );
            }
        }

        /// Multiple compactions maintain state integrity.
        #[test]
        fn multiple_compactions_maintain_integrity(
            num_compactions in 2usize..5,
            train_pr in arb_pr_number()
        ) {
            let dir = tempdir().unwrap();
            let mut snapshot = create_test_snapshot();
            snapshot.active_trains.insert(train_pr, TrainRecord::new(train_pr, test_timestamp()));

            for _ in 0..num_compactions {
                compact_clean(dir.path(), &mut snapshot);
            }

            let final_gen = read_generation(dir.path()).unwrap();
            prop_assert_eq!(final_gen, num_compactions as u64);

            let loaded = crate::persistence::snapshot::load_snapshot(
                &snapshot_path(dir.path(), final_gen)
            ).unwrap();

            prop_assert!(loaded.active_trains.contains_key(&train_pr));
        }

        /// Recovery from every legal mid-compaction on-disk state.
        ///
        /// The states are CONSTRUCTED directly from the documented on-disk
        /// protocol (not by interrupting `compact()` — its write ordering is
        /// covered by the `compact_rejects_*` and rollback tests):
        /// 0 = crash before anything (no-op)
        /// 1 = staged snapshot.N+1 exists, generation file still points at N
        /// 2 = generation file points at N+1, old generation files remain
        /// 3 = complete (no crash)
        ///
        /// Validates that `cleanup_stale_generations` + snapshot load + log
        /// replay recover the train data from each state, with correct
        /// `log_position` and `next_seq`.
        #[test]
        fn crash_during_compaction_is_recoverable(
            train_pr in arb_pr_number(),
            initial_gen in 0u64..5,
            crash_point in 0usize..4,
            num_events in 1usize..5
        ) {
            use crate::persistence::log::EventLog;
            use crate::persistence::event::StateEventPayload;

            let dir = tempdir().unwrap();

            // Set up initial state at generation N
            let mut snapshot = create_test_snapshot();
            snapshot.active_trains.insert(train_pr, TrainRecord::new(train_pr, test_timestamp()));
            snapshot.log_generation = initial_gen;
            snapshot.log_position = 0;
            snapshot.next_seq = 0;

            // Write initial generation files
            write_generation(dir.path(), initial_gen).unwrap();
            save_snapshot_atomic(&snapshot_path(dir.path(), initial_gen), &snapshot).unwrap();

            // Write events to the event log (simulating activity before compaction)
            let events_file = events_path(dir.path(), initial_gen);
            {
                let mut log = EventLog::open_with_seq(&events_file, 0).unwrap();
                for i in 0..num_events {
                    log.append(StateEventPayload::TrainStarted {
                        root_pr: PrNumber(1000 + i as u64),
                        current_pr: PrNumber(1000 + i as u64),
                    }).unwrap();
                }
            }

            let new_gen = initial_gen + 1;

            // For crash points 1-3, incorporate event-derived state into snapshot
            // (simulating that events were applied before compaction)
            if crash_point > 0 {
                for i in 0..num_events {
                    snapshot.active_trains.insert(
                        PrNumber(1000 + i as u64),
                        TrainRecord::new(PrNumber(1000 + i as u64), test_timestamp())
                    );
                }
            }

            // Simulate compaction with crash at different points
            match crash_point {
                0 => {
                    // Crash before anything - initial state should be recoverable
                    // snapshot.next_seq stays 0 (events written but not yet reflected in-memory)
                }
                1 => {
                    // Crash after writing new snapshot, before updating generation
                    snapshot.next_seq = num_events as u64;
                    snapshot.log_generation = new_gen;
                    snapshot.log_position = 0;
                    save_snapshot_atomic(&snapshot_path(dir.path(), new_gen), &snapshot).unwrap();
                    // Generation file still points to initial_gen
                }
                2 => {
                    // Crash after updating generation, before deleting old files
                    snapshot.next_seq = num_events as u64;
                    snapshot.log_generation = new_gen;
                    snapshot.log_position = 0;
                    save_snapshot_atomic(&snapshot_path(dir.path(), new_gen), &snapshot).unwrap();
                    write_generation(dir.path(), new_gen).unwrap();
                    // Old files still exist
                }
                3 => {
                    // Complete compaction (no crash)
                    snapshot.next_seq = num_events as u64;
                    compact_clean(dir.path(), &mut snapshot);
                }
                _ => unreachable!(),
            }

            // Recovery: cleanup stale files
            cleanup_stale_generations(dir.path()).unwrap();

            // Read the current generation
            let current_gen = read_generation(dir.path()).unwrap();

            // Load the snapshot for current generation
            let loaded = crate::persistence::snapshot::load_snapshot(
                &snapshot_path(dir.path(), current_gen)
            ).unwrap();

            // === Core assertion: Train data should always be preserved ===
            prop_assert!(
                loaded.active_trains.contains_key(&train_pr),
                "Train {} should be recoverable after crash at point {}", train_pr, crash_point
            );

            // === Validate log_position correctness ===
            // All cases: log_position should be 0 (either original snapshot or new snapshot)
            prop_assert_eq!(
                loaded.log_position, 0,
                "After crash at point {}, snapshot should have log_position=0", crash_point
            );

            // === Validate event log replay works ===
            let current_events_path = events_path(dir.path(), current_gen);
            let replay_result = EventLog::replay_from(&current_events_path, loaded.log_position);

            // Replay should succeed (file might not exist for new generation, which is OK)
            let (replayed_events, replayed_next_seq) = replay_result.unwrap();

            if crash_point == 0 {
                // Crash before anything: still at initial_gen, replay its events
                // Snapshot's next_seq is 0 (no events incorporated yet)
                prop_assert_eq!(
                    loaded.next_seq, 0,
                    "After crash at point 0, snapshot next_seq should be 0, got {}",
                    loaded.next_seq
                );
                prop_assert_eq!(
                    replayed_events.len(), num_events,
                    "After crash at point 0, should replay {} events, got {}",
                    num_events, replayed_events.len()
                );
                prop_assert_eq!(
                    replayed_next_seq, num_events as u64,
                    "After crash at point 0, replayed next_seq should be {}, got {}",
                    num_events, replayed_next_seq
                );
            } else {
                // crash_point 1, 2, 3: Recovery uses the higher generation (new_gen)
                // The new snapshot already has next_seq = num_events incorporated,
                // and there's no event log at the new generation yet.
                prop_assert_eq!(
                    loaded.next_seq, num_events as u64,
                    "After crash at point {}, snapshot next_seq should be {}, got {}",
                    crash_point, num_events, loaded.next_seq
                );
                prop_assert_eq!(
                    replayed_events.len(), 0,
                    "After crash at point {}, new generation should have 0 events, got {}",
                    crash_point, replayed_events.len()
                );
                prop_assert_eq!(
                    replayed_next_seq, 0,
                    "After crash at point {}, replayed next_seq should be 0, got {}",
                    crash_point, replayed_next_seq
                );
            }

            // === Validate total sequence count after full recovery ===
            // The effective next_seq after recovery is snapshot.next_seq + replayed event count
            // (since replayed events would update next_seq during actual replay application).
            // This catches sequence mismatches across generations.
            let effective_next_seq = loaded.next_seq + replayed_events.len() as u64;
            prop_assert_eq!(
                effective_next_seq, num_events as u64,
                "After crash at point {}, effective next_seq should be {} (snapshot.next_seq={} + replayed={})",
                crash_point, num_events, loaded.next_seq, replayed_events.len()
            );

            // === Verify event-derived state is preserved ===
            if crash_point == 0 {
                // Events not yet incorporated - verify they're in the replayed events
                for (i, event) in replayed_events.iter().enumerate() {
                    match &event.payload {
                        StateEventPayload::TrainStarted { root_pr, .. } => {
                            prop_assert_eq!(
                                root_pr.0, 1000 + i as u64,
                                "Replayed event {} should be TrainStarted for PR {}", i, 1000 + i
                            );
                        }
                        _ => prop_assert!(false, "Expected TrainStarted event at position {}", i),
                    }
                }
            } else {
                // Events incorporated into snapshot - verify trains exist
                for i in 0..num_events {
                    prop_assert!(
                        loaded.active_trains.contains_key(&PrNumber(1000 + i as u64)),
                        "Train from event {} should be preserved in snapshot after crash at point {}",
                        i, crash_point
                    );
                }
            }

            // === Verify only current generation files exist ===
            let files = list_generation_files(dir.path()).unwrap();
            for (file_gen, _) in &files {
                prop_assert_eq!(
                    *file_gen, current_gen,
                    "Only current generation {} files should exist, found generation {}", current_gen, file_gen
                );
            }
        }

        // ─── Generation file consistency properties ───

        /// Cleanup preserves exactly the files at the highest snapshot generation.
        ///
        /// For any set of existing snapshot generations, after cleanup only files
        /// at the highest generation remain, and the generation file is updated
        /// to match.
        #[test]
        fn cleanup_preserves_only_highest_snapshot_generation(
            gen_file_value in 0u64..10,
            snapshot_gens in prop::collection::hash_set(0u64..10, 1..5)
        ) {
            let dir = tempdir().unwrap();

            // Set up generation file (may not match actual snapshots)
            write_generation(dir.path(), gen_file_value).unwrap();

            // Create snapshot and event files at various generations.
            // Snapshots must be loadable: cleanup validates the candidate
            // before deleting anything.
            for g in &snapshot_gens {
                write_valid_snapshot(dir.path(), *g);
                File::create(events_path(dir.path(), *g)).unwrap();
            }

            // Determine expected: highest snapshot generation
            let expected_gen = *snapshot_gens.iter().max().unwrap();

            if gen_file_value > expected_gen {
                // The generation file claims a committed generation whose
                // snapshot does not exist: cleanup must refuse and delete
                // nothing rather than roll back committed state.
                let result = cleanup_stale_generations(dir.path());
                prop_assert!(
                    result.is_err(),
                    "cleanup must refuse when the generation file is ahead of \
                     every snapshot"
                );
                for g in &snapshot_gens {
                    prop_assert!(snapshot_path(dir.path(), *g).exists());
                    prop_assert!(events_path(dir.path(), *g).exists());
                }
                prop_assert_eq!(read_generation(dir.path()).unwrap(), gen_file_value);
                return Ok(());
            }

            // Run cleanup
            cleanup_stale_generations(dir.path()).unwrap();

            // Property: only files at highest snapshot generation remain
            let remaining = list_generation_files(dir.path()).unwrap();
            for (g, _) in &remaining {
                prop_assert_eq!(
                    *g, expected_gen,
                    "After cleanup, only generation {} should remain, found {}", expected_gen, g
                );
            }

            // Highest generation files should still exist
            prop_assert!(
                snapshot_path(dir.path(), expected_gen).exists(),
                "Snapshot at highest generation {} should exist", expected_gen
            );

            // Generation file should be updated to match
            let updated_gen = read_generation(dir.path()).unwrap();
            prop_assert_eq!(
                updated_gen, expected_gen,
                "Generation file should be updated to {}, got {}", expected_gen, updated_gen
            );
        }

        /// When generation file is stale/missing, cleanup should preserve newer snapshots.
        ///
        /// If the generation file points to an older generation than actually exists,
        /// cleanup should detect this (by scanning existing files) and NOT delete
        /// the newer data.
        #[test]
        fn stale_generation_file_preserves_newer_snapshots(
            stale_gen in 0u64..5,
            newer_gen in 5u64..10
        ) {
            // Precondition: stale_gen < newer_gen (ensured by ranges)
            prop_assume!(stale_gen < newer_gen);

            let dir = tempdir().unwrap();

            // Generation file points to old value (simulating corruption/loss)
            write_generation(dir.path(), stale_gen).unwrap();

            // But valid snapshot exists at newer generation
            let mut snapshot = create_test_snapshot();
            snapshot.log_generation = newer_gen;
            save_snapshot_atomic(&snapshot_path(dir.path(), newer_gen), &snapshot).unwrap();
            File::create(events_path(dir.path(), newer_gen)).unwrap();

            // Also create files at stale_gen (what the generation file thinks is current)
            File::create(snapshot_path(dir.path(), stale_gen)).unwrap();
            File::create(events_path(dir.path(), stale_gen)).unwrap();

            // Cleanup should detect the mismatch and preserve newer data
            cleanup_stale_generations(dir.path()).unwrap();

            // CORRECT BEHAVIOR: newer snapshot should be preserved
            prop_assert!(
                snapshot_path(dir.path(), newer_gen).exists(),
                "Newer snapshot at gen {} should be preserved even when generation file points to {}",
                newer_gen, stale_gen
            );
        }

        /// Cleanup is idempotent: running it twice has the same effect as once.
        #[test]
        fn cleanup_is_idempotent(
            current_gen in 0u64..10,
            other_gens in prop::collection::vec(0u64..10, 0..5)
        ) {
            let dir = tempdir().unwrap();

            write_generation(dir.path(), current_gen).unwrap();
            write_valid_snapshot(dir.path(), current_gen);
            File::create(events_path(dir.path(), current_gen)).unwrap();

            for g in &other_gens {
                write_valid_snapshot(dir.path(), *g);
                let _ = File::create(events_path(dir.path(), *g));
            }

            // First cleanup
            cleanup_stale_generations(dir.path()).unwrap();
            let after_first = list_generation_files(dir.path()).unwrap();

            // Second cleanup
            cleanup_stale_generations(dir.path()).unwrap();
            let after_second = list_generation_files(dir.path()).unwrap();

            // Property: same files remain after both cleanups
            prop_assert_eq!(
                after_first, after_second,
                "Cleanup should be idempotent"
            );
        }

        /// Cleanup either settles the directory to a single semantically
        /// valid generation, or fails having touched nothing.
        ///
        /// This is the no-data-loss contract of the recovery path: no file
        /// may be deleted (and the generation file may not be rewritten)
        /// unless the chosen generation passes the same semantic validation
        /// recovery will apply to it, and a successful cleanup must never
        /// discard evidence of a committed generation (the generation file
        /// value, or an event log above every snapshot).
        #[test]
        fn cleanup_settles_fully_or_touches_nothing(
            gen_file in arb_gen_file_state(),
            snapshots in prop::collection::vec(arb_snapshot_state(), 6),
            events in prop::collection::vec(prop::bool::ANY, 6),
        ) {
            let dir = tempdir().unwrap();

            match gen_file {
                GenFileState::Missing => {}
                GenFileState::Corrupt => {
                    std::fs::write(dir.path().join("generation"), "garbage\n").unwrap();
                }
                GenFileState::Value(g) => write_generation(dir.path(), g).unwrap(),
            }
            for (i, snap) in snapshots.iter().enumerate() {
                let g = i as u64;
                match snap {
                    SnapshotState::Absent => {}
                    SnapshotState::Valid => write_valid_snapshot(dir.path(), g),
                    SnapshotState::ClaimsGeneration(claimed) => {
                        let mut s = create_test_snapshot();
                        s.log_generation = *claimed;
                        save_snapshot_atomic(&snapshot_path(dir.path(), g), &s).unwrap();
                    }
                    SnapshotState::Corrupt => {
                        std::fs::write(snapshot_path(dir.path(), g), "{ not json").unwrap();
                    }
                }
            }
            for (i, has_events) in events.iter().enumerate() {
                if *has_events {
                    File::create(events_path(dir.path(), i as u64)).unwrap();
                }
            }

            let files_before = dir_listing(dir.path());
            let gen_file_before = std::fs::read(dir.path().join("generation")).ok();
            let max_snapshot_before = snapshots
                .iter()
                .enumerate()
                .filter(|(_, s)| !matches!(s, SnapshotState::Absent))
                .map(|(i, _)| i as u64)
                .max();

            match cleanup_stale_generations(dir.path()) {
                Err(_) => {
                    prop_assert_eq!(
                        dir_listing(dir.path()),
                        files_before,
                        "a failed cleanup must not create or delete any file"
                    );
                    prop_assert_eq!(
                        std::fs::read(dir.path().join("generation")).ok(),
                        gen_file_before,
                        "a failed cleanup must not rewrite the generation file"
                    );
                }
                Ok(()) => {
                    let settled = read_generation(dir.path()).unwrap();
                    match max_snapshot_before {
                        Some(max) => {
                            prop_assert_eq!(
                                settled, max,
                                "cleanup must settle on the highest snapshot generation"
                            );
                            let loaded =
                                load_snapshot(&snapshot_path(dir.path(), settled)).unwrap();
                            prop_assert_eq!(
                                loaded.log_generation, settled,
                                "the surviving snapshot must describe its own generation"
                            );
                        }
                        None => prop_assert_eq!(
                            settled, 0,
                            "no snapshots means a fresh directory at generation 0"
                        ),
                    }
                    // Evidence of committed generations must never be
                    // discarded by a successful cleanup.
                    if let GenFileState::Value(v) = gen_file {
                        prop_assert!(
                            settled >= v,
                            "cleanup rolled the generation file back from {} to {}",
                            v, settled
                        );
                    }
                    for (i, has_events) in events.iter().enumerate() {
                        if *has_events {
                            prop_assert!(
                                settled >= i as u64,
                                "cleanup succeeded despite an event log at generation {} \
                                 above the settled generation {}",
                                i, settled
                            );
                        }
                    }
                    for (g, _) in list_generation_files(dir.path()).unwrap() {
                        prop_assert_eq!(
                            g, settled,
                            "only the settled generation's files may remain"
                        );
                    }
                }
            }
        }

        /// Cleanup commits to the candidate generation (rewriting the
        /// generation file and deleting every other generation) only when the
        /// candidate will pass recovery's log-consistency checks: a successful
        /// cleanup implies a successful recovery, and a failed cleanup leaves
        /// the older generation and the generation file untouched.
        ///
        /// This is the regression property for the bug where a candidate with
        /// valid JSON but unsatisfiable log metadata (log_position past the
        /// log, next_seq disagreeing with replay) was promoted, the fallback
        /// generation deleted, and recovery then failed with nothing left.
        #[test]
        fn cleanup_commits_only_to_recoverable_state(
            applied in 0u64..4,
            extra in 0u64..4,
            tamper in arb_log_tamper(),
        ) {
            use crate::persistence::event::StateEventPayload;
            use crate::persistence::log::EventLog;

            let dir = tempdir().unwrap();

            // Older valid generation 1: the fallback that must survive a refusal.
            let mut old = create_snapshot_with_train(100);
            old.log_generation = 1;
            save_snapshot_atomic(&snapshot_path(dir.path(), 1), &old).unwrap();
            File::create(events_path(dir.path(), 1)).unwrap();
            write_generation(dir.path(), 1).unwrap();

            // Candidate generation 2, built through the real log API:
            // `applied` events folded into the snapshot (its log_position is
            // past them), `extra` events appended after the snapshot.
            let mut candidate = create_test_snapshot();
            candidate.log_generation = 2;
            {
                let mut log =
                    EventLog::open_with_seq(events_path(dir.path(), 2), 0).unwrap();
                for i in 0..applied {
                    log.append(StateEventPayload::TrainStarted {
                        root_pr: PrNumber(i),
                        current_pr: PrNumber(i),
                    })
                    .unwrap();
                }
                candidate.log_position = log.position().unwrap();
                candidate.next_seq = applied;
                for i in applied..applied + extra {
                    log.append(StateEventPayload::TrainStarted {
                        root_pr: PrNumber(i),
                        current_pr: PrNumber(i),
                    })
                    .unwrap();
                }
            }
            match tamper {
                LogTamper::Intact => {}
                LogTamper::DeleteLog => {
                    std::fs::remove_file(events_path(dir.path(), 2)).unwrap();
                }
                LogTamper::TruncateBelowPosition => {
                    let file = std::fs::OpenOptions::new()
                        .write(true)
                        .open(events_path(dir.path(), 2))
                        .unwrap();
                    file.set_len(candidate.log_position / 2).unwrap();
                }
                LogTamper::OverstateNextSeq => candidate.next_seq += 1,
                LogTamper::TornTail => {
                    let mut file = std::fs::OpenOptions::new()
                        .append(true)
                        .open(events_path(dir.path(), 2))
                        .unwrap();
                    write!(file, "{{\"seq\":").unwrap();
                }
            }
            save_snapshot_atomic(&snapshot_path(dir.path(), 2), &candidate).unwrap();

            match cleanup_stale_generations(dir.path()) {
                Ok(()) => {
                    // Cleanup committed and deleted the fallback: the
                    // surviving generation must actually be recoverable.
                    let recovered =
                        crate::persistence::recovery::recover(dir.path(), "main");
                    prop_assert!(
                        recovered.is_ok(),
                        "cleanup committed to generation 2 and deleted the \
                         fallback, but recovery then failed: {:?}",
                        recovered.err().map(|e| e.to_string())
                    );
                }
                Err(_) => {
                    prop_assert!(
                        snapshot_path(dir.path(), 1).exists(),
                        "a failed cleanup must preserve the fallback snapshot"
                    );
                    prop_assert!(
                        events_path(dir.path(), 1).exists(),
                        "a failed cleanup must preserve the fallback event log"
                    );
                    prop_assert!(
                        snapshot_path(dir.path(), 2).exists(),
                        "a failed cleanup must not delete the candidate"
                    );
                    prop_assert_eq!(
                        read_generation(dir.path()).unwrap(),
                        1,
                        "a failed cleanup must not rewrite the generation file"
                    );
                }
            }
        }

        // ─── Error handling properties ───

        /// Property: If compact returns Err, the in-memory snapshot is unchanged.
        ///
        /// This is critical for callers that may retry or continue after a failure.
        /// They need to know the snapshot still reflects the pre-compaction state.
        #[cfg(unix)]
        #[test]
        fn compact_failure_preserves_snapshot_state(
            train_pr in arb_pr_number(),
            initial_gen in 0u64..5,
        ) {
            use std::os::unix::fs::PermissionsExt;

            let dir = tempdir().unwrap();

            let mut snapshot = create_test_snapshot();
            snapshot.active_trains.insert(train_pr, TrainRecord::new(train_pr, test_timestamp()));
            snapshot.log_generation = initial_gen;
            snapshot.log_position = 42; // Non-zero to detect mutation

            // Set up initial state
            write_generation(dir.path(), initial_gen).unwrap();
            save_snapshot_atomic(&snapshot_path(dir.path(), initial_gen), &snapshot).unwrap();

            // Make the state directory read-only to force write failures
            let permissions = std::fs::Permissions::from_mode(0o555);
            std::fs::set_permissions(dir.path(), permissions).unwrap();

            let original_gen = snapshot.log_generation;
            let original_pos = snapshot.log_position;
            let original_trains = snapshot.active_trains.clone();

            let result = compact(dir.path(), &mut snapshot);

            // Restore permissions for cleanup
            let permissions = std::fs::Permissions::from_mode(0o755);
            std::fs::set_permissions(dir.path(), permissions).unwrap();

            // PROPERTY: On failure, snapshot should be unchanged
            prop_assert!(result.is_err(), "compact should fail with read-only directory");
            prop_assert_eq!(snapshot.log_generation, original_gen,
                "log_generation should be unchanged on error");
            prop_assert_eq!(snapshot.log_position, original_pos,
                "log_position should be unchanged on error");
            prop_assert_eq!(snapshot.active_trains, original_trains,
                "active_trains should be unchanged on error");
        }

        /// Property: compact should work correctly even if snapshots exist at higher generations.
        ///
        /// This tests the scenario where cleanup_stale_generations was called first
        /// (as documented in Prerequisites), ensuring the generation file is consistent.
        #[test]
        fn compact_after_cleanup_is_consistent(
            existing_gen in 2u64..10,
        ) {
            let dir = tempdir().unwrap();

            // Set up: snapshot exists at high generation but NO generation file
            let mut existing_snapshot = create_test_snapshot();
            existing_snapshot.log_generation = existing_gen;
            existing_snapshot.active_trains.insert(PrNumber(999), TrainRecord::new(PrNumber(999), test_timestamp()));
            save_snapshot_atomic(&snapshot_path(dir.path(), existing_gen), &existing_snapshot).unwrap();
            File::create(events_path(dir.path(), existing_gen)).unwrap();
            // Note: generation file intentionally NOT written

            // Run cleanup first (as documented in Prerequisites)
            cleanup_stale_generations(dir.path()).unwrap();

            // Verify cleanup detected the snapshot and restored generation file
            let gen_after_cleanup = read_generation(dir.path()).unwrap();
            prop_assert_eq!(gen_after_cleanup, existing_gen,
                "cleanup should restore generation file to match highest snapshot");

            // Now compact should advance from the correct generation
            let mut snapshot_to_compact = existing_snapshot.clone();
            compact_clean(dir.path(), &mut snapshot_to_compact);

            prop_assert_eq!(snapshot_to_compact.log_generation, existing_gen + 1,
                "compact should advance from the restored generation");
            prop_assert_eq!(read_generation(dir.path()).unwrap(), existing_gen + 1,
                "generation file should be updated after compact");
        }
    }

    // ─── Crash simulation tests ───

    #[test]
    fn recovery_after_crash_before_generation_update() {
        let dir = tempdir().unwrap();

        // Simulate state after writing new snapshot but before updating generation
        // Generation file says 0, but snapshot.1.json exists (with newer data)

        // Create initial state at generation 0
        let mut snapshot = create_snapshot_with_train(100);
        save_snapshot_atomic(&snapshot_path(dir.path(), 0), &snapshot).unwrap();
        write_generation(dir.path(), 0).unwrap();

        // Create event log with some events at generation 0
        let events = events_path(dir.path(), 0);
        let mut f = File::create(&events).unwrap();
        writeln!(f, r#"{{"seq":0,"ts":"2024-01-01T00:00:00Z","type":"train_started","root_pr":101,"current_pr":101}}"#).unwrap();

        // Simulate crash after writing snapshot.1 but before updating generation
        // This snapshot has MORE data (train 200 in addition to train 100).
        // compact() stages log_generation = new_gen into the new snapshot, so
        // the crash state has a matching embedded generation.
        snapshot.active_trains.insert(
            PrNumber(200),
            TrainRecord::new(PrNumber(200), test_timestamp()),
        );
        snapshot.log_generation = 1;
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        // Generation file still says 0!

        // Before cleanup, gen file says 0
        assert_eq!(read_generation(dir.path()).unwrap(), 0);

        // Both snapshots exist before cleanup
        assert!(snapshot_path(dir.path(), 0).exists());
        assert!(snapshot_path(dir.path(), 1).exists());

        // Cleanup should use the HIGHEST snapshot (generation 1) to preserve data
        cleanup_stale_generations(dir.path()).unwrap();

        // Generation file should be updated to 1
        assert_eq!(
            read_generation(dir.path()).unwrap(),
            1,
            "Generation file should be updated to match highest snapshot"
        );

        // Generation 1 files should exist (the newer, more complete snapshot)
        assert!(
            snapshot_path(dir.path(), 1).exists(),
            "Snapshot.1 (highest) should be preserved"
        );

        // Generation 0 files should be removed
        assert!(
            !snapshot_path(dir.path(), 0).exists(),
            "Snapshot.0 (older) should be removed"
        );

        // The preserved snapshot should contain ALL data (both trains)
        let loaded =
            crate::persistence::snapshot::load_snapshot(&snapshot_path(dir.path(), 1)).unwrap();
        assert!(
            loaded.active_trains.contains_key(&PrNumber(100)),
            "Train 100 should be in preserved snapshot"
        );
        assert!(
            loaded.active_trains.contains_key(&PrNumber(200)),
            "Train 200 should be in preserved snapshot"
        );
    }

    #[test]
    fn cleanup_preserves_highest_snapshot_generation() {
        // When multiple snapshot generations exist, cleanup keeps the highest one
        // and removes all others. This handles the crash scenario where a new
        // snapshot was written but the generation file wasn't updated.
        let dir = tempdir().unwrap();

        // Generation file says 2 (simulating stale state).
        // Only the candidate (highest) snapshot is validated by cleanup, so
        // the doomed lower generations may remain empty placeholder files.
        write_generation(dir.path(), 2).unwrap();
        File::create(snapshot_path(dir.path(), 2)).unwrap();
        File::create(events_path(dir.path(), 2)).unwrap();

        // Old generations (didn't get cleaned up from prior compaction)
        File::create(snapshot_path(dir.path(), 0)).unwrap();
        File::create(events_path(dir.path(), 0)).unwrap();
        File::create(snapshot_path(dir.path(), 1)).unwrap();
        File::create(events_path(dir.path(), 1)).unwrap();

        // Newer generation 3 (crash during compaction before gen file update)
        // This is the HIGHEST snapshot, so it should be preserved; it must be
        // loadable, since cleanup validates it before deleting anything.
        write_valid_snapshot(dir.path(), 3);
        // Note: events.3.log might not exist if crash was early in compaction

        // Verify all files exist before cleanup
        assert!(snapshot_path(dir.path(), 0).exists());
        assert!(snapshot_path(dir.path(), 1).exists());
        assert!(snapshot_path(dir.path(), 2).exists());
        assert!(snapshot_path(dir.path(), 3).exists());

        // Run cleanup
        cleanup_stale_generations(dir.path()).unwrap();

        // Only the HIGHEST snapshot generation (3) should remain
        assert!(
            !snapshot_path(dir.path(), 0).exists(),
            "Old generation 0 should be removed"
        );
        assert!(
            !events_path(dir.path(), 0).exists(),
            "Old generation 0 events should be removed"
        );
        assert!(
            !snapshot_path(dir.path(), 1).exists(),
            "Old generation 1 should be removed"
        );
        assert!(
            !events_path(dir.path(), 1).exists(),
            "Old generation 1 events should be removed"
        );
        assert!(
            !snapshot_path(dir.path(), 2).exists(),
            "Generation 2 should be removed (not highest)"
        );
        assert!(
            !events_path(dir.path(), 2).exists(),
            "Generation 2 events should be removed"
        );
        assert!(
            snapshot_path(dir.path(), 3).exists(),
            "Highest generation 3 should be preserved"
        );

        // Generation file should be updated to 3
        assert_eq!(
            read_generation(dir.path()).unwrap(),
            3,
            "Generation file should be updated to match highest snapshot"
        );
    }

    #[test]
    fn recovery_after_crash_after_generation_update() {
        let dir = tempdir().unwrap();

        // Simulate state after updating generation but before deleting old files

        // Create generation 1 state
        let mut snapshot = create_snapshot_with_train(100);
        snapshot.log_generation = 1;
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        write_generation(dir.path(), 1).unwrap();

        // Old files still exist (simulating crash before deletion)
        save_snapshot_atomic(&snapshot_path(dir.path(), 0), &snapshot).unwrap();
        File::create(events_path(dir.path(), 0)).unwrap();

        // Recovery should use generation 1
        let current_gen = read_generation(dir.path()).unwrap();
        assert_eq!(current_gen, 1);

        // Cleanup removes old generation
        cleanup_stale_generations(dir.path()).unwrap();
        assert!(!snapshot_path(dir.path(), 0).exists());
        assert!(!events_path(dir.path(), 0).exists());
        assert!(snapshot_path(dir.path(), 1).exists());
    }

    // ─── Missing/corrupt generation file tests ───
    //
    // These tests document behavior when the generation file is missing or corrupt
    // but snapshot files exist. This is critical for understanding data loss risks.

    #[test]
    fn cleanup_with_missing_generation_file_preserves_snapshots() {
        // When the generation file is missing but snapshots exist at higher generations,
        // cleanup_stale_generations should detect this by scanning existing files and
        // preserve the newest snapshot rather than deleting it.
        let dir = tempdir().unwrap();

        // Create valid snapshot at generation 3 (simulating prior successful compactions)
        let mut snapshot = create_snapshot_with_train(999);
        snapshot.log_generation = 3;
        save_snapshot_atomic(&snapshot_path(dir.path(), 3), &snapshot).unwrap();
        File::create(events_path(dir.path(), 3)).unwrap();

        // Verify snapshot exists and contains data
        assert!(snapshot_path(dir.path(), 3).exists());
        let loaded =
            crate::persistence::snapshot::load_snapshot(&snapshot_path(dir.path(), 3)).unwrap();
        assert!(loaded.active_trains.contains_key(&PrNumber(999)));

        // Generation file is missing (simulating corruption/loss)
        assert!(!dir.path().join("generation").exists());

        // Cleanup should detect existing snapshots and preserve them
        cleanup_stale_generations(dir.path()).unwrap();

        // CORRECT BEHAVIOR: snapshot should be preserved
        assert!(
            snapshot_path(dir.path(), 3).exists(),
            "Snapshot at gen 3 should be preserved even when generation file is missing"
        );
        assert!(
            events_path(dir.path(), 3).exists(),
            "Events at gen 3 should be preserved even when generation file is missing"
        );

        // After cleanup, the generation file should be restored to match existing snapshots
        assert_eq!(
            read_generation(dir.path()).unwrap(),
            3,
            "Generation file should be restored to match the highest existing snapshot"
        );
    }

    #[test]
    fn cleanup_with_corrupt_generation_file_recovers_from_snapshots() {
        // When the generation file is corrupt but snapshots exist, cleanup
        // treats it like a missing file and recovers from the highest snapshot.
        let dir = tempdir().unwrap();

        // Create valid snapshot at generation 2
        let mut snapshot = create_snapshot_with_train(888);
        snapshot.log_generation = 2;
        save_snapshot_atomic(&snapshot_path(dir.path(), 2), &snapshot).unwrap();
        File::create(events_path(dir.path(), 2)).unwrap();

        // Write corrupt generation file
        std::fs::write(dir.path().join("generation"), "garbage_data\n").unwrap();

        // cleanup_stale_generations should succeed by using the highest snapshot
        cleanup_stale_generations(dir.path()).unwrap();

        // Snapshot should be preserved
        assert!(
            snapshot_path(dir.path(), 2).exists(),
            "Snapshot should be preserved"
        );
        assert!(
            events_path(dir.path(), 2).exists(),
            "Events should be preserved"
        );

        // Generation file should be fixed to match the highest snapshot
        assert_eq!(
            read_generation(dir.path()).unwrap(),
            2,
            "Generation file should be restored to match highest snapshot"
        );

        // Data should be accessible
        let loaded =
            crate::persistence::snapshot::load_snapshot(&snapshot_path(dir.path(), 2)).unwrap();
        assert!(loaded.active_trains.contains_key(&PrNumber(888)));
    }

    #[test]
    fn compact_with_missing_generation_file_starts_from_zero() {
        // When starting fresh or after generation file loss, compact starts from gen 0
        let dir = tempdir().unwrap();

        // No generation file exists
        assert!(!dir.path().join("generation").exists());

        let mut snapshot = create_snapshot_with_train(777);

        // compact reads gen as 0 and creates gen 1
        compact_clean(dir.path(), &mut snapshot);

        assert_eq!(read_generation(dir.path()).unwrap(), 1);
        assert!(snapshot_path(dir.path(), 1).exists());
        assert_eq!(snapshot.log_generation, 1);
    }

    #[test]
    fn compact_with_corrupt_generation_file_returns_error() {
        // Compaction fails when generation file is corrupt, preventing further damage
        let dir = tempdir().unwrap();

        std::fs::write(dir.path().join("generation"), "not_valid\n").unwrap();

        let mut snapshot = create_snapshot_with_train(666);
        let result = compact(dir.path(), &mut snapshot);

        assert!(
            result.is_err(),
            "compact should fail with corrupt generation file"
        );
    }

    #[test]
    fn cleanup_refuses_to_delete_when_candidate_snapshot_corrupt() {
        // A higher-generation snapshot file EXISTS but is corrupt. Deleting the
        // last good generation based purely on the file's existence would destroy
        // the only loadable state. Cleanup must validate the candidate snapshot
        // and fail loudly without deleting anything.
        let dir = tempdir().unwrap();

        // Valid generation 1 state
        let mut snapshot = create_snapshot_with_train(100);
        snapshot.log_generation = 1;
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        File::create(events_path(dir.path(), 1)).unwrap();
        write_generation(dir.path(), 1).unwrap();

        // Corrupt snapshot at generation 2 (e.g. disk corruption, partial copy)
        std::fs::write(snapshot_path(dir.path(), 2), "{ not valid json").unwrap();

        let result = cleanup_stale_generations(dir.path());
        assert!(
            result.is_err(),
            "cleanup must fail loudly when the candidate snapshot is corrupt"
        );

        // The last good generation must be untouched.
        assert!(
            snapshot_path(dir.path(), 1).exists(),
            "valid snapshot.1 must not be deleted"
        );
        assert!(
            events_path(dir.path(), 1).exists(),
            "events.1.log must not be deleted"
        );
        assert_eq!(
            read_generation(dir.path()).unwrap(),
            1,
            "generation file must not be promoted to the corrupt generation"
        );
    }

    #[test]
    fn cleanup_refuses_when_candidate_snapshot_claims_other_generation() {
        // snapshot.2.json deserializes but its embedded log_generation says 7:
        // it does not describe generation 2. Promoting it would delete the
        // last good generation, and recovery would then fail its
        // GenerationMismatch hard check with the data already gone. Cleanup
        // must apply the same check first and delete nothing.
        let dir = tempdir().unwrap();

        // Valid generation 1 state.
        let mut snapshot = create_snapshot_with_train(100);
        snapshot.log_generation = 1;
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        File::create(events_path(dir.path(), 1)).unwrap();
        write_generation(dir.path(), 1).unwrap();

        // Loadable snapshot at generation 2 whose embedded generation is wrong.
        let mut liar = create_test_snapshot();
        liar.log_generation = 7;
        save_snapshot_atomic(&snapshot_path(dir.path(), 2), &liar).unwrap();

        let result = cleanup_stale_generations(dir.path());
        assert!(
            matches!(
                result,
                Err(CompactionError::CandidateGenerationMismatch {
                    snapshot_generation: 7,
                    file_generation: 2,
                    ..
                })
            ),
            "cleanup must reject a candidate whose embedded generation disagrees \
             with its filename, got {:?}",
            result
        );

        assert!(
            snapshot_path(dir.path(), 1).exists(),
            "the last good snapshot must survive"
        );
        assert!(
            events_path(dir.path(), 1).exists(),
            "the last good event log must survive"
        );
        assert!(
            snapshot_path(dir.path(), 2).exists(),
            "nothing may be deleted, not even the bad candidate"
        );
        assert_eq!(
            read_generation(dir.path()).unwrap(),
            1,
            "the generation file must be untouched"
        );
    }

    #[test]
    fn cleanup_refuses_when_candidate_log_position_past_log() {
        // snapshot.2.json deserializes and describes generation 2, but its
        // log_position points past the end of events.2.log (here: the log is
        // missing entirely, so its length is 0). Recovery would reject this
        // generation as LogTruncated; promoting it and deleting generation 1
        // first would destroy the only recoverable fallback.
        let dir = tempdir().unwrap();

        // Valid generation 1 state: the fallback that must survive.
        let mut snapshot = create_snapshot_with_train(100);
        snapshot.log_generation = 1;
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        File::create(events_path(dir.path(), 1)).unwrap();
        write_generation(dir.path(), 1).unwrap();

        // Candidate at generation 2 whose log metadata is unsatisfiable.
        let mut candidate = create_test_snapshot();
        candidate.log_generation = 2;
        candidate.log_position = 100;
        save_snapshot_atomic(&snapshot_path(dir.path(), 2), &candidate).unwrap();

        let result = cleanup_stale_generations(dir.path());
        assert!(
            matches!(
                result,
                Err(CompactionError::LogTruncated {
                    log_position: 100,
                    file_len: 0,
                    ..
                })
            ),
            "cleanup must reject a candidate whose log_position points past \
             its event log, got {:?}",
            result
        );

        assert!(
            snapshot_path(dir.path(), 1).exists(),
            "the last good snapshot must survive"
        );
        assert!(
            events_path(dir.path(), 1).exists(),
            "the last good event log must survive"
        );
        assert!(
            snapshot_path(dir.path(), 2).exists(),
            "nothing may be deleted, not even the bad candidate"
        );
        assert_eq!(
            read_generation(dir.path()).unwrap(),
            1,
            "the generation file must not be promoted to the bad candidate"
        );
    }

    #[test]
    fn cleanup_refuses_when_candidate_next_seq_disagrees_with_log() {
        // events.2.log holds events with seqs 0..3, but snapshot.2.json claims
        // next_seq 5 at log_position 0: recovery would reject this generation
        // as SequenceMismatch. Cleanup must apply the same check before
        // deleting the older generation.
        use crate::persistence::event::StateEventPayload;

        let dir = tempdir().unwrap();

        let mut snapshot = create_snapshot_with_train(100);
        snapshot.log_generation = 1;
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        File::create(events_path(dir.path(), 1)).unwrap();
        write_generation(dir.path(), 1).unwrap();

        let mut candidate = create_test_snapshot();
        candidate.log_generation = 2;
        candidate.next_seq = 5;
        save_snapshot_atomic(&snapshot_path(dir.path(), 2), &candidate).unwrap();
        {
            let mut log = EventLog::open_with_seq(events_path(dir.path(), 2), 0).unwrap();
            for i in 0..3 {
                log.append(StateEventPayload::TrainStarted {
                    root_pr: PrNumber(i),
                    current_pr: PrNumber(i),
                })
                .unwrap();
            }
        }

        let result = cleanup_stale_generations(dir.path());
        assert!(
            matches!(
                result,
                Err(CompactionError::SequenceMismatch {
                    snapshot_next_seq: 5,
                    replayed: 3,
                    log_next_seq: 3,
                    ..
                })
            ),
            "cleanup must reject a candidate whose next_seq disagrees with \
             its event log, got {:?}",
            result
        );

        assert!(
            snapshot_path(dir.path(), 1).exists(),
            "the last good snapshot must survive"
        );
        assert!(
            events_path(dir.path(), 1).exists(),
            "the last good event log must survive"
        );
        assert_eq!(
            read_generation(dir.path()).unwrap(),
            1,
            "the generation file must not be promoted to the bad candidate"
        );
    }

    #[test]
    fn cleanup_refuses_when_candidate_log_corrupt_mid_file() {
        // events.2.log has a garbage line FOLLOWED by a valid event: not a
        // torn tail write, so replay reports uncharacterizable corruption.
        // Cleanup must surface that error before deleting anything.
        let dir = tempdir().unwrap();

        let mut snapshot = create_snapshot_with_train(100);
        snapshot.log_generation = 1;
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        File::create(events_path(dir.path(), 1)).unwrap();
        write_generation(dir.path(), 1).unwrap();

        let mut candidate = create_test_snapshot();
        candidate.log_generation = 2;
        save_snapshot_atomic(&snapshot_path(dir.path(), 2), &candidate).unwrap();
        std::fs::write(
            events_path(dir.path(), 2),
            concat!(
                r#"{"seq":0,"ts":"2024-01-01T00:00:00Z","type":"train_completed","root_pr":1}"#,
                "\n",
                "not json\n",
                r#"{"seq":1,"ts":"2024-01-01T00:00:00Z","type":"train_completed","root_pr":1}"#,
                "\n",
            ),
        )
        .unwrap();

        let result = cleanup_stale_generations(dir.path());
        assert!(
            matches!(result, Err(CompactionError::EventLog(_))),
            "cleanup must reject a candidate whose log is corrupt mid-file, got {:?}",
            result
        );

        assert!(
            snapshot_path(dir.path(), 1).exists(),
            "the last good snapshot must survive"
        );
        assert!(
            events_path(dir.path(), 1).exists(),
            "the last good event log must survive"
        );
        assert_eq!(
            read_generation(dir.path()).unwrap(),
            1,
            "the generation file must not be promoted to the bad candidate"
        );
    }

    #[test]
    fn cleanup_accepts_candidate_with_consistent_nonempty_log() {
        // The candidate's log holds events 0..3 and the snapshot says
        // next_seq 0 at log_position 0: replay accounts for every event, so
        // the candidate is exactly what recovery will accept. Cleanup must
        // promote it and delete the older generation.
        use crate::persistence::event::StateEventPayload;

        let dir = tempdir().unwrap();

        write_valid_snapshot(dir.path(), 1);
        File::create(events_path(dir.path(), 1)).unwrap();
        write_generation(dir.path(), 1).unwrap();

        let mut candidate = create_test_snapshot();
        candidate.log_generation = 2;
        save_snapshot_atomic(&snapshot_path(dir.path(), 2), &candidate).unwrap();
        {
            let mut log = EventLog::open_with_seq(events_path(dir.path(), 2), 0).unwrap();
            for i in 0..3 {
                log.append(StateEventPayload::TrainStarted {
                    root_pr: PrNumber(i),
                    current_pr: PrNumber(i),
                })
                .unwrap();
            }
        }

        cleanup_stale_generations(dir.path()).unwrap();

        assert!(!snapshot_path(dir.path(), 1).exists());
        assert!(!events_path(dir.path(), 1).exists());
        assert!(snapshot_path(dir.path(), 2).exists());
        assert!(events_path(dir.path(), 2).exists());
        assert_eq!(read_generation(dir.path()).unwrap(), 2);
    }

    #[test]
    fn cleanup_refuses_when_generation_file_ahead_of_all_snapshots() {
        // The generation file says 5 — generation 5 was committed — but
        // snapshot.5.json is gone and only snapshot.3.json remains. Demoting
        // the generation file to 3 would silently roll back committed state,
        // and the cleanup loop would delete events.5.log (events that exist
        // nowhere else). Cleanup must fail loudly and delete nothing.
        let dir = tempdir().unwrap();

        write_valid_snapshot(dir.path(), 3);
        File::create(events_path(dir.path(), 3)).unwrap();
        File::create(events_path(dir.path(), 5)).unwrap();
        write_generation(dir.path(), 5).unwrap();

        let result = cleanup_stale_generations(dir.path());
        assert!(
            matches!(
                result,
                Err(CompactionError::MissingSnapshot {
                    file_gen: 5,
                    max_snapshot_gen: Some(3),
                })
            ),
            "cleanup must refuse when the generation file is ahead of every \
             snapshot, got {:?}",
            result
        );

        assert!(snapshot_path(dir.path(), 3).exists());
        assert!(events_path(dir.path(), 3).exists());
        assert!(
            events_path(dir.path(), 5).exists(),
            "the committed generation's event log must survive"
        );
        assert_eq!(
            read_generation(dir.path()).unwrap(),
            5,
            "the generation file must not be demoted"
        );
    }

    #[test]
    fn cleanup_refuses_when_generation_file_ahead_with_no_snapshots() {
        // Generation 5 was committed but no snapshot exists at all. Only
        // generation 0 may legitimately lack a snapshot (fresh directory),
        // so this state is uncharacterizable: nothing may be deleted.
        let dir = tempdir().unwrap();

        write_generation(dir.path(), 5).unwrap();
        File::create(events_path(dir.path(), 2)).unwrap();

        let result = cleanup_stale_generations(dir.path());
        assert!(
            matches!(
                result,
                Err(CompactionError::MissingSnapshot {
                    file_gen: 5,
                    max_snapshot_gen: None,
                })
            ),
            "got {:?}",
            result
        );
        assert!(
            events_path(dir.path(), 2).exists(),
            "no file may be deleted in an uncharacterizable state"
        );
        assert_eq!(read_generation(dir.path()).unwrap(), 5);
    }

    #[test]
    fn cleanup_refuses_when_event_log_ahead_of_all_snapshots() {
        // events.5.log exists but the highest snapshot is generation 3 and
        // the generation file is corrupt. Event logs are only created for
        // committed generations, so generation 5's snapshot (and the
        // generation file) have been lost. Falling back to generation 3
        // would delete events.5.log.
        let dir = tempdir().unwrap();

        write_valid_snapshot(dir.path(), 3);
        File::create(events_path(dir.path(), 3)).unwrap();
        File::create(events_path(dir.path(), 5)).unwrap();
        std::fs::write(dir.path().join("generation"), "garbage\n").unwrap();

        let result = cleanup_stale_generations(dir.path());
        assert!(
            matches!(
                result,
                Err(CompactionError::EventLogAheadOfSnapshots {
                    log_generation: 5,
                    max_snapshot_gen: Some(3),
                    ..
                })
            ),
            "got {:?}",
            result
        );

        assert!(snapshot_path(dir.path(), 3).exists());
        assert!(events_path(dir.path(), 3).exists());
        assert!(
            events_path(dir.path(), 5).exists(),
            "the committed generation's event log must survive"
        );
    }

    #[test]
    fn compact_rejects_snapshot_generation_mismatch() {
        // The snapshot's log_generation must match the on-disk generation file.
        // A mismatch means the caller's snapshot does not describe the on-disk
        // state, and compacting would commit a lie.
        let dir = tempdir().unwrap();

        let mut snapshot = create_snapshot_with_train(100);
        snapshot.log_generation = 2;
        save_snapshot_atomic(&snapshot_path(dir.path(), 2), &snapshot).unwrap();
        write_generation(dir.path(), 2).unwrap();

        // In-memory snapshot claims a different generation.
        snapshot.log_generation = 1;

        let result = compact(dir.path(), &mut snapshot);
        assert!(
            result.is_err(),
            "compact must reject a snapshot whose log_generation disagrees with disk"
        );
        assert_eq!(
            snapshot.log_generation, 1,
            "snapshot must be unchanged on error"
        );
    }

    #[test]
    fn compact_rejects_next_seq_inconsistent_with_log() {
        // The in-memory snapshot must have applied every event in the current
        // log: snapshot.next_seq == (max seq in log) + 1. If not, compaction
        // would delete events that were never folded into the snapshot.
        use crate::persistence::event::StateEventPayload;
        use crate::persistence::log::EventLog;

        let dir = tempdir().unwrap();

        let mut snapshot = create_snapshot_with_train(100);
        snapshot.log_generation = 1;
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        write_generation(dir.path(), 1).unwrap();

        // Three events in the log (seqs 0, 1, 2)
        {
            let mut log = EventLog::open_with_seq(events_path(dir.path(), 1), 0).unwrap();
            for i in 0..3 {
                log.append(StateEventPayload::TrainStarted {
                    root_pr: PrNumber(1000 + i),
                    current_pr: PrNumber(1000 + i),
                })
                .unwrap();
            }
        }

        // Snapshot claims only one event was applied.
        snapshot.next_seq = 1;

        let result = compact(dir.path(), &mut snapshot);
        assert!(
            result.is_err(),
            "compact must reject next_seq inconsistent with the on-disk log"
        );
        assert!(
            events_path(dir.path(), 1).exists(),
            "the event log must not be deleted on validation failure"
        );
    }

    #[test]
    fn compact_fails_when_cleanup_not_called() {
        // If cleanup_stale_generations wasn't called and higher snapshots exist,
        // compact() should fail rather than silently create conflicts.
        let dir = tempdir().unwrap();

        // Create snapshot at generation 5 but generation file says 2
        let mut snapshot = create_snapshot_with_train(999);
        snapshot.log_generation = 5;
        save_snapshot_atomic(&snapshot_path(dir.path(), 5), &snapshot).unwrap();
        write_generation(dir.path(), 2).unwrap();

        // compact() should detect the inconsistency and fail
        let result = compact(dir.path(), &mut snapshot);
        assert!(
            result.is_err(),
            "compact should fail when higher snapshot exists"
        );

        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("generation 5") && err.contains("generation file says 2"),
            "Error should explain the conflict: {}",
            err
        );
    }

    /// Documents the Unix filesystem behavior that `compact_rollback_on_write_generation_failure`
    /// relies on: opening a directory for writing fails with EISDIR.
    #[cfg(unix)]
    #[test]
    fn unix_open_directory_for_write_fails() {
        use std::fs::OpenOptions;
        use std::io::ErrorKind;

        let dir = tempdir().unwrap();
        let subdir = dir.path().join("subdir");
        std::fs::create_dir(&subdir).unwrap();

        // Opening a directory for writing fails with "Is a directory"
        let result = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&subdir);

        assert!(result.is_err());
        let err = result.unwrap_err();
        // EISDIR (21 on most Unix) maps to ErrorKind::Other or ErrorKind::IsADirectory
        assert!(
            err.kind() == ErrorKind::Other
                || err.kind() == ErrorKind::IsADirectory
                || err.raw_os_error() == Some(21), // EISDIR on Linux/macOS
            "Expected EISDIR error, got: {:?}",
            err
        );
    }

    /// Tests that compact() rolls the orphaned snapshot back when
    /// write_generation() fails before its commit point (`NotCommitted`).
    ///
    /// Creating generation.tmp as a directory causes write_generation() to
    /// fail when opening the temp file for writing — well before the rename,
    /// so the failure is a clean abort — while save_snapshot_atomic()
    /// succeeds (it writes to a different file), leaving an orphan to roll
    /// back. Verifies, through the real filesystem:
    /// - On a NotCommitted write_generation failure, the orphaned snapshot
    ///   is deleted
    /// - The in-memory snapshot remains unchanged
    /// - The original generation's files remain intact
    #[cfg(unix)]
    #[test]
    fn compact_rollback_on_write_generation_failure() {
        let dir = tempdir().unwrap();

        // 1. Set up initial state at generation 1
        let mut snapshot = create_snapshot_with_train(100);
        snapshot.log_generation = 1;
        snapshot.log_position = 0;
        write_generation(dir.path(), 1).unwrap();
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        File::create(events_path(dir.path(), 1)).unwrap();

        // Capture original state for comparison
        let original_gen = snapshot.log_generation;
        let original_pos = snapshot.log_position;
        let original_trains = snapshot.active_trains.clone();

        // 2. Create generation.tmp as a directory to break write_generation
        // write_generation() tries to open generation.tmp for writing, which fails
        // with EISDIR when it's a directory. This happens AFTER save_snapshot_atomic()
        // has already written the new snapshot.
        std::fs::create_dir(dir.path().join("generation.tmp")).unwrap();

        // 3. Call compact() - should fail but rollback the orphaned snapshot
        let result = compact(dir.path(), &mut snapshot);
        assert!(
            matches!(
                result,
                Err(CompactionError::WriteGeneration(
                    WriteGenerationError::NotCommitted(_)
                ))
            ),
            "compact must report the pre-commit write_generation failure, got {:?}",
            result
        );

        // 4. Verify rollback: snapshot.2.json should NOT exist
        assert!(
            !snapshot_path(dir.path(), 2).exists(),
            "Orphaned snapshot should be deleted by rollback"
        );

        // 5. Verify original generation files are intact
        assert!(
            snapshot_path(dir.path(), 1).exists(),
            "Original snapshot should still exist"
        );
        assert!(
            events_path(dir.path(), 1).exists(),
            "Original events should still exist"
        );
        assert_eq!(
            read_generation(dir.path()).unwrap(),
            1,
            "Generation file should still be 1"
        );

        // 6. Verify in-memory snapshot is unchanged
        assert_eq!(
            snapshot.log_generation, original_gen,
            "In-memory log_generation should be unchanged"
        );
        assert_eq!(
            snapshot.log_position, original_pos,
            "In-memory log_position should be unchanged"
        );
        assert_eq!(
            snapshot.active_trains, original_trains,
            "In-memory active_trains should be unchanged"
        );
    }

    #[test]
    fn rollback_success_returns_original_error_and_removes_orphan() {
        let dir = tempdir().unwrap();
        let orphan = snapshot_path(dir.path(), 2);
        std::fs::write(&orphan, "{}").unwrap();

        let original = CompactionError::Io(io::Error::other("generation update failed"));
        let err = rollback_orphaned_snapshot(dir.path(), &orphan, original);

        assert!(
            matches!(err, CompactionError::Io(_)),
            "successful rollback must surface the original error, got {:?}",
            err
        );
        assert!(!orphan.exists(), "the orphaned snapshot must be removed");
    }

    /// A failed rollback leaves `snapshot.<N+1>` on disk, which on the next
    /// startup would promote the generation and delete `events.<N>.log`.
    /// That must be reported as the distinct fatal `RollbackFailed`, never as
    /// the original (retriable-looking) error.
    #[cfg(unix)]
    #[test]
    fn rollback_failure_is_distinct_fatal_error() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let orphan = snapshot_path(dir.path(), 2);
        std::fs::write(&orphan, "{}").unwrap();

        // Unlinking requires write permission on the directory: deny it.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555)).unwrap();

        let original = CompactionError::Io(io::Error::other("generation update failed"));
        let err = rollback_orphaned_snapshot(dir.path(), &orphan, original);

        // Restore permissions so tempdir can clean up.
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(
            matches!(err, CompactionError::RollbackFailed { .. }),
            "failed rollback must be the fatal RollbackFailed, got {:?}",
            err
        );
        assert!(
            orphan.exists(),
            "the orphan really is still on disk (the hazard the error reports)"
        );
    }

    proptest! {
        /// Verifies that events written after compact() are preserved.
        ///
        /// This is a regression test for issue 2: compact() must update the in-memory
        /// snapshot before deleting old files to ensure snapshot.log_generation always
        /// points to a valid generation.
        #[test]
        fn events_after_compact_are_preserved(
            train_pr in arb_pr_number(),
            initial_gen in 1u64..5,
        ) {
            use crate::persistence::log::EventLog;
            use crate::persistence::event::StateEventPayload;

            let dir = tempdir().unwrap();

            // Set up initial state
            let mut snapshot = create_test_snapshot();
            snapshot.active_trains.insert(train_pr, TrainRecord::new(train_pr, test_timestamp()));
            snapshot.log_generation = initial_gen;
            snapshot.log_position = 0;
            snapshot.next_seq = 0;

            write_generation(dir.path(), initial_gen).unwrap();
            save_snapshot_atomic(&snapshot_path(dir.path(), initial_gen), &snapshot).unwrap();
            File::create(events_path(dir.path(), initial_gen)).unwrap();

            // Call compact()
            compact_clean(dir.path(), &mut snapshot);

            // After compact(), snapshot.log_generation should point to the new generation
            let new_gen = initial_gen + 1;
            prop_assert_eq!(snapshot.log_generation, new_gen);

            // Write an event using snapshot.log_generation
            {
                let events_file = events_path(dir.path(), snapshot.log_generation);
                let mut log = EventLog::open_with_seq(&events_file, 0).unwrap();
                log.append(StateEventPayload::TrainStopped { root_pr: train_pr }).unwrap();
            }

            // Simulate restart: cleanup and reload
            cleanup_stale_generations(dir.path()).unwrap();
            let recovered_gen = read_generation(dir.path()).unwrap();
            let recovered_snapshot = crate::persistence::snapshot::load_snapshot(
                &snapshot_path(dir.path(), recovered_gen)
            ).unwrap();

            let events_file = events_path(dir.path(), recovered_gen);
            let (events, _) = EventLog::replay_from(&events_file, recovered_snapshot.log_position).unwrap();

            // Event should be preserved
            prop_assert!(
                events.iter().any(|e| matches!(&e.payload, StateEventPayload::TrainStopped { root_pr } if *root_pr == train_pr)),
                "Event written after compact() should be preserved; got: {:?}",
                events.iter().map(|e| &e.payload).collect::<Vec<_>>()
            );
        }
    }
}
