//! Single recovery entry point and state-directory locking.
//!
//! The five-step recovery protocol (previously prose in the module docs) is
//! enforced here as code, in this order:
//!
//! 1. Acquire an exclusive advisory lock on the state directory
//! 2. Settle generations (`cleanup_stale_generations`: validates the surviving
//!    snapshot, repairs the generation file, removes stale generations)
//! 3. Load the snapshot for the settled generation
//! 4. Replay events from the snapshot's `log_position`
//! 5. Construct an [`EventLog`] whose `next_seq` is derived from replay
//!
//! [`recover`] returns the log inside a [`PersistenceHandle`] that also owns
//! the state-directory lock: the lock cannot be released while the log is
//! still usable, and compaction (which retires the log's file) goes through
//! [`PersistenceHandle::compact`], which swaps the handle to the new
//! generation's log. The raw [`EventLog`] constructors are crate-private
//! because opening an existing log with the wrong sequence number writes
//! duplicate sequence numbers, which replay then rejects as corruption.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use thiserror::Error;

use super::compaction::{
    CompactionError, CompactionOutcome, cleanup_stale_generations, validate_snapshot_against_log,
};
use super::event::{StateEvent, StateEventPayload};
use super::generation::{GenerationError, events_path, read_generation, snapshot_path};
use super::log::{EventLog, EventLogError};
use super::snapshot::{PersistedRepoSnapshot, SnapshotError, try_load_snapshot};

/// Name of the lock file inside the state directory.
const LOCK_FILE_NAME: &str = "lock";

/// Errors that can occur during recovery.
#[derive(Debug, Error)]
pub enum RecoverError {
    /// IO error during recovery.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Another process holds the state directory lock.
    #[error(
        "state directory {} is locked by another process (lock file {}); refusing to start",
        state_dir.display(),
        lock_path.display()
    )]
    AlreadyLocked {
        state_dir: PathBuf,
        lock_path: PathBuf,
    },

    /// Generation cleanup failed.
    #[error(transparent)]
    Compaction(#[from] CompactionError),

    /// Generation file error.
    #[error("generation error: {0}")]
    Generation(#[from] GenerationError),

    /// Snapshot error.
    #[error("snapshot error: {0}")]
    Snapshot(#[from] SnapshotError),

    /// The generation file points at a generation with no snapshot file.
    /// Compaction writes the snapshot before advancing the generation file,
    /// so a missing snapshot at a non-zero generation means it was deleted.
    #[error(
        "generation {generation} has no snapshot file ({}); the snapshot has been lost; \
         manual intervention required",
        path.display()
    )]
    MissingSnapshot { generation: u64, path: PathBuf },

    /// The snapshot's embedded `log_generation` disagrees with the generation
    /// it was loaded as.
    #[error(
        "snapshot {} says log_generation {snapshot_generation} but was resolved as \
         generation {disk_generation}; manual intervention required",
        path.display()
    )]
    GenerationMismatch {
        path: PathBuf,
        snapshot_generation: u64,
        disk_generation: u64,
    },
}

/// Exclusive advisory lock on a state directory.
///
/// Held for the lifetime of this value; dropping it releases the lock.
/// Crate-private: it only ever lives inside a [`PersistenceHandle`], which
/// keeps it alive for exactly as long as the [`EventLog`] is usable.
#[derive(Debug)]
pub(crate) struct StateDirLock {
    file: File,
    path: PathBuf,
}

impl StateDirLock {
    /// Acquires the lock, failing immediately (no blocking) if another
    /// process holds it.
    fn acquire(state_dir: &Path) -> Result<Self, RecoverError> {
        std::fs::create_dir_all(state_dir)?;
        let path = state_dir.join(LOCK_FILE_NAME);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        match file.try_lock_exclusive() {
            Ok(()) => Ok(StateDirLock { file, path }),
            Err(e) if e.raw_os_error() == fs2::lock_contended_error().raw_os_error() => {
                Err(RecoverError::AlreadyLocked {
                    state_dir: state_dir.to_path_buf(),
                    lock_path: path,
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Path of the lock file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for StateDirLock {
    fn drop(&mut self) {
        // Best-effort explicit unlock; closing the descriptor releases the
        // lock anyway.
        let _ = fs2::FileExt::unlock(&self.file);
    }
}

/// The result of recovering a state directory: the recovered data plus the
/// live [`PersistenceHandle`] for writing.
///
/// `snapshot` and `events` are plain data and may be moved out freely; the
/// handle is the only live resource.
pub struct RecoveredState {
    /// The loaded snapshot (or a fresh one, for a brand-new state directory).
    /// Does NOT include the replayed `events`; the caller must apply them to
    /// reconstruct current state.
    pub snapshot: PersistedRepoSnapshot,
    /// Events recorded after the snapshot was taken, in log order.
    pub events: Vec<StateEvent>,
    /// The event log bound to the exclusive state-directory lock.
    pub handle: PersistenceHandle,
}

/// The live persistence handle: the event log bound to the exclusive
/// state-directory lock.
///
/// Fields are private so the log cannot outlive the lock: the lock is
/// released exactly when this handle — and with it the log — is dropped.
/// Compaction goes through [`PersistenceHandle::compact`], which switches
/// the handle to the new generation's log; the old log's file is deleted by
/// compaction, so no separately-held log handle may survive it, and the
/// crate-private [`EventLog`] constructors ensure none can exist.
pub struct PersistenceHandle {
    /// Declared before `lock` so the log's file handle is dropped before
    /// the lock is released.
    log: EventLog,
    lock: StateDirLock,
    state_dir: PathBuf,
}

impl PersistenceHandle {
    /// Appends an event to the log. See [`EventLog::append`].
    pub fn append(&mut self, payload: StateEventPayload) -> Result<StateEvent, EventLogError> {
        self.log.append(payload)
    }

    /// Forces fsync of the log file. See [`EventLog::sync`].
    pub fn sync(&mut self) -> Result<(), EventLogError> {
        self.log.sync()
    }

    /// Returns the current byte position in the log file, for recording as
    /// `log_position` in snapshots. Fails once the log is poisoned, like
    /// append/sync/compact: a post-failure offset may include a torn tail
    /// that the next recovery truncates away, so recording it would make
    /// recovery fail. See [`EventLog::position`].
    pub fn position(&mut self) -> Result<u64, EventLogError> {
        self.log.position()
    }

    /// Returns the next sequence number that will be assigned.
    pub fn next_seq(&self) -> u64 {
        self.log.next_seq()
    }

    /// Returns the path to the current generation's log file.
    pub fn log_path(&self) -> &Path {
        self.log.path()
    }

    /// Returns the path of the held lock file.
    pub fn lock_path(&self) -> &Path {
        self.lock.path()
    }

    /// Returns the state directory this handle owns.
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    /// Whether the current generation's log has grown past `threshold_bytes`
    /// and is worth compacting.
    pub fn should_compact(&self, threshold_bytes: u64) -> io::Result<bool> {
        Ok(std::fs::metadata(self.log.path())?.len() >= threshold_bytes)
    }

    /// Compacts the state directory and switches this handle to the new
    /// generation's event log.
    ///
    /// `snapshot` is the caller's current in-memory state. Every event ever
    /// appended must already be folded into it (`snapshot.next_seq` equal to
    /// this handle's [`next_seq`](Self::next_seq)): compaction deletes the
    /// old log, so an unfolded event would survive nowhere. On success the
    /// snapshot's `log_generation`/`log_position` are updated in place and
    /// subsequent appends go to the new generation's log, continuing the
    /// same sequence numbering.
    ///
    /// # Failure
    ///
    /// On a clean failure before the commit point the old generation remains
    /// authoritative and this handle still appends to it; the caller may
    /// retry or carry on. Two failures are fatal for the handle (it poisons
    /// itself so appends fail loudly, and the process must restart and
    /// recover):
    ///
    /// - [`CompactionError::RollbackFailed`]: the orphaned `snapshot.<N+1>`
    ///   may survive on disk while the generation file still points at N.
    ///   The next recovery will promote the orphan and delete this
    ///   generation's log, so an event appended through this handle now
    ///   would be silently destroyed then. Nothing already appended is lost:
    ///   the orphan folds in every appended event.
    /// - The commit succeeded but the new generation's log cannot be opened:
    ///   the durable state is intact (the snapshot holds everything; the new
    ///   log has no events yet) but this handle's file is the old
    ///   generation's deleted log, so appends through it would land in an
    ///   unlinked file.
    pub fn compact(
        &mut self,
        snapshot: &mut PersistedRepoSnapshot,
    ) -> Result<CompactionOutcome, CompactionError> {
        // A poisoned log's in-memory next_seq no longer describes its file:
        // the state is uncharacterizable, so refuse to compact from it.
        if self.log.is_poisoned() {
            return Err(EventLogError::PoisonedLog {
                path: self.log.path().to_path_buf(),
            }
            .into());
        }

        // Stronger form of compaction's folded-everything guard: the handle
        // knows exactly how many events were appended, so a stale snapshot
        // is caught even when the current log file is empty (where the
        // on-disk check is vacuous).
        if snapshot.next_seq != self.log.next_seq() {
            return Err(CompactionError::NextSeqInconsistent {
                path: self.log.path().to_path_buf(),
                snapshot_next_seq: snapshot.next_seq,
                log_next_seq: self.log.next_seq(),
            });
        }

        let outcome = match super::compaction::compact(&self.state_dir, snapshot) {
            Ok(outcome) => outcome,
            Err(e) => {
                // RollbackFailed leaves snapshot.<N+1> on disk while the
                // generation file still points at N: the next recovery
                // promotes the orphan and deletes events.<N>.log, so an
                // event appended through this handle now would be silently
                // destroyed then. Refuse post-failure appends instead.
                if matches!(e, CompactionError::RollbackFailed { .. }) {
                    self.log.poison();
                }
                return Err(e);
            }
        };

        // The commit is durable: from here appends must go to the new
        // generation's log, and this handle's file is the old generation's
        // deleted log. Poison it before the reopen so that a reopen failure
        // leaves the handle refusing appends rather than losing them.
        self.log.poison();
        self.log = EventLog::open_with_seq(
            events_path(&self.state_dir, snapshot.log_generation),
            snapshot.next_seq,
        )?;
        Ok(outcome)
    }
}

/// Recovers a state directory into its `(snapshot, events)` data plus a
/// [`PersistenceHandle`] holding an exclusive lock on the directory.
///
/// This is the single entry point for opening persisted state. A brand-new
/// state directory (generation 0, no snapshot) yields an empty snapshot
/// built with `default_branch_if_fresh`.
///
/// # Guarantees
///
/// On success:
/// - this process holds the exclusive state-directory lock
///   (double-start fails with [`RecoverError::AlreadyLocked`]),
/// - exactly one generation's files remain on disk,
/// - `snapshot` plus `events` (applied in order) reconstruct the durable
///   state, and
/// - the handle's `next_seq` is consistent with both the snapshot and the
///   log file.
///
/// Any state that cannot be characterized (mid-file corruption, missing
/// snapshot, sequence mismatch between snapshot and log) is a hard error:
/// nothing is repaired or deleted beyond truncating a torn tail write in the
/// event log.
pub fn recover(
    state_dir: &Path,
    default_branch_if_fresh: &str,
) -> Result<RecoveredState, RecoverError> {
    // Step 1: lock out concurrent instances before touching anything.
    let lock = StateDirLock::acquire(state_dir)?;

    // Step 2: settle generations. This semantically validates the surviving
    // snapshot (it loads, describes the generation it is stored as, and is
    // consistent with its event log) and refuses states showing a committed
    // generation's snapshot was lost, all before deleting anything; then
    // repairs the generation file and removes stale generations and temp
    // files.
    cleanup_stale_generations(state_dir)?;

    // Step 3: load the snapshot for the settled generation.
    let generation = read_generation(state_dir)?;
    let snap_path = snapshot_path(state_dir, generation);
    let snapshot = match try_load_snapshot(&snap_path)? {
        Some(s) => {
            if s.log_generation != generation {
                return Err(RecoverError::GenerationMismatch {
                    path: snap_path,
                    snapshot_generation: s.log_generation,
                    disk_generation: generation,
                });
            }
            s
        }
        // Generation 0 with no snapshot is a brand-new state directory.
        None if generation == 0 => PersistedRepoSnapshot::new(default_branch_if_fresh),
        None => {
            return Err(RecoverError::MissingSnapshot {
                generation,
                path: snap_path,
            });
        }
    };

    // Step 4: replay events from the snapshot's position, with the shared
    // log-consistency checks (log at least `log_position` bytes long,
    // sequence numbers lining up with `next_seq`). Generation settling
    // already applied these checks to the candidate before deleting
    // anything; this run is the backstop, and produces the replayed events.
    let (events, next_seq) = validate_snapshot_against_log(state_dir, &snapshot)?;

    // Step 5: open the log for appending with the replay-derived next_seq.
    let log_path = events_path(state_dir, generation);
    let log = EventLog::open_with_seq(&log_path, next_seq)?;

    Ok(RecoveredState {
        snapshot,
        events,
        handle: PersistenceHandle {
            log,
            lock,
            state_dir: state_dir.to_path_buf(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::fsync::fault;
    use crate::persistence::generation::write_generation;
    use crate::persistence::snapshot::save_snapshot_atomic;
    use crate::test_utils::arb_state_event_payload;
    use crate::types::PrNumber;
    use proptest::prelude::*;
    use std::io::Write;
    use tempfile::tempdir;

    fn payload(n: u64) -> StateEventPayload {
        StateEventPayload::TrainStarted {
            root_pr: PrNumber(n),
            current_pr: PrNumber(n),
        }
    }

    /// One step of a caller's lifetime: append an event or compact.
    #[derive(Debug, Clone)]
    #[allow(clippy::large_enum_variant)]
    enum Step {
        Append(StateEventPayload),
        Compact,
    }

    fn arb_step() -> impl Strategy<Value = Step> {
        prop_oneof![
            4 => arb_state_event_payload().prop_map(Step::Append),
            1 => Just(Step::Compact),
        ]
    }

    // ─── Fresh directory ───

    #[test]
    fn recover_fresh_directory_yields_empty_state() {
        let dir = tempdir().unwrap();

        let recovered = recover(dir.path(), "main").unwrap();
        assert_eq!(recovered.snapshot.default_branch, "main");
        assert_eq!(recovered.snapshot.next_seq, 0);
        assert!(recovered.events.is_empty());
        assert_eq!(recovered.handle.next_seq(), 0);
    }

    #[test]
    fn recover_roundtrip_replays_appended_events() {
        let dir = tempdir().unwrap();

        {
            let mut recovered = recover(dir.path(), "main").unwrap();
            for i in 0..3 {
                recovered.handle.append(payload(i)).unwrap();
            }
            // recovered (and its lock) dropped here: simulated shutdown
        }

        let recovered = recover(dir.path(), "main").unwrap();
        assert_eq!(recovered.events.len(), 3);
        for (i, event) in recovered.events.iter().enumerate() {
            assert_eq!(event.seq, i as u64);
        }
        assert_eq!(recovered.handle.next_seq(), 3);
    }

    // ─── Locking ───

    #[test]
    fn double_start_fails_loudly() {
        let dir = tempdir().unwrap();

        let first = recover(dir.path(), "main").unwrap();
        let second = recover(dir.path(), "main");
        assert!(
            matches!(second, Err(RecoverError::AlreadyLocked { .. })),
            "second recover while first is alive must fail with AlreadyLocked, got {:?}",
            second.as_ref().map(|_| "Ok").map_err(|e| e.to_string())
        );
        drop(first);
    }

    #[test]
    fn lock_released_on_drop() {
        let dir = tempdir().unwrap();

        let first = recover(dir.path(), "main").unwrap();
        drop(first);
        let second = recover(dir.path(), "main");
        assert!(second.is_ok(), "lock must be released when handle dropped");
    }

    #[test]
    fn handle_moved_out_of_recovered_state_keeps_lock() {
        // The misuse the review flagged: `recover(..).unwrap().log` used to
        // drop the lock at the end of the statement while the log stayed
        // usable. The handle now carries the lock with it.
        let dir = tempdir().unwrap();

        let mut handle = recover(dir.path(), "main").unwrap().handle;
        let second = recover(dir.path(), "main");
        assert!(
            matches!(second, Err(RecoverError::AlreadyLocked { .. })),
            "the lock must travel with the handle, got {:?}",
            second.as_ref().map(|_| "Ok").map_err(|e| e.to_string())
        );

        // The handle is fully usable on its own.
        handle.append(payload(0)).unwrap();

        drop(handle);
        assert!(
            recover(dir.path(), "main").is_ok(),
            "dropping the handle must release the lock"
        );
    }

    // ─── Hard error cases ───

    #[test]
    fn missing_snapshot_at_nonzero_generation_is_hard_error() {
        let dir = tempdir().unwrap();
        write_generation(dir.path(), 2).unwrap();

        let result = recover(dir.path(), "main");
        // Generation settling already refuses this state (before it would
        // delete or repair anything); recover's own MissingSnapshot check
        // remains as a backstop behind it.
        assert!(
            matches!(
                result,
                Err(RecoverError::Compaction(CompactionError::MissingSnapshot {
                    file_gen: 2,
                    max_snapshot_gen: None,
                }))
            ),
            "generation 2 with no snapshot must be a hard error, got {:?}",
            result.as_ref().map(|_| "Ok").map_err(|e| e.to_string())
        );
    }

    #[test]
    fn generation_file_ahead_of_snapshots_fails_recovery_without_deletion() {
        // The generation file records a committed generation (5) whose
        // snapshot is gone, while an older snapshot (3) survives. Recovery
        // must fail loudly without demoting the generation file or deleting
        // the older generation's files or events.5.log.
        let dir = tempdir().unwrap();

        let mut snapshot = PersistedRepoSnapshot::new("main");
        snapshot.log_generation = 3;
        save_snapshot_atomic(&snapshot_path(dir.path(), 3), &snapshot).unwrap();
        std::fs::write(events_path(dir.path(), 3), b"").unwrap();
        std::fs::write(events_path(dir.path(), 5), b"").unwrap();
        write_generation(dir.path(), 5).unwrap();

        let result = recover(dir.path(), "main");
        assert!(
            matches!(
                result,
                Err(RecoverError::Compaction(CompactionError::MissingSnapshot {
                    file_gen: 5,
                    max_snapshot_gen: Some(3),
                }))
            ),
            "a lost committed generation must abort recovery, got {:?}",
            result.as_ref().map(|_| "Ok").map_err(|e| e.to_string())
        );
        assert!(
            snapshot_path(dir.path(), 3).exists(),
            "the previous generation's snapshot must survive"
        );
        assert!(
            events_path(dir.path(), 3).exists(),
            "the previous generation's event log must survive"
        );
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
    fn snapshot_log_generation_mismatch_is_hard_error() {
        let dir = tempdir().unwrap();

        let mut snapshot = PersistedRepoSnapshot::new("main");
        snapshot.log_generation = 7; // disagrees with its filename / gen file
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        write_generation(dir.path(), 1).unwrap();

        let result = recover(dir.path(), "main");
        // Generation settling already refuses this candidate (before it
        // would delete anything); recover's own GenerationMismatch check
        // remains as a backstop behind it.
        assert!(
            matches!(
                result,
                Err(RecoverError::Compaction(
                    CompactionError::CandidateGenerationMismatch { .. }
                ))
            ),
            "snapshot claiming a different generation must be a hard error, got {:?}",
            result.as_ref().map(|_| "Ok").map_err(|e| e.to_string())
        );
    }

    #[test]
    fn log_shorter_than_snapshot_position_is_hard_error() {
        let dir = tempdir().unwrap();

        let mut snapshot = PersistedRepoSnapshot::new("main");
        snapshot.log_position = 10_000; // far beyond the actual log
        save_snapshot_atomic(&snapshot_path(dir.path(), 0), &snapshot).unwrap();
        write_generation(dir.path(), 0).unwrap();
        std::fs::write(events_path(dir.path(), 0), b"").unwrap();

        let result = recover(dir.path(), "main");
        // Generation settling already refuses this candidate (before it
        // would delete anything); recover applies the same shared check to
        // the settled generation as a backstop.
        assert!(
            matches!(
                result,
                Err(RecoverError::Compaction(
                    CompactionError::LogTruncated { .. }
                ))
            ),
            "log shorter than snapshot.log_position must be a hard error, got {:?}",
            result.as_ref().map(|_| "Ok").map_err(|e| e.to_string())
        );
    }

    #[test]
    fn snapshot_next_seq_disagreeing_with_log_is_hard_error() {
        let dir = tempdir().unwrap();

        // Snapshot claims next_seq = 5 at log_position 0, but the log's
        // events start at seq 0: the snapshot and log disagree.
        let mut snapshot = PersistedRepoSnapshot::new("main");
        snapshot.next_seq = 5;
        save_snapshot_atomic(&snapshot_path(dir.path(), 0), &snapshot).unwrap();
        write_generation(dir.path(), 0).unwrap();

        let mut file = std::fs::File::create(events_path(dir.path(), 0)).unwrap();
        for seq in 0..3 {
            writeln!(
                file,
                r#"{{"seq":{},"ts":"2024-01-01T00:00:00Z","type":"train_completed","root_pr":1}}"#,
                seq
            )
            .unwrap();
        }
        drop(file);

        let result = recover(dir.path(), "main");
        // Generation settling already refuses this candidate (before it
        // would delete anything); recover applies the same shared check to
        // the settled generation as a backstop.
        assert!(
            matches!(
                result,
                Err(RecoverError::Compaction(
                    CompactionError::SequenceMismatch { .. }
                ))
            ),
            "snapshot/log next_seq disagreement must be a hard error, got {:?}",
            result.as_ref().map(|_| "Ok").map_err(|e| e.to_string())
        );
    }

    #[test]
    fn corrupt_candidate_snapshot_fails_recovery_without_deletion() {
        let dir = tempdir().unwrap();

        // Valid generation 1, corrupt snapshot at generation 2.
        let mut snapshot = PersistedRepoSnapshot::new("main");
        snapshot.log_generation = 1;
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        write_generation(dir.path(), 1).unwrap();
        std::fs::write(snapshot_path(dir.path(), 2), "{ corrupt").unwrap();

        let result = recover(dir.path(), "main");
        assert!(
            matches!(result, Err(RecoverError::Compaction(_))),
            "corrupt candidate snapshot must abort recovery"
        );
        assert!(
            snapshot_path(dir.path(), 1).exists(),
            "the last good snapshot must survive the failed recovery"
        );
    }

    // ─── Integration with compaction ───

    /// Compacts through the handle, asserting commit and cleanup succeeded.
    fn compact_clean(handle: &mut PersistenceHandle, snapshot: &mut PersistedRepoSnapshot) {
        match handle.compact(snapshot).unwrap() {
            CompactionOutcome::Clean => {}
            CompactionOutcome::CleanupPending(e) => panic!("cleanup failed: {}", e),
        }
    }

    /// Records the instrumented filesystem-op sequence of one successful
    /// [`PersistenceHandle::compact`] on a fresh state directory with
    /// `num_events` appended events.
    ///
    /// Fault-driven tests locate their target op index in this recording
    /// (made on an identical setup) instead of hardcoding it, so they don't
    /// silently drift when the compaction procedure changes shape.
    fn record_compact_ops(num_events: u64) -> Vec<fault::RecordedOp> {
        let dir = tempdir().unwrap();
        let recovered = recover(dir.path(), "main").unwrap();
        let mut snapshot = recovered.snapshot;
        let mut handle = recovered.handle;
        for i in 0..num_events {
            handle.append(payload(i)).unwrap();
        }
        snapshot.next_seq = handle.next_seq();

        // Armed strictly around the compact call: appends and recover
        // perform instrumented ops of their own.
        let guard = fault::arm(fault::FaultMode::Record);
        compact_clean(&mut handle, &mut snapshot);
        guard.take_recording()
    }

    /// Index of `write_generation`'s tmp-file fsync within a recorded
    /// compact: the `FsyncFile` immediately preceding the rename onto
    /// `generation`.
    fn write_generation_tmp_fsync_index(ops: &[fault::RecordedOp]) -> usize {
        let rename_idx = ops
            .iter()
            .position(|op| {
                op.kind == fault::OpKind::Rename
                    && op.path.file_name() == Some(std::ffi::OsStr::new("generation"))
            })
            .expect("a successful compact must rename onto the generation file");
        let idx = rename_idx
            .checked_sub(1)
            .expect("the generation rename cannot be the first op");
        assert_eq!(
            ops[idx].kind,
            fault::OpKind::FsyncFile,
            "the op before the generation rename must be the tmp-file fsync"
        );
        idx
    }

    #[test]
    fn compact_op_sequence_is_pinned() {
        // The fault-driven tests below sweep or target indices within the
        // op-stream of a compact. Pin the stream's shape so a change to the
        // compaction procedure updates those tests knowingly rather than
        // silently shifting every fault index.
        use fault::OpKind::*;
        let ops = record_compact_ops(3);
        let kinds: Vec<fault::OpKind> = ops.iter().map(|op| op.kind).collect();
        assert_eq!(
            kinds,
            vec![
                // save_snapshot_atomic(snapshot.<N+1>): fsync tmp, rename, fsync dir
                FsyncFile, Rename, FsyncDir,
                // write_generation(N+1): fsync tmp, rename, fsync dir
                FsyncFile, Rename, FsyncDir,
                // delete_old_generation(N): unlink snapshot, unlink log, fsync dir
                RemoveFile, RemoveFile, FsyncDir,
                // the handle reopens (creates) the new generation's log
                FsyncDir,
            ]
        );
    }

    #[test]
    fn append_after_compaction_is_durable() {
        // Regression test: compacting used to leave a live log handle
        // writing to the old generation's deleted log file, silently losing
        // every subsequent append. The handle now switches itself to the new
        // generation's log.
        let dir = tempdir().unwrap();

        let recovered = recover(dir.path(), "main").unwrap();
        let mut snapshot = recovered.snapshot;
        let mut handle = recovered.handle;

        handle.append(payload(0)).unwrap();
        // The caller folds the appended event into its snapshot and compacts.
        snapshot.next_seq = handle.next_seq();
        compact_clean(&mut handle, &mut snapshot);

        assert_eq!(snapshot.log_generation, 1);
        assert_eq!(
            handle.log_path(),
            events_path(dir.path(), 1).as_path(),
            "the handle must now write to the new generation's log"
        );
        assert_eq!(handle.next_seq(), 1, "sequence continues across compaction");

        // An append after compaction must land in the new generation's log.
        let event = handle.append(payload(1)).unwrap();
        assert_eq!(event.seq, 1);
        drop(handle);

        let recovered = recover(dir.path(), "main").unwrap();
        assert_eq!(recovered.snapshot.log_generation, 1);
        assert_eq!(recovered.snapshot.next_seq, 1);
        assert_eq!(
            recovered.events.len(),
            1,
            "the post-compaction append must survive recovery"
        );
        assert_eq!(recovered.events[0].seq, 1);
        assert_eq!(recovered.handle.next_seq(), 2);
    }

    #[test]
    fn compact_with_unfolded_event_is_refused_and_handle_survives() {
        let dir = tempdir().unwrap();

        let recovered = recover(dir.path(), "main").unwrap();
        let mut snapshot = recovered.snapshot;
        let mut handle = recovered.handle;

        handle.append(payload(0)).unwrap();
        // snapshot.next_seq deliberately NOT updated: the event is unfolded,
        // and compacting would delete the only copy of it.

        let result = handle.compact(&mut snapshot);
        assert!(
            matches!(result, Err(CompactionError::NextSeqInconsistent { .. })),
            "compacting with an unfolded event must be refused, got {:?}",
            result
        );

        // The old generation is still authoritative and the handle usable.
        handle.append(payload(1)).unwrap();
        drop(handle);
        let recovered = recover(dir.path(), "main").unwrap();
        assert_eq!(recovered.events.len(), 2);
    }

    #[test]
    fn compact_with_stale_snapshot_and_empty_log_is_refused() {
        // The on-disk folded-everything check is vacuous when the log file
        // is empty; the handle's own next_seq check must still catch a
        // snapshot claiming events that were never appended.
        let dir = tempdir().unwrap();

        let recovered = recover(dir.path(), "main").unwrap();
        let mut snapshot = recovered.snapshot;
        let mut handle = recovered.handle;

        snapshot.next_seq = 5;
        let result = handle.compact(&mut snapshot);
        assert!(
            matches!(
                result,
                Err(CompactionError::NextSeqInconsistent {
                    snapshot_next_seq: 5,
                    log_next_seq: 0,
                    ..
                })
            ),
            "a snapshot claiming unappended events must be refused, got {:?}",
            result
        );
    }

    #[test]
    fn failed_compact_leaves_handle_on_old_log() {
        let dir = tempdir().unwrap();

        let recovered = recover(dir.path(), "main").unwrap();
        let mut snapshot = recovered.snapshot;
        let mut handle = recovered.handle;

        handle.append(payload(0)).unwrap();
        snapshot.next_seq = handle.next_seq();
        snapshot.log_generation = 7; // does not describe the on-disk state

        let result = handle.compact(&mut snapshot);
        assert!(
            matches!(
                result,
                Err(CompactionError::SnapshotGenerationMismatch { .. })
            ),
            "got {:?}",
            result
        );
        assert_eq!(
            handle.log_path(),
            events_path(dir.path(), 0).as_path(),
            "a failed compaction must leave the handle on the old log"
        );

        // The old generation remains authoritative; appends still land.
        handle.append(payload(1)).unwrap();
        drop(handle);
        let recovered = recover(dir.path(), "main").unwrap();
        assert_eq!(recovered.events.len(), 2);
    }

    #[test]
    fn position_on_poisoned_handle_is_refused() {
        // The misuse the review flagged: after an append/sync failure the
        // file may hold a torn tail beyond the last durable event. If
        // position() still reported an offset and the caller recorded it as
        // a snapshot's log_position, recovery would truncate the torn tail
        // below the saved position and fail with LogTruncated.
        let dir = tempdir().unwrap();

        let recovered = recover(dir.path(), "main").unwrap();
        let mut handle = recovered.handle;

        handle.append(payload(0)).unwrap();
        handle.position().unwrap();

        // Simulate an earlier failed write (test-only direct field access).
        handle.log.poison();

        let err = handle.position().unwrap_err();
        assert!(
            matches!(err, EventLogError::PoisonedLog { .. }),
            "position on a poisoned handle must be refused, got {:?}",
            err
        );
    }

    #[test]
    fn rollback_failed_compact_poisons_handle() {
        // RollbackFailed leaves snapshot.<N+1> on disk while the generation
        // file still points at N. An append through a still-usable handle
        // would land in events.<N>.log, which the next recovery deletes when
        // it promotes the orphan: the handle must poison itself instead.
        //
        // Induced with a dead disk (FailFrom) starting at write_generation's
        // tmp-file fsync: the generation update fails before its rename, and
        // the orphan's rollback unlink then fails too. The injected unlink
        // failure suppresses the unlink itself, so the orphan survives.
        let fail_from = write_generation_tmp_fsync_index(&record_compact_ops(3));

        let dir = tempdir().unwrap();

        let recovered = recover(dir.path(), "main").unwrap();
        let mut snapshot = recovered.snapshot;
        let mut handle = recovered.handle;

        for i in 0..3 {
            handle.append(payload(i)).unwrap();
        }
        snapshot.next_seq = handle.next_seq();

        let guard = fault::arm(fault::FaultMode::FailFrom(fail_from));
        let err = handle.compact(&mut snapshot).unwrap_err();
        // Disarm before the post-failure probes and final recovery below:
        // the dead disk ends with the failed compact.
        drop(guard);
        assert!(
            matches!(err, CompactionError::RollbackFailed { .. }),
            "got {:?}",
            err
        );

        // The hazard state really is on disk: the orphan exists while the
        // generation file still points at the old generation.
        assert!(snapshot_path(dir.path(), 1).exists());
        assert_eq!(read_generation(dir.path()).unwrap(), 0);
        assert_eq!(snapshot.log_generation, 0, "in-memory snapshot unchanged");

        // Every write-path operation must now refuse.
        assert!(matches!(
            handle.append(payload(9)),
            Err(EventLogError::PoisonedLog { .. })
        ));
        assert!(matches!(
            handle.sync(),
            Err(EventLogError::PoisonedLog { .. })
        ));
        assert!(matches!(
            handle.position(),
            Err(EventLogError::PoisonedLog { .. })
        ));
        assert!(matches!(
            handle.compact(&mut snapshot),
            Err(CompactionError::EventLog(EventLogError::PoisonedLog { .. }))
        ));

        // Because nothing could be appended after the failure, a restart
        // recovers cleanly: the orphan (which folds every appended event) is
        // promoted and no event is lost.
        drop(handle);
        let recovered = recover(dir.path(), "main").unwrap();
        assert_eq!(recovered.snapshot.log_generation, 1);
        assert_eq!(recovered.snapshot.next_seq, 3);
        assert!(recovered.events.is_empty());
        assert_eq!(recovered.handle.next_seq(), 3);
    }

    #[test]
    fn failed_snapshot_write_during_compact_leaves_handle_usable() {
        // A snapshot write that fails before its rename leaves no orphan:
        // the rollback is a no-op, the old generation remains authoritative,
        // and the handle must NOT be poisoned (the failure is retriable).
        let dir = tempdir().unwrap();

        let recovered = recover(dir.path(), "main").unwrap();
        let mut snapshot = recovered.snapshot;
        let mut handle = recovered.handle;

        handle.append(payload(0)).unwrap();
        snapshot.next_seq = handle.next_seq();

        // Obstruct the snapshot's tmp path so the write fails before rename.
        let obstruction = dir.path().join("snapshot.1.json.tmp");
        std::fs::create_dir(&obstruction).unwrap();

        let err = handle.compact(&mut snapshot).unwrap_err();
        assert!(
            matches!(err, CompactionError::Snapshot(_)),
            "a clean snapshot-write failure must surface as Snapshot, got {:?}",
            err
        );
        assert!(
            !snapshot_path(dir.path(), 1).exists(),
            "no orphan may be left behind"
        );
        assert_eq!(snapshot.log_generation, 0, "in-memory snapshot unchanged");

        // The failure was clean: appends still land, and compaction succeeds
        // once the obstruction is gone.
        handle.append(payload(1)).unwrap();
        snapshot.next_seq = handle.next_seq();
        std::fs::remove_dir(&obstruction).unwrap();
        compact_clean(&mut handle, &mut snapshot);
        handle.append(payload(2)).unwrap();
        drop(handle);

        let recovered = recover(dir.path(), "main").unwrap();
        assert_eq!(recovered.snapshot.log_generation, 1);
        assert_eq!(recovered.snapshot.next_seq, 2);
        assert_eq!(recovered.events.len(), 1);
        assert_eq!(recovered.handle.next_seq(), 3);
    }

    #[test]
    fn compact_on_poisoned_log_is_refused() {
        let dir = tempdir().unwrap();

        let recovered = recover(dir.path(), "main").unwrap();
        let mut snapshot = recovered.snapshot;
        let mut handle = recovered.handle;

        // Simulate an earlier failed write (test-only direct field access:
        // the handle's in-memory next_seq no longer describes the file).
        handle.log.poison();

        let result = handle.compact(&mut snapshot);
        assert!(
            matches!(
                result,
                Err(CompactionError::EventLog(EventLogError::PoisonedLog { .. }))
            ),
            "compacting a poisoned log must be refused, got {:?}",
            result
        );
    }

    #[test]
    fn recover_after_compaction_uses_new_generation() {
        let dir = tempdir().unwrap();

        // Build some state: events at generation 0, then compact. In a real
        // system the snapshot would have the events applied; here it is
        // enough that next_seq is carried.
        {
            let recovered = recover(dir.path(), "main").unwrap();
            let mut snapshot = recovered.snapshot;
            let mut handle = recovered.handle;
            for i in 0..3 {
                handle.append(payload(i)).unwrap();
            }
            snapshot.next_seq = handle.next_seq();
            compact_clean(&mut handle, &mut snapshot);
        }

        // Recovery must use the new generation and carry next_seq forward.
        let mut recovered = recover(dir.path(), "main").unwrap();
        assert_eq!(recovered.snapshot.log_generation, 1);
        assert_eq!(recovered.snapshot.next_seq, 3);
        assert!(recovered.events.is_empty());
        assert_eq!(recovered.handle.next_seq(), 3);

        // Appends continue the global sequence.
        let event = recovered.handle.append(payload(99)).unwrap();
        assert_eq!(event.seq, 3);
    }

    // ─── Properties ───

    proptest! {
        /// Whatever was appended before a clean shutdown is replayed by the
        /// next recovery, in order, with contiguous sequence numbers.
        #[test]
        fn recover_replays_exactly_what_was_appended(
            payloads in prop::collection::vec(arb_state_event_payload(), 1..10)
        ) {
            let dir = tempdir().unwrap();

            {
                let mut recovered = recover(dir.path(), "main").unwrap();
                for p in &payloads {
                    recovered.handle.append(p.clone()).unwrap();
                }
            }

            let recovered = recover(dir.path(), "main").unwrap();
            prop_assert_eq!(recovered.events.len(), payloads.len());
            for (i, (event, expected)) in
                recovered.events.iter().zip(payloads.iter()).enumerate()
            {
                prop_assert_eq!(event.seq, i as u64);
                prop_assert_eq!(&event.payload, expected);
            }
            prop_assert_eq!(recovered.handle.next_seq(), payloads.len() as u64);
        }

        /// Recovery is idempotent in the absence of writes: a second recovery
        /// observes the same snapshot and events as the first.
        #[test]
        fn recover_is_idempotent_without_writes(
            payloads in prop::collection::vec(arb_state_event_payload(), 0..6)
        ) {
            let dir = tempdir().unwrap();

            {
                let mut recovered = recover(dir.path(), "main").unwrap();
                for p in &payloads {
                    recovered.handle.append(p.clone()).unwrap();
                }
            }

            let first = recover(dir.path(), "main").unwrap();
            let first_events = first.events.clone();
            let first_snapshot = first.snapshot.clone();
            let first_next = first.handle.next_seq();
            drop(first);

            let second = recover(dir.path(), "main").unwrap();
            prop_assert_eq!(first_events, second.events);
            // When no snapshot file exists, recover synthesizes a fresh
            // in-memory snapshot stamped at recovery time; `snapshot_at` is
            // not durable state, so normalize it before comparing.
            let mut second_snapshot = second.snapshot.clone();
            second_snapshot.snapshot_at = first_snapshot.snapshot_at;
            prop_assert_eq!(first_snapshot, second_snapshot);
            prop_assert_eq!(first_next, second.handle.next_seq());
        }

        /// A torn tail write (arbitrary garbage at EOF) does not prevent
        /// recovery, and recovery then continues the sequence correctly.
        #[test]
        fn recover_survives_torn_tail_write(
            payloads in prop::collection::vec(arb_state_event_payload(), 1..6),
            garbage in prop::collection::vec(prop::num::u8::ANY, 1..40),
        ) {
            // Garbage that happens to start with a newline-terminated valid
            // event line is astronomically unlikely; a partial JSON prefix is
            // the realistic torn write.
            let dir = tempdir().unwrap();

            {
                let mut recovered = recover(dir.path(), "main").unwrap();
                for p in &payloads {
                    recovered.handle.append(p.clone()).unwrap();
                }
            }

            // Torn write: partial JSON then arbitrary bytes, no newline before it.
            {
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(events_path(dir.path(), 0))
                    .unwrap();
                write!(file, r#"{{"seq":"#).unwrap();
                file.write_all(&garbage).unwrap();
            }

            let recovered = recover(dir.path(), "main").unwrap();
            prop_assert_eq!(recovered.events.len(), payloads.len());
            prop_assert_eq!(recovered.handle.next_seq(), payloads.len() as u64);
        }

        /// Any interleaving of appends and compactions through the handle
        /// loses nothing: recovery accounts for every append, and replays
        /// exactly the events appended since the last compaction.
        #[test]
        fn interleaved_appends_and_compactions_lose_nothing(
            steps in prop::collection::vec(arb_step(), 1..20)
        ) {
            let dir = tempdir().unwrap();

            let recovered = recover(dir.path(), "main").unwrap();
            let mut snapshot = recovered.snapshot;
            let mut handle = recovered.handle;

            let mut total: u64 = 0;
            let mut since_compact: Vec<StateEventPayload> = Vec::new();

            for step in &steps {
                match step {
                    Step::Append(p) => {
                        let event = handle.append(p.clone()).unwrap();
                        prop_assert_eq!(event.seq, total);
                        total += 1;
                        since_compact.push(p.clone());
                        // The caller folds each event into its in-memory state.
                        snapshot.next_seq = handle.next_seq();
                    }
                    Step::Compact => {
                        compact_clean(&mut handle, &mut snapshot);
                        since_compact.clear();
                    }
                }
            }
            drop(handle);

            let recovered = recover(dir.path(), "main").unwrap();
            prop_assert_eq!(recovered.handle.next_seq(), total);
            prop_assert_eq!(
                recovered.snapshot.next_seq + recovered.events.len() as u64,
                total,
                "snapshot plus replayed events must account for every append"
            );
            prop_assert_eq!(recovered.events.len(), since_compact.len());
            for (event, expected) in recovered.events.iter().zip(since_compact.iter()) {
                prop_assert_eq!(&event.payload, expected);
            }
        }
    }
}
