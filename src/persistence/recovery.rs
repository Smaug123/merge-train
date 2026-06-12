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
//! [`recover`] is the only way to obtain an [`EventLog`] from outside this
//! crate: the raw constructors are crate-private because opening an existing
//! log with the wrong sequence number writes duplicate sequence numbers,
//! which replay then rejects as corruption.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use thiserror::Error;

use super::compaction::{CompactionError, cleanup_stale_generations};
use super::event::StateEvent;
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

    /// Event log error.
    #[error("event log error: {0}")]
    EventLog(#[from] EventLogError),

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
}

/// Exclusive advisory lock on a state directory.
///
/// Held for the lifetime of this value; dropping it releases the lock.
/// Keep it alive for as long as the [`EventLog`] returned by [`recover`]
/// is in use.
#[derive(Debug)]
pub struct StateDirLock {
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

/// A fully recovered, ready-to-use persistence handle.
pub struct RecoveredState {
    /// The loaded snapshot (or a fresh one, for a brand-new state directory).
    /// Does NOT include the replayed `events`; the caller must apply them to
    /// reconstruct current state.
    pub snapshot: PersistedRepoSnapshot,
    /// Events recorded after the snapshot was taken, in log order.
    pub events: Vec<StateEvent>,
    /// Event log positioned for appending; its `next_seq` is replay-derived.
    pub log: EventLog,
    /// Exclusive lock on the state directory. Keep it alive for as long as
    /// `log` is in use; dropping it releases the lock.
    pub lock: StateDirLock,
}

/// Recovers a state directory into a ready-to-use `(snapshot, events, log)`,
/// holding an exclusive lock on the directory.
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
/// - `log.next_seq()` is consistent with both the snapshot and the log file.
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
    // snapshot (it loads and describes the generation it is stored as) and
    // refuses states showing a committed generation's snapshot was lost,
    // all before deleting anything; then repairs the generation file and
    // removes stale generations and temp files.
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

    // Step 4: replay events from the snapshot's position.
    let log_path = events_path(state_dir, generation);
    let file_len = match std::fs::metadata(&log_path) {
        Ok(m) => m.len(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
        Err(e) => return Err(e.into()),
    };
    if snapshot.log_position > file_len {
        return Err(RecoverError::LogTruncated {
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
        return Err(RecoverError::LogTruncated {
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
    let next_seq = if log_next_seq == 0 {
        snapshot.next_seq
    } else if log_next_seq == snapshot.next_seq + events.len() as u64 {
        log_next_seq
    } else {
        return Err(RecoverError::SequenceMismatch {
            path: log_path,
            snapshot_next_seq: snapshot.next_seq,
            replayed: events.len(),
            log_next_seq,
        });
    };

    // Step 5: open the log for appending with the replay-derived next_seq.
    let log = EventLog::open_with_seq(&log_path, next_seq)?;

    Ok(RecoveredState {
        snapshot,
        events,
        log,
        lock,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::compaction::{CompactionOutcome, compact};
    use crate::persistence::event::StateEventPayload;
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

    // ─── Fresh directory ───

    #[test]
    fn recover_fresh_directory_yields_empty_state() {
        let dir = tempdir().unwrap();

        let recovered = recover(dir.path(), "main").unwrap();
        assert_eq!(recovered.snapshot.default_branch, "main");
        assert_eq!(recovered.snapshot.next_seq, 0);
        assert!(recovered.events.is_empty());
        assert_eq!(recovered.log.next_seq(), 0);
    }

    #[test]
    fn recover_roundtrip_replays_appended_events() {
        let dir = tempdir().unwrap();

        {
            let mut recovered = recover(dir.path(), "main").unwrap();
            for i in 0..3 {
                recovered.log.append(payload(i)).unwrap();
            }
            // recovered (and its lock) dropped here: simulated shutdown
        }

        let recovered = recover(dir.path(), "main").unwrap();
        assert_eq!(recovered.events.len(), 3);
        for (i, event) in recovered.events.iter().enumerate() {
            assert_eq!(event.seq, i as u64);
        }
        assert_eq!(recovered.log.next_seq(), 3);
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
        assert!(
            matches!(result, Err(RecoverError::LogTruncated { .. })),
            "log shorter than snapshot.log_position must be a hard error"
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
        assert!(
            matches!(result, Err(RecoverError::SequenceMismatch { .. })),
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

    #[test]
    fn recover_after_compaction_uses_new_generation() {
        let dir = tempdir().unwrap();

        // Build some state: events at generation 0.
        let next_seq = {
            let mut recovered = recover(dir.path(), "main").unwrap();
            for i in 0..3 {
                recovered.log.append(payload(i)).unwrap();
            }
            recovered.log.next_seq()
        };

        // Compact: in a real system the snapshot would have the events
        // applied; here it is enough that next_seq is carried.
        let mut snapshot = PersistedRepoSnapshot::new("main");
        snapshot.log_generation = 0;
        snapshot.next_seq = next_seq;
        match compact(dir.path(), &mut snapshot).unwrap() {
            CompactionOutcome::Clean => {}
            CompactionOutcome::CleanupPending(e) => panic!("cleanup failed: {}", e),
        }

        // Recovery must use the new generation and carry next_seq forward.
        let mut recovered = recover(dir.path(), "main").unwrap();
        assert_eq!(recovered.snapshot.log_generation, 1);
        assert_eq!(recovered.snapshot.next_seq, 3);
        assert!(recovered.events.is_empty());
        assert_eq!(recovered.log.next_seq(), 3);

        // Appends continue the global sequence.
        let event = recovered.log.append(payload(99)).unwrap();
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
                    recovered.log.append(p.clone()).unwrap();
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
            prop_assert_eq!(recovered.log.next_seq(), payloads.len() as u64);
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
                    recovered.log.append(p.clone()).unwrap();
                }
            }

            let first = recover(dir.path(), "main").unwrap();
            let first_events = first.events.clone();
            let first_snapshot = first.snapshot.clone();
            let first_next = first.log.next_seq();
            drop(first);

            let second = recover(dir.path(), "main").unwrap();
            prop_assert_eq!(first_events, second.events);
            // When no snapshot file exists, recover synthesizes a fresh
            // in-memory snapshot stamped at recovery time; `snapshot_at` is
            // not durable state, so normalize it before comparing.
            let mut second_snapshot = second.snapshot.clone();
            second_snapshot.snapshot_at = first_snapshot.snapshot_at;
            prop_assert_eq!(first_snapshot, second_snapshot);
            prop_assert_eq!(first_next, second.log.next_seq());
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
                    recovered.log.append(p.clone()).unwrap();
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
            prop_assert_eq!(recovered.log.next_seq(), payloads.len() as u64);
        }
    }
}
