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
//! # Recovery
//!
//! 1. Read `generation` file to find current generation N
//! 2. Load `snapshot.<N>.json` (or `snapshot.<N-1>.json` if crash during compaction)
//! 3. Replay `events.<N>.log` from snapshot's `log_position`
//! 4. Clean up stale files from older generations

use std::io;
use std::path::Path;

use thiserror::Error;

use super::generation::{
    delete_old_generation, events_path, list_generation_files, read_generation, snapshot_path,
    write_generation,
};
use super::snapshot::{PersistedRepoSnapshot, save_snapshot_atomic};

/// Errors that can occur during compaction.
#[derive(Debug, Error)]
pub enum CompactionError {
    /// IO error during file operations.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Generation file error.
    #[error("generation error: {0}")]
    Generation(#[from] super::generation::GenerationError),

    /// Snapshot error.
    #[error("snapshot error: {0}")]
    Snapshot(#[from] super::snapshot::SnapshotError),
}

/// Result type for compaction operations.
pub type Result<T> = std::result::Result<T, CompactionError>;

/// Performs generation-based compaction.
///
/// This creates a new generation with a fresh snapshot and empty event log,
/// then cleans up old generation files.
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
/// If any I/O operation fails, the in-memory `snapshot` is left unchanged.
/// The caller can safely retry or continue with the original generation.
pub fn compact(state_dir: &Path, snapshot: &mut PersistedRepoSnapshot) -> Result<()> {
    // 1. Read current generation
    let old_gen = read_generation(state_dir)?;
    let new_gen = old_gen + 1;

    // 2. Stage changes in a clone (don't mutate original until disk writes succeed)
    let mut staged = snapshot.clone();
    staged.log_generation = new_gen;
    staged.log_position = 0; // Start of new event log
    staged.touch(); // Update snapshot_at timestamp

    // 3. Write new snapshot (atomic)
    let new_snapshot_path = snapshot_path(state_dir, new_gen);
    save_snapshot_atomic(&new_snapshot_path, &staged)?;

    // 4. Update generation file (atomic)
    // This is the commit point - once this completes, the new generation is active
    write_generation(state_dir, new_gen)?;

    // 5. Clean up old generation files
    // Only do this after the new generation is fully durable
    delete_old_generation(state_dir, old_gen)?;

    // 6. Commit to in-memory state only after all disk writes succeed
    *snapshot = staged;

    Ok(())
}

/// Cleans up stale generation files that may remain from interrupted compaction.
///
/// This should be called during startup to ensure a clean state.
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
/// # Recovery from stale/missing generation file
///
/// If the generation file is missing or points to a lower generation than the
/// highest existing snapshot, this function uses the snapshot's generation and
/// restores the generation file. This prevents data loss when the generation
/// file is corrupted or lost.
pub fn cleanup_stale_generations(state_dir: &Path) -> Result<()> {
    let file_gen = read_generation(state_dir)?;
    let files = list_generation_files(state_dir)?;

    // Find the highest generation among existing snapshot files.
    // Snapshots are written atomically, so if a snapshot file exists, it's complete.
    let max_snapshot_gen = files
        .iter()
        .filter(|(_, file_type)| *file_type == "snapshot")
        .map(|(generation, _)| *generation)
        .max();

    // Determine the actual current generation:
    // - If snapshots exist, use the highest snapshot generation. This handles:
    //   - Stale generation file (points to older gen than exists)
    //   - Missing generation file (returns 0, but snapshots exist)
    //   - Crash after snapshot write but before gen file update
    // - If no snapshots exist, trust the generation file (fresh state)
    let current_gen = match max_snapshot_gen {
        Some(max_gen) => {
            // If there's a mismatch, update the generation file to be consistent
            if max_gen != file_gen {
                write_generation(state_dir, max_gen)?;
            }
            max_gen
        }
        None => file_gen, // No snapshots - trust the file (fresh state or generation 0)
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
    use crate::types::{CachedPr, MergeStateStatus, PrNumber, PrState, Sha, TrainRecord};
    use proptest::prelude::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    // ─── Test helpers ───

    fn create_test_snapshot() -> PersistedRepoSnapshot {
        PersistedRepoSnapshot::new("main")
    }

    fn create_snapshot_with_train(pr_num: u64) -> PersistedRepoSnapshot {
        let mut snapshot = create_test_snapshot();
        let train = TrainRecord::new(PrNumber(pr_num));
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

    // ─── Unit tests ───

    #[test]
    fn compact_creates_new_generation() {
        let dir = tempdir().unwrap();
        let mut snapshot = create_test_snapshot();

        // Initial state: no generation file
        assert_eq!(read_generation(dir.path()).unwrap(), 0);

        // First compaction creates generation 1
        compact(dir.path(), &mut snapshot).unwrap();

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
        compact(dir.path(), &mut snapshot).unwrap();
        assert_eq!(read_generation(dir.path()).unwrap(), 1);

        // Second compaction
        compact(dir.path(), &mut snapshot).unwrap();
        assert_eq!(read_generation(dir.path()).unwrap(), 2);
        assert!(snapshot_path(dir.path(), 2).exists());
        assert_eq!(snapshot.log_generation, 2);
    }

    #[test]
    fn compact_deletes_old_generation() {
        let dir = tempdir().unwrap();
        let mut snapshot = create_test_snapshot();

        // First compaction
        compact(dir.path(), &mut snapshot).unwrap();
        let old_snapshot = snapshot_path(dir.path(), 1);
        assert!(old_snapshot.exists());

        // Second compaction
        compact(dir.path(), &mut snapshot).unwrap();

        // Old generation should be deleted
        assert!(!old_snapshot.exists());
        assert!(snapshot_path(dir.path(), 2).exists());
    }

    #[test]
    fn compact_preserves_active_trains() {
        let dir = tempdir().unwrap();
        let mut snapshot = create_snapshot_with_train(123);

        compact(dir.path(), &mut snapshot).unwrap();

        // Reload and verify
        let loaded =
            crate::persistence::snapshot::load_snapshot(&snapshot_path(dir.path(), 1)).unwrap();
        assert!(loaded.active_trains.contains_key(&PrNumber(123)));
    }

    #[test]
    fn compact_preserves_prs() {
        let dir = tempdir().unwrap();
        let mut snapshot = create_snapshot_with_pr(456);

        compact(dir.path(), &mut snapshot).unwrap();

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
            File::create(snapshot_path(dir.path(), generation)).unwrap();
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

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        (1u64..10000).prop_map(PrNumber)
    }

    fn arb_sha() -> impl Strategy<Value = Sha> {
        "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
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
                snapshot.active_trains.insert(*pr, TrainRecord::new(*pr));
            }

            compact(dir.path(), &mut snapshot).unwrap();

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

            compact(dir.path(), &mut snapshot).unwrap();

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
            snapshot.active_trains.insert(train_pr, TrainRecord::new(train_pr));

            for _ in 0..num_compactions {
                compact(dir.path(), &mut snapshot).unwrap();
            }

            let final_gen = read_generation(dir.path()).unwrap();
            prop_assert_eq!(final_gen, num_compactions as u64);

            let loaded = crate::persistence::snapshot::load_snapshot(
                &snapshot_path(dir.path(), final_gen)
            ).unwrap();

            prop_assert!(loaded.active_trains.contains_key(&train_pr));
        }

        /// Crash at any point during compaction leaves recoverable state.
        ///
        /// Simulates crashes at different points in the compaction sequence:
        /// 0 = crash before anything (no-op)
        /// 1 = crash after writing new snapshot, before updating generation
        /// 2 = crash after updating generation, before deleting old files
        /// 3 = complete (no crash)
        ///
        /// Validates:
        /// - Snapshot data is preserved
        /// - `log_position` is correct for the recovered generation
        /// - Event log replay works and returns correct `next_seq`
        /// - File cleanup removes stale generations
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
            snapshot.active_trains.insert(train_pr, TrainRecord::new(train_pr));
            snapshot.log_generation = initial_gen;
            snapshot.log_position = 0;
            snapshot.next_seq = 0;

            // Write initial generation files
            write_generation(dir.path(), initial_gen).unwrap();
            save_snapshot_atomic(&snapshot_path(dir.path(), initial_gen), &snapshot).unwrap();

            // Write events to the event log (simulating activity before compaction)
            let events_file = events_path(dir.path(), initial_gen);
            {
                let mut log = EventLog::open(&events_file).unwrap();
                for i in 0..num_events {
                    log.append(StateEventPayload::TrainStarted {
                        root_pr: PrNumber(1000 + i as u64),
                        current_pr: PrNumber(1000 + i as u64),
                    }).unwrap();
                }
            }

            let new_gen = initial_gen + 1;

            // Simulate compaction with crash at different points
            match crash_point {
                0 => {
                    // Crash before anything - initial state should be recoverable
                    // snapshot.next_seq stays 0 (events written but not yet reflected in-memory)
                }
                1 => {
                    // Crash after writing new snapshot, before updating generation
                    // In real usage, next_seq would be updated as events are applied
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
                    // In real usage, next_seq would already be updated as events are applied
                    snapshot.next_seq = num_events as u64;
                    compact(dir.path(), &mut snapshot).unwrap();
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

            // Create snapshot and event files at various generations
            for g in &snapshot_gens {
                File::create(snapshot_path(dir.path(), *g)).unwrap();
                File::create(events_path(dir.path(), *g)).unwrap();
            }

            // Determine expected: highest snapshot generation
            let expected_gen = *snapshot_gens.iter().max().unwrap();

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
        /// cleanup should detect this (e.g., by scanning existing files) and NOT delete
        /// the newer data. Currently this bug exists and this test fails.
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
            File::create(snapshot_path(dir.path(), current_gen)).unwrap();
            File::create(events_path(dir.path(), current_gen)).unwrap();

            for g in &other_gens {
                let _ = File::create(snapshot_path(dir.path(), *g));
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
            snapshot.active_trains.insert(train_pr, TrainRecord::new(train_pr));
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
            existing_snapshot.active_trains.insert(PrNumber(999), TrainRecord::new(PrNumber(999)));
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
            compact(dir.path(), &mut snapshot_to_compact).unwrap();

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
        // This snapshot has MORE data (train 200 in addition to train 100)
        snapshot
            .active_trains
            .insert(PrNumber(200), TrainRecord::new(PrNumber(200)));
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

        // Generation file says 2 (simulating stale state)
        write_generation(dir.path(), 2).unwrap();
        File::create(snapshot_path(dir.path(), 2)).unwrap();
        File::create(events_path(dir.path(), 2)).unwrap();

        // Old generations (didn't get cleaned up from prior compaction)
        File::create(snapshot_path(dir.path(), 0)).unwrap();
        File::create(events_path(dir.path(), 0)).unwrap();
        File::create(snapshot_path(dir.path(), 1)).unwrap();
        File::create(events_path(dir.path(), 1)).unwrap();

        // Newer generation 3 (crash during compaction before gen file update)
        // This is the HIGHEST snapshot, so it should be preserved
        File::create(snapshot_path(dir.path(), 3)).unwrap();
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
        //
        // Currently this test FAILS because read_generation returns 0 for missing file,
        // and cleanup deletes all files with generation != 0.
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
    fn cleanup_with_corrupt_generation_file_returns_error() {
        // When the generation file is corrupt, cleanup_stale_generations returns an error.
        // This is safer than the missing file case - at least data isn't silently deleted.
        let dir = tempdir().unwrap();

        // Create valid snapshot at generation 2
        let mut snapshot = create_snapshot_with_train(888);
        snapshot.log_generation = 2;
        save_snapshot_atomic(&snapshot_path(dir.path(), 2), &snapshot).unwrap();
        File::create(events_path(dir.path(), 2)).unwrap();

        // Write corrupt generation file
        std::fs::write(dir.path().join("generation"), "garbage_data\n").unwrap();

        // cleanup_stale_generations fails with an error
        let result = cleanup_stale_generations(dir.path());
        assert!(
            result.is_err(),
            "Should fail when generation file is corrupt"
        );

        // Importantly, the snapshots are preserved (not deleted)
        assert!(
            snapshot_path(dir.path(), 2).exists(),
            "Snapshot should be preserved when cleanup fails due to corrupt generation"
        );
        assert!(
            events_path(dir.path(), 2).exists(),
            "Events should be preserved when cleanup fails due to corrupt generation"
        );

        // Data can be recovered manually by fixing the generation file
        write_generation(dir.path(), 2).unwrap();
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
        compact(dir.path(), &mut snapshot).unwrap();

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
}
