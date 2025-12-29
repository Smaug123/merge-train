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
/// # Crash Safety
///
/// At any crash point during compaction:
/// - The old generation remains complete and usable
/// - The new generation is either complete or can be detected as incomplete
///
/// Recovery will use whichever generation is complete.
pub fn compact(state_dir: &Path, snapshot: &mut PersistedRepoSnapshot) -> Result<()> {
    // 1. Read current generation
    let old_gen = read_generation(state_dir)?;
    let new_gen = old_gen + 1;

    // 2. Update snapshot metadata for new generation
    snapshot.log_generation = new_gen;
    snapshot.log_position = 0; // Start of new event log
    snapshot.touch(); // Update snapshot_at timestamp

    // 3. Write new snapshot (atomic)
    let new_snapshot_path = snapshot_path(state_dir, new_gen);
    save_snapshot_atomic(&new_snapshot_path, snapshot)?;

    // 4. Update generation file (atomic)
    // This is the commit point - once this completes, the new generation is active
    write_generation(state_dir, new_gen)?;

    // 5. Clean up old generation files
    // Only do this after the new generation is fully durable
    delete_old_generation(state_dir, old_gen)?;

    Ok(())
}

/// Cleans up stale generation files that may remain from interrupted compaction.
///
/// This should be called during startup to ensure a clean state.
///
/// # Logic
///
/// 1. Read current generation N
/// 2. Delete any files from generations other than N
/// 3. Delete any orphaned .tmp files
pub fn cleanup_stale_generations(state_dir: &Path) -> Result<()> {
    let current_gen = read_generation(state_dir)?;
    let files = list_generation_files(state_dir)?;

    // Delete files from generations other than current (both old and orphaned future generations)
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
        /// In all cases, cleanup + load should yield valid state.
        #[test]
        fn crash_during_compaction_is_recoverable(
            train_pr in arb_pr_number(),
            initial_gen in 0u64..5,
            crash_point in 0usize..4
        ) {
            let dir = tempdir().unwrap();

            // Set up initial state at generation N
            let mut snapshot = create_test_snapshot();
            snapshot.active_trains.insert(train_pr, TrainRecord::new(train_pr));
            snapshot.log_generation = initial_gen;

            // Write initial generation files
            write_generation(dir.path(), initial_gen).unwrap();
            save_snapshot_atomic(&snapshot_path(dir.path(), initial_gen), &snapshot).unwrap();
            File::create(events_path(dir.path(), initial_gen)).unwrap();

            let new_gen = initial_gen + 1;

            // Simulate compaction with crash at different points
            match crash_point {
                0 => {
                    // Crash before anything - initial state should be recoverable
                }
                1 => {
                    // Crash after writing new snapshot, before updating generation
                    snapshot.log_generation = new_gen;
                    snapshot.log_position = 0;
                    save_snapshot_atomic(&snapshot_path(dir.path(), new_gen), &snapshot).unwrap();
                    // Generation file still points to initial_gen
                }
                2 => {
                    // Crash after updating generation, before deleting old files
                    snapshot.log_generation = new_gen;
                    snapshot.log_position = 0;
                    save_snapshot_atomic(&snapshot_path(dir.path(), new_gen), &snapshot).unwrap();
                    write_generation(dir.path(), new_gen).unwrap();
                    // Old files still exist
                }
                3 => {
                    // Complete compaction (no crash)
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

            // The train data should always be preserved
            prop_assert!(
                loaded.active_trains.contains_key(&train_pr),
                "Train {} should be recoverable after crash at point {}", train_pr, crash_point
            );

            // Verify only current generation files exist
            let files = list_generation_files(dir.path()).unwrap();
            for (file_gen, _) in &files {
                prop_assert_eq!(
                    *file_gen, current_gen,
                    "Only current generation {} files should exist, found generation {}", current_gen, file_gen
                );
            }
        }
    }

    // ─── Crash simulation tests ───

    #[test]
    fn recovery_after_crash_before_generation_update() {
        let dir = tempdir().unwrap();

        // Simulate state after writing new snapshot but before updating generation
        // Generation file says 0, but snapshot.1.json exists

        // Create initial state
        let mut snapshot = create_snapshot_with_train(100);
        save_snapshot_atomic(&snapshot_path(dir.path(), 0), &snapshot).unwrap();
        write_generation(dir.path(), 0).unwrap();

        // Create event log with some events
        let events = events_path(dir.path(), 0);
        let mut f = File::create(&events).unwrap();
        writeln!(f, r#"{{"seq":0,"ts":"2024-01-01T00:00:00Z","type":"train_started","root_pr":101,"current_pr":101}}"#).unwrap();

        // Simulate crash after writing snapshot.1 but before updating generation
        snapshot
            .active_trains
            .insert(PrNumber(200), TrainRecord::new(PrNumber(200)));
        save_snapshot_atomic(&snapshot_path(dir.path(), 1), &snapshot).unwrap();
        // Generation still says 0!

        // Recovery should use generation 0 (ignoring orphaned snapshot.1)
        let current_gen = read_generation(dir.path()).unwrap();
        assert_eq!(current_gen, 0);

        // The old snapshot should be the one we load
        let loaded =
            crate::persistence::snapshot::load_snapshot(&snapshot_path(dir.path(), 0)).unwrap();
        assert!(loaded.active_trains.contains_key(&PrNumber(100)));
        // The train added to snapshot.1 should NOT be visible
        assert!(!loaded.active_trains.contains_key(&PrNumber(200)));

        // Cleanup removes the orphaned snapshot.1
        assert!(
            snapshot_path(dir.path(), 1).exists(),
            "Orphaned snapshot.1 should exist before cleanup"
        );
        cleanup_stale_generations(dir.path()).unwrap();

        // Generation 0 files still exist
        assert!(snapshot_path(dir.path(), 0).exists());
        assert!(events_path(dir.path(), 0).exists());

        // Orphaned snapshot.1 is removed
        assert!(
            !snapshot_path(dir.path(), 1).exists(),
            "Orphaned snapshot.1 should be removed by cleanup"
        );
    }

    #[test]
    fn cleanup_removes_orphaned_future_generations() {
        // This test specifically verifies that cleanup handles generation > current,
        // which can happen if a crash occurs after writing a new snapshot but before
        // updating the generation file.
        let dir = tempdir().unwrap();

        // Set up current generation as 2
        write_generation(dir.path(), 2).unwrap();
        File::create(snapshot_path(dir.path(), 2)).unwrap();
        File::create(events_path(dir.path(), 2)).unwrap();

        // Create orphaned files from both past AND future generations
        // Past: generation 0 and 1 (didn't get cleaned up from prior compaction)
        File::create(snapshot_path(dir.path(), 0)).unwrap();
        File::create(events_path(dir.path(), 0)).unwrap();
        File::create(snapshot_path(dir.path(), 1)).unwrap();
        File::create(events_path(dir.path(), 1)).unwrap();

        // Future: generation 3 (crash during compaction before gen file update)
        File::create(snapshot_path(dir.path(), 3)).unwrap();
        // Note: events.3.log might not exist if crash was early in compaction

        // Verify all files exist before cleanup
        assert!(snapshot_path(dir.path(), 0).exists());
        assert!(snapshot_path(dir.path(), 1).exists());
        assert!(snapshot_path(dir.path(), 2).exists());
        assert!(snapshot_path(dir.path(), 3).exists());

        // Run cleanup
        cleanup_stale_generations(dir.path()).unwrap();

        // Only current generation (2) should remain
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
            snapshot_path(dir.path(), 2).exists(),
            "Current generation 2 should remain"
        );
        assert!(
            events_path(dir.path(), 2).exists(),
            "Current generation 2 events should remain"
        );
        assert!(
            !snapshot_path(dir.path(), 3).exists(),
            "Orphaned future generation 3 should be removed"
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
}
