//! Generation file management for crash-safe compaction.
//!
//! The generation file (`generation`) contains a single integer that tracks
//! the current generation number. This enables the generation-based compaction
//! scheme described in DESIGN.md.
//!
//! # Crash Safety
//!
//! The generation file is written atomically using write-to-temp-then-rename.
//! At any crash point, either the old or new generation file is complete.
//!
//! # Recovery
//!
//! On startup:
//! 1. Read the generation file to find current generation N
//! 2. Load `snapshot.<N>.json` (fallback to `snapshot.<N-1>.json` if crash during compaction)
//! 3. Replay `events.<N>.log` from the snapshot's `log_position`
//! 4. Clean up any stale files from older generations

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;

use thiserror::Error;

use super::fsync::{fsync_dir, fsync_file};

/// Errors that can occur during generation file operations.
#[derive(Debug, Error)]
pub enum GenerationError {
    /// IO error during file operations.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Failed to parse generation number.
    #[error("invalid generation number: {0}")]
    InvalidNumber(String),
}

/// Result type for generation file operations.
pub type Result<T> = std::result::Result<T, GenerationError>;

/// Reads the current generation number from the generation file.
///
/// Returns `Ok(0)` if the file doesn't exist (fresh state directory).
pub fn read_generation(state_dir: &Path) -> Result<u64> {
    let path = state_dir.join("generation");

    match File::open(&path) {
        Ok(file) => {
            let reader = BufReader::new(file);
            let first_line = reader.lines().next();

            match first_line {
                Some(Ok(line)) => line
                    .trim()
                    .parse()
                    .map_err(|_| GenerationError::InvalidNumber(line)),
                Some(Err(e)) => Err(e.into()),
                None => Ok(0), // Empty file treated as 0
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(e.into()),
    }
}

/// Writes a new generation number atomically.
///
/// Uses the write-to-temp-then-rename pattern:
/// 1. Write to `generation.tmp`
/// 2. fsync the file
/// 3. Rename to `generation`
/// 4. fsync the directory
pub fn write_generation(state_dir: &Path, generation: u64) -> Result<()> {
    let path = state_dir.join("generation");
    let tmp_path = state_dir.join("generation.tmp");

    // Ensure directory exists
    std::fs::create_dir_all(state_dir)?;

    // Write to temp file
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        writeln!(file, "{}", generation)?;
        fsync_file(&file)?;
    }

    // Atomic rename
    std::fs::rename(&tmp_path, &path)?;

    // fsync directory
    fsync_dir(state_dir)?;

    Ok(())
}

/// Increments the generation number and returns the new value.
///
/// This is an atomic read-modify-write operation.
pub fn increment_generation(state_dir: &Path) -> Result<u64> {
    let current = read_generation(state_dir)?;
    let next = current + 1;
    write_generation(state_dir, next)?;
    Ok(next)
}

/// Returns the path to the snapshot file for a given generation.
pub fn snapshot_path(state_dir: &Path, generation: u64) -> std::path::PathBuf {
    state_dir.join(format!("snapshot.{}.json", generation))
}

/// Returns the path to the event log file for a given generation.
pub fn events_path(state_dir: &Path, generation: u64) -> std::path::PathBuf {
    state_dir.join(format!("events.{}.log", generation))
}

/// Deletes old generation files (snapshot and events log).
///
/// This should only be called after the new generation is fully durable.
pub fn delete_old_generation(state_dir: &Path, generation: u64) -> Result<()> {
    let snapshot = snapshot_path(state_dir, generation);
    let events = events_path(state_dir, generation);

    // Ignore errors if files don't exist
    let _ = std::fs::remove_file(&snapshot);
    let _ = std::fs::remove_file(&events);

    // fsync directory to ensure deletions are durable
    fsync_dir(state_dir)?;

    Ok(())
}

/// Lists all generation files in the state directory.
///
/// Returns a sorted list of (generation, file_type) pairs where file_type
/// is either "snapshot" or "events".
pub fn list_generation_files(state_dir: &Path) -> io::Result<Vec<(u64, &'static str)>> {
    let mut files = Vec::new();

    if !state_dir.exists() {
        return Ok(files);
    }

    for entry in std::fs::read_dir(state_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if let Some(gen_str) = name
            .strip_prefix("snapshot.")
            .and_then(|s| s.strip_suffix(".json"))
            && let Ok(generation) = gen_str.parse()
        {
            files.push((generation, "snapshot"));
        } else if let Some(gen_str) = name
            .strip_prefix("events.")
            .and_then(|s| s.strip_suffix(".log"))
            && let Ok(generation) = gen_str.parse()
        {
            files.push((generation, "events"));
        }
    }

    files.sort_by_key(|(generation, _)| *generation);
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tempfile::tempdir;

    // ─── Property tests ───

    proptest! {
        /// Write and read roundtrip preserves generation number.
        #[test]
        fn write_read_roundtrip(generation in 0u64..1000000) {
            let dir = tempdir().unwrap();
            write_generation(dir.path(), generation).unwrap();
            let read_gen = read_generation(dir.path()).unwrap();
            prop_assert_eq!(generation, read_gen);
        }

        /// Increment increases generation by 1.
        #[test]
        fn increment_adds_one(initial in 0u64..1000000) {
            let dir = tempdir().unwrap();
            write_generation(dir.path(), initial).unwrap();
            let next = increment_generation(dir.path()).unwrap();
            prop_assert_eq!(next, initial + 1);

            // Verify persistence
            let read = read_generation(dir.path()).unwrap();
            prop_assert_eq!(read, next);
        }

        /// Generation file is atomic - temp file shouldn't remain.
        #[test]
        fn no_temp_file_remains(generation in 0u64..1000) {
            let dir = tempdir().unwrap();
            let tmp_path = dir.path().join("generation.tmp");

            write_generation(dir.path(), generation).unwrap();

            prop_assert!(!tmp_path.exists(), "Temp file should not exist after write");
        }
    }

    // ─── Unit tests ───

    #[test]
    fn read_nonexistent_returns_zero() {
        let dir = tempdir().unwrap();
        let generation = read_generation(dir.path()).unwrap();
        assert_eq!(generation, 0);
    }

    #[test]
    fn read_empty_file_returns_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("generation");
        File::create(&path).unwrap();

        let generation = read_generation(dir.path()).unwrap();
        assert_eq!(generation, 0);
    }

    #[test]
    fn read_invalid_content_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("generation");
        std::fs::write(&path, "not a number").unwrap();

        let result = read_generation(dir.path());
        assert!(matches!(result, Err(GenerationError::InvalidNumber(_))));
    }

    #[test]
    fn snapshot_path_format() {
        let dir = Path::new("/tmp/state");
        assert_eq!(
            snapshot_path(dir, 0),
            Path::new("/tmp/state/snapshot.0.json")
        );
        assert_eq!(
            snapshot_path(dir, 42),
            Path::new("/tmp/state/snapshot.42.json")
        );
    }

    #[test]
    fn events_path_format() {
        let dir = Path::new("/tmp/state");
        assert_eq!(events_path(dir, 0), Path::new("/tmp/state/events.0.log"));
        assert_eq!(events_path(dir, 42), Path::new("/tmp/state/events.42.log"));
    }

    #[test]
    fn delete_old_generation_handles_missing_files() {
        let dir = tempdir().unwrap();
        // Should not error even if files don't exist
        delete_old_generation(dir.path(), 0).unwrap();
    }

    #[test]
    fn delete_old_generation_removes_files() {
        let dir = tempdir().unwrap();
        let snapshot = snapshot_path(dir.path(), 0);
        let events = events_path(dir.path(), 0);

        // Create files
        File::create(&snapshot).unwrap();
        File::create(&events).unwrap();

        assert!(snapshot.exists());
        assert!(events.exists());

        // Delete
        delete_old_generation(dir.path(), 0).unwrap();

        assert!(!snapshot.exists());
        assert!(!events.exists());
    }

    #[test]
    fn list_generation_files_works() {
        let dir = tempdir().unwrap();

        // Create some generation files
        File::create(snapshot_path(dir.path(), 0)).unwrap();
        File::create(events_path(dir.path(), 0)).unwrap();
        File::create(snapshot_path(dir.path(), 1)).unwrap();
        File::create(events_path(dir.path(), 1)).unwrap();
        File::create(snapshot_path(dir.path(), 5)).unwrap();

        // Create some unrelated files
        File::create(dir.path().join("generation")).unwrap();
        File::create(dir.path().join("other.txt")).unwrap();

        let files = list_generation_files(dir.path()).unwrap();

        assert_eq!(files.len(), 5);
        assert!(files.contains(&(0, "snapshot")));
        assert!(files.contains(&(0, "events")));
        assert!(files.contains(&(1, "snapshot")));
        assert!(files.contains(&(1, "events")));
        assert!(files.contains(&(5, "snapshot")));
    }

    #[test]
    fn list_generation_files_empty_dir() {
        let dir = tempdir().unwrap();
        let files = list_generation_files(dir.path()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn list_generation_files_nonexistent_dir() {
        let dir = tempdir().unwrap();
        let nonexistent = dir.path().join("nonexistent");
        let files = list_generation_files(&nonexistent).unwrap();
        assert!(files.is_empty());
    }

    // ─── Missing/corrupt generation file tests ───
    //
    // These tests document behavior when the generation file is missing or corrupt
    // but snapshot files exist. This is an important recovery scenario.

    #[test]
    fn missing_generation_file_with_existing_snapshots_returns_zero() {
        // Scenario: generation file deleted/missing, but snapshots exist
        // Risk: read_generation returns 0, which may not match existing snapshots
        let dir = tempdir().unwrap();

        // Create snapshots at generation 3 (simulating prior successful compactions)
        File::create(snapshot_path(dir.path(), 3)).unwrap();
        File::create(events_path(dir.path(), 3)).unwrap();

        // No generation file exists
        assert!(!dir.path().join("generation").exists());

        // read_generation returns 0 (default for missing file)
        let generation = read_generation(dir.path()).unwrap();
        assert_eq!(generation, 0);

        // Document the risk: list_generation_files shows snapshots exist at gen 3
        let files = list_generation_files(dir.path()).unwrap();
        assert!(files.iter().any(|(g, _)| *g == 3));

        // This mismatch (gen=0, but files at gen=3) could cause data loss
        // if cleanup_stale_generations is called - it would delete gen 3 files
    }

    #[test]
    fn corrupt_generation_file_with_existing_snapshots_returns_error() {
        // Scenario: generation file contains garbage, but valid snapshots exist
        // Behavior: Returns error, preventing any operations until manually fixed
        let dir = tempdir().unwrap();

        // Create valid snapshots at generation 2
        File::create(snapshot_path(dir.path(), 2)).unwrap();
        File::create(events_path(dir.path(), 2)).unwrap();

        // Write corrupt generation file
        std::fs::write(dir.path().join("generation"), "not_a_number\n").unwrap();

        // read_generation returns an error
        let result = read_generation(dir.path());
        assert!(
            matches!(result, Err(GenerationError::InvalidNumber(_))),
            "Expected InvalidNumber error for corrupt generation file"
        );

        // The snapshots still exist and could be recovered manually
        let files = list_generation_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|(g, _)| *g == 2));
    }

    #[test]
    fn list_generation_files_can_find_max_generation_when_file_missing() {
        // This test demonstrates how to recover from a missing generation file:
        // scan for highest generation number among existing snapshots
        let dir = tempdir().unwrap();

        // Create snapshots at multiple generations
        File::create(snapshot_path(dir.path(), 1)).unwrap();
        File::create(snapshot_path(dir.path(), 3)).unwrap();
        File::create(snapshot_path(dir.path(), 5)).unwrap();
        File::create(events_path(dir.path(), 5)).unwrap();

        // No generation file
        assert!(!dir.path().join("generation").exists());

        // Recovery strategy: find max generation from existing files
        let files = list_generation_files(dir.path()).unwrap();
        let max_gen = files.iter().map(|(g, _)| *g).max().unwrap_or(0);

        assert_eq!(max_gen, 5);

        // Could then write_generation(dir.path(), max_gen) to restore consistency
    }
}
