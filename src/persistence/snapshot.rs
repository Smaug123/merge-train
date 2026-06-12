//! Snapshot persistence for the merge train bot.
//!
//! Snapshots capture the complete state at a point in time, enabling fast
//! recovery without replaying the entire event log.
//!
//! # File Format
//!
//! Snapshots are stored as `snapshot.<gen>.json` where `<gen>` is the
//! generation number. The generation-based naming enables crash-safe
//! compaction (see `compaction.rs`).
//!
//! # Atomic Writes
//!
//! Snapshots are written atomically using a write-to-temp-then-rename pattern:
//! 1. Write to `snapshot.<gen>.json.tmp`
//! 2. fsync the file
//! 3. Rename to `snapshot.<gen>.json`
//! 4. fsync the directory
//!
//! This ensures that readers always see either the old or new snapshot,
//! never a partial write.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::fsync::{fsync_dir, fsync_file};
use crate::types::{CachedPr, PrNumber, TrainRecord};

/// Current schema version. Increment when making breaking changes.
pub const SCHEMA_VERSION: u32 = 1;

/// Errors that can occur during snapshot operations.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// IO error during file operations.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Schema version mismatch.
    #[error("schema version mismatch: expected {expected}, got {got}")]
    SchemaMismatch { expected: u32, got: u32 },
}

/// Result type for snapshot operations.
pub type Result<T> = std::result::Result<T, SnapshotError>;

/// Persisted state snapshot.
///
/// This is the JSON structure stored at `<state_dir>/<owner>/<repo>/snapshot.<gen>.json`.
/// The `<gen>` suffix is the current generation number (see compaction).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedRepoSnapshot {
    /// Schema version for forward-compatible migrations.
    pub schema_version: u32,

    /// When this snapshot was last updated (ISO 8601).
    pub snapshot_at: DateTime<Utc>,

    /// The generation number this snapshot belongs to (matches filename suffix).
    pub log_generation: u64,

    /// Byte offset in `events.<log_generation>.log` at which this snapshot was taken.
    /// On replay, seek to this offset and read forward.
    pub log_position: u64,

    /// Next sequence number to assign (globally monotonic).
    /// This value is preserved across generations during compaction.
    /// Events written to any generation's log file use this as their
    /// starting sequence number, ensuring global uniqueness.
    pub next_seq: u64,

    /// Cached default branch name.
    pub default_branch: String,

    /// Cached PR info, keyed by PR number.
    pub prs: HashMap<PrNumber, CachedPr>,

    /// Active trains, keyed by original root PR number.
    pub active_trains: HashMap<PrNumber, TrainRecord>,

    /// Seen dedupe keys with timestamps for TTL-based pruning.
    /// Key format: "event_type:pr:id" or "event_type:pr:action:sha"
    pub seen_dedupe_keys: HashMap<String, DateTime<Utc>>,
}

impl PersistedRepoSnapshot {
    /// Creates a new empty snapshot.
    pub fn new(default_branch: impl Into<String>) -> Self {
        PersistedRepoSnapshot {
            schema_version: SCHEMA_VERSION,
            snapshot_at: Utc::now(),
            log_generation: 0,
            log_position: 0,
            next_seq: 0,
            default_branch: default_branch.into(),
            prs: HashMap::new(),
            active_trains: HashMap::new(),
            seen_dedupe_keys: HashMap::new(),
        }
    }

    /// Checks if the snapshot is stale (older than the given threshold).
    pub fn is_stale(&self, threshold: Duration) -> bool {
        Utc::now() - self.snapshot_at > threshold
    }

    /// Updates the `snapshot_at` timestamp to now.
    pub fn touch(&mut self) {
        self.snapshot_at = Utc::now();
    }
}

/// Saves a snapshot atomically to disk.
///
/// Uses the write-to-temp-then-rename pattern for crash safety:
/// 1. Write to `<path>.tmp`
/// 2. fsync the temp file
/// 3. Rename to `<path>`
/// 4. fsync the parent directory
///
/// # Errors
///
/// Returns an error if any IO operation fails.
pub fn save_snapshot_atomic(path: &Path, snapshot: &PersistedRepoSnapshot) -> Result<()> {
    use std::fs::OpenOptions;
    use std::io::Write;

    // Ensure parent directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write to temp file
    let tmp_path = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(snapshot)?;

    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&bytes)?;
        fsync_file(&file)?;
    }

    // Atomic rename
    std::fs::rename(&tmp_path, path)?;

    // fsync directory to ensure rename is durable
    if let Some(parent) = path.parent() {
        fsync_dir(parent)?;
    }

    Ok(())
}

/// Loads a snapshot from disk.
///
/// # Errors
///
/// Returns an error if:
/// - The file doesn't exist or can't be read
/// - The JSON is malformed
/// - The schema version is incompatible
pub fn load_snapshot(path: &Path) -> Result<PersistedRepoSnapshot> {
    let bytes = std::fs::read(path)?;
    let snapshot: PersistedRepoSnapshot = serde_json::from_slice(&bytes)?;

    // Check schema version
    if snapshot.schema_version != SCHEMA_VERSION {
        return Err(SnapshotError::SchemaMismatch {
            expected: SCHEMA_VERSION,
            got: snapshot.schema_version,
        });
    }

    Ok(snapshot)
}

/// Attempts to load a snapshot, returning None if the file doesn't exist.
///
/// Other errors (malformed JSON, schema mismatch) are propagated.
pub fn try_load_snapshot(path: &Path) -> Result<Option<PersistedRepoSnapshot>> {
    match load_snapshot(path) {
        Ok(snapshot) => Ok(Some(snapshot)),
        Err(SnapshotError::Io(e)) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CommentId, MergeStateStatus, PrState, Sha};
    use proptest::prelude::*;
    use tempfile::tempdir;

    // ─── Arbitrary implementations ───

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        (1u64..100000).prop_map(PrNumber)
    }

    fn arb_sha() -> impl Strategy<Value = Sha> {
        "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
    }

    fn arb_datetime() -> impl Strategy<Value = DateTime<Utc>> {
        // Generate timestamps in a reasonable range (year 2000-2100)
        (946684800i64..4102444800i64).prop_map(|secs| DateTime::from_timestamp(secs, 0).unwrap())
    }

    fn arb_pr_state() -> impl Strategy<Value = PrState> {
        prop_oneof![
            Just(PrState::Open),
            Just(PrState::Closed),
            arb_sha().prop_map(|sha| PrState::Merged {
                merge_commit_sha: sha
            }),
        ]
    }

    /// Generate valid (merge_state_status, is_draft) pairs.
    fn arb_merge_state_and_draft() -> impl Strategy<Value = (MergeStateStatus, bool)> {
        prop_oneof![
            Just(MergeStateStatus::Clean),
            Just(MergeStateStatus::Unstable),
            Just(MergeStateStatus::Blocked),
            Just(MergeStateStatus::Behind),
            Just(MergeStateStatus::Dirty),
            Just(MergeStateStatus::Unknown),
            Just(MergeStateStatus::Draft),
            Just(MergeStateStatus::HasHooks),
        ]
        .prop_flat_map(|status| {
            let is_draft_strategy: proptest::strategy::BoxedStrategy<bool> = match status {
                MergeStateStatus::Draft => Just(true).boxed(),
                MergeStateStatus::Clean => Just(false).boxed(),
                _ => any::<bool>().boxed(),
            };
            is_draft_strategy.prop_map(move |is_draft| (status, is_draft))
        })
    }

    fn arb_branch_name() -> impl Strategy<Value = String> {
        "[a-zA-Z][a-zA-Z0-9_-]{0,20}".prop_map(String::from)
    }

    fn arb_comment_id() -> impl Strategy<Value = CommentId> {
        any::<u64>().prop_map(CommentId)
    }

    fn arb_cached_pr() -> impl Strategy<Value = CachedPr> {
        (
            arb_pr_number(),
            arb_sha(),
            arb_branch_name(),
            arb_branch_name(),
            prop::option::of(arb_pr_number()),
            prop::option::of(arb_comment_id()),
            arb_pr_state(),
            arb_merge_state_and_draft(),
            prop::option::of(arb_datetime()),
            prop::option::of(arb_sha()),
        )
            .prop_map(
                |(
                    number,
                    head_sha,
                    head_ref,
                    base_ref,
                    predecessor,
                    predecessor_comment_id,
                    state,
                    (merge_state_status, is_draft),
                    closed_at,
                    predecessor_squash_reconciled,
                )| {
                    let mut pr = CachedPr::new(
                        number,
                        head_sha,
                        head_ref,
                        base_ref,
                        predecessor,
                        state,
                        merge_state_status,
                        is_draft,
                    );
                    pr.predecessor_comment_id = predecessor_comment_id;
                    pr.closed_at = closed_at;
                    pr.predecessor_squash_reconciled = predecessor_squash_reconciled;
                    pr
                },
            )
    }

    fn arb_train_record() -> impl Strategy<Value = TrainRecord> {
        arb_pr_number().prop_map(TrainRecord::new)
    }

    fn arb_dedupe_key() -> impl Strategy<Value = String> {
        "[a-z_]+:[0-9]+:[a-z_]+".prop_map(String::from)
    }

    fn arb_snapshot() -> impl Strategy<Value = PersistedRepoSnapshot> {
        (
            arb_datetime(),
            0u64..1000,
            0u64..1000000,
            0u64..1000000,
            arb_branch_name(),
            prop::collection::hash_map(arb_pr_number(), arb_cached_pr(), 0..10),
            prop::collection::hash_map(arb_pr_number(), arb_train_record(), 0..3),
            prop::collection::hash_map(arb_dedupe_key(), arb_datetime(), 0..10),
        )
            .prop_map(
                |(
                    snapshot_at,
                    log_generation,
                    log_position,
                    next_seq,
                    default_branch,
                    prs,
                    active_trains,
                    seen_dedupe_keys,
                )| {
                    PersistedRepoSnapshot {
                        schema_version: SCHEMA_VERSION,
                        snapshot_at,
                        log_generation,
                        log_position,
                        next_seq,
                        default_branch,
                        prs,
                        active_trains,
                        seen_dedupe_keys,
                    }
                },
            )
    }

    // ─── Property tests ───

    proptest! {
        /// Snapshot serialization roundtrip preserves all data.
        #[test]
        fn snapshot_serde_roundtrip(snapshot in arb_snapshot()) {
            let json = serde_json::to_string(&snapshot).unwrap();
            let parsed: PersistedRepoSnapshot = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(snapshot, parsed);
        }

        /// Atomic save and load roundtrip preserves all data.
        #[test]
        fn atomic_save_load_roundtrip(snapshot in arb_snapshot()) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("snapshot.0.json");

            save_snapshot_atomic(&path, &snapshot).unwrap();
            let loaded = load_snapshot(&path).unwrap();

            prop_assert_eq!(snapshot, loaded);
        }

        /// Temp file is cleaned up after successful save.
        #[test]
        fn temp_file_cleaned_up(snapshot in arb_snapshot()) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("snapshot.0.json");
            let tmp_path = path.with_extension("json.tmp");

            save_snapshot_atomic(&path, &snapshot).unwrap();

            prop_assert!(path.exists(), "Snapshot file should exist");
            prop_assert!(!tmp_path.exists(), "Temp file should be cleaned up");
        }
    }

    // ─── Unit tests ───

    #[test]
    fn new_snapshot_has_correct_defaults() {
        let snapshot = PersistedRepoSnapshot::new("main");

        assert_eq!(snapshot.schema_version, SCHEMA_VERSION);
        assert_eq!(snapshot.log_generation, 0);
        assert_eq!(snapshot.log_position, 0);
        assert_eq!(snapshot.next_seq, 0);
        assert_eq!(snapshot.default_branch, "main");
        assert!(snapshot.prs.is_empty());
        assert!(snapshot.active_trains.is_empty());
        assert!(snapshot.seen_dedupe_keys.is_empty());
    }

    #[test]
    fn is_stale_works() {
        let mut snapshot = PersistedRepoSnapshot::new("main");

        // Fresh snapshot is not stale
        assert!(!snapshot.is_stale(Duration::hours(1)));

        // Snapshot from 2 hours ago is stale with 1 hour threshold
        snapshot.snapshot_at = Utc::now() - Duration::hours(2);
        assert!(snapshot.is_stale(Duration::hours(1)));

        // But not with 3 hour threshold
        assert!(!snapshot.is_stale(Duration::hours(3)));
    }

    #[test]
    fn load_nonexistent_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        let result = load_snapshot(&path);
        assert!(matches!(result, Err(SnapshotError::Io(_))));
    }

    #[test]
    fn try_load_nonexistent_returns_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        let result = try_load_snapshot(&path).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn load_invalid_json_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("invalid.json");
        std::fs::write(&path, "not valid json").unwrap();

        let result = load_snapshot(&path);
        assert!(matches!(result, Err(SnapshotError::Json(_))));
    }

    #[test]
    fn load_wrong_schema_version_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wrong_version.json");

        let mut snapshot = PersistedRepoSnapshot::new("main");
        snapshot.schema_version = SCHEMA_VERSION + 1;

        // Write directly to avoid the schema check on save
        let json = serde_json::to_string(&snapshot).unwrap();
        std::fs::write(&path, json).unwrap();

        let result = load_snapshot(&path);
        assert!(matches!(
            result,
            Err(SnapshotError::SchemaMismatch {
                expected: 1,
                got: 2
            })
        ));
    }

    #[test]
    fn save_creates_parent_directories() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nested/dir/snapshot.0.json");

        let snapshot = PersistedRepoSnapshot::new("main");
        save_snapshot_atomic(&path, &snapshot).unwrap();

        assert!(path.exists());
    }
}
