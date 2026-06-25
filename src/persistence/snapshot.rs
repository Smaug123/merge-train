//! The materialized-state snapshot type.
//!
//! [`PersistedRepoSnapshot`] is the serialized form of a [`crate::state::RepoState`]:
//! the SQLite [`crate::store::Store`] keeps it (as JSON) in its single
//! `repo_state` row, the `/state` endpoint returns it, and the status-comment
//! backup embeds it. Durability is the Store's job; this module is just the
//! shared data type and its serde.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{CachedPr, PrNumber, TrainRecord};

/// Current schema version. Increment when making breaking changes.
///
/// Bumped to 2 by the machine-enforced core: `TrainState::Stopped`/`Aborted`
/// moved from string tags with top-level `ended_at`/`error` to nested struct
/// variants, so v1 `TrainRecord`s no longer deserialize. Backward compatibility
/// is intentionally dropped; the version bump makes a stale snapshot reject with
/// a clear mismatch rather than a cryptic serde error.
pub const SCHEMA_VERSION: u32 = 2;

/// Persisted state snapshot — the serialized `RepoState` the Store caches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedRepoSnapshot {
    /// Schema version for forward-compatible migrations.
    pub schema_version: u32,

    /// When this snapshot was last updated (ISO 8601).
    pub snapshot_at: DateTime<Utc>,

    /// Legacy log generation (always 0 under SQLite; retained for format
    /// stability with the status-comment backup).
    pub log_generation: u64,

    /// Legacy log position (always 0 under SQLite; retained for format
    /// stability).
    pub log_position: u64,

    /// Next sequence number to assign (globally monotonic).
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

    /// Updates the `snapshot_at` timestamp to now.
    pub fn touch(&mut self) {
        self.snapshot_at = Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CommentId, MergeStateStatus, PrState};
    use proptest::prelude::*;

    // ─── Arbitrary implementations ───
    // (arb_sha/arb_datetime come from crate::test_utils; arb_pr_number stays
    // local because it deliberately uses a small range for readable failures.)

    use crate::test_utils::{arb_datetime, arb_sha};

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        (1u64..100000).prop_map(PrNumber)
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
        (arb_pr_number(), arb_datetime()).prop_map(|(pr, t)| TrainRecord::new(pr, t))
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

    proptest! {
        /// Snapshot serialization roundtrip preserves all data — the property the
        /// Store relies on to cache and restore `RepoState`.
        #[test]
        fn snapshot_serde_roundtrip(snapshot in arb_snapshot()) {
            let json = serde_json::to_string(&snapshot).unwrap();
            let parsed: PersistedRepoSnapshot = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(snapshot, parsed);
        }
    }

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
}
