//! State pruning logic for the merge train bot.
//!
//! Pruning removes stale PR data and dedupe keys from snapshots to prevent
//! unbounded growth while preserving data needed for active operations.
//!
//! # Retention Policy
//!
//! | PR state | Retention |
//! |----------|-----------|
//! | Open | Always keep (needed for stack tracking) |
//! | Merged/closed, in active train | Always keep (train still references it) |
//! | Merged/closed, referenced as predecessor by a kept PR | Keep (transitive closure) |
//! | Merged/closed, not referenced | Prune after retention period |
//!
//! # Size Limit Fallback
//!
//! If the `prs` map exceeds `max_prs_in_snapshot`, aggressively prune the
//! oldest unreferenced merged/closed PRs (by PR number, lower = older) until
//! under limit. This prevents unbounded growth even in pathological cases.

use std::collections::HashSet;

use chrono::{Duration, Utc};

use super::snapshot::PersistedRepoSnapshot;
use crate::types::PrNumber;

/// Configuration for pruning.
#[derive(Debug, Clone)]
pub struct PruneConfig {
    /// Number of days to retain unreferenced merged/closed PRs.
    /// Default: 30 days.
    pub pr_retention_days: u32,

    /// Maximum number of PRs to store in the snapshot.
    /// Default: 1000.
    pub max_prs_in_snapshot: usize,

    /// Number of hours to retain dedupe keys.
    /// Default: 24 hours.
    pub dedupe_ttl_hours: u32,
}

impl Default for PruneConfig {
    fn default() -> Self {
        PruneConfig {
            pr_retention_days: 30,
            max_prs_in_snapshot: 1000,
            dedupe_ttl_hours: 24,
        }
    }
}

/// Prunes stale data from a snapshot.
///
/// This modifies the snapshot in place, removing:
/// 1. Merged/closed PRs older than the retention period (unless referenced)
/// 2. Excess PRs if over the size limit
/// 3. Expired dedupe keys
///
/// # Algorithm
///
/// 1. Identify PRs that must be kept:
///    - All open PRs
///    - All PRs in active trains (current_pr)
///    - Transitive closure: predecessors of kept PRs
/// 2. Remove PRs not in the keep set if they're:
///    - Merged/closed AND
///    - Older than retention period (based on closed_at)
/// 3. If still over size limit, remove oldest unreferenced merged/closed PRs
/// 4. Remove expired dedupe keys
pub fn prune_snapshot(snapshot: &mut PersistedRepoSnapshot, config: &PruneConfig) {
    prune_prs(snapshot, config);
    prune_dedupe_keys(snapshot, config);
}

/// Prunes stale PRs from the snapshot.
fn prune_prs(snapshot: &mut PersistedRepoSnapshot, config: &PruneConfig) {
    let now = Utc::now();
    let retention = Duration::days(config.pr_retention_days as i64);

    // 1. Collect PRs to keep
    let mut keep: HashSet<PrNumber> = HashSet::new();

    // Keep all open PRs
    for (pr_num, pr) in &snapshot.prs {
        if pr.state.is_open() {
            keep.insert(*pr_num);
        }
    }

    // Keep all PRs in active trains (including frozen descendants)
    for train in snapshot.active_trains.values() {
        keep.insert(train.current_pr);
        keep.insert(train.original_root_pr);

        // Keep all frozen descendants - they're part of the active cascade
        // and needed for the operation to complete correctly
        if let Some(progress) = train.cascade_phase.progress() {
            for pr in &progress.frozen_descendants {
                keep.insert(*pr);
            }
        }
    }

    // Transitive closure: keep predecessors of kept PRs
    loop {
        let mut added = false;
        for (pr_num, pr) in &snapshot.prs {
            if keep.contains(pr_num)
                && let Some(pred) = pr.predecessor
                && keep.insert(pred)
            {
                added = true;
            }
        }
        if !added {
            break;
        }
    }

    // 2. Prune unreferenced merged/closed PRs older than retention period
    snapshot.prs.retain(|pr_num, pr| {
        // Always keep if in the keep set
        if keep.contains(pr_num) {
            return true;
        }

        // Only prune merged/closed PRs
        if pr.state.is_open() {
            return true;
        }

        // Check closed_at if available
        if let Some(closed_at) = &pr.closed_at {
            let age = now - *closed_at;
            if age < retention {
                return true;
            }
        } else {
            // No closed_at timestamp - keep for now (legacy entries)
            return true;
        }

        // Prune: merged/closed, not referenced, older than retention
        false
    });

    // 3. Size limit fallback
    if snapshot.prs.len() > config.max_prs_in_snapshot {
        // Collect prunable PRs: merged/closed and not in keep set
        let mut prunable: Vec<PrNumber> = snapshot
            .prs
            .iter()
            .filter(|(pr_num, pr)| !keep.contains(pr_num) && !pr.state.is_open())
            .map(|(pr_num, _)| *pr_num)
            .collect();

        // Sort by PR number (ascending = oldest first)
        prunable.sort_by_key(|pr| pr.0);

        // Remove oldest until under limit
        let to_remove = snapshot
            .prs
            .len()
            .saturating_sub(config.max_prs_in_snapshot);
        for pr_num in prunable.into_iter().take(to_remove) {
            snapshot.prs.remove(&pr_num);
        }
    }
}

/// Prunes expired dedupe keys from the snapshot.
fn prune_dedupe_keys(snapshot: &mut PersistedRepoSnapshot, config: &PruneConfig) {
    let now = Utc::now();
    let ttl = Duration::hours(config.dedupe_ttl_hours as i64);

    snapshot.seen_dedupe_keys.retain(|_key, timestamp| {
        let age = now - *timestamp;
        age < ttl
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        CachedPr, CascadePhase, DescendantProgress, MergeStateStatus, PrState, Sha, TrainRecord,
    };
    use chrono::{Duration, Utc};
    use proptest::prelude::*;

    // ─── Test helpers ───

    fn create_open_pr(number: u64) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::parse("a".repeat(40)).unwrap(),
            format!("branch-{}", number),
            "main".to_string(),
            None,
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        )
    }

    fn create_merged_pr(number: u64, closed_at: chrono::DateTime<Utc>) -> CachedPr {
        let mut pr = CachedPr::new(
            PrNumber(number),
            Sha::parse("a".repeat(40)).unwrap(),
            format!("branch-{}", number),
            "main".to_string(),
            None,
            PrState::Merged {
                merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
            },
            MergeStateStatus::Clean,
            false,
        );
        pr.closed_at = Some(closed_at);
        pr
    }

    fn create_closed_pr(number: u64, closed_at: chrono::DateTime<Utc>) -> CachedPr {
        let mut pr = CachedPr::new(
            PrNumber(number),
            Sha::parse("a".repeat(40)).unwrap(),
            format!("branch-{}", number),
            "main".to_string(),
            None,
            PrState::Closed,
            MergeStateStatus::Clean,
            false,
        );
        pr.closed_at = Some(closed_at);
        pr
    }

    fn create_pr_with_predecessor(number: u64, predecessor: u64) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::parse("a".repeat(40)).unwrap(),
            format!("branch-{}", number),
            format!("branch-{}", predecessor),
            Some(PrNumber(predecessor)),
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        )
    }

    fn default_config() -> PruneConfig {
        PruneConfig {
            pr_retention_days: 30,
            max_prs_in_snapshot: 1000,
            dedupe_ttl_hours: 24,
        }
    }

    // ─── Unit tests ───

    #[test]
    fn keeps_open_prs() {
        let mut snapshot = PersistedRepoSnapshot::new("main");
        snapshot.prs.insert(PrNumber(1), create_open_pr(1));
        snapshot.prs.insert(PrNumber(2), create_open_pr(2));

        prune_snapshot(&mut snapshot, &default_config());

        assert_eq!(snapshot.prs.len(), 2);
        assert!(snapshot.prs.contains_key(&PrNumber(1)));
        assert!(snapshot.prs.contains_key(&PrNumber(2)));
    }

    #[test]
    fn keeps_prs_in_active_trains() {
        let mut snapshot = PersistedRepoSnapshot::new("main");

        // Add merged PR
        let old_date = Utc::now() - Duration::days(60);
        snapshot
            .prs
            .insert(PrNumber(1), create_merged_pr(1, old_date));

        // Add train referencing that PR
        let train = TrainRecord::new(PrNumber(1));
        snapshot.active_trains.insert(PrNumber(1), train);

        prune_snapshot(&mut snapshot, &default_config());

        // PR should be kept because it's in an active train
        assert!(snapshot.prs.contains_key(&PrNumber(1)));
    }

    #[test]
    fn keeps_predecessors_transitively() {
        let mut snapshot = PersistedRepoSnapshot::new("main");

        // Chain: 3 -> 2 -> 1 (3 is open, 2 and 1 are merged)
        let old_date = Utc::now() - Duration::days(60);

        let pr1 = create_merged_pr(1, old_date);
        let mut pr2 = create_merged_pr(2, old_date);
        pr2.predecessor = Some(PrNumber(1));
        let pr3 = create_pr_with_predecessor(3, 2);

        snapshot.prs.insert(PrNumber(1), pr1);
        snapshot.prs.insert(PrNumber(2), pr2);
        snapshot.prs.insert(PrNumber(3), pr3);

        prune_snapshot(&mut snapshot, &default_config());

        // All PRs should be kept (1 is predecessor of 2, 2 is predecessor of 3, 3 is open)
        assert_eq!(snapshot.prs.len(), 3);
    }

    #[test]
    fn prunes_old_unreferenced_merged_prs() {
        let mut snapshot = PersistedRepoSnapshot::new("main");

        // Old merged PR (60 days ago)
        let old_date = Utc::now() - Duration::days(60);
        snapshot
            .prs
            .insert(PrNumber(1), create_merged_pr(1, old_date));

        // Recent merged PR (5 days ago)
        let recent_date = Utc::now() - Duration::days(5);
        snapshot
            .prs
            .insert(PrNumber(2), create_merged_pr(2, recent_date));

        prune_snapshot(&mut snapshot, &default_config());

        // Old PR should be pruned, recent PR should be kept
        assert!(!snapshot.prs.contains_key(&PrNumber(1)));
        assert!(snapshot.prs.contains_key(&PrNumber(2)));
    }

    #[test]
    fn prunes_old_unreferenced_closed_prs() {
        let mut snapshot = PersistedRepoSnapshot::new("main");

        let old_date = Utc::now() - Duration::days(60);
        snapshot
            .prs
            .insert(PrNumber(1), create_closed_pr(1, old_date));

        prune_snapshot(&mut snapshot, &default_config());

        assert!(!snapshot.prs.contains_key(&PrNumber(1)));
    }

    #[test]
    fn keeps_merged_prs_without_closed_at() {
        let mut snapshot = PersistedRepoSnapshot::new("main");

        // Merged PR without closed_at (legacy)
        let pr = CachedPr::new(
            PrNumber(1),
            Sha::parse("a".repeat(40)).unwrap(),
            "branch-1".to_string(),
            "main".to_string(),
            None,
            PrState::Merged {
                merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
            },
            MergeStateStatus::Clean,
            false,
        );
        // No closed_at set
        snapshot.prs.insert(PrNumber(1), pr);

        prune_snapshot(&mut snapshot, &default_config());

        // Should be kept because no closed_at
        assert!(snapshot.prs.contains_key(&PrNumber(1)));
    }

    #[test]
    fn size_limit_prunes_oldest_first() {
        let mut snapshot = PersistedRepoSnapshot::new("main");

        let old_date = Utc::now() - Duration::days(5);

        // Add 10 merged PRs
        for i in 1..=10 {
            snapshot
                .prs
                .insert(PrNumber(i), create_merged_pr(i, old_date));
        }

        // Set very small limit
        let config = PruneConfig {
            max_prs_in_snapshot: 5,
            ..default_config()
        };

        prune_snapshot(&mut snapshot, &config);

        // Should have 5 PRs (the newest ones)
        assert_eq!(snapshot.prs.len(), 5);

        // PRs 1-5 should be pruned (oldest by PR number)
        for i in 1..=5 {
            assert!(
                !snapshot.prs.contains_key(&PrNumber(i)),
                "PR {} should be pruned",
                i
            );
        }

        // PRs 6-10 should be kept
        for i in 6..=10 {
            assert!(
                snapshot.prs.contains_key(&PrNumber(i)),
                "PR {} should be kept",
                i
            );
        }
    }

    #[test]
    fn size_limit_preserves_keep_set() {
        let mut snapshot = PersistedRepoSnapshot::new("main");

        let old_date = Utc::now() - Duration::days(5);

        // Add merged PRs
        for i in 1..=10 {
            snapshot
                .prs
                .insert(PrNumber(i), create_merged_pr(i, old_date));
        }

        // Add active train referencing PR 1
        let train = TrainRecord::new(PrNumber(1));
        snapshot.active_trains.insert(PrNumber(1), train);

        // Set limit that requires pruning
        let config = PruneConfig {
            max_prs_in_snapshot: 5,
            ..default_config()
        };

        prune_snapshot(&mut snapshot, &config);

        // PR 1 should be kept (in active train)
        assert!(snapshot.prs.contains_key(&PrNumber(1)));
    }

    #[test]
    fn prunes_expired_dedupe_keys() {
        let mut snapshot = PersistedRepoSnapshot::new("main");

        // Add old key (48 hours ago)
        let old_time = Utc::now() - Duration::hours(48);
        snapshot
            .seen_dedupe_keys
            .insert("old_key".to_string(), old_time);

        // Add recent key (1 hour ago)
        let recent_time = Utc::now() - Duration::hours(1);
        snapshot
            .seen_dedupe_keys
            .insert("recent_key".to_string(), recent_time);

        prune_snapshot(&mut snapshot, &default_config());

        // Old key should be pruned
        assert!(!snapshot.seen_dedupe_keys.contains_key("old_key"));
        // Recent key should be kept
        assert!(snapshot.seen_dedupe_keys.contains_key("recent_key"));
    }

    // ─── Property tests ───

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        (1u64..10000).prop_map(PrNumber)
    }

    fn arb_sha() -> impl Strategy<Value = Sha> {
        "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
    }

    proptest! {
        /// Open PRs are never pruned.
        #[test]
        fn open_prs_never_pruned(prs in prop::collection::vec(arb_pr_number(), 1..20)) {
            let mut snapshot = PersistedRepoSnapshot::new("main");

            for pr_num in &prs {
                snapshot.prs.insert(*pr_num, create_open_pr(pr_num.0));
            }

            prune_snapshot(&mut snapshot, &default_config());

            for pr_num in &prs {
                prop_assert!(
                    snapshot.prs.contains_key(pr_num),
                    "Open PR {} should not be pruned", pr_num
                );
            }
        }

        /// PRs in active trains are never pruned.
        #[test]
        fn train_prs_never_pruned(train_pr in arb_pr_number()) {
            let mut snapshot = PersistedRepoSnapshot::new("main");

            // Add old merged PR
            let old_date = Utc::now() - Duration::days(60);
            snapshot.prs.insert(train_pr, create_merged_pr(train_pr.0, old_date));

            // Add train
            let train = TrainRecord::new(train_pr);
            snapshot.active_trains.insert(train_pr, train);

            prune_snapshot(&mut snapshot, &default_config());

            prop_assert!(snapshot.prs.contains_key(&train_pr));
        }

        /// Predecessor chains are preserved.
        #[test]
        fn predecessor_chains_preserved(
            chain_len in 2usize..5,
            base_pr in 1u64..1000
        ) {
            let mut snapshot = PersistedRepoSnapshot::new("main");
            let old_date = Utc::now() - Duration::days(60);

            // Create chain: N -> N-1 -> ... -> 1
            // Last PR in chain is open, others are merged
            for i in 0..chain_len {
                let pr_num = base_pr + i as u64;
                let predecessor = if i > 0 { Some(PrNumber(base_pr + (i - 1) as u64)) } else { None };

                if i == chain_len - 1 {
                    // Last PR is open
                    let mut pr = create_open_pr(pr_num);
                    pr.predecessor = predecessor;
                    snapshot.prs.insert(PrNumber(pr_num), pr);
                } else {
                    // Others are merged
                    let mut pr = create_merged_pr(pr_num, old_date);
                    pr.predecessor = predecessor;
                    snapshot.prs.insert(PrNumber(pr_num), pr);
                }
            }

            prune_snapshot(&mut snapshot, &default_config());

            // All PRs in chain should be preserved
            for i in 0..chain_len {
                let pr_num = PrNumber(base_pr + i as u64);
                prop_assert!(
                    snapshot.prs.contains_key(&pr_num),
                    "PR {} in chain should be preserved", pr_num
                );
            }
        }

        /// Size limit is respected.
        #[test]
        fn size_limit_respected(
            num_prs in 20usize..50,
            max_prs in 5usize..15
        ) {
            let mut snapshot = PersistedRepoSnapshot::new("main");
            let old_date = Utc::now() - Duration::days(5);

            for i in 1..=num_prs as u64 {
                snapshot.prs.insert(PrNumber(i), create_merged_pr(i, old_date));
            }

            let config = PruneConfig {
                max_prs_in_snapshot: max_prs,
                ..default_config()
            };

            prune_snapshot(&mut snapshot, &config);

            prop_assert!(
                snapshot.prs.len() <= max_prs,
                "Should have at most {} PRs, got {}",
                max_prs,
                snapshot.prs.len()
            );
        }

        /// Pruning is idempotent.
        #[test]
        fn pruning_is_idempotent(
            open_prs in prop::collection::vec(1u64..100, 1..5),
            merged_prs in prop::collection::vec(100u64..200, 1..5)
        ) {
            let mut snapshot = PersistedRepoSnapshot::new("main");
            let old_date = Utc::now() - Duration::days(60);

            for pr_num in &open_prs {
                snapshot.prs.insert(PrNumber(*pr_num), create_open_pr(*pr_num));
            }
            for pr_num in &merged_prs {
                snapshot.prs.insert(PrNumber(*pr_num), create_merged_pr(*pr_num, old_date));
            }

            // First prune
            prune_snapshot(&mut snapshot, &default_config());
            let after_first = snapshot.prs.len();

            // Second prune
            prune_snapshot(&mut snapshot, &default_config());
            let after_second = snapshot.prs.len();

            prop_assert_eq!(after_first, after_second, "Pruning should be idempotent");
        }

        /// PRs in frozen_descendants of active train cascade phases are never pruned.
        ///
        /// When a train is mid-cascade (not Idle), its frozen_descendants list contains
        /// PRs that are part of the active operation. These PRs might be closed/merged
        /// (e.g., a descendant that failed during preparation), but they must be preserved
        /// for the cascade to complete correctly and for recovery to work.
        #[test]
        fn frozen_descendants_in_active_trains_never_pruned(
            train_pr in 1u64..1000,
            // Use distinct range for descendants to avoid overlap with train_pr
            frozen_descendants in prop::collection::hash_set(1000u64..2000, 1..5)
                .prop_map(|set| set.into_iter().map(PrNumber).collect::<Vec<_>>()),
            sha in arb_sha(),
        ) {
            let mut snapshot = PersistedRepoSnapshot::new("main");
            let old_date = Utc::now() - Duration::days(60);

            // Add old merged PRs for the frozen descendants - these would normally be pruned
            for pr_num in &frozen_descendants {
                snapshot.prs.insert(*pr_num, create_merged_pr(pr_num.0, old_date));
            }

            // Add the train PR itself
            snapshot.prs.insert(PrNumber(train_pr), create_merged_pr(train_pr, old_date));

            // Create train with non-Idle phase containing frozen_descendants
            let progress = DescendantProgress::new(frozen_descendants.clone());
            let mut train = TrainRecord::new(PrNumber(train_pr));
            train.cascade_phase = CascadePhase::Reconciling {
                progress,
                squash_sha: sha,
            };
            snapshot.active_trains.insert(PrNumber(train_pr), train);

            prune_snapshot(&mut snapshot, &default_config());

            // All frozen descendants should survive pruning
            for pr_num in &frozen_descendants {
                prop_assert!(
                    snapshot.prs.contains_key(pr_num),
                    "Frozen descendant {} in active train should not be pruned", pr_num
                );
            }
        }
    }
}
