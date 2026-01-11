//! Descendant management and indexing.
//!
//! Pure functions for building and querying the descendants index,
//! which is the reverse mapping from predecessor -> descendants.

use std::collections::{HashMap, HashSet};

use crate::types::{CachedPr, CascadePhase, PrNumber};

/// Builds a reverse index from predecessor to descendants.
///
/// Given a map of PRs, returns a map where each key is a PR number and
/// the value is the set of PRs that declare that key as their predecessor.
///
/// This is the inverse of the predecessor relationship:
/// - If PR #2 has predecessor = Some(#1)
/// - Then the index will have: #1 -> {#2}
pub fn build_descendants_index(
    prs: &HashMap<PrNumber, CachedPr>,
) -> HashMap<PrNumber, HashSet<PrNumber>> {
    let mut index: HashMap<PrNumber, HashSet<PrNumber>> = HashMap::new();

    for pr in prs.values() {
        if let Some(pred) = pr.predecessor {
            index.entry(pred).or_default().insert(pr.number);
        }
    }

    index
}

/// Returns the PRs that still need to be processed in the current phase.
///
/// This is computed as: frozen_descendants - completed - skipped
///
/// Uses frozen_descendants from the phase, NOT the current descendants index.
/// Late additions (PRs added after the freeze point) are not included and will
/// be handled when their predecessor becomes a new root.
pub fn remaining_descendants(phase: &CascadePhase) -> Vec<PrNumber> {
    match phase.progress() {
        Some(progress) => progress.remaining().copied().collect(),
        None => Vec::new(),
    }
}

/// Returns all frozen descendants for a phase, regardless of completion status.
///
/// Returns an empty vector for phases that don't track descendants (e.g., Idle).
pub fn frozen_descendants(phase: &CascadePhase) -> Vec<PrNumber> {
    match phase.progress() {
        Some(progress) => progress.frozen_descendants.clone(),
        None => Vec::new(),
    }
}

/// Collects all descendants of a PR (direct and transitive).
///
/// Uses the descendants index to walk the tree and collect all PRs
/// that are transitively descended from the given root.
///
/// IMPORTANT: Closed PRs are neither returned NOR traversed. This means that
/// closing a PR in a train stops the cascade for that PR and all its dependents.
/// For example, if `#1 <- #2 (closed) <- #3 (open)`, calling
/// `collect_all_descendants(#1, ...)` will NOT find #3 because #2 blocks traversal.
/// This is intentional: closing a PR should stop the train for that branch.
///
/// Traversal order is not guaranteed.
pub fn collect_all_descendants(
    root: PrNumber,
    descendants_index: &HashMap<PrNumber, HashSet<PrNumber>>,
    prs: &HashMap<PrNumber, CachedPr>,
) -> Vec<PrNumber> {
    let mut result = Vec::new();
    let mut visited = HashSet::new();
    let mut queue = Vec::new();

    // Start with direct descendants
    if let Some(direct) = descendants_index.get(&root) {
        for &desc in direct {
            if !visited.contains(&desc) {
                // Only enqueue open PRs - closed PRs block traversal
                if prs.get(&desc).map(|p| p.state.is_open()).unwrap_or(false) {
                    queue.push(desc);
                    visited.insert(desc);
                }
            }
        }
    }

    // Collect all transitive descendants (only open PRs are in queue)
    while let Some(current) = queue.pop() {
        result.push(current);

        if let Some(children) = descendants_index.get(&current) {
            for &child in children {
                if !visited.contains(&child) {
                    // Only enqueue open PRs - closed PRs block traversal
                    if prs.get(&child).map(|p| p.state.is_open()).unwrap_or(false) {
                        queue.push(child);
                        visited.insert(child);
                    }
                }
            }
        }
    }

    result
}

/// Collects only direct (immediate) descendants of a PR.
///
/// Returns only PRs that directly declare the given PR as their predecessor,
/// filtering to only open PRs.
pub fn collect_direct_descendants(
    pr: PrNumber,
    descendants_index: &HashMap<PrNumber, HashSet<PrNumber>>,
    prs: &HashMap<PrNumber, CachedPr>,
) -> Vec<PrNumber> {
    descendants_index
        .get(&pr)
        .map(|descs| {
            descs
                .iter()
                .filter(|&d| prs.get(d).map(|p| p.state.is_open()).unwrap_or(false))
                .copied()
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DescendantProgress, MergeStateStatus, PrState, Sha};
    use proptest::prelude::*;

    fn make_open_pr(number: u64, predecessor: Option<u64>) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::parse("abc123def456789012345678901234567890abcd").unwrap(),
            format!("branch-{}", number),
            "main".to_string(),
            predecessor.map(PrNumber),
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        )
    }

    fn make_closed_pr(number: u64, predecessor: Option<u64>) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::parse("abc123def456789012345678901234567890abcd").unwrap(),
            format!("branch-{}", number),
            "main".to_string(),
            predecessor.map(PrNumber),
            PrState::Closed,
            MergeStateStatus::Clean,
            false,
        )
    }

    mod build_descendants_index_tests {
        use super::*;

        #[test]
        fn empty_map_returns_empty_index() {
            let prs = HashMap::new();
            let index = build_descendants_index(&prs);
            assert!(index.is_empty());
        }

        #[test]
        fn single_predecessor_relationship() {
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, None));
            prs.insert(PrNumber(2), make_open_pr(2, Some(1)));

            let index = build_descendants_index(&prs);

            assert_eq!(index.len(), 1);
            assert!(index.get(&PrNumber(1)).unwrap().contains(&PrNumber(2)));
        }

        #[test]
        fn multiple_descendants_of_same_predecessor() {
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, None));
            prs.insert(PrNumber(2), make_open_pr(2, Some(1)));
            prs.insert(PrNumber(3), make_open_pr(3, Some(1)));

            let index = build_descendants_index(&prs);

            let descs = index.get(&PrNumber(1)).unwrap();
            assert_eq!(descs.len(), 2);
            assert!(descs.contains(&PrNumber(2)));
            assert!(descs.contains(&PrNumber(3)));
        }

        #[test]
        fn chain_of_predecessors() {
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, None));
            prs.insert(PrNumber(2), make_open_pr(2, Some(1)));
            prs.insert(PrNumber(3), make_open_pr(3, Some(2)));

            let index = build_descendants_index(&prs);

            assert_eq!(index.len(), 2);
            assert!(index.get(&PrNumber(1)).unwrap().contains(&PrNumber(2)));
            assert!(index.get(&PrNumber(2)).unwrap().contains(&PrNumber(3)));
            assert!(!index.contains_key(&PrNumber(3)));
        }
    }

    mod remaining_descendants_tests {
        use super::*;

        #[test]
        fn idle_phase_returns_empty() {
            let phase = CascadePhase::Idle;
            let remaining = remaining_descendants(&phase);
            assert!(remaining.is_empty());
        }

        #[test]
        fn preparing_with_no_completed_returns_all_frozen() {
            let frozen = vec![PrNumber(2), PrNumber(3)];
            let progress = DescendantProgress::new(frozen.clone());
            let phase = CascadePhase::Preparing { progress };

            let remaining = remaining_descendants(&phase);

            assert_eq!(remaining.len(), 2);
            assert!(remaining.contains(&PrNumber(2)));
            assert!(remaining.contains(&PrNumber(3)));
        }

        #[test]
        fn preparing_with_some_completed_returns_remaining() {
            let frozen = vec![PrNumber(2), PrNumber(3), PrNumber(4)];
            let mut progress = DescendantProgress::new(frozen);
            progress.mark_completed(PrNumber(2));

            let phase = CascadePhase::Preparing { progress };
            let remaining = remaining_descendants(&phase);

            assert_eq!(remaining.len(), 2);
            assert!(!remaining.contains(&PrNumber(2)));
            assert!(remaining.contains(&PrNumber(3)));
            assert!(remaining.contains(&PrNumber(4)));
        }

        #[test]
        fn preparing_with_some_skipped_excludes_skipped() {
            let frozen = vec![PrNumber(2), PrNumber(3)];
            let mut progress = DescendantProgress::new(frozen);
            progress.mark_skipped(PrNumber(3));

            let phase = CascadePhase::Preparing { progress };
            let remaining = remaining_descendants(&phase);

            assert_eq!(remaining.len(), 1);
            assert!(remaining.contains(&PrNumber(2)));
            assert!(!remaining.contains(&PrNumber(3)));
        }

        #[test]
        fn all_completed_or_skipped_returns_empty() {
            let frozen = vec![PrNumber(2), PrNumber(3)];
            let mut progress = DescendantProgress::new(frozen);
            progress.mark_completed(PrNumber(2));
            progress.mark_skipped(PrNumber(3));

            let phase = CascadePhase::Preparing { progress };
            let remaining = remaining_descendants(&phase);

            assert!(remaining.is_empty());
        }
    }

    mod collect_all_descendants_tests {
        use super::*;

        #[test]
        fn no_descendants_returns_empty() {
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, None));

            let index = build_descendants_index(&prs);
            let all = collect_all_descendants(PrNumber(1), &index, &prs);

            assert!(all.is_empty());
        }

        #[test]
        fn collects_direct_descendants() {
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, None));
            prs.insert(PrNumber(2), make_open_pr(2, Some(1)));
            prs.insert(PrNumber(3), make_open_pr(3, Some(1)));

            let index = build_descendants_index(&prs);
            let all = collect_all_descendants(PrNumber(1), &index, &prs);

            assert_eq!(all.len(), 2);
            assert!(all.contains(&PrNumber(2)));
            assert!(all.contains(&PrNumber(3)));
        }

        #[test]
        fn collects_transitive_descendants() {
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, None));
            prs.insert(PrNumber(2), make_open_pr(2, Some(1)));
            prs.insert(PrNumber(3), make_open_pr(3, Some(2)));
            prs.insert(PrNumber(4), make_open_pr(4, Some(3)));

            let index = build_descendants_index(&prs);
            let all = collect_all_descendants(PrNumber(1), &index, &prs);

            assert_eq!(all.len(), 3);
            assert!(all.contains(&PrNumber(2)));
            assert!(all.contains(&PrNumber(3)));
            assert!(all.contains(&PrNumber(4)));
        }

        #[test]
        fn excludes_closed_prs() {
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, None));
            prs.insert(PrNumber(2), make_open_pr(2, Some(1)));
            prs.insert(PrNumber(3), make_closed_pr(3, Some(1)));

            let index = build_descendants_index(&prs);
            let all = collect_all_descendants(PrNumber(1), &index, &prs);

            assert_eq!(all.len(), 1);
            assert!(all.contains(&PrNumber(2)));
            assert!(!all.contains(&PrNumber(3)));
        }
    }

    mod property_tests {
        use super::*;

        proptest! {
            /// The descendants index is the inverse of the predecessor relationship:
            /// For every (predecessor, descendant) in the index, the descendant's
            /// predecessor field points to the predecessor.
            #[test]
            fn descendants_index_is_inverse_of_predecessor(
                pr_count in 1usize..20
            ) {
                // Build a set of PRs with some predecessor relationships
                let mut prs = HashMap::new();
                prs.insert(PrNumber(1), make_open_pr(1, None));

                for i in 2..=pr_count as u64 {
                    // Each PR has a ~50% chance of having a predecessor
                    let pred = if i % 2 == 0 { Some((i / 2).max(1)) } else { None };
                    prs.insert(PrNumber(i), make_open_pr(i, pred));
                }

                let index = build_descendants_index(&prs);

                // Verify: for each (pred, descs) in index, each desc's predecessor == pred
                for (&pred, descs) in &index {
                    for &desc in descs {
                        let desc_pr = prs.get(&desc).unwrap();
                        prop_assert_eq!(desc_pr.predecessor, Some(pred));
                    }
                }

                // Verify: for each PR with a predecessor, that predecessor has it in descendants
                for pr in prs.values() {
                    if let Some(pred) = pr.predecessor {
                        let descs = index.get(&pred);
                        prop_assert!(
                            descs.is_some(),
                            "PR {} has predecessor {} but index has no entry for {}",
                            pr.number, pred, pred
                        );
                        prop_assert!(descs.unwrap().contains(&pr.number));
                    }
                }
            }

            /// remaining_descendants never includes late additions:
            /// The frozen_descendants list determines what's remaining, not the current index.
            #[test]
            fn remaining_descendants_uses_frozen_list(
                frozen_count in 0usize..10,
                completed_count in 0usize..5,
                skipped_count in 0usize..5
            ) {
                // Create a frozen list
                let frozen: Vec<PrNumber> = (1..=frozen_count as u64)
                    .map(PrNumber)
                    .collect();

                let mut progress = DescendantProgress::new(frozen.clone());

                // Mark some as completed (avoiding overflow)
                for i in 0..completed_count.min(frozen_count) {
                    if i < frozen.len() {
                        progress.mark_completed(frozen[i]);
                    }
                }

                // Mark some as skipped (different from completed)
                for i in completed_count..(completed_count + skipped_count).min(frozen_count) {
                    if i < frozen.len() {
                        progress.mark_skipped(frozen[i]);
                    }
                }

                let phase = CascadePhase::Preparing { progress: progress.clone() };
                let remaining = remaining_descendants(&phase);

                // Verify: remaining is exactly frozen - completed - skipped

                // All returned elements are valid (in frozen, not in completed/skipped)
                for pr in &remaining {
                    prop_assert!(frozen.contains(pr));
                    prop_assert!(!progress.completed.contains(pr));
                    prop_assert!(!progress.skipped.contains(pr));
                }

                // All expected elements are returned (completeness)
                let expected: HashSet<_> = frozen.iter()
                    .filter(|pr| !progress.completed.contains(pr) && !progress.skipped.contains(pr))
                    .copied()
                    .collect();
                let actual: HashSet<_> = remaining.iter().copied().collect();
                prop_assert_eq!(expected, actual, "remaining should equal frozen - completed - skipped");
            }

            /// Closing a PR in a linear chain cuts off all descendants behind it.
            /// Given: main <- #1 <- #2 <- ... <- #N, with one PR closed
            /// PRs before the closed one should be found, PRs at or after should not.
            #[test]
            fn closed_pr_cuts_linear_chain(
                chain_length in 3usize..8,
                closed_idx in 1usize..6  // Which PR in the chain to close (0 = root)
            ) {
                let closed_idx = closed_idx.min(chain_length - 1);

                // Build a linear chain: #1 <- #2 <- #3 <- ... <- #N
                let mut prs = HashMap::new();

                // Root PR (no predecessor)
                prs.insert(PrNumber(1), make_open_pr(1, None));

                // Chain of PRs
                for i in 2..=chain_length as u64 {
                    let pr_idx = (i - 1) as usize; // 0-indexed position in chain
                    if pr_idx == closed_idx {
                        prs.insert(PrNumber(i), make_closed_pr(i, Some(i - 1)));
                    } else {
                        prs.insert(PrNumber(i), make_open_pr(i, Some(i - 1)));
                    }
                }

                let index = build_descendants_index(&prs);
                let descendants = collect_all_descendants(PrNumber(1), &index, &prs);

                // PRs before the closed one should be found
                for i in 2..=(closed_idx as u64) {
                    if i <= closed_idx as u64 {
                        // This PR is before the closed one, should be found (if open)
                        let is_closed = (i - 1) as usize == closed_idx;
                        if !is_closed {
                            prop_assert!(
                                descendants.contains(&PrNumber(i)),
                                "PR #{} should be found (before closed PR #{})",
                                i, closed_idx + 1
                            );
                        }
                    }
                }

                // The closed PR should NOT be found
                let closed_pr_num = (closed_idx + 1) as u64;
                prop_assert!(
                    !descendants.contains(&PrNumber(closed_pr_num)),
                    "Closed PR #{} should not be found",
                    closed_pr_num
                );

                // PRs after the closed one should NOT be found (blocked by closed PR)
                for i in (closed_pr_num + 1)..=(chain_length as u64) {
                    prop_assert!(
                        !descendants.contains(&PrNumber(i)),
                        "PR #{} should not be found (blocked by closed PR #{})",
                        i, closed_pr_num
                    );
                }
            }

            /// Closing a PR with fan-out blocks all its descendants.
            /// Given: main <- #1 <- #2 (closed) <- {#3, #4, #5, ...}
            /// None of the fan-out descendants should be found.
            #[test]
            fn closed_pr_blocks_fanout_descendants(
                num_fanout in 2usize..6
            ) {
                let mut prs = HashMap::new();

                // Root: #1 (open, no predecessor)
                prs.insert(PrNumber(1), make_open_pr(1, None));

                // #2 is closed, depends on #1
                prs.insert(PrNumber(2), make_closed_pr(2, Some(1)));

                // #3, #4, #5, ... all depend on #2 (fan-out from closed PR)
                for i in 3..=(num_fanout as u64 + 2) {
                    prs.insert(PrNumber(i), make_open_pr(i, Some(2)));
                }

                let index = build_descendants_index(&prs);
                let descendants = collect_all_descendants(PrNumber(1), &index, &prs);

                // No descendants should be found - #2 is closed and blocks all
                prop_assert!(
                    descendants.is_empty(),
                    "No descendants should be found when direct descendant is closed. Found: {:?}",
                    descendants
                );
            }

            /// Closing a PR does not affect unrelated branches in the DAG.
            /// Given: main <- #1 <- {#2 (closed) <- #3, #4 <- #5}
            /// #4 and #5 should be found; #2 and #3 should not.
            #[test]
            fn closed_pr_does_not_affect_other_branches(
                closed_branch_depth in 1usize..4,
                open_branch_depth in 1usize..4
            ) {
                let mut prs = HashMap::new();

                // Root: #1 (open, no predecessor)
                prs.insert(PrNumber(1), make_open_pr(1, None));

                // Closed branch: #2 (closed) <- #3 <- #4 <- ...
                // Start numbering at 2
                let closed_branch_start = 2u64;
                prs.insert(PrNumber(closed_branch_start), make_closed_pr(closed_branch_start, Some(1)));
                for i in 1..closed_branch_depth as u64 {
                    let pr_num = closed_branch_start + i;
                    prs.insert(PrNumber(pr_num), make_open_pr(pr_num, Some(pr_num - 1)));
                }

                // Open branch: starts after closed branch
                // Numbering: closed_branch_start + closed_branch_depth, ...
                let open_branch_start = closed_branch_start + closed_branch_depth as u64;
                prs.insert(PrNumber(open_branch_start), make_open_pr(open_branch_start, Some(1)));
                for i in 1..open_branch_depth as u64 {
                    let pr_num = open_branch_start + i;
                    prs.insert(PrNumber(pr_num), make_open_pr(pr_num, Some(pr_num - 1)));
                }

                let index = build_descendants_index(&prs);
                let descendants = collect_all_descendants(PrNumber(1), &index, &prs);
                let descendants_set: HashSet<_> = descendants.iter().copied().collect();

                // Closed branch PRs should NOT be found
                for i in 0..closed_branch_depth as u64 {
                    let pr_num = closed_branch_start + i;
                    prop_assert!(
                        !descendants_set.contains(&PrNumber(pr_num)),
                        "PR #{} (closed branch) should not be found",
                        pr_num
                    );
                }

                // Open branch PRs SHOULD be found
                for i in 0..open_branch_depth as u64 {
                    let pr_num = open_branch_start + i;
                    prop_assert!(
                        descendants_set.contains(&PrNumber(pr_num)),
                        "PR #{} (open branch) should be found",
                        pr_num
                    );
                }
            }

            /// PRs earlier in the train (before any closed PR) are still found.
            /// Given: main <- #1 <- #2 <- #3 (closed) <- #4
            /// #2 should be found even though #3 and #4 are blocked.
            #[test]
            fn prs_before_closed_pr_are_found(
                prs_before_closed in 1usize..5,
                prs_after_closed in 1usize..4
            ) {
                let mut prs = HashMap::new();

                // Root: #1 (open, no predecessor)
                prs.insert(PrNumber(1), make_open_pr(1, None));

                // Open PRs before the closed one: #2, #3, ...
                for i in 2..=(prs_before_closed as u64 + 1) {
                    prs.insert(PrNumber(i), make_open_pr(i, Some(i - 1)));
                }

                // Closed PR
                let closed_pr_num = prs_before_closed as u64 + 2;
                prs.insert(
                    PrNumber(closed_pr_num),
                    make_closed_pr(closed_pr_num, Some(closed_pr_num - 1)),
                );

                // Open PRs after the closed one
                for i in 1..=prs_after_closed as u64 {
                    let pr_num = closed_pr_num + i;
                    prs.insert(PrNumber(pr_num), make_open_pr(pr_num, Some(pr_num - 1)));
                }

                let index = build_descendants_index(&prs);
                let descendants = collect_all_descendants(PrNumber(1), &index, &prs);
                let descendants_set: HashSet<_> = descendants.iter().copied().collect();

                // All open PRs before the closed one should be found
                for i in 2..=(prs_before_closed as u64 + 1) {
                    prop_assert!(
                        descendants_set.contains(&PrNumber(i)),
                        "PR #{} (before closed) should be found",
                        i
                    );
                }

                // The closed PR and all after should NOT be found
                for i in closed_pr_num..=(closed_pr_num + prs_after_closed as u64) {
                    prop_assert!(
                        !descendants_set.contains(&PrNumber(i)),
                        "PR #{} (closed or after) should not be found",
                        i
                    );
                }

                // Verify the count is exactly the number of open PRs before closed
                prop_assert_eq!(
                    descendants.len(),
                    prs_before_closed,
                    "Should find exactly {} PRs (those before closed)",
                    prs_before_closed
                );
            }
        }
    }
}
