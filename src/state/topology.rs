//! Stack topology computation.
//!
//! Pure functions for determining which PRs are roots and computing
//! the structure of PR stacks.

use std::collections::{HashMap, HashSet};

use crate::types::{CachedPr, PrNumber};

/// A complete stack from root to tip (linear portion only).
///
/// Fan-out points terminate the stack: if a PR has multiple open descendants,
/// the stack ends at that PR. After it merges, each descendant becomes the
/// root of its own independent stack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeStack {
    /// Ordered from root (index 0) to tip. Ends at fan-out points.
    pub prs: Vec<PrNumber>,
}

impl MergeStack {
    /// Creates a new merge stack with the given PRs.
    pub fn new(prs: Vec<PrNumber>) -> Self {
        MergeStack { prs }
    }

    /// Returns true if the stack is empty.
    pub fn is_empty(&self) -> bool {
        self.prs.is_empty()
    }

    /// Returns the root PR (first in the stack).
    pub fn root(&self) -> Option<PrNumber> {
        self.prs.first().copied()
    }

    /// Returns the tip PR (last in the stack).
    pub fn tip(&self) -> Option<PrNumber> {
        self.prs.last().copied()
    }

    /// Returns the number of PRs in the stack.
    pub fn len(&self) -> usize {
        self.prs.len()
    }
}

/// Determines if a PR is a root (eligible to start a new train).
///
/// A PR is a root if it targets the default branch AND either:
/// - Has no predecessor declaration, OR
/// - Its predecessor has been merged AND reconciliation has completed
///
/// The second case represents a "resolved" predecessor relationship.
/// After a cascade step completes, the descendant targets main but still
/// has its predecessor declaration. This is the expected post-cascade state.
///
/// IMPORTANT: We always require base_ref == default_branch. A PR whose
/// predecessor merged but which hasn't been retargeted yet is NOT a root.
///
/// For the merged-predecessor case, we require `predecessor_squash_reconciled`
/// to be set. This prevents races where someone manually retargets a late descendant
/// to main before the bot's reconciliation runs.
pub fn is_root(pr: &CachedPr, default_branch: &str, prs: &HashMap<PrNumber, CachedPr>) -> bool {
    // Must target the default branch to be a root
    if pr.base_ref != default_branch {
        return false;
    }

    match pr.predecessor {
        // No predecessor + targets default branch = root
        None => true,
        Some(pred) => {
            match prs.get(&pred) {
                // Predecessor merged — but ONLY a root if reconciliation completed.
                // This prevents manually-retargeted PRs from bypassing ours-merge.
                Some(p) if p.state.is_merged() => pr.predecessor_squash_reconciled.is_some(),
                // Predecessor exists but not merged = not a root (still stacked)
                Some(_) => false,
                // Predecessor missing from cache = NOT a root (data integrity issue)
                None => false,
            }
        }
    }
}

/// Computes all stacks from the given PR cache.
///
/// Returns a list of `MergeStack` structures, each representing a linear
/// chain from a root PR to either:
/// - A PR with no descendants
/// - A fan-out point (PR with multiple open descendants)
///
/// PRs that are closed or not connected to a root are excluded.
pub fn compute_stacks(
    prs: &HashMap<PrNumber, CachedPr>,
    default_branch: &str,
    descendants_index: &HashMap<PrNumber, HashSet<PrNumber>>,
) -> Vec<MergeStack> {
    // Find all root PRs (target default branch, no predecessor or predecessor merged)
    let roots: Vec<PrNumber> = prs
        .values()
        .filter(|pr| pr.state.is_open())
        .filter(|pr| is_root(pr, default_branch, prs))
        .map(|pr| pr.number)
        .collect();

    // Build stack for each root
    roots
        .into_iter()
        .filter_map(|root| build_stack(root, prs, descendants_index))
        .collect()
}

/// Builds a single stack starting from the given root PR.
fn build_stack(
    root: PrNumber,
    prs: &HashMap<PrNumber, CachedPr>,
    descendants_index: &HashMap<PrNumber, HashSet<PrNumber>>,
) -> Option<MergeStack> {
    let mut stack_prs = vec![root];
    let mut visited = HashSet::new();
    visited.insert(root);

    // Walk descendants using the reverse index
    let mut current = root;
    while let Some(descendants) = descendants_index.get(&current) {
        // Find open descendants
        let open_descendants: Vec<PrNumber> = descendants
            .iter()
            .filter(|n| prs.get(n).map(|p| p.state.is_open()).unwrap_or(false))
            .copied()
            .collect();

        // If multiple open descendants (fan-out), stack ends here.
        // Each descendant will become its own root after this PR merges.
        if open_descendants.len() != 1 {
            break;
        }

        let next = open_descendants[0];

        // Cycle detection
        if visited.contains(&next) {
            break;
        }

        visited.insert(next);
        stack_prs.push(next);
        current = next;
    }

    Some(MergeStack::new(stack_prs))
}

/// Detects cycles in the predecessor graph.
///
/// Returns `Some(cycle)` if a cycle is found, where `cycle` is a vector of
/// PR numbers forming the cycle (in order). Returns `None` if no cycle exists.
///
/// Uses depth-first search with three-color marking:
/// - White (unvisited): not yet processed
/// - Gray (in progress): currently being explored (on the recursion stack)
/// - Black (finished): fully explored, all descendants processed
///
/// A back edge to a gray node indicates a cycle.
pub fn detect_cycle(prs: &HashMap<PrNumber, CachedPr>) -> Option<Vec<PrNumber>> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Color {
        White,
        Gray,
        Black,
    }

    let mut colors: HashMap<PrNumber, Color> = prs.keys().map(|&pr| (pr, Color::White)).collect();
    let mut parent: HashMap<PrNumber, Option<PrNumber>> = HashMap::new();

    // Build the forward edge map (pr -> its predecessor)
    // For cycle detection, we traverse predecessor edges
    let predecessor_of: HashMap<PrNumber, PrNumber> = prs
        .iter()
        .filter_map(|(&num, pr)| pr.predecessor.map(|pred| (num, pred)))
        .collect();

    fn dfs(
        node: PrNumber,
        predecessor_of: &HashMap<PrNumber, PrNumber>,
        colors: &mut HashMap<PrNumber, Color>,
        parent: &mut HashMap<PrNumber, Option<PrNumber>>,
        path: &mut Vec<PrNumber>,
    ) -> Option<Vec<PrNumber>> {
        colors.insert(node, Color::Gray);
        path.push(node);

        if let Some(&pred) = predecessor_of.get(&node) {
            match colors.get(&pred) {
                Some(Color::Gray) => {
                    // Found a cycle! Extract it from the path
                    if let Some(pos) = path.iter().position(|&p| p == pred) {
                        let cycle: Vec<PrNumber> = path[pos..].to_vec();
                        return Some(cycle);
                    }
                }
                Some(Color::White) => {
                    parent.insert(pred, Some(node));
                    if let Some(cycle) = dfs(pred, predecessor_of, colors, parent, path) {
                        return Some(cycle);
                    }
                }
                Some(Color::Black) | None => {
                    // Already fully explored or not in our set, no cycle through here
                }
            }
        }

        path.pop();
        colors.insert(node, Color::Black);
        None
    }

    // Run DFS from each unvisited node
    for &pr in prs.keys() {
        if colors.get(&pr) == Some(&Color::White) {
            let mut path = Vec::new();
            if let Some(cycle) = dfs(pr, &predecessor_of, &mut colors, &mut parent, &mut path) {
                return Some(cycle);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::descendants::build_descendants_index;
    use crate::types::{MergeStateStatus, PrState, Sha};
    use proptest::prelude::*;

    fn make_pr(number: u64, base_ref: &str, predecessor: Option<u64>, state: PrState) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::parse("abc123def456789012345678901234567890abcd").unwrap(),
            format!("branch-{}", number),
            base_ref.to_string(),
            predecessor.map(PrNumber),
            state,
            MergeStateStatus::Clean,
            false,
        )
    }

    fn make_open_pr(number: u64, base_ref: &str, predecessor: Option<u64>) -> CachedPr {
        make_pr(number, base_ref, predecessor, PrState::Open)
    }

    fn make_merged_pr(number: u64, merge_sha: &str) -> CachedPr {
        make_pr(
            number,
            "main",
            None,
            PrState::Merged {
                merge_commit_sha: Sha::parse(merge_sha).unwrap(),
            },
        )
    }

    mod is_root_tests {
        use super::*;

        #[test]
        fn pr_targeting_main_no_predecessor_is_root() {
            let pr = make_open_pr(1, "main", None);
            let prs = HashMap::from([(PrNumber(1), pr.clone())]);

            assert!(is_root(&pr, "main", &prs));
        }

        #[test]
        fn pr_not_targeting_main_is_not_root() {
            let pr = make_open_pr(1, "feature", None);
            let prs = HashMap::from([(PrNumber(1), pr.clone())]);

            assert!(!is_root(&pr, "main", &prs));
        }

        #[test]
        fn pr_with_open_predecessor_is_not_root() {
            let pred = make_open_pr(1, "main", None);
            let pr = make_open_pr(2, "main", Some(1));
            let prs = HashMap::from([(PrNumber(1), pred), (PrNumber(2), pr.clone())]);

            assert!(!is_root(&pr, "main", &prs));
        }

        #[test]
        fn pr_with_merged_predecessor_without_reconciliation_is_not_root() {
            let pred = make_merged_pr(1, "abc123def456789012345678901234567890abcd");
            let mut pr = make_open_pr(2, "main", Some(1));
            pr.predecessor_squash_reconciled = None;

            let prs = HashMap::from([(PrNumber(1), pred), (PrNumber(2), pr.clone())]);

            assert!(!is_root(&pr, "main", &prs));
        }

        #[test]
        fn pr_with_merged_predecessor_with_reconciliation_is_root() {
            let pred = make_merged_pr(1, "abc123def456789012345678901234567890abcd");
            let mut pr = make_open_pr(2, "main", Some(1));
            pr.predecessor_squash_reconciled =
                Some(Sha::parse("abc123def456789012345678901234567890abcd").unwrap());

            let prs = HashMap::from([(PrNumber(1), pred), (PrNumber(2), pr.clone())]);

            assert!(is_root(&pr, "main", &prs));
        }

        #[test]
        fn pr_with_missing_predecessor_is_not_root() {
            let pr = make_open_pr(2, "main", Some(999));
            let prs = HashMap::from([(PrNumber(2), pr.clone())]);

            assert!(!is_root(&pr, "main", &prs));
        }

        #[test]
        fn closed_pr_state_does_not_affect_is_root_check() {
            // is_root checks base_ref and predecessor, not the PR's own state
            // The caller should filter for open PRs before calling is_root
            let pr = make_pr(1, "main", None, PrState::Closed);
            let prs = HashMap::from([(PrNumber(1), pr.clone())]);

            // Still returns true because it only checks targeting and predecessor
            assert!(is_root(&pr, "main", &prs));
        }
    }

    mod compute_stacks_tests {
        use super::*;

        #[test]
        fn single_root_no_descendants() {
            let pr = make_open_pr(1, "main", None);
            let prs = HashMap::from([(PrNumber(1), pr)]);
            let descendants = build_descendants_index(&prs);

            let stacks = compute_stacks(&prs, "main", &descendants);

            assert_eq!(stacks.len(), 1);
            assert_eq!(stacks[0].prs, vec![PrNumber(1)]);
        }

        #[test]
        fn linear_stack() {
            // main <- #1 <- #2 <- #3
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, "main", None));
            let mut pr2 = make_open_pr(2, "branch-1", Some(1));
            pr2.head_ref = "branch-2".to_string();
            prs.insert(PrNumber(2), pr2);
            let mut pr3 = make_open_pr(3, "branch-2", Some(2));
            pr3.head_ref = "branch-3".to_string();
            prs.insert(PrNumber(3), pr3);

            let descendants = build_descendants_index(&prs);
            let stacks = compute_stacks(&prs, "main", &descendants);

            assert_eq!(stacks.len(), 1);
            assert_eq!(stacks[0].prs, vec![PrNumber(1), PrNumber(2), PrNumber(3)]);
        }

        #[test]
        fn fan_out_creates_single_stack_to_fan_point() {
            // main <- #1 <- {#2, #3} (fan-out)
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, "main", None));
            prs.insert(PrNumber(2), make_open_pr(2, "branch-1", Some(1)));
            prs.insert(PrNumber(3), make_open_pr(3, "branch-1", Some(1)));

            let descendants = build_descendants_index(&prs);
            let stacks = compute_stacks(&prs, "main", &descendants);

            // Stack ends at #1 due to fan-out
            assert_eq!(stacks.len(), 1);
            assert_eq!(stacks[0].prs, vec![PrNumber(1)]);
        }

        #[test]
        fn multiple_independent_stacks() {
            // main <- #1 and main <- #2 (independent)
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, "main", None));
            prs.insert(PrNumber(2), make_open_pr(2, "main", None));

            let descendants = build_descendants_index(&prs);
            let stacks = compute_stacks(&prs, "main", &descendants);

            assert_eq!(stacks.len(), 2);
            // Order may vary, so check both are present
            let stack_roots: HashSet<_> = stacks.iter().map(|s| s.root().unwrap()).collect();
            assert!(stack_roots.contains(&PrNumber(1)));
            assert!(stack_roots.contains(&PrNumber(2)));
        }

        #[test]
        fn closed_prs_excluded() {
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, "main", None));
            prs.insert(
                PrNumber(2),
                make_pr(2, "branch-1", Some(1), PrState::Closed),
            );

            let descendants = build_descendants_index(&prs);
            let stacks = compute_stacks(&prs, "main", &descendants);

            // Only #1 in the stack, #2 is closed
            assert_eq!(stacks.len(), 1);
            assert_eq!(stacks[0].prs, vec![PrNumber(1)]);
        }
    }

    mod detect_cycle_tests {
        use super::*;

        #[test]
        fn no_cycle_in_linear_chain() {
            // #1 -> #2 -> #3 (no cycle)
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, "main", None));
            prs.insert(PrNumber(2), make_open_pr(2, "branch-1", Some(1)));
            prs.insert(PrNumber(3), make_open_pr(3, "branch-2", Some(2)));

            assert!(detect_cycle(&prs).is_none());
        }

        #[test]
        fn simple_cycle() {
            // #1 -> #2 -> #1 (cycle)
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, "branch-2", Some(2)));
            prs.insert(PrNumber(2), make_open_pr(2, "branch-1", Some(1)));

            let cycle = detect_cycle(&prs);
            assert!(cycle.is_some());
            let cycle = cycle.unwrap();
            assert!(cycle.contains(&PrNumber(1)));
            assert!(cycle.contains(&PrNumber(2)));
        }

        #[test]
        fn self_loop() {
            // #1 -> #1 (self-loop)
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, "branch-1", Some(1)));

            let cycle = detect_cycle(&prs);
            assert!(cycle.is_some());
            let cycle = cycle.unwrap();
            assert!(cycle.contains(&PrNumber(1)));
        }

        #[test]
        fn no_cycle_with_multiple_independent_chains() {
            // Two independent chains, no cycles
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, "main", None));
            prs.insert(PrNumber(2), make_open_pr(2, "branch-1", Some(1)));
            prs.insert(PrNumber(3), make_open_pr(3, "main", None));
            prs.insert(PrNumber(4), make_open_pr(4, "branch-3", Some(3)));

            assert!(detect_cycle(&prs).is_none());
        }

        #[test]
        fn cycle_in_larger_graph() {
            // #1 -> #2 -> #3 -> #2 (cycle in #2, #3)
            let mut prs = HashMap::new();
            prs.insert(PrNumber(1), make_open_pr(1, "main", None));
            prs.insert(PrNumber(2), make_open_pr(2, "branch-3", Some(3)));
            prs.insert(PrNumber(3), make_open_pr(3, "branch-2", Some(2)));

            let cycle = detect_cycle(&prs);
            assert!(cycle.is_some());
        }
    }

    mod property_tests {
        use super::*;

        prop_compose! {
            fn arb_open_pr_with_predecessor(
                valid_predecessors: Vec<PrNumber>
            )(
                number in 1u64..100,
                pred_idx in prop::option::of(0usize..valid_predecessors.len().max(1))
            ) -> CachedPr {
                let predecessor = pred_idx
                    .filter(|_| !valid_predecessors.is_empty())
                    .map(|i| valid_predecessors[i % valid_predecessors.len()]);
                make_open_pr(number, "feature", predecessor.map(|p| p.0))
            }
        }

        proptest! {
            #[test]
            fn is_root_requires_default_branch(base_ref in "[a-z]{3,10}") {
                let pr = make_open_pr(1, &base_ref, None);
                let prs = HashMap::from([(PrNumber(1), pr.clone())]);

                let result = is_root(&pr, "main", &prs);

                // Only true if base_ref happens to be "main"
                prop_assert_eq!(result, base_ref == "main");
            }

            #[test]
            fn compute_stacks_returns_only_connected_components(
                pr_count in 1usize..10
            ) {
                // Generate PRs with some targeting main (roots) and others chained
                let mut prs = HashMap::new();

                // First PR is always a root
                prs.insert(PrNumber(1), make_open_pr(1, "main", None));

                // Subsequent PRs chain off previous ones
                for i in 2..=pr_count as u64 {
                    let pred = if i <= 3 { 1 } else { i - 1 };
                    let mut pr = make_open_pr(i, &format!("branch-{}", pred), Some(pred));
                    pr.head_ref = format!("branch-{}", i);
                    prs.insert(PrNumber(i), pr);
                }

                let descendants = build_descendants_index(&prs);
                let stacks = compute_stacks(&prs, "main", &descendants);

                // All stacks should start with a root
                for stack in &stacks {
                    if let Some(root) = stack.root() {
                        let root_pr = prs.get(&root).unwrap();
                        prop_assert!(is_root(root_pr, "main", &prs));
                    }
                }
            }

            #[test]
            fn detect_cycle_finds_cycles_in_cyclic_graphs(
                cycle_size in 2usize..5
            ) {
                // Create a cycle: #1 -> #2 -> ... -> #n -> #1
                let mut prs = HashMap::new();
                for i in 1..=cycle_size as u64 {
                    let pred = if i == 1 { cycle_size as u64 } else { i - 1 };
                    prs.insert(
                        PrNumber(i),
                        make_open_pr(i, &format!("branch-{}", pred), Some(pred)),
                    );
                }

                let cycle = detect_cycle(&prs);
                prop_assert!(cycle.is_some());

                // Cycle should contain at least 2 elements
                let cycle = cycle.unwrap();
                prop_assert!(cycle.len() >= 2);
            }

            #[test]
            fn detect_cycle_returns_none_for_acyclic_graphs(
                chain_length in 1usize..10
            ) {
                // Create a simple chain: #1 <- #2 <- ... <- #n
                let mut prs = HashMap::new();
                prs.insert(PrNumber(1), make_open_pr(1, "main", None));

                for i in 2..=chain_length as u64 {
                    let mut pr = make_open_pr(i, &format!("branch-{}", i - 1), Some(i - 1));
                    pr.head_ref = format!("branch-{}", i);
                    prs.insert(PrNumber(i), pr);
                }

                prop_assert!(detect_cycle(&prs).is_none());
            }
        }
    }
}
