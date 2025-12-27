//! Validation logic for predecessor declarations and cascade preconditions.
//!
//! Pure functions for validating that predecessor relationships are correct
//! and that PRs are in valid states for cascade operations.

use std::collections::HashMap;

use crate::types::{CachedPr, PrNumber, PrState};

/// Error types for predecessor validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredecessorValidationError {
    /// The predecessor PR does not exist or is not in the cache.
    PredecessorNotFound { predecessor: PrNumber },

    /// The predecessor has no predecessor declaration and does not target the default branch.
    /// This means the predecessor is not part of a valid stack.
    PredecessorNotInStack {
        predecessor: PrNumber,
        predecessor_base: String,
        default_branch: String,
    },

    /// The PR's base branch does not match the predecessor's head branch.
    /// The PR must actually be stacked on the predecessor, not just claiming to be.
    BaseBranchMismatch {
        pr: PrNumber,
        pr_base: String,
        predecessor: PrNumber,
        predecessor_head: String,
    },

    /// The predecessor is closed without being merged.
    PredecessorClosed { predecessor: PrNumber },

    /// A cycle would be created by this declaration.
    CycleDetected { cycle: Vec<PrNumber> },

    /// The PR already has a predecessor declaration.
    AlreadyHasPredecessor {
        pr: PrNumber,
        existing_predecessor: PrNumber,
    },
}

impl std::fmt::Display for PredecessorValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PredecessorValidationError::PredecessorNotFound { predecessor } => {
                write!(f, "Predecessor {} not found", predecessor)
            }
            PredecessorValidationError::PredecessorNotInStack {
                predecessor,
                predecessor_base,
                default_branch,
            } => {
                write!(
                    f,
                    "Predecessor {} targets '{}' but has no predecessor declaration. \
                     It must either target '{}' or have its own predecessor.",
                    predecessor, predecessor_base, default_branch
                )
            }
            PredecessorValidationError::BaseBranchMismatch {
                pr,
                pr_base,
                predecessor,
                predecessor_head,
            } => {
                write!(
                    f,
                    "PR {} declares predecessor {}, but its base branch '{}' \
                     doesn't match {}'s head branch '{}'. \
                     The PR must be based on the predecessor's branch.",
                    pr, predecessor, pr_base, predecessor, predecessor_head
                )
            }
            PredecessorValidationError::PredecessorClosed { predecessor } => {
                write!(
                    f,
                    "Predecessor {} was closed without merging. \
                     The predecessor must be open or merged.",
                    predecessor
                )
            }
            PredecessorValidationError::CycleDetected { cycle } => {
                let cycle_str: Vec<String> = cycle.iter().map(|p| p.to_string()).collect();
                write!(
                    f,
                    "Cycle detected in predecessor chain: {}",
                    cycle_str.join(" -> ")
                )
            }
            PredecessorValidationError::AlreadyHasPredecessor {
                pr,
                existing_predecessor,
            } => {
                write!(
                    f,
                    "PR {} already has predecessor declaration pointing to {}. \
                     Edit or delete that declaration first.",
                    pr, existing_predecessor
                )
            }
        }
    }
}

impl std::error::Error for PredecessorValidationError {}

/// Validates a predecessor declaration.
///
/// This is called when a `@merge-train predecessor #N` comment is posted.
/// It performs all the validations required by DESIGN.md:
///
/// 1. Validates that the predecessor exists and is open or merged
/// 2. Validates that the predecessor either targets the default branch OR
///    itself has a predecessor declaration (i.e., is part of a valid stack)
/// 3. Validates that the PR's base branch matches the predecessor's head branch
/// 4. Checks for cycles
///
/// Returns `Ok(())` if the declaration is valid, or an error describing the problem.
pub fn validate_predecessor_declaration(
    pr: &CachedPr,
    predecessor_number: PrNumber,
    prs: &HashMap<PrNumber, CachedPr>,
    default_branch: &str,
) -> Result<(), PredecessorValidationError> {
    // Check if PR already has a predecessor
    if let Some(existing) = pr.predecessor {
        return Err(PredecessorValidationError::AlreadyHasPredecessor {
            pr: pr.number,
            existing_predecessor: existing,
        });
    }

    // Look up the predecessor
    let predecessor =
        prs.get(&predecessor_number)
            .ok_or(PredecessorValidationError::PredecessorNotFound {
                predecessor: predecessor_number,
            })?;

    // Check predecessor state
    match &predecessor.state {
        PrState::Open => { /* OK */ }
        PrState::Merged { .. } => {
            // Merged predecessor is OK - this becomes a "resolved" predecessor
            // The PR may be a late addition
        }
        PrState::Closed => {
            return Err(PredecessorValidationError::PredecessorClosed {
                predecessor: predecessor_number,
            });
        }
    }

    // Validate predecessor is part of a valid stack:
    // It must either target the default branch OR have its own predecessor
    if predecessor.base_ref != default_branch && predecessor.predecessor.is_none() {
        return Err(PredecessorValidationError::PredecessorNotInStack {
            predecessor: predecessor_number,
            predecessor_base: predecessor.base_ref.clone(),
            default_branch: default_branch.to_string(),
        });
    }

    // Validate base branch matches predecessor's head branch
    // (only for open predecessors - merged predecessors may have been retargeted)
    if predecessor.state.is_open() && pr.base_ref != predecessor.head_ref {
        return Err(PredecessorValidationError::BaseBranchMismatch {
            pr: pr.number,
            pr_base: pr.base_ref.clone(),
            predecessor: predecessor_number,
            predecessor_head: predecessor.head_ref.clone(),
        });
    }

    // Check for cycles: would adding this edge create a cycle?
    // Walk from predecessor up the chain, checking if we reach this PR
    if would_create_cycle(pr.number, predecessor_number, prs) {
        let cycle = find_cycle_path(pr.number, predecessor_number, prs);
        return Err(PredecessorValidationError::CycleDetected { cycle });
    }

    Ok(())
}

/// Validates that a PR's base branch still matches its predecessor's head branch.
///
/// This is called:
/// - On `pull_request.edited` events when the base branch changes
/// - At cascade time, before entering the Preparing phase
///
/// Returns `Ok(())` if valid, or a `BaseBranchMismatch` error.
///
/// Note: If the predecessor is already merged, base branch mismatch is expected
/// (the cascade retargets descendants to main). In this case, validation passes.
pub fn validate_base_branch_matches_predecessor(
    pr: &CachedPr,
    prs: &HashMap<PrNumber, CachedPr>,
) -> Result<(), PredecessorValidationError> {
    let predecessor_number = match pr.predecessor {
        Some(pred) => pred,
        None => return Ok(()), // No predecessor, nothing to validate
    };

    let predecessor = match prs.get(&predecessor_number) {
        Some(p) => p,
        None => {
            // Predecessor not found - this is a data integrity issue
            // but we don't fail here; is_root will handle it
            return Ok(());
        }
    };

    // If predecessor is merged, base mismatch is expected (post-cascade state)
    if predecessor.state.is_merged() {
        return Ok(());
    }

    // If predecessor is closed (not merged), the stack is broken
    // We don't validate here - let the caller handle the closed predecessor
    if !predecessor.state.is_open() {
        return Ok(());
    }

    // Predecessor is open - base must match head
    if pr.base_ref != predecessor.head_ref {
        return Err(PredecessorValidationError::BaseBranchMismatch {
            pr: pr.number,
            pr_base: pr.base_ref.clone(),
            predecessor: predecessor_number,
            predecessor_head: predecessor.head_ref.clone(),
        });
    }

    Ok(())
}

/// Checks if adding an edge from `pr` to `predecessor` would create a cycle.
fn would_create_cycle(
    pr: PrNumber,
    predecessor: PrNumber,
    prs: &HashMap<PrNumber, CachedPr>,
) -> bool {
    // Walk from predecessor up the chain
    let mut current = Some(predecessor);
    let mut visited = std::collections::HashSet::new();

    while let Some(curr) = current {
        if curr == pr {
            return true; // Found a cycle
        }
        if visited.contains(&curr) {
            break; // Already visited, no cycle through here
        }
        visited.insert(curr);

        current = prs.get(&curr).and_then(|p| p.predecessor);
    }

    false
}

/// Finds the cycle path if one exists.
fn find_cycle_path(
    pr: PrNumber,
    predecessor: PrNumber,
    prs: &HashMap<PrNumber, CachedPr>,
) -> Vec<PrNumber> {
    let mut path = vec![pr, predecessor];
    let mut current = prs.get(&predecessor).and_then(|p| p.predecessor);

    while let Some(curr) = current {
        path.push(curr);
        if curr == pr {
            break;
        }
        current = prs.get(&curr).and_then(|p| p.predecessor);
    }

    path
}

/// Validates that a squash merge can be performed.
///
/// Checks that:
/// - The merge commit has exactly one parent (is a squash, not a merge commit)
/// - The parent is the expected prior main HEAD
///
/// This is critical for late-addition reconciliation where we need to use
/// $SHA^ (parent of squash) for the ours-merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SquashValidation {
    /// The SHA of the squash commit.
    pub squash_sha: String,
    /// The SHA of the parent (prior main HEAD).
    pub parent_sha: String,
}

/// Error returned when squash validation fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SquashValidationError {
    /// The commit has multiple parents (is a merge commit, not squash).
    NotASquash { commit: String, parent_count: usize },

    /// The parent is not the expected prior main HEAD.
    WrongParent {
        commit: String,
        actual_parent: String,
        expected_parent: String,
    },
}

impl std::fmt::Display for SquashValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SquashValidationError::NotASquash {
                commit,
                parent_count,
            } => {
                write!(
                    f,
                    "Commit {} has {} parents; expected exactly 1 for a squash merge",
                    commit, parent_count
                )
            }
            SquashValidationError::WrongParent {
                commit,
                actual_parent,
                expected_parent,
            } => {
                write!(
                    f,
                    "Commit {}'s parent is {}, expected {} (prior main HEAD)",
                    commit, actual_parent, expected_parent
                )
            }
        }
    }
}

impl std::error::Error for SquashValidationError {}

/// Validates that a commit is a valid squash commit.
///
/// A valid squash commit:
/// 1. Has exactly one parent
/// 2. That parent is the expected prior main HEAD
///
/// This validation is required for late-addition reconciliation.
pub fn validate_squash_commit(
    commit_sha: &str,
    parents: &[String],
    expected_prior_main: &str,
) -> Result<SquashValidation, SquashValidationError> {
    if parents.len() != 1 {
        return Err(SquashValidationError::NotASquash {
            commit: commit_sha.to_string(),
            parent_count: parents.len(),
        });
    }

    let parent = &parents[0];
    if parent != expected_prior_main {
        return Err(SquashValidationError::WrongParent {
            commit: commit_sha.to_string(),
            actual_parent: parent.clone(),
            expected_parent: expected_prior_main.to_string(),
        });
    }

    Ok(SquashValidation {
        squash_sha: commit_sha.to_string(),
        parent_sha: parent.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MergeStateStatus, Sha};

    fn make_open_pr(
        number: u64,
        head_ref: &str,
        base_ref: &str,
        predecessor: Option<u64>,
    ) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::new("abc123def456789012345678901234567890abcd"),
            head_ref.to_string(),
            base_ref.to_string(),
            predecessor.map(PrNumber),
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        )
    }

    fn make_merged_pr(number: u64, head_ref: &str, merge_sha: &str) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::new("abc123def456789012345678901234567890abcd"),
            head_ref.to_string(),
            "main".to_string(),
            None,
            PrState::Merged {
                merge_commit_sha: Sha::new(merge_sha),
            },
            MergeStateStatus::Clean,
            false,
        )
    }

    fn make_closed_pr(number: u64) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::new("abc123def456789012345678901234567890abcd"),
            "branch".to_string(),
            "main".to_string(),
            None,
            PrState::Closed,
            MergeStateStatus::Clean,
            false,
        )
    }

    mod validate_predecessor_declaration_tests {
        use super::*;

        #[test]
        fn valid_declaration_targeting_main() {
            // Predecessor targets main, PR is based on predecessor's head
            let predecessor = make_open_pr(1, "feature-1", "main", None);
            let pr = make_open_pr(2, "feature-2", "feature-1", None);

            let prs = HashMap::from([(PrNumber(1), predecessor), (PrNumber(2), pr.clone())]);

            let result = validate_predecessor_declaration(&pr, PrNumber(1), &prs, "main");
            assert!(result.is_ok());
        }

        #[test]
        fn valid_declaration_chained() {
            // #1 targets main, #2 targets #1, #3 targets #2
            let pr1 = make_open_pr(1, "feature-1", "main", None);
            let pr2 = make_open_pr(2, "feature-2", "feature-1", Some(1));
            let pr3 = make_open_pr(3, "feature-3", "feature-2", None);

            let prs = HashMap::from([
                (PrNumber(1), pr1),
                (PrNumber(2), pr2),
                (PrNumber(3), pr3.clone()),
            ]);

            let result = validate_predecessor_declaration(&pr3, PrNumber(2), &prs, "main");
            assert!(result.is_ok());
        }

        #[test]
        fn predecessor_not_found() {
            let pr = make_open_pr(2, "feature-2", "feature-1", None);
            let prs = HashMap::from([(PrNumber(2), pr.clone())]);

            let result = validate_predecessor_declaration(&pr, PrNumber(1), &prs, "main");

            assert!(matches!(
                result,
                Err(PredecessorValidationError::PredecessorNotFound {
                    predecessor: PrNumber(1)
                })
            ));
        }

        #[test]
        fn predecessor_not_in_stack() {
            // Predecessor targets a feature branch but has no predecessor itself
            let predecessor = make_open_pr(1, "feature-1", "other-branch", None);
            let pr = make_open_pr(2, "feature-2", "feature-1", None);

            let prs = HashMap::from([(PrNumber(1), predecessor), (PrNumber(2), pr.clone())]);

            let result = validate_predecessor_declaration(&pr, PrNumber(1), &prs, "main");

            assert!(matches!(
                result,
                Err(PredecessorValidationError::PredecessorNotInStack { .. })
            ));
        }

        #[test]
        fn base_branch_mismatch() {
            let predecessor = make_open_pr(1, "feature-1", "main", None);
            let pr = make_open_pr(2, "feature-2", "wrong-branch", None);

            let prs = HashMap::from([(PrNumber(1), predecessor), (PrNumber(2), pr.clone())]);

            let result = validate_predecessor_declaration(&pr, PrNumber(1), &prs, "main");

            assert!(matches!(
                result,
                Err(PredecessorValidationError::BaseBranchMismatch { .. })
            ));
        }

        #[test]
        fn predecessor_closed() {
            let predecessor = make_closed_pr(1);
            let pr = make_open_pr(2, "feature-2", "branch", None);

            let prs = HashMap::from([(PrNumber(1), predecessor), (PrNumber(2), pr.clone())]);

            let result = validate_predecessor_declaration(&pr, PrNumber(1), &prs, "main");

            assert!(matches!(
                result,
                Err(PredecessorValidationError::PredecessorClosed { .. })
            ));
        }

        #[test]
        fn predecessor_merged_is_ok() {
            let predecessor =
                make_merged_pr(1, "feature-1", "abc123def456789012345678901234567890abcd");
            // When predecessor is merged, base_ref mismatch is expected
            let pr = make_open_pr(2, "feature-2", "main", None);

            let prs = HashMap::from([(PrNumber(1), predecessor), (PrNumber(2), pr.clone())]);

            let result = validate_predecessor_declaration(&pr, PrNumber(1), &prs, "main");
            assert!(result.is_ok());
        }

        #[test]
        fn already_has_predecessor() {
            let predecessor = make_open_pr(1, "feature-1", "main", None);
            let pr = make_open_pr(2, "feature-2", "feature-1", Some(3));

            let prs = HashMap::from([(PrNumber(1), predecessor), (PrNumber(2), pr.clone())]);

            let result = validate_predecessor_declaration(&pr, PrNumber(1), &prs, "main");

            assert!(matches!(
                result,
                Err(PredecessorValidationError::AlreadyHasPredecessor {
                    existing_predecessor: PrNumber(3),
                    ..
                })
            ));
        }

        #[test]
        fn cycle_detection() {
            // #1 -> #2 -> #1 would be a cycle
            let pr1 = make_open_pr(1, "feature-1", "feature-2", Some(2));
            let pr2 = make_open_pr(2, "feature-2", "feature-1", None);

            let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2.clone())]);

            // Trying to set #2's predecessor to #1 would create cycle
            let result = validate_predecessor_declaration(&pr2, PrNumber(1), &prs, "main");

            assert!(matches!(
                result,
                Err(PredecessorValidationError::CycleDetected { .. })
            ));
        }
    }

    mod validate_base_branch_tests {
        use super::*;

        #[test]
        fn no_predecessor_always_valid() {
            let pr = make_open_pr(1, "feature", "main", None);
            let prs = HashMap::from([(PrNumber(1), pr.clone())]);

            assert!(validate_base_branch_matches_predecessor(&pr, &prs).is_ok());
        }

        #[test]
        fn matching_base_is_valid() {
            let predecessor = make_open_pr(1, "feature-1", "main", None);
            let pr = make_open_pr(2, "feature-2", "feature-1", Some(1));

            let prs = HashMap::from([(PrNumber(1), predecessor), (PrNumber(2), pr.clone())]);

            assert!(validate_base_branch_matches_predecessor(&pr, &prs).is_ok());
        }

        #[test]
        fn mismatched_base_is_invalid() {
            let predecessor = make_open_pr(1, "feature-1", "main", None);
            let pr = make_open_pr(2, "feature-2", "wrong-branch", Some(1));

            let prs = HashMap::from([(PrNumber(1), predecessor), (PrNumber(2), pr.clone())]);

            let result = validate_base_branch_matches_predecessor(&pr, &prs);
            assert!(matches!(
                result,
                Err(PredecessorValidationError::BaseBranchMismatch { .. })
            ));
        }

        #[test]
        fn merged_predecessor_allows_mismatch() {
            let predecessor =
                make_merged_pr(1, "feature-1", "abc123def456789012345678901234567890abcd");
            let pr = make_open_pr(2, "feature-2", "main", Some(1));

            let prs = HashMap::from([(PrNumber(1), predecessor), (PrNumber(2), pr.clone())]);

            // After cascade, descendant targets main but still has predecessor declaration
            // This is the expected post-cascade state
            assert!(validate_base_branch_matches_predecessor(&pr, &prs).is_ok());
        }
    }

    mod validate_squash_tests {
        use super::*;

        #[test]
        fn valid_squash_commit() {
            let result = validate_squash_commit("abc123", &["parent123".to_string()], "parent123");

            assert!(result.is_ok());
            let validation = result.unwrap();
            assert_eq!(validation.squash_sha, "abc123");
            assert_eq!(validation.parent_sha, "parent123");
        }

        #[test]
        fn merge_commit_has_two_parents() {
            let result = validate_squash_commit(
                "abc123",
                &["parent1".to_string(), "parent2".to_string()],
                "parent1",
            );

            assert!(matches!(
                result,
                Err(SquashValidationError::NotASquash {
                    parent_count: 2,
                    ..
                })
            ));
        }

        #[test]
        fn wrong_parent() {
            let result =
                validate_squash_commit("abc123", &["wrong_parent".to_string()], "expected_parent");

            assert!(matches!(
                result,
                Err(SquashValidationError::WrongParent { .. })
            ));
        }

        #[test]
        fn no_parents() {
            let result = validate_squash_commit("abc123", &[], "parent123");

            assert!(matches!(
                result,
                Err(SquashValidationError::NotASquash {
                    parent_count: 0,
                    ..
                })
            ));
        }
    }

    mod property_tests {
        use super::*;
        use proptest::prelude::*;

        fn arb_branch_name() -> impl Strategy<Value = String> {
            "[a-z][a-z0-9-]{1,20}".prop_map(String::from)
        }

        proptest! {
            /// Base branch validation always passes for PRs without predecessors.
            #[test]
            fn no_predecessor_always_valid(
                head in arb_branch_name(),
                base in arb_branch_name()
            ) {
                let pr = make_open_pr(1, &head, &base, None);
                let prs = HashMap::from([(PrNumber(1), pr.clone())]);

                prop_assert!(validate_base_branch_matches_predecessor(&pr, &prs).is_ok());
            }

            /// Cycle detection prevents self-loops.
            #[test]
            fn self_loop_detected(
                head in arb_branch_name()
            ) {
                let pr = make_open_pr(1, &head, &head, None);
                let prs = HashMap::from([(PrNumber(1), pr.clone())]);

                // Can't declare self as predecessor (would be a cycle)
                prop_assert!(would_create_cycle(PrNumber(1), PrNumber(1), &prs));
            }

            /// Valid squash commits have exactly one parent matching expected.
            #[test]
            fn valid_squash_has_matching_parent(
                sha in "[0-9a-f]{40}",
                parent in "[0-9a-f]{40}"
            ) {
                let result = validate_squash_commit(&sha, &[parent.clone()], &parent);
                prop_assert!(result.is_ok());
            }

            /// Squash validation fails when parent doesn't match.
            #[test]
            fn squash_fails_on_wrong_parent(
                sha in "[0-9a-f]{40}",
                parent1 in "[0-9a-f]{40}",
                parent2 in "[0-9a-f]{40}"
            ) {
                prop_assume!(parent1 != parent2);

                let result = validate_squash_commit(&sha, &[parent1], &parent2);
                let is_wrong_parent = matches!(result, Err(SquashValidationError::WrongParent { .. }));
                prop_assert!(is_wrong_parent, "Expected WrongParent error, got {:?}", result);
            }
        }
    }
}
