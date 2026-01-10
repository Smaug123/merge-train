//! Phase-specific logic for the cascade state machine.
//!
//! Each phase of the cascade has specific requirements and behaviors:
//!
//! - **Preparing**: Merge predecessor head into descendants (before squash)
//! - **SquashPending**: Ready to squash-merge the current PR
//! - **Reconciling**: Merge squash commit into descendants using ours-strategy
//! - **CatchingUp**: Merge origin/main into descendants
//! - **Retargeting**: Update descendant PR base branches via GitHub API

use std::collections::HashMap;

use crate::effects::{Effect, GitEffect, GitHubEffect, MergeStrategy};
use crate::types::{AbortReason, CachedPr, CascadePhase, DescendantProgress, PrNumber, Sha};

/// Action to take for a specific phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhaseAction {
    /// Prepare a descendant by merging predecessor head.
    Prepare {
        descendant: PrNumber,
        predecessor_head: Sha,
        target_branch: String,
    },

    /// Squash-merge the current PR.
    Squash { pr: PrNumber, expected_sha: Sha },

    /// Reconcile a descendant with the squash commit.
    Reconcile {
        descendant: PrNumber,
        squash_sha: Sha,
        default_branch: String,
        target_branch: String,
    },

    /// Catch up a descendant by merging origin/main.
    CatchUp {
        descendant: PrNumber,
        default_branch: String,
        target_branch: String,
    },

    /// Retarget a descendant PR to the default branch.
    Retarget {
        descendant: PrNumber,
        new_base: String,
    },

    /// Phase is complete, ready to transition.
    PhaseComplete,

    /// Waiting (no action needed right now).
    Wait,
}

/// Result of executing a phase action.
#[derive(Debug, Clone)]
pub enum PhaseExecutionResult {
    /// Action succeeded, descendant was processed.
    Success { descendant: PrNumber },

    /// Action succeeded, PR was squash-merged.
    Squashed { pr: PrNumber, squash_sha: Sha },

    /// Action failed due to merge conflict.
    MergeConflict {
        descendant: PrNumber,
        details: String,
    },

    /// Action failed due to push rejection.
    PushRejected {
        descendant: PrNumber,
        details: String,
    },

    /// Action failed because the PR/descendant is no longer open.
    PrClosed { pr: PrNumber },

    /// Action failed due to API error.
    ApiError { details: String },
}

impl PhaseExecutionResult {
    /// Returns true if the result indicates success.
    pub fn is_success(&self) -> bool {
        matches!(
            self,
            PhaseExecutionResult::Success { .. } | PhaseExecutionResult::Squashed { .. }
        )
    }

    /// Converts a failure result to an abort reason.
    pub fn to_abort_reason(&self) -> Option<AbortReason> {
        match self {
            PhaseExecutionResult::Success { .. } | PhaseExecutionResult::Squashed { .. } => None,
            PhaseExecutionResult::MergeConflict { details, .. } => {
                Some(AbortReason::MergeConflict {
                    details: details.clone(),
                })
            }
            PhaseExecutionResult::PushRejected { details, .. } => Some(AbortReason::PushRejected {
                details: details.clone(),
            }),
            PhaseExecutionResult::PrClosed { .. } => Some(AbortReason::PrClosed),
            PhaseExecutionResult::ApiError { details } => Some(AbortReason::ApiError {
                details: details.clone(),
            }),
        }
    }
}

/// Determines the next action for the Preparing phase.
pub fn preparing_next_action(
    progress: &DescendantProgress,
    current_pr: &CachedPr,
    prs: &HashMap<PrNumber, CachedPr>,
) -> PhaseAction {
    // Find the next descendant to prepare
    for pr_number in progress.remaining() {
        if let Some(desc_pr) = prs.get(pr_number)
            && desc_pr.state.is_open()
        {
            return PhaseAction::Prepare {
                descendant: *pr_number,
                predecessor_head: current_pr.head_sha.clone(),
                target_branch: desc_pr.head_ref.clone(),
            };
        }
        // If PR not found or closed, it will be skipped on the next step
    }

    // All remaining descendants are either done or invalid
    PhaseAction::PhaseComplete
}

/// Determines the next action for the SquashPending phase.
pub fn squash_pending_next_action(current_pr: &CachedPr) -> PhaseAction {
    PhaseAction::Squash {
        pr: current_pr.number,
        expected_sha: current_pr.head_sha.clone(),
    }
}

/// Determines the next action for the Reconciling phase.
pub fn reconciling_next_action(
    progress: &DescendantProgress,
    squash_sha: &Sha,
    default_branch: &str,
    prs: &HashMap<PrNumber, CachedPr>,
) -> PhaseAction {
    for pr_number in progress.remaining() {
        if let Some(desc_pr) = prs.get(pr_number)
            && desc_pr.state.is_open()
        {
            return PhaseAction::Reconcile {
                descendant: *pr_number,
                squash_sha: squash_sha.clone(),
                default_branch: default_branch.to_string(),
                target_branch: desc_pr.head_ref.clone(),
            };
        }
    }

    PhaseAction::PhaseComplete
}

/// Determines the next action for the CatchingUp phase.
pub fn catching_up_next_action(
    progress: &DescendantProgress,
    default_branch: &str,
    prs: &HashMap<PrNumber, CachedPr>,
) -> PhaseAction {
    for pr_number in progress.remaining() {
        if let Some(desc_pr) = prs.get(pr_number)
            && desc_pr.state.is_open()
        {
            return PhaseAction::CatchUp {
                descendant: *pr_number,
                default_branch: default_branch.to_string(),
                target_branch: desc_pr.head_ref.clone(),
            };
        }
    }

    PhaseAction::PhaseComplete
}

/// Determines the next action for the Retargeting phase.
pub fn retargeting_next_action(
    progress: &DescendantProgress,
    default_branch: &str,
    prs: &HashMap<PrNumber, CachedPr>,
) -> PhaseAction {
    for pr_number in progress.remaining() {
        if let Some(desc_pr) = prs.get(pr_number)
            && desc_pr.state.is_open()
        {
            return PhaseAction::Retarget {
                descendant: *pr_number,
                new_base: default_branch.to_string(),
            };
        }
    }

    PhaseAction::PhaseComplete
}

/// Determines the next action based on the current phase.
pub fn next_action(
    phase: &CascadePhase,
    current_pr: &CachedPr,
    prs: &HashMap<PrNumber, CachedPr>,
    default_branch: &str,
) -> PhaseAction {
    match phase {
        CascadePhase::Idle => PhaseAction::Wait,

        CascadePhase::Preparing { progress } => preparing_next_action(progress, current_pr, prs),

        CascadePhase::SquashPending { .. } => squash_pending_next_action(current_pr),

        CascadePhase::Reconciling {
            progress,
            squash_sha,
        } => reconciling_next_action(progress, squash_sha, default_branch, prs),

        CascadePhase::CatchingUp { progress, .. } => {
            catching_up_next_action(progress, default_branch, prs)
        }

        CascadePhase::Retargeting { progress, .. } => {
            retargeting_next_action(progress, default_branch, prs)
        }
    }
}

/// Convert a PhaseAction to Effects.
///
/// **WARNING**: This function produces a MINIMAL effect set that omits Fetch and
/// Checkout effects. The actual `execute_cascade_step` in step.rs builds a more
/// complete sequence with Fetch/Checkout to ensure operations run on fresh refs.
///
/// If you use this function directly, you MUST prepend appropriate Fetch and
/// Checkout effects yourself. Otherwise operations may run on stale local refs.
///
/// This function is primarily useful for testing effect serialization or for
/// contexts where the caller handles Fetch/Checkout separately.
pub fn action_to_effects(action: &PhaseAction) -> Vec<Effect> {
    match action {
        PhaseAction::Prepare {
            predecessor_head,
            target_branch,
            ..
        } => {
            // NOTE: This is INCOMPLETE - does not include Fetch and Checkout!
            // execute_cascade_step adds those effects before Merge.
            // See step.rs execute_preparing() for the complete sequence.
            vec![
                Effect::Git(GitEffect::Merge {
                    target: predecessor_head.as_str().to_string(),
                    strategy: MergeStrategy::Default,
                    message: format!(
                        "Merge predecessor into {} (merge train preparation)",
                        target_branch
                    ),
                }),
                Effect::Git(GitEffect::Push {
                    refspec: format!("HEAD:refs/heads/{}", target_branch),
                    force: false,
                }),
            ]
        }

        PhaseAction::Squash { pr, expected_sha } => {
            vec![Effect::GitHub(GitHubEffect::SquashMerge {
                pr: *pr,
                expected_sha: expected_sha.clone(),
            })]
        }

        PhaseAction::Reconcile {
            squash_sha,
            default_branch,
            target_branch,
            ..
        } => {
            // NOTE: This function is not currently used by execute_cascade_step
            // (which builds effects directly with Fetch/Checkout). If used in
            // the future, callers must prepend Fetch and Checkout effects.
            //
            // The interpreter must validate squash_sha even though expected_squash_parent
            // is None. See MergeReconcile docs for validation requirements.
            vec![
                Effect::Git(GitEffect::MergeReconcile {
                    squash_sha: squash_sha.clone(),
                    expected_squash_parent: None,
                    default_branch: default_branch.clone(),
                    target_branch: target_branch.clone(),
                }),
                Effect::Git(GitEffect::Push {
                    refspec: format!("HEAD:refs/heads/{}", target_branch),
                    force: false,
                }),
            ]
        }

        PhaseAction::CatchUp {
            default_branch,
            target_branch,
            ..
        } => {
            // NOTE: This is INCOMPLETE - does not include Fetch and Checkout!
            // execute_cascade_step adds those effects before Merge.
            // See step.rs execute_catching_up() for the complete sequence.
            vec![
                Effect::Git(GitEffect::Merge {
                    target: format!("origin/{}", default_branch),
                    strategy: MergeStrategy::Default,
                    message: format!(
                        "Merge {} into {} (merge train catch-up)",
                        default_branch, target_branch
                    ),
                }),
                Effect::Git(GitEffect::Push {
                    refspec: format!("HEAD:refs/heads/{}", target_branch),
                    force: false,
                }),
            ]
        }

        PhaseAction::Retarget {
            descendant,
            new_base,
        } => {
            vec![Effect::GitHub(GitHubEffect::RetargetPr {
                pr: *descendant,
                new_base: new_base.clone(),
            })]
        }

        PhaseAction::PhaseComplete | PhaseAction::Wait => vec![],
    }
}

/// Validates that a phase transition is valid.
///
/// This performs the same check as `CascadePhase::can_transition_to`, but
/// returns a Result with a descriptive error message.
pub fn validate_transition(from: &CascadePhase, to: &CascadePhase) -> Result<(), String> {
    if from.can_transition_to(to) {
        Ok(())
    } else {
        Err(format!(
            "Invalid phase transition: {} -> {}",
            from.name(),
            to.name()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MergeStateStatus, PrState};

    fn make_sha(n: u64) -> Sha {
        Sha::parse(format!("{:0>40x}", n)).unwrap()
    }

    fn make_open_pr(number: u64, base_ref: &str, predecessor: Option<u64>) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            make_sha(number),
            format!("branch-{}", number),
            base_ref.to_string(),
            predecessor.map(PrNumber),
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        )
    }

    #[test]
    fn preparing_action_finds_first_remaining_descendant() {
        let mut progress = DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]);
        let current_pr = make_open_pr(1, "main", None);
        let pr2 = make_open_pr(2, "branch-1", Some(1));
        let pr3 = make_open_pr(3, "branch-1", Some(1));

        let prs = HashMap::from([
            (PrNumber(1), current_pr.clone()),
            (PrNumber(2), pr2),
            (PrNumber(3), pr3),
        ]);

        let action = preparing_next_action(&progress, &current_pr, &prs);

        // Should target the first remaining descendant
        assert!(matches!(
            action,
            PhaseAction::Prepare {
                descendant: PrNumber(2),
                ..
            }
        ));

        // After marking #2 as completed, should target #3
        progress.mark_completed(PrNumber(2));
        let action = preparing_next_action(&progress, &current_pr, &prs);
        assert!(matches!(
            action,
            PhaseAction::Prepare {
                descendant: PrNumber(3),
                ..
            }
        ));

        // After marking #3 as completed, should be phase complete
        progress.mark_completed(PrNumber(3));
        let action = preparing_next_action(&progress, &current_pr, &prs);
        assert!(matches!(action, PhaseAction::PhaseComplete));
    }

    #[test]
    fn preparing_skips_closed_descendants() {
        let progress = DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]);
        let current_pr = make_open_pr(1, "main", None);

        // PR #2 is closed
        let mut pr2 = make_open_pr(2, "branch-1", Some(1));
        pr2.state = PrState::Closed;
        let pr3 = make_open_pr(3, "branch-1", Some(1));

        let prs = HashMap::from([
            (PrNumber(1), current_pr.clone()),
            (PrNumber(2), pr2),
            (PrNumber(3), pr3),
        ]);

        let action = preparing_next_action(&progress, &current_pr, &prs);

        // Should skip #2 and target #3
        assert!(matches!(
            action,
            PhaseAction::Prepare {
                descendant: PrNumber(3),
                ..
            }
        ));
    }

    #[test]
    fn squash_pending_generates_squash_action() {
        let current_pr = make_open_pr(1, "main", None);

        let action = squash_pending_next_action(&current_pr);

        assert!(matches!(
            action,
            PhaseAction::Squash {
                pr: PrNumber(1),
                ..
            }
        ));
    }

    #[test]
    fn reconciling_action_uses_squash_sha() {
        let progress = DescendantProgress::new(vec![PrNumber(2)]);
        let squash_sha = make_sha(1000);
        let pr2 = make_open_pr(2, "branch-1", Some(1));

        let prs = HashMap::from([(PrNumber(2), pr2)]);

        let action = reconciling_next_action(&progress, &squash_sha, "main", &prs);

        match action {
            PhaseAction::Reconcile {
                descendant,
                squash_sha: action_sha,
                default_branch,
                ..
            } => {
                assert_eq!(descendant, PrNumber(2));
                assert_eq!(action_sha, squash_sha);
                assert_eq!(default_branch, "main");
            }
            _ => panic!("Expected Reconcile action"),
        }
    }

    #[test]
    fn retargeting_action_uses_default_branch() {
        let progress = DescendantProgress::new(vec![PrNumber(2)]);
        let pr2 = make_open_pr(2, "branch-1", Some(1));
        let prs = HashMap::from([(PrNumber(2), pr2)]);

        let action = retargeting_next_action(&progress, "main", &prs);

        match action {
            PhaseAction::Retarget {
                descendant,
                new_base,
            } => {
                assert_eq!(descendant, PrNumber(2));
                assert_eq!(new_base, "main");
            }
            _ => panic!("Expected Retarget action"),
        }
    }

    #[test]
    fn action_to_effects_prepare() {
        let action = PhaseAction::Prepare {
            descendant: PrNumber(2),
            predecessor_head: make_sha(2000),
            target_branch: "branch-2".to_string(),
        };

        let effects = action_to_effects(&action);

        assert_eq!(effects.len(), 2);
        assert!(matches!(effects[0], Effect::Git(GitEffect::Merge { .. })));
        assert!(matches!(effects[1], Effect::Git(GitEffect::Push { .. })));
    }

    #[test]
    fn action_to_effects_squash() {
        let action = PhaseAction::Squash {
            pr: PrNumber(1),
            expected_sha: make_sha(3000),
        };

        let effects = action_to_effects(&action);

        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::GitHub(GitHubEffect::SquashMerge { .. })
        ));
    }

    #[test]
    fn action_to_effects_reconcile() {
        let action = PhaseAction::Reconcile {
            descendant: PrNumber(2),
            squash_sha: make_sha(1000),
            default_branch: "main".to_string(),
            target_branch: "branch-2".to_string(),
        };

        let effects = action_to_effects(&action);

        assert_eq!(effects.len(), 2);
        assert!(matches!(
            &effects[0],
            Effect::Git(GitEffect::MergeReconcile { default_branch, .. }) if default_branch == "main"
        ));
        assert!(matches!(&effects[1], Effect::Git(GitEffect::Push { .. })));
    }

    #[test]
    fn action_to_effects_retarget() {
        let action = PhaseAction::Retarget {
            descendant: PrNumber(2),
            new_base: "main".to_string(),
        };

        let effects = action_to_effects(&action);

        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::GitHub(GitHubEffect::RetargetPr { .. })
        ));
    }

    #[test]
    fn validate_transition_allows_valid() {
        let idle = CascadePhase::Idle;
        let preparing = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![]),
        };

        assert!(validate_transition(&idle, &preparing).is_ok());
    }

    #[test]
    fn validate_transition_rejects_invalid() {
        let idle = CascadePhase::Idle;
        let reconciling = CascadePhase::Reconciling {
            progress: DescendantProgress::new(vec![]),
            squash_sha: make_sha(4000),
        };

        assert!(validate_transition(&idle, &reconciling).is_err());
    }
}
