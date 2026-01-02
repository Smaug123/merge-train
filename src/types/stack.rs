//! Cascade step outcome types.
//!
//! These types represent the possible outcomes of cascade operations and
//! the reasons for blocking or aborting.

use serde::{Deserialize, Serialize};

use super::ids::PrNumber;

/// Reason why a cascade step is blocked and waiting.
///
/// Note: `MergeStateStatus::Dirty` (merge conflicts) is NOT a block reason—it causes
/// an immediate abort with `AbortReason::MergeConflict`. Block reasons are conditions
/// that may resolve on their own or with minor user action, whereas merge conflicts
/// require explicit intervention and cannot auto-resolve.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockReason {
    /// GitHub reports BLOCKED (required checks/approvals not satisfied).
    Blocked,

    /// GitHub reports BEHIND (head branch behind base, strict mode).
    Behind,

    /// User issued `@merge-train stop`.
    Stopped,

    /// PR is still a draft.
    Draft,

    /// GitHub reports UNKNOWN (state not yet computed).
    Unknown,
}

impl BlockReason {
    /// Returns true if this block reason can auto-resolve.
    ///
    /// Most block reasons can auto-resolve when external conditions change
    /// (CI passes, approvals granted, branch updated, etc.). The exception
    /// is `Stopped`, which requires explicit user action to restart.
    pub fn can_auto_resolve(&self) -> bool {
        matches!(
            self,
            BlockReason::Blocked | BlockReason::Behind | BlockReason::Draft | BlockReason::Unknown
        )
    }

    /// Returns true if this block reason requires human intervention.
    ///
    /// Currently only `Stopped` requires intervention—the user must issue
    /// `@merge-train start` to resume.
    pub fn requires_intervention(&self) -> bool {
        matches!(self, BlockReason::Stopped)
    }

    /// Returns a human-readable description of the block reason.
    pub fn description(&self) -> &'static str {
        match self {
            BlockReason::Blocked => "Required checks or approvals not satisfied",
            BlockReason::Behind => "Head branch is behind base branch",
            BlockReason::Stopped => "Train was stopped by user",
            BlockReason::Draft => "PR is a draft",
            BlockReason::Unknown => "GitHub is still computing merge status",
        }
    }
}

/// Reason why a cascade was aborted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum AbortReason {
    /// Merge conflict occurred during cascade operations.
    MergeConflict {
        /// Details about where the conflict occurred.
        details: String,
    },

    /// Git push was rejected (e.g., non-fast-forward).
    PushRejected {
        /// Details about why the push was rejected.
        details: String,
    },

    /// The PR was closed (not merged) during the cascade.
    PrClosed,

    /// Required CI checks failed.
    CiFailed,

    /// A cycle was detected in the predecessor graph.
    CycleDetected,

    /// Review approval was withdrawn or dismissed.
    ReviewDismissed,

    /// Approval was withdrawn (detected via mergeStateStatus, not webhook).
    ApprovalWithdrawn,

    /// GitHub API error.
    ApiError {
        /// Details about the API error.
        details: String,
    },

    /// Branch was deleted during cascade.
    BranchDeleted,

    /// Predecessor was not a squash merge (merge/rebase method used).
    NonSquashMerge {
        /// The predecessor PR number.
        predecessor: PrNumber,
        /// Details about why this is a problem.
        details: String,
    },

    /// Status comment size limit exceeded.
    StatusCommentTooLarge,

    /// Train is too large (exceeds maximum stack size).
    TrainTooLarge {
        /// Number of PRs in the train.
        pr_count: usize,
        /// Maximum allowed.
        max_allowed: usize,
    },

    /// Preparation was not completed before squash.
    PreparationMissing {
        /// The descendant that wasn't prepared.
        descendant: PrNumber,
    },

    /// Repository has merge hooks or merge queue enabled (incompatible config).
    ///
    /// This occurs when GitHub reports `HAS_HOOKS` status, indicating either
    /// GitHub Enterprise pre-receive hooks or GitHub's merge queue is enabled.
    /// Both are incompatible with merge-train (see DESIGN.md non-goals).
    MergeHooksEnabled,

    /// PR base branch doesn't match predecessor's head branch.
    ///
    /// This occurs when a PR was retargeted between cascade start and the
    /// preparation phase. The stack structure no longer matches what was
    /// declared, so the cascade must abort.
    BaseBranchMismatch {
        /// The PR with the mismatched base branch.
        pr: PrNumber,
        /// The expected base branch (predecessor's head branch).
        expected_base: String,
        /// The actual base branch on the PR.
        actual_base: String,
    },
}

impl AbortReason {
    /// Returns the error type string for logging/serialization.
    pub fn error_type(&self) -> &'static str {
        match self {
            AbortReason::MergeConflict { .. } => "merge_conflict",
            AbortReason::PushRejected { .. } => "push_rejected",
            AbortReason::PrClosed => "pr_closed",
            AbortReason::CiFailed => "ci_failed",
            AbortReason::CycleDetected => "cycle_detected",
            AbortReason::ReviewDismissed => "review_dismissed",
            AbortReason::ApprovalWithdrawn => "approval_withdrawn",
            AbortReason::ApiError { .. } => "api_error",
            AbortReason::BranchDeleted => "branch_deleted",
            AbortReason::NonSquashMerge { .. } => "non_squash_merge",
            AbortReason::StatusCommentTooLarge => "status_comment_too_large",
            AbortReason::TrainTooLarge { .. } => "train_too_large",
            AbortReason::PreparationMissing { .. } => "preparation_missing",
            AbortReason::MergeHooksEnabled => "merge_hooks_enabled",
            AbortReason::BaseBranchMismatch { .. } => "base_branch_mismatch",
        }
    }

    /// Returns a human-readable description of the abort reason.
    pub fn description(&self) -> String {
        match self {
            AbortReason::MergeConflict { details } => {
                format!("Merge conflict: {}", details)
            }
            AbortReason::PushRejected { details } => {
                format!("Push rejected: {}", details)
            }
            AbortReason::PrClosed => "PR was closed without merging".to_string(),
            AbortReason::CiFailed => "Required CI checks failed".to_string(),
            AbortReason::CycleDetected => "Cycle detected in predecessor graph".to_string(),
            AbortReason::ReviewDismissed => "Review approval was dismissed".to_string(),
            AbortReason::ApprovalWithdrawn => "Review approval was withdrawn".to_string(),
            AbortReason::ApiError { details } => {
                format!("GitHub API error: {}", details)
            }
            AbortReason::BranchDeleted => "Branch was deleted during cascade".to_string(),
            AbortReason::NonSquashMerge {
                predecessor,
                details,
            } => {
                format!(
                    "Predecessor {} was not squash-merged: {}",
                    predecessor, details
                )
            }
            AbortReason::StatusCommentTooLarge => {
                "Status comment size limit exceeded unexpectedly".to_string()
            }
            AbortReason::TrainTooLarge {
                pr_count,
                max_allowed,
            } => {
                format!(
                    "Train too large: {} PRs, maximum allowed is {}",
                    pr_count, max_allowed
                )
            }
            AbortReason::PreparationMissing { descendant } => {
                format!("Descendant {} was not prepared before squash", descendant)
            }
            AbortReason::MergeHooksEnabled => {
                "Repository has merge hooks or merge queue enabled, which is incompatible with merge-train".to_string()
            }
            AbortReason::BaseBranchMismatch {
                pr,
                expected_base,
                actual_base,
            } => {
                format!(
                    "PR #{} base branch '{}' doesn't match predecessor's head branch '{}' (was PR retargeted?)",
                    pr, actual_base, expected_base
                )
            }
        }
    }
}

/// Outcome of attempting a cascade step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum CascadeStepOutcome {
    /// Successfully merged a PR, cascade continues to next PR.
    Merged {
        /// The PR that was just merged.
        pr_number: PrNumber,
    },

    /// Waiting for CI/checks on the next PR.
    WaitingOnCi {
        /// The PR we're waiting on.
        pr_number: PrNumber,
    },

    /// Stack fully merged (no more descendants).
    Complete,

    /// Fan-out: multiple descendants, each becomes an independent root.
    FanOut {
        /// The descendant PRs that will become new roots.
        descendants: Vec<PrNumber>,
    },

    /// Cascade is blocked, waiting for something to change.
    Blocked {
        /// The PR that is blocked.
        pr_number: PrNumber,
        /// Why it's blocked.
        reason: BlockReason,
    },

    /// Something went wrong, cascade aborted.
    Aborted {
        /// The PR where the abort occurred.
        pr_number: PrNumber,
        /// Why it was aborted.
        reason: AbortReason,
    },
}

impl CascadeStepOutcome {
    /// Returns true if the cascade can immediately proceed to the next step.
    ///
    /// True for `Merged` (advance to next PR) and `FanOut` (spawn new trains).
    /// False for terminal states (`Complete`, `Aborted`) and waiting states
    /// (`WaitingOnCi`, `Blocked`).
    pub fn can_continue(&self) -> bool {
        matches!(
            self,
            CascadeStepOutcome::Merged { .. } | CascadeStepOutcome::FanOut { .. }
        )
    }

    /// Returns true if the cascade is waiting for something.
    pub fn is_waiting(&self) -> bool {
        matches!(
            self,
            CascadeStepOutcome::WaitingOnCi { .. } | CascadeStepOutcome::Blocked { .. }
        )
    }

    /// Returns true if the cascade completed successfully.
    pub fn is_complete(&self) -> bool {
        matches!(self, CascadeStepOutcome::Complete)
    }

    /// Returns true if the cascade was aborted.
    pub fn is_aborted(&self) -> bool {
        matches!(self, CascadeStepOutcome::Aborted { .. })
    }

    /// Returns the PR number involved, if any.
    pub fn pr_number(&self) -> Option<PrNumber> {
        match self {
            CascadeStepOutcome::Merged { pr_number }
            | CascadeStepOutcome::WaitingOnCi { pr_number }
            | CascadeStepOutcome::Blocked { pr_number, .. }
            | CascadeStepOutcome::Aborted { pr_number, .. } => Some(*pr_number),
            CascadeStepOutcome::Complete | CascadeStepOutcome::FanOut { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        any::<u64>().prop_map(PrNumber)
    }

    fn arb_block_reason() -> impl Strategy<Value = BlockReason> {
        prop_oneof![
            Just(BlockReason::Blocked),
            Just(BlockReason::Behind),
            Just(BlockReason::Stopped),
            Just(BlockReason::Draft),
            Just(BlockReason::Unknown),
        ]
    }

    fn arb_abort_reason() -> impl Strategy<Value = AbortReason> {
        prop_oneof![
            "[a-zA-Z0-9 ]{1,50}".prop_map(|details| AbortReason::MergeConflict { details }),
            "[a-zA-Z0-9 ]{1,50}".prop_map(|details| AbortReason::PushRejected { details }),
            Just(AbortReason::PrClosed),
            Just(AbortReason::CiFailed),
            Just(AbortReason::CycleDetected),
            Just(AbortReason::ReviewDismissed),
            Just(AbortReason::ApprovalWithdrawn),
            "[a-zA-Z0-9 ]{1,50}".prop_map(|details| AbortReason::ApiError { details }),
            Just(AbortReason::BranchDeleted),
            (arb_pr_number(), "[a-zA-Z0-9 ]{1,50}").prop_map(|(predecessor, details)| {
                AbortReason::NonSquashMerge {
                    predecessor,
                    details,
                }
            }),
            Just(AbortReason::StatusCommentTooLarge),
            (1usize..100, 50usize..100).prop_map(|(pr_count, max_allowed)| {
                AbortReason::TrainTooLarge {
                    pr_count,
                    max_allowed,
                }
            }),
            arb_pr_number().prop_map(|descendant| AbortReason::PreparationMissing { descendant }),
            Just(AbortReason::MergeHooksEnabled),
            (
                arb_pr_number(),
                "[a-zA-Z0-9_/-]{1,30}",
                "[a-zA-Z0-9_/-]{1,30}"
            )
                .prop_map(|(pr, expected_base, actual_base)| {
                    AbortReason::BaseBranchMismatch {
                        pr,
                        expected_base,
                        actual_base,
                    }
                }),
        ]
    }

    fn arb_cascade_outcome() -> impl Strategy<Value = CascadeStepOutcome> {
        prop_oneof![
            arb_pr_number().prop_map(|pr| CascadeStepOutcome::Merged { pr_number: pr }),
            arb_pr_number().prop_map(|pr| CascadeStepOutcome::WaitingOnCi { pr_number: pr }),
            Just(CascadeStepOutcome::Complete),
            prop::collection::vec(arb_pr_number(), 1..5)
                .prop_map(|descendants| CascadeStepOutcome::FanOut { descendants }),
            (arb_pr_number(), arb_block_reason()).prop_map(|(pr, reason)| {
                CascadeStepOutcome::Blocked {
                    pr_number: pr,
                    reason,
                }
            }),
            (arb_pr_number(), arb_abort_reason()).prop_map(|(pr, reason)| {
                CascadeStepOutcome::Aborted {
                    pr_number: pr,
                    reason,
                }
            }),
        ]
    }

    mod block_reason {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(reason in arb_block_reason()) {
                let json = serde_json::to_string(&reason).unwrap();
                let parsed: BlockReason = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(reason, parsed);
            }
        }

        #[test]
        fn can_auto_resolve_correct() {
            assert!(BlockReason::Blocked.can_auto_resolve());
            assert!(BlockReason::Behind.can_auto_resolve());
            assert!(BlockReason::Draft.can_auto_resolve());
            assert!(BlockReason::Unknown.can_auto_resolve());
            assert!(!BlockReason::Stopped.can_auto_resolve());
        }

        #[test]
        fn requires_intervention_correct() {
            assert!(!BlockReason::Blocked.requires_intervention());
            assert!(!BlockReason::Behind.requires_intervention());
            assert!(!BlockReason::Draft.requires_intervention());
            assert!(!BlockReason::Unknown.requires_intervention());
            assert!(BlockReason::Stopped.requires_intervention());
        }

        #[test]
        fn auto_resolve_and_intervention_are_mutually_exclusive() {
            let all_reasons = [
                BlockReason::Blocked,
                BlockReason::Behind,
                BlockReason::Stopped,
                BlockReason::Draft,
                BlockReason::Unknown,
            ];

            for reason in &all_reasons {
                // XOR: exactly one should be true
                assert_ne!(
                    reason.can_auto_resolve(),
                    reason.requires_intervention(),
                    "BlockReason::{:?} should have exactly one of can_auto_resolve/requires_intervention true",
                    reason
                );
            }
        }
    }

    mod abort_reason {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(reason in arb_abort_reason()) {
                let json = serde_json::to_string(&reason).unwrap();
                let parsed: AbortReason = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(reason, parsed);
            }
        }

        #[test]
        fn error_type_is_lowercase_snake_case() {
            let reasons = [
                AbortReason::MergeConflict {
                    details: "test".into(),
                },
                AbortReason::PushRejected {
                    details: "test".into(),
                },
                AbortReason::PrClosed,
                AbortReason::CiFailed,
                AbortReason::CycleDetected,
                AbortReason::ReviewDismissed,
                AbortReason::ApprovalWithdrawn,
                AbortReason::ApiError {
                    details: "test".into(),
                },
                AbortReason::BranchDeleted,
                AbortReason::NonSquashMerge {
                    predecessor: PrNumber(1),
                    details: "test".into(),
                },
                AbortReason::StatusCommentTooLarge,
                AbortReason::TrainTooLarge {
                    pr_count: 51,
                    max_allowed: 50,
                },
                AbortReason::PreparationMissing {
                    descendant: PrNumber(2),
                },
                AbortReason::MergeHooksEnabled,
                AbortReason::BaseBranchMismatch {
                    pr: PrNumber(3),
                    expected_base: "feature-1".into(),
                    actual_base: "main".into(),
                },
            ];

            for reason in &reasons {
                let error_type = reason.error_type();
                assert!(
                    error_type
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c == '_'),
                    "error_type '{}' should be lowercase snake_case",
                    error_type
                );
            }
        }
    }

    mod cascade_step_outcome {
        use super::*;

        proptest! {
            #[test]
            fn serde_roundtrip(outcome in arb_cascade_outcome()) {
                let json = serde_json::to_string(&outcome).unwrap();
                let parsed: CascadeStepOutcome = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(outcome, parsed);
            }
        }

        #[test]
        fn can_continue_correct() {
            assert!(
                CascadeStepOutcome::Merged {
                    pr_number: PrNumber(1)
                }
                .can_continue()
            );
            assert!(
                CascadeStepOutcome::FanOut {
                    descendants: vec![PrNumber(2)]
                }
                .can_continue()
            );
            assert!(
                !CascadeStepOutcome::WaitingOnCi {
                    pr_number: PrNumber(1)
                }
                .can_continue()
            );
            assert!(!CascadeStepOutcome::Complete.can_continue());
            assert!(
                !CascadeStepOutcome::Blocked {
                    pr_number: PrNumber(1),
                    reason: BlockReason::Blocked
                }
                .can_continue()
            );
            assert!(
                !CascadeStepOutcome::Aborted {
                    pr_number: PrNumber(1),
                    reason: AbortReason::PrClosed
                }
                .can_continue()
            );
        }

        #[test]
        fn is_waiting_correct() {
            assert!(
                !CascadeStepOutcome::Merged {
                    pr_number: PrNumber(1)
                }
                .is_waiting()
            );
            assert!(
                !CascadeStepOutcome::FanOut {
                    descendants: vec![PrNumber(2)]
                }
                .is_waiting()
            );
            assert!(
                CascadeStepOutcome::WaitingOnCi {
                    pr_number: PrNumber(1)
                }
                .is_waiting()
            );
            assert!(!CascadeStepOutcome::Complete.is_waiting());
            assert!(
                CascadeStepOutcome::Blocked {
                    pr_number: PrNumber(1),
                    reason: BlockReason::Blocked
                }
                .is_waiting()
            );
            assert!(
                !CascadeStepOutcome::Aborted {
                    pr_number: PrNumber(1),
                    reason: AbortReason::PrClosed
                }
                .is_waiting()
            );
        }
    }
}
