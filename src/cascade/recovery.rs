//! Recovery logic for trains interrupted by crashes.
//!
//! When the bot crashes mid-operation, it must be able to resume from the
//! persisted state. This module provides the logic for:
//!
//! 1. Determining what operations were in progress
//! 2. Checking if those operations completed (idempotency)
//! 3. Generating effects to complete or retry operations
//!
//! # Key Principles
//!
//! - **Use frozen descendants**: Always use `progress.frozen_descendants`, never
//!   re-query the descendants index. New descendants may have arrived during
//!   spool replay.
//!
//! - **Intent/done pairs**: For irreversible operations (push), check if the
//!   operation completed by comparing tree SHAs and parent chains.
//!
//! - **Idempotency**: All operations must be safe to retry. Git merge commits
//!   aren't reproducible (timestamps vary), so we compare tree content instead.

use std::collections::HashMap;
use std::path::Path;

use crate::effects::{Effect, GitHubEffect};
use crate::git::PushIntent;
use crate::types::{
    AbortReason, CachedPr, CascadePhase, DescendantProgress, PrNumber, PrState, Sha, TrainRecord,
    TrainState,
};

/// A plan for recovering a train.
#[derive(Debug, Clone)]
pub struct RecoveryPlan {
    /// The train being recovered.
    pub train: TrainRecord,

    /// Actions needed to complete recovery.
    pub actions: Vec<RecoveryAction>,

    /// Effects to execute.
    pub effects: Vec<Effect>,
}

/// An action to take during recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Verify a push completed by checking remote state.
    VerifyPush {
        branch: String,
        expected_tree: Sha,
        pre_push_sha: Sha,
    },

    /// Retry a merge operation.
    RetryMerge {
        descendant: PrNumber,
        source: String,
        target_branch: String,
    },

    /// Retry a push operation.
    RetryPush { branch: String },

    /// Verify squash-merge completed by checking PR state.
    VerifySquash { pr: PrNumber },

    /// Retry squash-merge.
    RetrySquash { pr: PrNumber, expected_sha: Sha },

    /// Verify retarget completed.
    VerifyRetarget { pr: PrNumber },

    /// Retry retarget.
    RetryRetarget { pr: PrNumber, new_base: String },

    /// Resume from a clean state (no in-progress operations).
    ResumeClean,

    /// Mark the train as needing manual review.
    NeedsManualReview { reason: String },
}

/// Compute a recovery plan for a train.
///
/// This function analyzes the train's current state and determines what
/// actions are needed to safely resume operations.
///
/// # Arguments
///
/// * `train` - The train record to recover
/// * `prs` - The cached PR information
/// * `pending_intents` - Any pending intent events (push intents without done events)
/// * `default_branch` - The default branch name
///
/// # Returns
///
/// A `RecoveryPlan` with the actions and effects needed.
pub fn compute_recovery_plan(
    train: &TrainRecord,
    prs: &HashMap<PrNumber, CachedPr>,
    pending_intents: &[PendingIntent],
    default_branch: &str,
) -> RecoveryPlan {
    // If train is not active, nothing to recover
    if !train.state.is_active() {
        return RecoveryPlan {
            train: train.clone(),
            actions: vec![RecoveryAction::ResumeClean],
            effects: vec![],
        };
    }

    let mut actions = Vec::new();
    let mut effects = Vec::new();

    // Check for pending push intents first
    for intent in pending_intents {
        if intent.train_root == train.original_root_pr {
            actions.push(RecoveryAction::VerifyPush {
                branch: intent.branch.clone(),
                expected_tree: intent.expected_tree.clone(),
                pre_push_sha: intent.pre_push_sha.clone(),
            });
        }
    }

    // Phase-specific recovery
    match &train.cascade_phase {
        CascadePhase::Idle => {
            // Nothing in progress
            actions.push(RecoveryAction::ResumeClean);
        }

        CascadePhase::Preparing { progress } => {
            // CRITICAL: Use frozen_descendants, not current descendants
            recover_multi_descendant_phase(&mut actions, &mut effects, progress, prs, |desc_pr| {
                if let Some(pred_sha) = &train.predecessor_head_sha {
                    RecoveryAction::RetryMerge {
                        descendant: desc_pr.number,
                        source: pred_sha.as_str().to_string(),
                        target_branch: desc_pr.head_ref.clone(),
                    }
                } else {
                    RecoveryAction::NeedsManualReview {
                        reason: "Missing predecessor_head_sha for Preparing recovery".to_string(),
                    }
                }
            });
        }

        CascadePhase::SquashPending { progress: _ } => {
            // Check if the PR was already merged
            if let Some(current_pr) = prs.get(&train.current_pr) {
                match &current_pr.state {
                    PrState::Merged {
                        merge_commit_sha: _,
                    } => {
                        // Already merged - we may have crashed after squash but before
                        // transitioning. The train state will be updated by the caller.
                        actions.push(RecoveryAction::ResumeClean);
                    }
                    PrState::Open => {
                        // Need to retry squash
                        actions.push(RecoveryAction::RetrySquash {
                            pr: train.current_pr,
                            expected_sha: current_pr.head_sha.clone(),
                        });
                        effects.push(Effect::GitHub(GitHubEffect::SquashMerge {
                            pr: train.current_pr,
                            expected_sha: current_pr.head_sha.clone(),
                        }));
                    }
                    PrState::Closed => {
                        actions.push(RecoveryAction::NeedsManualReview {
                            reason: "Current PR was closed without merging".to_string(),
                        });
                    }
                }
            } else {
                actions.push(RecoveryAction::NeedsManualReview {
                    reason: "Current PR not found in cache".to_string(),
                });
            }
        }

        CascadePhase::Reconciling {
            progress,
            squash_sha,
        } => {
            recover_multi_descendant_phase(&mut actions, &mut effects, progress, prs, |desc_pr| {
                RecoveryAction::RetryMerge {
                    descendant: desc_pr.number,
                    source: format!("reconcile:{}", squash_sha),
                    target_branch: desc_pr.head_ref.clone(),
                }
            });
        }

        CascadePhase::CatchingUp {
            progress,
            squash_sha: _,
        } => {
            recover_multi_descendant_phase(&mut actions, &mut effects, progress, prs, |desc_pr| {
                RecoveryAction::RetryMerge {
                    descendant: desc_pr.number,
                    source: format!("origin/{}", default_branch),
                    target_branch: desc_pr.head_ref.clone(),
                }
            });
        }

        CascadePhase::Retargeting {
            progress,
            squash_sha: _,
        } => {
            // Retargeting uses GitHub API, so we verify each descendant's base
            for pr_number in progress.remaining() {
                if let Some(desc_pr) = prs.get(pr_number)
                    && desc_pr.state.is_open()
                {
                    if desc_pr.base_ref == default_branch {
                        // Already retargeted
                        continue;
                    }
                    actions.push(RecoveryAction::RetryRetarget {
                        pr: *pr_number,
                        new_base: default_branch.to_string(),
                    });
                    effects.push(Effect::GitHub(GitHubEffect::RetargetPr {
                        pr: *pr_number,
                        new_base: default_branch.to_string(),
                    }));
                }
            }

            if actions.is_empty() {
                actions.push(RecoveryAction::ResumeClean);
            }
        }
    }

    if actions.is_empty() {
        actions.push(RecoveryAction::ResumeClean);
    }

    RecoveryPlan {
        train: train.clone(),
        actions,
        effects,
    }
}

/// Helper for recovering phases that process multiple descendants.
fn recover_multi_descendant_phase<F>(
    actions: &mut Vec<RecoveryAction>,
    _effects: &mut Vec<Effect>,
    progress: &DescendantProgress,
    prs: &HashMap<PrNumber, CachedPr>,
    make_action: F,
) where
    F: Fn(&CachedPr) -> RecoveryAction,
{
    // Use frozen_descendants, not current descendants
    for pr_number in progress.remaining() {
        if let Some(desc_pr) = prs.get(pr_number)
            && desc_pr.state.is_open()
        {
            actions.push(make_action(desc_pr));
        }
        // Closed PRs will be skipped when processing resumes
    }

    if actions.is_empty() {
        actions.push(RecoveryAction::ResumeClean);
    }
}

/// A pending intent event (push intent without corresponding done event).
#[derive(Debug, Clone)]
pub struct PendingIntent {
    /// The train root this intent belongs to.
    pub train_root: PrNumber,

    /// The type of intent.
    pub intent_type: IntentType,

    /// The branch being pushed.
    pub branch: String,

    /// The expected tree SHA after push.
    pub expected_tree: Sha,

    /// The remote SHA before push.
    pub pre_push_sha: Sha,
}

/// Types of intent events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentType {
    /// Preparation push.
    PushPrep,
    /// Reconciliation push.
    PushReconcile,
    /// Catch-up push.
    PushCatchup,
}

/// Verify that a push operation completed successfully.
///
/// This checks the remote branch to see if our push landed. Because merge
/// commits aren't reproducible (timestamps, signatures vary), we compare
/// tree SHAs and verify the parent chain.
///
/// # Arguments
///
/// * `worktree` - Path to the git worktree
/// * `intent` - The push intent to verify
///
/// # Returns
///
/// `true` if the push was completed, `false` if it needs to be retried.
pub fn verify_push_completed(
    worktree: &Path,
    intent: &PushIntent,
) -> Result<bool, crate::git::GitError> {
    crate::git::is_push_completed(
        worktree,
        &intent.branch,
        &intent.expected_tree,
        &intent.pre_push_sha,
    )
}

/// Recover a train by applying the recovery plan.
///
/// This function takes a recovery plan and updates the train state based on
/// verification results.
///
/// # Arguments
///
/// * `plan` - The recovery plan to apply
/// * `verification_results` - Results of verifying pending operations
///
/// # Returns
///
/// The updated train record and any abort reason if recovery failed.
pub fn apply_recovery_plan(
    mut plan: RecoveryPlan,
    verification_results: &HashMap<String, bool>,
) -> (TrainRecord, Option<AbortReason>) {
    let mut needs_manual_review = false;
    let mut manual_review_reason = String::new();

    for action in &plan.actions {
        match action {
            RecoveryAction::VerifyPush {
                branch,
                expected_tree: _,
                pre_push_sha: _,
            } => {
                // Check verification result
                if let Some(&completed) = verification_results.get(branch)
                    && !completed
                {
                    // Push didn't complete - will be retried
                }
            }
            RecoveryAction::NeedsManualReview { reason } => {
                needs_manual_review = true;
                manual_review_reason = reason.clone();
            }
            _ => {
                // Other actions don't affect the recovery state directly
            }
        }
    }

    if needs_manual_review {
        plan.train.state = TrainState::NeedsManualReview;
        return (
            plan.train,
            Some(AbortReason::ApiError {
                details: manual_review_reason,
            }),
        );
    }

    (plan.train, None)
}

/// Check if a descendant is ready to be processed in recovery.
///
/// A descendant is ready if:
/// 1. It's still open
/// 2. It's in the frozen descendants list
/// 3. It hasn't been completed or skipped
pub fn is_descendant_ready(
    pr_number: PrNumber,
    progress: &DescendantProgress,
    prs: &HashMap<PrNumber, CachedPr>,
) -> bool {
    // Must be in frozen set and not completed/skipped
    if !progress.frozen_descendants.contains(&pr_number) {
        return false;
    }
    if progress.completed.contains(&pr_number) || progress.skipped.contains(&pr_number) {
        return false;
    }

    // Must still be open
    prs.get(&pr_number)
        .map(|pr| pr.state.is_open())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MergeStateStatus;

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
    fn recovery_plan_for_idle_is_clean() {
        let train = TrainRecord::new(PrNumber(1));
        let prs = HashMap::new();

        let plan = compute_recovery_plan(&train, &prs, &[], "main");

        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(plan.actions[0], RecoveryAction::ResumeClean));
    }

    #[test]
    fn recovery_plan_for_inactive_train_is_clean() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.stop();
        let prs = HashMap::new();

        let plan = compute_recovery_plan(&train, &prs, &[], "main");

        assert_eq!(plan.actions.len(), 1);
        assert!(matches!(plan.actions[0], RecoveryAction::ResumeClean));
    }

    #[test]
    fn recovery_plan_for_preparing_uses_frozen_descendants() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
        };
        train.predecessor_head_sha = Some(make_sha(2000));

        let pr2 = make_open_pr(2, "branch-1", Some(1));
        let pr3 = make_open_pr(3, "branch-1", Some(1));
        let prs = HashMap::from([(PrNumber(2), pr2), (PrNumber(3), pr3)]);

        let plan = compute_recovery_plan(&train, &prs, &[], "main");

        // Should have retry actions for both frozen descendants
        let retry_count = plan
            .actions
            .iter()
            .filter(|a| matches!(a, RecoveryAction::RetryMerge { .. }))
            .count();
        assert_eq!(retry_count, 2);
    }

    #[test]
    fn recovery_plan_for_preparing_without_predecessor_sha_needs_review() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        // No predecessor_head_sha set

        let pr2 = make_open_pr(2, "branch-1", Some(1));
        let prs = HashMap::from([(PrNumber(2), pr2)]);

        let plan = compute_recovery_plan(&train, &prs, &[], "main");

        assert!(
            plan.actions
                .iter()
                .any(|a| matches!(a, RecoveryAction::NeedsManualReview { .. }))
        );
    }

    #[test]
    fn recovery_plan_for_squash_pending_with_open_pr() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::SquashPending {
            progress: DescendantProgress::new(vec![]),
        };

        let pr = make_open_pr(1, "main", None);
        let prs = HashMap::from([(PrNumber(1), pr)]);

        let plan = compute_recovery_plan(&train, &prs, &[], "main");

        assert!(
            plan.actions
                .iter()
                .any(|a| matches!(a, RecoveryAction::RetrySquash { .. }))
        );
        assert!(
            plan.effects
                .iter()
                .any(|e| matches!(e, Effect::GitHub(GitHubEffect::SquashMerge { .. })))
        );
    }

    #[test]
    fn recovery_plan_for_squash_pending_with_merged_pr() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::SquashPending {
            progress: DescendantProgress::new(vec![]),
        };

        let mut pr = make_open_pr(1, "main", None);
        pr.state = PrState::Merged {
            merge_commit_sha: make_sha(5000),
        };
        let prs = HashMap::from([(PrNumber(1), pr)]);

        let plan = compute_recovery_plan(&train, &prs, &[], "main");

        // Already merged - should be clean
        assert!(
            plan.actions
                .iter()
                .any(|a| matches!(a, RecoveryAction::ResumeClean))
        );
    }

    #[test]
    fn recovery_plan_for_retargeting_skips_already_retargeted() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::Retargeting {
            progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
            squash_sha: make_sha(1000),
        };

        // PR #2 already retargeted to main
        let mut pr2 = make_open_pr(2, "main", Some(1));
        pr2.base_ref = "main".to_string();

        // PR #3 still needs retargeting
        let pr3 = make_open_pr(3, "branch-1", Some(1));

        let prs = HashMap::from([(PrNumber(2), pr2), (PrNumber(3), pr3)]);

        let plan = compute_recovery_plan(&train, &prs, &[], "main");

        // Should only have retarget for #3
        let retarget_count = plan
            .actions
            .iter()
            .filter(|a| {
                matches!(
                    a,
                    RecoveryAction::RetryRetarget {
                        pr: PrNumber(3),
                        ..
                    }
                )
            })
            .count();
        assert_eq!(retarget_count, 1);
    }

    #[test]
    fn is_descendant_ready_checks_frozen_set() {
        let progress = DescendantProgress::new(vec![PrNumber(2)]);
        let pr2 = make_open_pr(2, "branch-1", Some(1));
        let pr3 = make_open_pr(3, "branch-1", Some(1)); // Not in frozen set
        let prs = HashMap::from([(PrNumber(2), pr2), (PrNumber(3), pr3)]);

        assert!(is_descendant_ready(PrNumber(2), &progress, &prs));
        assert!(!is_descendant_ready(PrNumber(3), &progress, &prs)); // Not frozen
    }

    #[test]
    fn is_descendant_ready_checks_completed() {
        let mut progress = DescendantProgress::new(vec![PrNumber(2)]);
        progress.mark_completed(PrNumber(2));

        let pr2 = make_open_pr(2, "branch-1", Some(1));
        let prs = HashMap::from([(PrNumber(2), pr2)]);

        assert!(!is_descendant_ready(PrNumber(2), &progress, &prs));
    }

    #[test]
    fn is_descendant_ready_checks_pr_state() {
        let progress = DescendantProgress::new(vec![PrNumber(2)]);

        let mut pr2 = make_open_pr(2, "branch-1", Some(1));
        pr2.state = PrState::Closed;
        let prs = HashMap::from([(PrNumber(2), pr2)]);

        assert!(!is_descendant_ready(PrNumber(2), &progress, &prs));
    }

    #[test]
    fn apply_recovery_plan_handles_manual_review() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![]),
        };

        let plan = RecoveryPlan {
            train: train.clone(),
            actions: vec![RecoveryAction::NeedsManualReview {
                reason: "Test reason".to_string(),
            }],
            effects: vec![],
        };

        let (updated_train, abort_reason) = apply_recovery_plan(plan, &HashMap::new());

        assert_eq!(updated_train.state, TrainState::NeedsManualReview);
        assert!(abort_reason.is_some());
    }
}
