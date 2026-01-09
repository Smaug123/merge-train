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

use crate::cascade::engine::format_phase_comment;
use crate::effects::{Effect, GitEffect, GitHubEffect};
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

    /// Verify all descendants were prepared before reconciliation.
    ///
    /// Per DESIGN.md "Verifying preparation before reconciliation": on recovery
    /// to Reconciling phase, verify that `predecessor_head_sha` is an ancestor
    /// of each descendant's head. This ensures preparation actually occurred
    /// before the squash.
    ///
    /// Uses `predecessor_head_sha` if available, otherwise falls back to fetching
    /// `refs/pull/<predecessor_pr>/head` via git ls-remote.
    VerifyPrepared {
        /// The predecessor head SHA (if known). Use this first.
        predecessor_head_sha: Option<Sha>,
        /// The predecessor PR number (for fallback fetch if SHA is missing).
        predecessor_pr: Option<PrNumber>,
        /// List of (descendant PR, head SHA) to verify.
        descendants: Vec<(PrNumber, Sha)>,
    },

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

    let mut plan_train = train.clone();
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
            // CRITICAL: Check if the current PR was merged externally during the crash.
            // If so, we need to handle it like handle_external_merge does in step.rs.
            if let Some(current_pr) = prs.get(&train.current_pr)
                && let PrState::Merged { merge_commit_sha } = &current_pr.state
            {
                // PR was merged externally while in Preparing phase.
                // Check if all descendants were prepared before the merge.
                let remaining: Vec<_> = progress.remaining().collect();

                if !remaining.is_empty() {
                    // ABORT: Unprepared descendants exist. Reconciling them now would
                    // skip preparation, potentially losing predecessor content.
                    // This matches the behavior in handle_external_merge.
                    actions.push(RecoveryAction::NeedsManualReview {
                            reason: format!(
                                "PR #{} was merged externally while {} descendants were still unprepared. \
                                 Reconciling now would skip preparation and risk data loss. \
                                 Manual intervention required.",
                                train.current_pr,
                                remaining.len()
                            ),
                        });
                    return RecoveryPlan {
                        train: plan_train,
                        actions,
                        effects,
                    };
                }

                // All descendants are prepared. Transition to Reconciling with reset progress.
                // The "completed" set tracked preparation, not reconciliation - reset it.
                let mut reset_progress =
                    DescendantProgress::new(progress.frozen_descendants.clone());
                // Preserve skipped set (closed PRs should stay skipped)
                reset_progress.skipped = progress.skipped.clone();

                // Per DESIGN.md: Any transition TO Reconciling must verify preparation.
                // Collect remaining open descendants for verification BEFORE moving reset_progress.
                let remaining_descendants: Vec<_> = reset_progress
                    .remaining()
                    .filter_map(|pr_num| {
                        prs.get(pr_num)
                            .filter(|p| p.state.is_open())
                            .map(|p| (*pr_num, p.head_sha.clone()))
                    })
                    .collect();

                plan_train.cascade_phase = CascadePhase::Reconciling {
                    progress: reset_progress,
                    squash_sha: merge_commit_sha.clone(),
                };
                plan_train.last_squash_sha = Some(merge_commit_sha.clone());
                plan_train.increment_seq();

                // Add squash validation effect - must run first
                effects.push(Effect::Git(GitEffect::ValidateSquashCommit {
                    squash_sha: merge_commit_sha.clone(),
                    default_branch: default_branch.to_string(),
                }));

                // Add status comment update for GitHub-based recovery.
                // If comment formatting fails (e.g., oversize), abort gracefully instead of panicking.
                if let Some(comment_id) = plan_train.status_comment_id {
                    match format_phase_comment(&plan_train) {
                        Ok(body) => {
                            effects.push(Effect::GitHub(GitHubEffect::UpdateComment {
                                comment_id,
                                body,
                            }));
                        }
                        Err(e) => {
                            // Comment formatting failed - abort gracefully
                            actions.push(RecoveryAction::NeedsManualReview {
                                reason: format!(
                                    "Failed to format status comment during recovery: {}",
                                    e
                                ),
                            });
                            return RecoveryPlan {
                                train: plan_train,
                                actions,
                                effects,
                            };
                        }
                    }
                }

                if !remaining_descendants.is_empty() {
                    actions.push(RecoveryAction::VerifyPrepared {
                        predecessor_head_sha: train.predecessor_head_sha.clone(),
                        predecessor_pr: train.predecessor_pr,
                        descendants: remaining_descendants,
                    });
                }

                actions.push(RecoveryAction::ResumeClean);
                return RecoveryPlan {
                    train: plan_train,
                    actions,
                    effects,
                };
            }

            // Normal case: PR not merged, continue with preparation recovery.
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

        CascadePhase::SquashPending { progress } => {
            // Check if the PR was already merged
            if let Some(current_pr) = prs.get(&train.current_pr) {
                match &current_pr.state {
                    PrState::Merged { merge_commit_sha } => {
                        // Already merged - transition to Reconciling phase with the merge SHA.
                        // CRITICAL: Reset progress.completed because it tracks PREPARATION, not
                        // reconciliation. Preserve skipped (closed PRs stay skipped).
                        //
                        // CRITICAL: We need to validate the merge was a squash before proceeding.
                        // The validation effect will run before any reconciliation effects.
                        let mut reset_progress =
                            DescendantProgress::new(progress.frozen_descendants.clone());
                        reset_progress.skipped = progress.skipped.clone();

                        // Per DESIGN.md: Any transition TO Reconciling must verify preparation.
                        // Collect remaining open descendants for verification BEFORE moving reset_progress.
                        let remaining_descendants: Vec<_> = reset_progress
                            .remaining()
                            .filter_map(|pr_num| {
                                prs.get(pr_num)
                                    .filter(|p| p.state.is_open())
                                    .map(|p| (*pr_num, p.head_sha.clone()))
                            })
                            .collect();

                        plan_train.cascade_phase = CascadePhase::Reconciling {
                            progress: reset_progress,
                            squash_sha: merge_commit_sha.clone(),
                        };
                        plan_train.last_squash_sha = Some(merge_commit_sha.clone());
                        plan_train.increment_seq();

                        // Add squash validation effect - must run first
                        effects.push(Effect::Git(GitEffect::ValidateSquashCommit {
                            squash_sha: merge_commit_sha.clone(),
                            default_branch: default_branch.to_string(),
                        }));

                        // Add status comment update for GitHub-based recovery.
                        // This ensures a second crash can see the Reconciling phase and squash SHA.
                        // If comment formatting fails (e.g., oversize), abort gracefully instead of panicking.
                        if let Some(comment_id) = plan_train.status_comment_id {
                            match format_phase_comment(&plan_train) {
                                Ok(body) => {
                                    effects.push(Effect::GitHub(GitHubEffect::UpdateComment {
                                        comment_id,
                                        body,
                                    }));
                                }
                                Err(e) => {
                                    // Comment formatting failed - abort gracefully
                                    actions.push(RecoveryAction::NeedsManualReview {
                                        reason: format!(
                                            "Failed to format status comment during recovery: {}",
                                            e
                                        ),
                                    });
                                    return RecoveryPlan {
                                        train: plan_train,
                                        actions,
                                        effects,
                                    };
                                }
                            }
                        }

                        if !remaining_descendants.is_empty() {
                            actions.push(RecoveryAction::VerifyPrepared {
                                predecessor_head_sha: train.predecessor_head_sha.clone(),
                                predecessor_pr: train.predecessor_pr,
                                descendants: remaining_descendants,
                            });
                        }

                        actions.push(RecoveryAction::ResumeClean);
                    }
                    PrState::Open => {
                        // CRITICAL: Verify the PR head hasn't changed since preparation.
                        // Same guard as execute_squash_pending in step.rs:416-439.
                        // If someone pushed new commits after preparation, those commits
                        // would be squashed without proper preparation of descendants.
                        if let Some(recorded_sha) = &train.predecessor_head_sha
                            && current_pr.head_sha != *recorded_sha
                        {
                            actions.push(RecoveryAction::NeedsManualReview {
                                reason: format!(
                                    "PR head changed since preparation: {} -> {}. \
                                         New commits were not prepared into descendants.",
                                    recorded_sha, current_pr.head_sha
                                ),
                            });
                            return RecoveryPlan {
                                train: plan_train,
                                actions,
                                effects,
                            };
                        }

                        // Use recorded predecessor_head_sha for expected_sha, falling back to
                        // current head_sha only if no recorded SHA exists (shouldn't happen
                        // in normal operation, but handles edge cases gracefully).
                        let expected_sha = train
                            .predecessor_head_sha
                            .clone()
                            .unwrap_or_else(|| current_pr.head_sha.clone());

                        actions.push(RecoveryAction::RetrySquash {
                            pr: train.current_pr,
                            expected_sha: expected_sha.clone(),
                        });
                        effects.push(Effect::GitHub(GitHubEffect::SquashMerge {
                            pr: train.current_pr,
                            expected_sha,
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
            // CRITICAL: Per DESIGN.md "Verifying preparation before reconciliation",
            // verify each remaining descendant was prepared before the squash.
            // This prevents data loss if the PR was merged externally mid-prepare.
            let remaining_descendants: Vec<_> = progress
                .remaining()
                .filter_map(|pr_num| {
                    prs.get(pr_num)
                        .filter(|p| p.state.is_open())
                        .map(|p| (*pr_num, p.head_sha.clone()))
                })
                .collect();

            if !remaining_descendants.is_empty() {
                // Per DESIGN.md: use predecessor_head_sha, or fallback to fetching
                // refs/pull/<predecessor_pr>/head via git ls-remote
                actions.push(RecoveryAction::VerifyPrepared {
                    predecessor_head_sha: train.predecessor_head_sha.clone(),
                    predecessor_pr: train.predecessor_pr,
                    descendants: remaining_descendants,
                });
            }

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
        train: plan_train,
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
        intent.expected_second_parent.as_ref(),
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
            RecoveryAction::VerifyPrepared { descendants, .. } => {
                // Check verification results for each descendant.
                // The key format is "prepared:<pr_number>" and value is true if
                // the predecessor head is an ancestor of the descendant's head.
                let mut unprepared = Vec::new();
                for (pr_num, _head_sha) in descendants {
                    let key = format!("prepared:{}", pr_num.0);
                    if let Some(&is_prepared) = verification_results.get(&key)
                        && !is_prepared
                    {
                        unprepared.push(*pr_num);
                    }
                    // If no result, verification hasn't been run yet - will be handled by executor
                }

                if !unprepared.is_empty() {
                    needs_manual_review = true;
                    manual_review_reason = format!(
                        "Descendants {:?} were not prepared before squash. \
                         The predecessor head is not an ancestor of their heads. \
                         Manual intervention required: merge main into these branches or rebase.",
                        unprepared.iter().map(|p| p.0).collect::<Vec<_>>()
                    );
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

/// Verify that descendants were prepared before reconciliation.
///
/// Per DESIGN.md "Verifying preparation before reconciliation": checks that
/// the predecessor head SHA is an ancestor of each descendant's head.
///
/// # Arguments
///
/// * `worktree` - Path to the git worktree
/// * `action` - The VerifyPrepared action containing what to verify
///
/// # Returns
///
/// A map of `"prepared:<pr_number>" -> bool` indicating whether each descendant
/// was properly prepared. Returns `Err` if git operations fail or if the
/// predecessor SHA cannot be determined.
pub fn verify_descendants_prepared(
    worktree: &Path,
    action: &RecoveryAction,
) -> Result<HashMap<String, bool>, crate::git::GitError> {
    let RecoveryAction::VerifyPrepared {
        predecessor_head_sha,
        predecessor_pr,
        descendants,
    } = action
    else {
        return Ok(HashMap::new());
    };

    // Determine the predecessor head SHA to use
    let pred_sha = match predecessor_head_sha {
        Some(sha) => sha.clone(),
        None => {
            // Fallback: fetch refs/pull/<predecessor_pr>/head via git ls-remote
            match predecessor_pr {
                Some(pr_num) => {
                    let output = crate::git::git_command(worktree)
                        .arg("ls-remote")
                        .arg("origin")
                        .arg(format!("refs/pull/{}/head", pr_num.0))
                        .output()
                        .map_err(|e| crate::git::GitError::CommandFailed {
                            command: "git ls-remote".to_string(),
                            stderr: e.to_string(),
                        })?;

                    if !output.status.success() {
                        return Err(crate::git::GitError::CommandFailed {
                            command: "git ls-remote".to_string(),
                            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                        });
                    }

                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let sha_str = stdout.split_whitespace().next().ok_or_else(|| {
                        crate::git::GitError::CommandFailed {
                            command: "git ls-remote".to_string(),
                            stderr: format!("No SHA found for refs/pull/{}/head", pr_num.0),
                        }
                    })?;

                    Sha::parse(sha_str.to_string()).map_err(|_| {
                        crate::git::GitError::CommandFailed {
                            command: "git ls-remote".to_string(),
                            stderr: format!("Invalid SHA: {}", sha_str),
                        }
                    })?
                }
                None => {
                    return Err(crate::git::GitError::CommandFailed {
                        command: "verify_descendants_prepared".to_string(),
                        stderr: "Neither predecessor_head_sha nor predecessor_pr available for verification".to_string(),
                    });
                }
            }
        }
    };

    // CRITICAL: Fetch descendant PR refs before verification.
    // Without this, git merge-base may fail if objects aren't local, returning
    // non-zero (same as "not ancestor") and causing false negatives.
    let refspecs: Vec<String> = descendants
        .iter()
        .map(|(pr_num, _)| format!("+refs/pull/{}/head", pr_num.0))
        .collect();

    if !refspecs.is_empty() {
        let mut cmd = crate::git::git_command(worktree);
        cmd.arg("fetch").arg("origin");
        for refspec in &refspecs {
            cmd.arg(refspec);
        }
        let output = cmd
            .output()
            .map_err(|e| crate::git::GitError::CommandFailed {
                command: "git fetch".to_string(),
                stderr: e.to_string(),
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(crate::git::GitError::FetchFailed {
                refspec: refspecs.join(", "),
                details: stderr.to_string(),
            });
        }
    }

    // Verify each descendant
    let mut results = HashMap::new();
    for (pr_num, head_sha) in descendants {
        let key = format!("prepared:{}", pr_num.0);

        // Run: git merge-base --is-ancestor <pred_sha> <head_sha>
        // Use output() instead of status() to distinguish error types.
        let output = crate::git::git_command(worktree)
            .arg("merge-base")
            .arg("--is-ancestor")
            .arg(pred_sha.as_str())
            .arg(head_sha.as_str())
            .output()
            .map_err(|e| crate::git::GitError::CommandFailed {
                command: "git merge-base --is-ancestor".to_string(),
                stderr: e.to_string(),
            })?;

        match output.status.code() {
            // Exit code 0 = is ancestor (prepared)
            Some(0) => {
                results.insert(key, true);
            }
            // Exit code 1 = not ancestor (unprepared)
            Some(1) => {
                results.insert(key, false);
            }
            // Other exit codes (e.g., 128) = fatal error, likely missing objects
            _ => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(crate::git::GitError::CommandFailed {
                    command: "git merge-base --is-ancestor".to_string(),
                    stderr: format!("Exit code {:?}: {}", output.status.code(), stderr),
                });
            }
        }
    }

    Ok(results)
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

        // CRITICAL: Must emit ValidateSquashCommit effect
        assert!(
            plan.effects
                .iter()
                .any(|e| matches!(e, Effect::Git(GitEffect::ValidateSquashCommit { .. }))),
            "Recovery must validate squash commit. Effects: {:?}",
            plan.effects
        );
    }

    /// Regression test: recovery from SquashPending → Reconciling must emit status comment update.
    /// Without this, a second crash before the next transition would see stale phase in GitHub.
    #[test]
    fn recovery_squash_pending_merged_emits_status_comment_update() {
        use crate::types::CommentId;

        let mut train = TrainRecord::new(PrNumber(1));
        train.status_comment_id = Some(CommentId(12345));
        train.cascade_phase = CascadePhase::SquashPending {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };

        let mut pr1 = make_open_pr(1, "main", None);
        pr1.state = PrState::Merged {
            merge_commit_sha: make_sha(5000),
        };
        let pr2 = make_open_pr(2, "branch-1", Some(1));
        let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);

        let plan = compute_recovery_plan(&train, &prs, &[], "main");

        // Must have status comment update effect
        let has_comment_update = plan.effects.iter().any(|e| {
            matches!(
                e,
                Effect::GitHub(GitHubEffect::UpdateComment {
                    comment_id: CommentId(12345),
                    ..
                })
            )
        });
        assert!(
            has_comment_update,
            "Recovery must emit status comment update. Effects: {:?}",
            plan.effects
        );

        // Validation effect must come before comment update (run first)
        let validation_idx = plan
            .effects
            .iter()
            .position(|e| matches!(e, Effect::Git(GitEffect::ValidateSquashCommit { .. })));
        let comment_idx = plan
            .effects
            .iter()
            .position(|e| matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { .. })));

        assert!(
            validation_idx < comment_idx,
            "Validation must run before comment update"
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

    mod property_tests {
        use super::*;
        use proptest::prelude::*;

        fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
            (2u64..100).prop_map(PrNumber)
        }

        fn arb_sha() -> impl Strategy<Value = Sha> {
            "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
        }

        fn arb_unique_descendants(min: usize, max: usize) -> impl Strategy<Value = Vec<PrNumber>> {
            prop::collection::hash_set(arb_pr_number(), min..max)
                .prop_map(|set| set.into_iter().collect())
        }

        fn make_open_pr_for_test(
            number: u64,
            base_ref: &str,
            predecessor: Option<PrNumber>,
            sha: Sha,
        ) -> CachedPr {
            CachedPr::new(
                PrNumber(number),
                sha,
                format!("branch-{}", number),
                base_ref.to_string(),
                predecessor,
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            )
        }

        /// Arbitrary crash phase: generate a valid phase with frozen descendants.
        fn arb_crash_phase(
            descendants: Vec<PrNumber>,
            sha: Sha,
        ) -> impl Strategy<Value = CascadePhase> {
            let desc = descendants.clone();
            let sha2 = sha.clone();
            prop_oneof![
                Just(CascadePhase::Preparing {
                    progress: DescendantProgress::new(descendants.clone()),
                }),
                Just(CascadePhase::SquashPending {
                    progress: DescendantProgress::new(descendants.clone()),
                }),
                Just(CascadePhase::Reconciling {
                    progress: DescendantProgress::new(desc.clone()),
                    squash_sha: sha.clone(),
                }),
                Just(CascadePhase::CatchingUp {
                    progress: DescendantProgress::new(desc.clone()),
                    squash_sha: sha.clone(),
                }),
                Just(CascadePhase::Retargeting {
                    progress: DescendantProgress::new(descendants),
                    squash_sha: sha2,
                }),
            ]
        }

        proptest! {
            /// Property: Recovery from any crash point produces a valid recovery plan
            /// AND the recovered train can continue cascading.
            ///
            /// For any phase the train might crash in, compute_recovery_plan should:
            /// 1. Return a plan with at least one action
            /// 2. Use the frozen_descendants from the train, not re-query
            /// 3. Produce a plan that can be applied without panicking
            /// 4. Result in a train that can be used with execute_cascade_step
            ///
            /// Note: Uses prop_flat_map to generate crash_phase from frozen_descendants,
            /// ensuring proper cartesian product testing (not nested proptest! calls).
            #[test]
            fn recovery_from_any_crash_point_produces_valid_plan(
                (frozen_descendants, sha, crash_phase) in arb_unique_descendants(1, 5)
                    .prop_flat_map(|desc| {
                        (Just(desc.clone()), arb_sha())
                            .prop_flat_map(move |(desc2, sha)| {
                                let desc3 = desc2.clone();
                                let sha2 = sha.clone();
                                arb_crash_phase(desc2, sha.clone())
                                    .prop_map(move |phase| (desc3.clone(), sha2.clone(), phase))
                            })
                    })
            ) {
                use crate::cascade::step::{execute_cascade_step, StepContext};
                use crate::types::CascadeStepOutcome;

                let mut train = TrainRecord::new(PrNumber(1));
                train.cascade_phase = crash_phase.clone();
                train.predecessor_head_sha = Some(sha.clone());
                train.last_squash_sha = crash_phase.squash_sha().cloned();

                // Build PR map with all frozen descendants
                let mut prs = HashMap::new();
                let root_pr = make_open_pr_for_test(1, "main", None, sha.clone());
                prs.insert(PrNumber(1), root_pr);

                for &desc in &frozen_descendants {
                    let pr = make_open_pr_for_test(desc.0, "branch-1", Some(PrNumber(1)), sha.clone());
                    prs.insert(desc, pr);
                }

                let plan = compute_recovery_plan(&train, &prs, &[], "main");

                // Property 1: Plan should have at least one action
                prop_assert!(!plan.actions.is_empty(), "Recovery plan should have at least one action");

                // Property 2: Train in plan should preserve frozen_descendants
                if let Some(progress) = plan.train.cascade_phase.progress() {
                    prop_assert_eq!(
                        &progress.frozen_descendants,
                        &frozen_descendants,
                        "Recovery plan should preserve frozen_descendants"
                    );
                }

                // Property 3: Plan can be applied without panicking
                let verification_results = HashMap::new();
                let (recovered_train, abort_reason) = apply_recovery_plan(plan.clone(), &verification_results);

                // The recovered train should be in a valid state
                prop_assert!(
                    recovered_train.state == TrainState::Running
                        || recovered_train.state == TrainState::NeedsManualReview
                        || recovered_train.state == TrainState::Aborted,
                    "Recovered train should be in a valid state, got: {:?}",
                    recovered_train.state
                );

                // Property 4: If train is still running, it should be usable with execute_cascade_step
                if recovered_train.state == TrainState::Running && abort_reason.is_none() {
                    let ctx = StepContext::new("main");
                    let step_result = execute_cascade_step(recovered_train.clone(), &prs, &ctx);

                    // The step should not panic and should produce a valid outcome
                    prop_assert!(
                        matches!(
                            step_result.outcome,
                            CascadeStepOutcome::WaitingOnCi { .. }
                                | CascadeStepOutcome::Complete
                                | CascadeStepOutcome::FanOut { .. }
                                | CascadeStepOutcome::Merged { .. }
                                | CascadeStepOutcome::Aborted { .. }
                        ),
                        "Recovered train should produce valid step outcome, got: {:?}",
                        step_result.outcome
                    );

                    // The step should preserve frozen_descendants
                    if let Some(progress) = step_result.train.cascade_phase.progress() {
                        prop_assert_eq!(
                            &progress.frozen_descendants,
                            &frozen_descendants,
                            "execute_cascade_step should preserve frozen_descendants after recovery"
                        );
                    }
                }
            }

            /// Property: Recovery always uses frozen descendants, never re-queries.
            ///
            /// Even if new PRs appear in the PR map after a crash, recovery should
            /// only operate on the descendants that were frozen at cascade start.
            #[test]
            fn recovery_uses_frozen_descendants_not_current_state(
                frozen_descendants in arb_unique_descendants(1, 3),
                late_addition in arb_pr_number(),
                sha in arb_sha()
            ) {
                prop_assume!(!frozen_descendants.contains(&late_addition));

                let mut train = TrainRecord::new(PrNumber(1));
                train.cascade_phase = CascadePhase::Preparing {
                    progress: DescendantProgress::new(frozen_descendants.clone()),
                };
                train.predecessor_head_sha = Some(sha.clone());

                // Build PR map with frozen descendants + late addition
                let mut prs = HashMap::new();
                let root_pr = make_open_pr_for_test(1, "main", None, sha.clone());
                prs.insert(PrNumber(1), root_pr);

                for &desc in &frozen_descendants {
                    let pr = make_open_pr_for_test(desc.0, "branch-1", Some(PrNumber(1)), sha.clone());
                    prs.insert(desc, pr);
                }

                // Add the "late" descendant that appeared after crash
                let late_pr = make_open_pr_for_test(late_addition.0, "branch-1", Some(PrNumber(1)), sha.clone());
                prs.insert(late_addition, late_pr);

                let plan = compute_recovery_plan(&train, &prs, &[], "main");

                // Recovery should NOT include late_addition in any RetryMerge actions
                for action in &plan.actions {
                    if let RecoveryAction::RetryMerge { descendant, .. } = action {
                        prop_assert!(
                            frozen_descendants.contains(descendant),
                            "RetryMerge should only target frozen descendants, but found {}",
                            descendant
                        );
                        prop_assert_ne!(
                            *descendant, late_addition,
                            "Late addition {} should not be in recovery plan",
                            late_addition
                        );
                    }
                }
            }

            /// Property: Recovery preserves phase-specific data.
            ///
            /// When recovering from Reconciling, CatchingUp, or Retargeting phases,
            /// the squash_sha must be preserved in the recovery plan.
            #[test]
            fn recovery_preserves_squash_sha(
                frozen_descendants in arb_unique_descendants(1, 3),
                squash_sha in arb_sha()
            ) {
                let phases_with_squash = vec![
                    CascadePhase::Reconciling {
                        progress: DescendantProgress::new(frozen_descendants.clone()),
                        squash_sha: squash_sha.clone(),
                    },
                    CascadePhase::CatchingUp {
                        progress: DescendantProgress::new(frozen_descendants.clone()),
                        squash_sha: squash_sha.clone(),
                    },
                    CascadePhase::Retargeting {
                        progress: DescendantProgress::new(frozen_descendants.clone()),
                        squash_sha: squash_sha.clone(),
                    },
                ];

                for phase in phases_with_squash {
                    let mut train = TrainRecord::new(PrNumber(1));
                    train.cascade_phase = phase.clone();
                    train.last_squash_sha = Some(squash_sha.clone());

                    let mut prs = HashMap::new();
                    let sha_for_pr = Sha::parse("a".repeat(40)).unwrap();
                    let root_pr = make_open_pr_for_test(1, "main", None, sha_for_pr.clone());
                    prs.insert(PrNumber(1), root_pr);

                    for &desc in &frozen_descendants {
                        let pr = make_open_pr_for_test(desc.0, "branch-1", Some(PrNumber(1)), sha_for_pr.clone());
                        prs.insert(desc, pr);
                    }

                    let plan = compute_recovery_plan(&train, &prs, &[], "main");

                    // The recovery plan's train should preserve squash_sha
                    prop_assert_eq!(
                        plan.train.cascade_phase.squash_sha(),
                        Some(&squash_sha),
                        "Recovery plan should preserve squash_sha for phase {}",
                        phase.name()
                    );
                }
            }

            // ─── Bug Fix Tests ───────────────────────────────────────────────────────
            //
            // These tests expose bugs described in review comments.

            /// Property: Recovery for SquashPending with merged PR captures merge_commit_sha
            /// and transitions train to Reconciling.
            ///
            /// BUG: When the train is in SquashPending and the PR was merged externally,
            /// recovery returns ResumeClean without capturing merge_commit_sha or
            /// transitioning the train to Reconciling phase.
            #[test]
            fn recovery_squash_pending_with_merged_pr_transitions_to_reconciling(
                frozen_descendants in arb_unique_descendants(1, 3),
                merge_sha in arb_sha(),
                pr_sha in arb_sha()
            ) {
                // Create train in SquashPending phase
                let mut train = TrainRecord::new(PrNumber(1));
                train.cascade_phase = CascadePhase::SquashPending {
                    progress: DescendantProgress::new(frozen_descendants.clone()),
                };

                // Create PR map with merged root and open descendants
                let mut prs = HashMap::new();
                let mut root_pr = make_open_pr_for_test(1, "main", None, pr_sha.clone());
                root_pr.state = PrState::Merged { merge_commit_sha: merge_sha.clone() };
                prs.insert(PrNumber(1), root_pr);

                for &desc in &frozen_descendants {
                    let pr = make_open_pr_for_test(desc.0, "branch-1", Some(PrNumber(1)), pr_sha.clone());
                    prs.insert(desc, pr);
                }

                let plan = compute_recovery_plan(&train, &prs, &[], "main");

                // The train should be transitioned to Reconciling phase
                prop_assert!(
                    matches!(&plan.train.cascade_phase, CascadePhase::Reconciling { squash_sha, .. }
                        if *squash_sha == merge_sha),
                    "Recovery for SquashPending with merged PR should transition to Reconciling with merge_commit_sha, got: {:?}",
                    plan.train.cascade_phase
                );

                // The frozen descendants should be preserved
                if let CascadePhase::Reconciling { progress, .. } = &plan.train.cascade_phase {
                    prop_assert_eq!(
                        &progress.frozen_descendants,
                        &frozen_descendants,
                        "Recovery should preserve frozen descendants"
                    );
                }

                // The train's last_squash_sha should be updated
                prop_assert_eq!(
                    plan.train.last_squash_sha.as_ref(),
                    Some(&merge_sha),
                    "Recovery should update last_squash_sha"
                );
            }

            /// Property: Recovery transitions that change phase MUST emit status comment update.
            /// Without this, a second crash before the next operation leaves stale phase in GitHub.
            ///
            /// BUG: Recovery from SquashPending (with merged PR) transitions to Reconciling
            /// but doesn't emit a status comment update effect.
            #[test]
            fn recovery_phase_transitions_emit_status_comment_update(
                frozen_descendants in arb_unique_descendants(1, 3),
                merge_sha in arb_sha(),
                pr_sha in arb_sha()
            ) {
                use crate::types::CommentId;

                // Create train in SquashPending phase with status comment
                let mut train = TrainRecord::new(PrNumber(1));
                train.status_comment_id = Some(CommentId(12345));
                train.cascade_phase = CascadePhase::SquashPending {
                    progress: DescendantProgress::new(frozen_descendants.clone()),
                };

                // Create PR map with merged root
                let mut prs = HashMap::new();
                let mut root_pr = make_open_pr_for_test(1, "main", None, pr_sha.clone());
                root_pr.state = PrState::Merged { merge_commit_sha: merge_sha.clone() };
                prs.insert(PrNumber(1), root_pr);

                for &desc in &frozen_descendants {
                    let pr = make_open_pr_for_test(desc.0, "branch-1", Some(PrNumber(1)), pr_sha.clone());
                    prs.insert(desc, pr);
                }

                let plan = compute_recovery_plan(&train, &prs, &[], "main");

                // Phase changed from SquashPending to Reconciling
                prop_assert!(
                    matches!(&plan.train.cascade_phase, CascadePhase::Reconciling { .. }),
                    "Recovery should transition to Reconciling"
                );

                // MUST have status comment update effect
                let has_comment_update = plan.effects.iter().any(|e| {
                    matches!(e, Effect::GitHub(GitHubEffect::UpdateComment {
                        comment_id: CommentId(12345), ..
                    }))
                });
                prop_assert!(
                    has_comment_update,
                    "BUG: Recovery phase transition did not emit status comment update! \
                     Phase changed from SquashPending to Reconciling, but no UpdateComment effect. \
                     Effects: {:?}",
                    plan.effects
                );

                // Also must have ValidateSquashCommit effect
                let has_validation = plan.effects.iter().any(|e| {
                    matches!(e, Effect::Git(GitEffect::ValidateSquashCommit { .. }))
                });
                prop_assert!(
                    has_validation,
                    "BUG: Recovery should emit ValidateSquashCommit effect. Effects: {:?}",
                    plan.effects
                );

                // MUST have VerifyPrepared action for remaining descendants
                // Per DESIGN.md: Any transition TO Reconciling must verify preparation
                let has_verify_prepared = plan.actions.iter().any(|a| {
                    matches!(a, RecoveryAction::VerifyPrepared { .. })
                });
                prop_assert!(
                    has_verify_prepared,
                    "BUG: Recovery transitioning to Reconciling MUST emit VerifyPrepared action. \
                     Actions: {:?}",
                    plan.actions
                );
            }

            /// Review Comment #1 fix: Recovery from Preparing with externally merged PR
            /// must abort if descendants are unprepared.
            ///
            /// Property: When train is in Preparing phase and root PR was merged externally
            /// with unprepared descendants remaining, recovery MUST produce NeedsManualReview.
            #[test]
            fn recovery_preparing_external_merge_with_unprepared_descendants_aborts(
                frozen_descendants in arb_unique_descendants(1, 3),
                merge_sha in arb_sha(),
                pr_sha in arb_sha()
            ) {
                // Create train in Preparing phase with unprepared descendants
                let mut train = TrainRecord::new(PrNumber(1));
                // No descendants marked completed - they're all unprepared
                let progress = DescendantProgress::new(frozen_descendants.clone());
                train.cascade_phase = CascadePhase::Preparing { progress };
                train.predecessor_head_sha = Some(pr_sha.clone());

                // Root PR was merged externally while in Preparing
                let mut prs = HashMap::new();
                let mut root_pr = make_open_pr_for_test(1, "main", None, pr_sha.clone());
                root_pr.state = PrState::Merged { merge_commit_sha: merge_sha.clone() };
                prs.insert(PrNumber(1), root_pr);

                for &desc in &frozen_descendants {
                    let pr = make_open_pr_for_test(desc.0, "branch-1", Some(PrNumber(1)), pr_sha.clone());
                    prs.insert(desc, pr);
                }

                let plan = compute_recovery_plan(&train, &prs, &[], "main");

                // MUST produce NeedsManualReview action
                let has_manual_review = plan.actions.iter().any(|a| {
                    matches!(a, RecoveryAction::NeedsManualReview { .. })
                });
                prop_assert!(
                    has_manual_review,
                    "BUG (Review Comment #1): Recovery from Preparing with externally merged PR \
                     and unprepared descendants MUST abort with NeedsManualReview. \
                     Unprepared: {:?}, Actions: {:?}",
                    frozen_descendants,
                    plan.actions
                );
            }

            /// Review Comment #1 fix (continued): Recovery from Preparing with ALL descendants
            /// prepared should transition to Reconciling (not abort).
            ///
            /// Property: When all descendants are prepared, external merge in Preparing should
            /// successfully transition to Reconciling with reset progress.
            #[test]
            fn recovery_preparing_external_merge_all_prepared_transitions(
                frozen_descendants in arb_unique_descendants(1, 3),
                merge_sha in arb_sha(),
                pr_sha in arb_sha()
            ) {
                // Create train in Preparing phase with ALL descendants prepared
                let mut train = TrainRecord::new(PrNumber(1));
                let mut progress = DescendantProgress::new(frozen_descendants.clone());
                for &desc in &frozen_descendants {
                    progress.mark_completed(desc);
                }
                train.cascade_phase = CascadePhase::Preparing { progress };
                train.predecessor_head_sha = Some(pr_sha.clone());

                // Root PR was merged externally
                let mut prs = HashMap::new();
                let mut root_pr = make_open_pr_for_test(1, "main", None, pr_sha.clone());
                root_pr.state = PrState::Merged { merge_commit_sha: merge_sha.clone() };
                prs.insert(PrNumber(1), root_pr);

                for &desc in &frozen_descendants {
                    let pr = make_open_pr_for_test(desc.0, "branch-1", Some(PrNumber(1)), pr_sha.clone());
                    prs.insert(desc, pr);
                }

                let plan = compute_recovery_plan(&train, &prs, &[], "main");

                // Should NOT have NeedsManualReview (all were prepared)
                let has_manual_review = plan.actions.iter().any(|a| {
                    matches!(a, RecoveryAction::NeedsManualReview { .. })
                });
                prop_assert!(
                    !has_manual_review,
                    "Recovery from Preparing with all descendants prepared should NOT abort. \
                     Actions: {:?}",
                    plan.actions
                );

                // Should transition to Reconciling
                prop_assert!(
                    matches!(&plan.train.cascade_phase, CascadePhase::Reconciling { squash_sha, .. }
                        if *squash_sha == merge_sha),
                    "Recovery should transition to Reconciling. Got: {:?}",
                    plan.train.cascade_phase
                );

                // CRITICAL: The completed set should be RESET (prepared != reconciled)
                if let CascadePhase::Reconciling { progress, .. } = &plan.train.cascade_phase {
                    prop_assert!(
                        progress.completed.is_empty(),
                        "BUG: completed set should be reset on transition to Reconciling. \
                         Completed: {:?}. These were PREPARED, not RECONCILED.",
                        progress.completed
                    );
                    prop_assert_eq!(
                        progress.remaining().count(),
                        frozen_descendants.len(),
                        "All descendants should need reconciliation"
                    );
                }

                // MUST have VerifyPrepared action for remaining descendants
                // Per DESIGN.md: Any transition TO Reconciling must verify preparation
                let has_verify_prepared = plan.actions.iter().any(|a| {
                    matches!(a, RecoveryAction::VerifyPrepared { .. })
                });
                prop_assert!(
                    has_verify_prepared,
                    "BUG: Recovery transitioning to Reconciling MUST emit VerifyPrepared action. \
                     Actions: {:?}",
                    plan.actions
                );
            }

            // ─── Reconciling Verification Tests ─────────────────────────────────────────
            //
            // These tests verify the DESIGN.md requirement: "On recovery to Reconciling phase,
            // before proceeding, the bot MUST verify for each descendant not in completed".

            /// Property: Recovery for Reconciling phase with remaining descendants MUST emit
            /// VerifyPrepared action before RetryMerge actions.
            ///
            /// Per DESIGN.md "Verifying preparation before reconciliation": verification must
            /// happen BEFORE reconciliation proceeds, to prevent silent data loss.
            #[test]
            fn reconciling_recovery_emits_verify_prepared_before_retry(
                frozen_descendants in arb_unique_descendants(1, 3),
                squash_sha in arb_sha(),
                pr_sha in arb_sha()
            ) {
                // Create train in Reconciling phase
                let mut train = TrainRecord::new(PrNumber(1));
                train.cascade_phase = CascadePhase::Reconciling {
                    progress: DescendantProgress::new(frozen_descendants.clone()),
                    squash_sha: squash_sha.clone(),
                };
                train.predecessor_head_sha = Some(pr_sha.clone());
                train.predecessor_pr = Some(PrNumber(999)); // For fallback

                // Create PR map
                let mut prs = HashMap::new();
                let root_pr = make_open_pr_for_test(1, "main", None, pr_sha.clone());
                prs.insert(PrNumber(1), root_pr);

                for &desc in &frozen_descendants {
                    let pr = make_open_pr_for_test(desc.0, "branch-1", Some(PrNumber(1)), pr_sha.clone());
                    prs.insert(desc, pr);
                }

                let plan = compute_recovery_plan(&train, &prs, &[], "main");

                // MUST have VerifyPrepared action
                let has_verify_prepared = plan.actions.iter().any(|a| {
                    matches!(a, RecoveryAction::VerifyPrepared { .. })
                });
                prop_assert!(
                    has_verify_prepared,
                    "BUG: Recovery for Reconciling phase MUST emit VerifyPrepared action. \
                     Actions: {:?}",
                    plan.actions
                );

                // VerifyPrepared MUST come before any RetryMerge
                let verify_idx = plan.actions.iter().position(|a| {
                    matches!(a, RecoveryAction::VerifyPrepared { .. })
                });
                let first_retry_idx = plan.actions.iter().position(|a| {
                    matches!(a, RecoveryAction::RetryMerge { .. })
                });

                if let (Some(v_idx), Some(r_idx)) = (verify_idx, first_retry_idx) {
                    prop_assert!(
                        v_idx < r_idx,
                        "VerifyPrepared (idx {}) must come before RetryMerge (idx {}). \
                         Actions: {:?}",
                        v_idx, r_idx, plan.actions
                    );
                }
            }

            /// Property: VerifyPrepared action contains all remaining open descendants
            /// with their correct head SHAs.
            #[test]
            fn verify_prepared_contains_all_remaining_descendants(
                frozen_descendants in arb_unique_descendants(1, 3),
                squash_sha in arb_sha(),
                pr_sha in arb_sha()
            ) {
                let mut train = TrainRecord::new(PrNumber(1));
                train.cascade_phase = CascadePhase::Reconciling {
                    progress: DescendantProgress::new(frozen_descendants.clone()),
                    squash_sha: squash_sha.clone(),
                };
                train.predecessor_head_sha = Some(pr_sha.clone());

                let mut prs = HashMap::new();
                prs.insert(PrNumber(1), make_open_pr_for_test(1, "main", None, pr_sha.clone()));

                for &desc in &frozen_descendants {
                    let pr = make_open_pr_for_test(desc.0, "branch-1", Some(PrNumber(1)), pr_sha.clone());
                    prs.insert(desc, pr);
                }

                let plan = compute_recovery_plan(&train, &prs, &[], "main");

                // Find the VerifyPrepared action
                let verify_action = plan.actions.iter().find(|a| {
                    matches!(a, RecoveryAction::VerifyPrepared { .. })
                });

                if let Some(RecoveryAction::VerifyPrepared { descendants, predecessor_head_sha, .. }) = verify_action {
                    // Should contain all frozen descendants
                    let verified_prs: std::collections::HashSet<_> = descendants.iter().map(|(pr, _)| *pr).collect();
                    let expected_prs: std::collections::HashSet<_> = frozen_descendants.iter().copied().collect();

                    prop_assert_eq!(
                        verified_prs,
                        expected_prs,
                        "VerifyPrepared should contain all frozen descendants"
                    );

                    // predecessor_head_sha should match train's
                    prop_assert_eq!(
                        predecessor_head_sha.as_ref(),
                        Some(&pr_sha),
                        "VerifyPrepared should use train's predecessor_head_sha"
                    );
                } else {
                    prop_assert!(false, "No VerifyPrepared action found");
                }
            }

            /// Property: apply_recovery_plan with failed verification produces NeedsManualReview.
            #[test]
            fn apply_recovery_plan_failed_verification_needs_review(
                pr_number in 2u64..100,
            ) {
                let descendants = vec![(PrNumber(pr_number), Sha::parse("a".repeat(40)).unwrap())];
                let actions = vec![
                    RecoveryAction::VerifyPrepared {
                        predecessor_head_sha: Some(Sha::parse("b".repeat(40)).unwrap()),
                        predecessor_pr: None,
                        descendants: descendants.clone(),
                    },
                    RecoveryAction::RetryMerge {
                        descendant: PrNumber(pr_number),
                        source: "reconcile:abc".to_string(),
                        target_branch: format!("branch-{}", pr_number),
                    },
                ];

                let plan = RecoveryPlan {
                    train: TrainRecord::new(PrNumber(1)),
                    actions,
                    effects: vec![],
                };

                // Simulate failed verification - descendant was NOT prepared
                let mut verification_results = HashMap::new();
                verification_results.insert(format!("prepared:{}", pr_number), false);

                let (train, abort_reason) = apply_recovery_plan(plan, &verification_results);

                prop_assert_eq!(
                    train.state,
                    TrainState::NeedsManualReview,
                    "Failed verification should result in NeedsManualReview"
                );
                prop_assert!(
                    abort_reason.is_some(),
                    "Failed verification should produce abort reason"
                );
            }

            /// Property: apply_recovery_plan with passed verification does NOT produce NeedsManualReview.
            #[test]
            fn apply_recovery_plan_passed_verification_continues(
                pr_number in 2u64..100,
            ) {
                let descendants = vec![(PrNumber(pr_number), Sha::parse("a".repeat(40)).unwrap())];
                let actions = vec![
                    RecoveryAction::VerifyPrepared {
                        predecessor_head_sha: Some(Sha::parse("b".repeat(40)).unwrap()),
                        predecessor_pr: None,
                        descendants: descendants.clone(),
                    },
                    RecoveryAction::ResumeClean,
                ];

                let plan = RecoveryPlan {
                    train: TrainRecord::new(PrNumber(1)),
                    actions,
                    effects: vec![],
                };

                // Simulate passed verification - descendant WAS prepared
                let mut verification_results = HashMap::new();
                verification_results.insert(format!("prepared:{}", pr_number), true);

                let (train, abort_reason) = apply_recovery_plan(plan, &verification_results);

                prop_assert_eq!(
                    train.state,
                    TrainState::Running,
                    "Passed verification should keep train running"
                );
                prop_assert!(
                    abort_reason.is_none(),
                    "Passed verification should NOT produce abort reason"
                );
            }
        }
    }
}
