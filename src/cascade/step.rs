//! Cascade step execution.
//!
//! This module handles executing a single step of the cascade state machine.
//! A "step" processes one descendant through the current phase, potentially
//! triggering a phase transition when all descendants are processed.

use std::collections::HashMap;

use crate::cascade::engine::{MAX_TRAIN_SIZE, format_phase_comment};
use crate::effects::{Effect, GitEffect, GitHubEffect, MergeStrategy};
use crate::state::transitions::{PhaseOutcome, next_phase};
use crate::types::{
    AbortReason, CachedPr, CascadePhase, CascadeStepOutcome, DescendantProgress, PrNumber, PrState,
    Sha, TrainError, TrainRecord,
};

/// Context for executing a cascade step.
#[derive(Debug, Clone)]
pub struct StepContext {
    /// The default branch name.
    pub default_branch: String,

    /// Whether to sign git commits.
    pub sign_commits: bool,
}

impl StepContext {
    pub fn new(default_branch: impl Into<String>) -> Self {
        StepContext {
            default_branch: default_branch.into(),
            sign_commits: false,
        }
    }

    pub fn with_signing(mut self, sign: bool) -> Self {
        self.sign_commits = sign;
        self
    }
}

/// Result of executing a cascade step.
#[derive(Debug)]
pub struct StepResult {
    /// The updated train record.
    pub train: TrainRecord,

    /// The step outcome.
    pub outcome: CascadeStepOutcome,

    /// Effects to execute.
    pub effects: Vec<Effect>,
}

/// Executes a single cascade step.
///
/// This is the main function that drives the cascade forward. It:
/// 1. Determines what operation to perform based on the current phase
/// 2. Returns the effects needed to perform that operation
/// 3. Updates the train record with the new state
///
/// The caller is responsible for:
/// 1. Executing the returned effects
/// 2. Persisting the updated train record
/// 3. Calling `process_step_result` with the operation outcome
///
/// # Arguments
///
/// * `train` - The current train record
/// * `prs` - The cached PR information
/// * `ctx` - Execution context
///
/// # Returns
///
/// A `StepResult` containing the updated train, outcome, and effects.
pub fn execute_cascade_step(
    mut train: TrainRecord,
    prs: &HashMap<PrNumber, CachedPr>,
    ctx: &StepContext,
) -> StepResult {
    let current_pr_number = train.current_pr;

    // Get current PR info
    let Some(current_pr) = prs.get(&current_pr_number) else {
        // PR not found - abort
        train.abort(TrainError::new(
            "pr_not_found",
            format!("PR #{} not found in cache", current_pr_number),
        ));
        return StepResult {
            train,
            outcome: CascadeStepOutcome::Aborted {
                pr_number: current_pr_number,
                reason: AbortReason::PrClosed,
            },
            effects: vec![],
        };
    };

    // Check if PR is still open
    if !current_pr.state.is_open() {
        if let PrState::Merged { merge_commit_sha } = &current_pr.state {
            // PR was merged externally - handle this case
            return handle_external_merge(&mut train, merge_commit_sha.clone(), prs, ctx);
        }
        train.abort(TrainError::new(
            "pr_closed",
            "PR was closed without merging",
        ));
        return StepResult {
            train,
            outcome: CascadeStepOutcome::Aborted {
                pr_number: current_pr_number,
                reason: AbortReason::PrClosed,
            },
            effects: vec![],
        };
    }

    // Clone the phase to avoid borrowing issues
    let phase = train.cascade_phase.clone();

    // Execute based on current phase
    match phase {
        CascadePhase::Idle => execute_from_idle(&mut train, prs, ctx),
        CascadePhase::Preparing { progress } => execute_preparing(&mut train, progress, prs, ctx),
        CascadePhase::SquashPending { progress } => {
            execute_squash_pending(&mut train, progress, current_pr, ctx)
        }
        CascadePhase::Reconciling {
            progress,
            squash_sha,
        } => execute_reconciling(&mut train, progress, squash_sha, prs, ctx),
        CascadePhase::CatchingUp {
            progress,
            squash_sha,
        } => execute_catching_up(&mut train, progress, squash_sha, prs, ctx),
        CascadePhase::Retargeting {
            progress,
            squash_sha,
        } => execute_retargeting(&mut train, progress, squash_sha, prs, ctx),
    }
}

/// Execute from Idle phase - determine if we have descendants and transition appropriately.
///
/// After transitioning to the next phase, immediately execute that phase to produce
/// effects (rather than returning empty effects which would cause the train to stall).
fn execute_from_idle(
    train: &mut TrainRecord,
    prs: &HashMap<PrNumber, CachedPr>,
    ctx: &StepContext,
) -> StepResult {
    // Compute frozen descendants
    let descendants = compute_open_descendants(train.current_pr, prs);

    // Check max-train-size when entering Preparing phase.
    // This catches late additions that could exceed the limit even though
    // the initial train was within bounds.
    // The +1 accounts for the current PR itself.
    if descendants.len() + 1 > MAX_TRAIN_SIZE {
        train.abort(TrainError::new(
            "train_too_large",
            format!(
                "Train size {} exceeds maximum {}. Too many descendants were added.",
                descendants.len() + 1,
                MAX_TRAIN_SIZE
            ),
        ));
        return StepResult {
            train: train.clone(),
            outcome: CascadeStepOutcome::Aborted {
                pr_number: train.current_pr,
                reason: AbortReason::TrainTooLarge {
                    pr_count: descendants.len() + 1,
                    max_allowed: MAX_TRAIN_SIZE,
                },
            },
            effects: vec![],
        };
    }

    // Transition to next phase
    let new_phase = crate::state::transitions::start_preparing(descendants.clone());
    train.cascade_phase = new_phase.clone();
    train.increment_seq();

    // Immediately execute the new phase to produce effects
    match new_phase {
        CascadePhase::Preparing { progress } => {
            // Has descendants - immediately start preparing the first one
            execute_preparing(train, progress, prs, ctx)
        }
        CascadePhase::SquashPending { progress: _ } => {
            // No descendants - immediately emit the squash effect
            let Some(current_pr) = prs.get(&train.current_pr) else {
                train.abort(TrainError::new("pr_not_found", "Current PR not found"));
                return StepResult {
                    train: train.clone(),
                    outcome: CascadeStepOutcome::Aborted {
                        pr_number: train.current_pr,
                        reason: AbortReason::PrClosed,
                    },
                    effects: vec![],
                };
            };
            execute_squash_pending(train, DescendantProgress::new(vec![]), current_pr, ctx)
        }
        _ => unreachable!("start_preparing only returns Preparing or SquashPending"),
    }
}

/// Execute Preparing phase - merge predecessor head into the next descendant.
fn execute_preparing(
    train: &mut TrainRecord,
    progress: DescendantProgress,
    prs: &HashMap<PrNumber, CachedPr>,
    ctx: &StepContext,
) -> StepResult {
    // Get current PR info first (needed for transition)
    let Some(current_pr) = prs.get(&train.current_pr) else {
        train.abort(TrainError::new("pr_not_found", "Current PR not found"));
        return StepResult {
            train: train.clone(),
            outcome: CascadeStepOutcome::Aborted {
                pr_number: train.current_pr,
                reason: AbortReason::PrClosed,
            },
            effects: vec![],
        };
    };

    // Find the next descendant to prepare
    let Some(&next_descendant) = progress.remaining().next() else {
        // All descendants prepared - transition to SquashPending
        return transition_to_squash_pending(train, progress, current_pr, ctx);
    };

    // Get descendant PR info
    let Some(desc_pr) = prs.get(&next_descendant) else {
        // Descendant not found - skip it
        return skip_descendant(train, progress, next_descendant, "PR not found in cache");
    };

    // Check if descendant is still open
    if !desc_pr.state.is_open() {
        return skip_descendant(train, progress, next_descendant, "PR is no longer open");
    }

    // CRITICAL: Base-branch revalidation before preparing.
    // DESIGN.md requires verifying that descendant.base_ref matches the predecessor's
    // head branch. This prevents merging unrelated stacks if someone retargets a PR
    // between declaration and cascade time.
    //
    // ABORT rather than skip: skipping can silently drop retargeted descendants,
    // leading to incomplete cascades. Aborting makes the issue visible so the user
    // can fix the stack structure and restart.
    if desc_pr.base_ref != current_pr.head_ref {
        train.abort(TrainError::new(
            "base_branch_mismatch",
            format!(
                "PR #{} base branch '{}' doesn't match predecessor's head branch '{}'. \
                 The PR was likely retargeted. Please fix the stack structure and restart.",
                next_descendant, desc_pr.base_ref, current_pr.head_ref
            ),
        ));
        return StepResult {
            train: train.clone(),
            outcome: CascadeStepOutcome::Aborted {
                pr_number: next_descendant,
                reason: AbortReason::BaseBranchMismatch {
                    pr: next_descendant,
                    expected_base: current_pr.head_ref.clone(),
                    actual_base: desc_pr.base_ref.clone(),
                },
            },
            effects: vec![],
        };
    }

    // Store predecessor info for recovery
    train.predecessor_pr = Some(train.current_pr);
    train.predecessor_head_sha = Some(current_pr.head_sha.clone());
    train.increment_seq();

    // Generate effects for preparation
    // Fetch via PR refs to ensure we have the latest state
    // CRITICAL: We must checkout the fetched PR ref (detached), not the local branch name.
    // The local branch may be stale if the remote advanced. Checking out the fetched ref
    // ensures we start from the latest state on GitHub.
    let effects = vec![
        // Fetch both the predecessor and descendant PR refs
        Effect::Git(GitEffect::Fetch {
            refspecs: vec![
                format!(
                    "refs/pull/{}/head:refs/remotes/origin/pr/{}",
                    train.current_pr, train.current_pr
                ),
                format!(
                    "refs/pull/{}/head:refs/remotes/origin/pr/{}",
                    next_descendant, next_descendant
                ),
            ],
        }),
        // Checkout the descendant's PR ref (fetched state, not local branch)
        // Using detached mode since we're checking out a remote ref
        Effect::Git(GitEffect::Checkout {
            target: format!("refs/remotes/origin/pr/{}", next_descendant),
            detach: true,
        }),
        // Merge predecessor head (using the fetched PR ref)
        Effect::Git(GitEffect::Merge {
            target: format!("refs/remotes/origin/pr/{}", train.current_pr),
            strategy: MergeStrategy::Default,
            message: format!(
                "Merge predecessor into {} (merge train preparation)",
                desc_pr.head_ref
            ),
        }),
        // Push the result to the descendant's branch
        Effect::Git(GitEffect::Push {
            refspec: format!("HEAD:refs/heads/{}", desc_pr.head_ref),
            force: false,
        }),
    ];

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::WaitingOnCi {
            pr_number: next_descendant,
        },
        effects,
    }
}

/// Execute SquashPending phase - perform the squash-merge.
fn execute_squash_pending(
    train: &mut TrainRecord,
    _progress: DescendantProgress,
    current_pr: &CachedPr,
    _ctx: &StepContext,
) -> StepResult {
    // Generate squash-merge effect
    let effects = vec![Effect::GitHub(GitHubEffect::SquashMerge {
        pr: train.current_pr,
        expected_sha: current_pr.head_sha.clone(),
    })];

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::WaitingOnCi {
            pr_number: train.current_pr,
        },
        effects,
    }
}

/// Execute Reconciling phase - merge squash commit into descendants.
fn execute_reconciling(
    train: &mut TrainRecord,
    progress: DescendantProgress,
    squash_sha: Sha,
    prs: &HashMap<PrNumber, CachedPr>,
    ctx: &StepContext,
) -> StepResult {
    // Find the next descendant to reconcile
    let Some(&next_descendant) = progress.remaining().next() else {
        // All descendants reconciled - transition to CatchingUp
        return transition_to_catching_up(train, progress, squash_sha, prs, ctx);
    };

    // Get descendant PR info
    let Some(desc_pr) = prs.get(&next_descendant) else {
        return skip_descendant(train, progress, next_descendant, "PR not found in cache");
    };

    if !desc_pr.state.is_open() {
        return skip_descendant(train, progress, next_descendant, "PR is no longer open");
    }

    // Generate reconciliation effects (two merges: $SQUASH_SHA^ then ours-merge $SQUASH_SHA)
    let effects = vec![
        // Perform the two-step reconciliation merge
        Effect::Git(GitEffect::MergeReconcile {
            squash_sha: squash_sha.clone(),
            target_branch: desc_pr.head_ref.clone(),
        }),
        // Push the result
        Effect::Git(GitEffect::Push {
            refspec: format!("HEAD:refs/heads/{}", desc_pr.head_ref),
            force: false,
        }),
    ];

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::WaitingOnCi {
            pr_number: next_descendant,
        },
        effects,
    }
}

/// Execute CatchingUp phase - merge origin/main into descendants.
fn execute_catching_up(
    train: &mut TrainRecord,
    progress: DescendantProgress,
    squash_sha: Sha,
    prs: &HashMap<PrNumber, CachedPr>,
    ctx: &StepContext,
) -> StepResult {
    // Find the next descendant to catch up
    let Some(&next_descendant) = progress.remaining().next() else {
        // All descendants caught up - transition to Retargeting
        return transition_to_retargeting(train, progress, squash_sha, prs, ctx);
    };

    // Get descendant PR info
    let Some(desc_pr) = prs.get(&next_descendant) else {
        return skip_descendant(train, progress, next_descendant, "PR not found in cache");
    };

    if !desc_pr.state.is_open() {
        return skip_descendant(train, progress, next_descendant, "PR is no longer open");
    }

    // Generate catch-up effects
    // CRITICAL: Must fetch and checkout the descendant branch before merging.
    // The worktree may still be on a different branch from a prior operation.
    let effects = vec![
        // Fetch the descendant's latest state and the default branch
        Effect::Git(GitEffect::Fetch {
            refspecs: vec![
                format!(
                    "refs/pull/{}/head:refs/remotes/origin/pr/{}",
                    next_descendant, next_descendant
                ),
                ctx.default_branch.clone(),
            ],
        }),
        // Checkout the descendant's PR ref (fetched state)
        Effect::Git(GitEffect::Checkout {
            target: format!("refs/remotes/origin/pr/{}", next_descendant),
            detach: true,
        }),
        // Merge origin/main
        Effect::Git(GitEffect::Merge {
            target: format!("origin/{}", ctx.default_branch),
            strategy: MergeStrategy::Default,
            message: format!(
                "Merge {} into {} (merge train catch-up)",
                ctx.default_branch, desc_pr.head_ref
            ),
        }),
        // Push the result
        Effect::Git(GitEffect::Push {
            refspec: format!("HEAD:refs/heads/{}", desc_pr.head_ref),
            force: false,
        }),
    ];

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::WaitingOnCi {
            pr_number: next_descendant,
        },
        effects,
    }
}

/// Execute Retargeting phase - retarget descendants to default branch.
fn execute_retargeting(
    train: &mut TrainRecord,
    progress: DescendantProgress,
    _squash_sha: Sha,
    prs: &HashMap<PrNumber, CachedPr>,
    ctx: &StepContext,
) -> StepResult {
    // Find the next descendant to retarget
    let Some(&next_descendant) = progress.remaining().next() else {
        // All descendants retargeted - complete!
        return complete_cascade(train, progress);
    };

    // Get descendant PR info
    let Some(desc_pr) = prs.get(&next_descendant) else {
        return skip_descendant(train, progress, next_descendant, "PR not found in cache");
    };

    if !desc_pr.state.is_open() {
        return skip_descendant(train, progress, next_descendant, "PR is no longer open");
    }

    // Generate retarget effect
    let effects = vec![Effect::GitHub(GitHubEffect::RetargetPr {
        pr: next_descendant,
        new_base: ctx.default_branch.clone(),
    })];

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::WaitingOnCi {
            pr_number: next_descendant,
        },
        effects,
    }
}

/// Handle the case where a PR was merged externally (not by us).
///
/// Uses the frozen descendants from the current phase if available. If in Idle
/// phase (no frozen set yet), computes the descendant set - these descendants
/// weren't yet promised preparation, so this is a valid freeze point.
fn handle_external_merge(
    train: &mut TrainRecord,
    merge_sha: Sha,
    prs: &HashMap<PrNumber, CachedPr>,
    _ctx: &StepContext,
) -> StepResult {
    // Store the squash SHA
    train.last_squash_sha = Some(merge_sha.clone());

    // Get frozen descendants from current phase if available.
    // If in Idle phase (external merge before we started cascade), we need to
    // compute and freeze the descendants now - they haven't been promised
    // preparation yet, so this is a valid freeze point.
    let progress = match &train.cascade_phase {
        CascadePhase::Idle => {
            // Compute descendants now - this becomes the frozen set
            let descendants = compute_open_descendants(train.current_pr, prs);
            DescendantProgress::new(descendants)
        }
        _ => {
            // Use the existing frozen set from the current phase
            train
                .cascade_phase
                .progress()
                .cloned()
                .unwrap_or_else(|| DescendantProgress::new(vec![]))
        }
    };

    // Check remaining (frozen - completed - skipped)
    let remaining: Vec<PrNumber> = progress.remaining().copied().collect();

    if remaining.is_empty() {
        // No descendants to process - cascade complete
        return complete_cascade(train, progress);
    }

    // Transition to Reconciling phase with frozen descendants
    train.cascade_phase = CascadePhase::Reconciling {
        progress,
        squash_sha: merge_sha,
    };
    train.increment_seq();

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::Merged {
            pr_number: train.current_pr,
        },
        effects: vec![],
    }
}

/// Transition from Preparing to SquashPending.
///
/// After transitioning, immediately executes the SquashPending phase to produce
/// effects (rather than returning empty effects which would cause the train to stall).
/// Also emits a status comment update for GitHub-based recovery.
fn transition_to_squash_pending(
    train: &mut TrainRecord,
    progress: DescendantProgress,
    current_pr: &CachedPr,
    ctx: &StepContext,
) -> StepResult {
    // All descendants prepared
    let new_phase = next_phase(
        &CascadePhase::Preparing {
            progress: progress.clone(),
        },
        PhaseOutcome::AllComplete,
    )
    .expect("Preparing -> SquashPending is valid");

    train.cascade_phase = new_phase;
    train.increment_seq();

    // Immediately execute SquashPending to produce effects
    let mut result = execute_squash_pending(train, progress, current_pr, ctx);

    // Add status comment update for recovery (if we have a comment ID)
    if let Some(comment_id) = train.status_comment_id {
        result
            .effects
            .push(Effect::GitHub(GitHubEffect::UpdateComment {
                comment_id,
                body: format_phase_comment(&result.train),
            }));
    }

    result
}

/// Transition from Reconciling to CatchingUp.
///
/// After transitioning, immediately executes the CatchingUp phase to produce
/// effects (rather than returning empty effects which would cause the train to stall).
/// Also emits a status comment update for GitHub-based recovery.
fn transition_to_catching_up(
    train: &mut TrainRecord,
    progress: DescendantProgress,
    squash_sha: Sha,
    prs: &HashMap<PrNumber, CachedPr>,
    ctx: &StepContext,
) -> StepResult {
    let current_phase = CascadePhase::Reconciling {
        progress: progress.clone(),
        squash_sha: squash_sha.clone(),
    };

    let new_phase = next_phase(&current_phase, PhaseOutcome::AllComplete)
        .expect("Reconciling -> CatchingUp is valid");

    train.cascade_phase = new_phase.clone();
    train.increment_seq();

    // Immediately execute CatchingUp to produce effects
    let mut result = if let CascadePhase::CatchingUp {
        progress: new_progress,
        squash_sha: new_squash_sha,
    } = new_phase
    {
        execute_catching_up(train, new_progress, new_squash_sha, prs, ctx)
    } else {
        unreachable!("next_phase for Reconciling -> CatchingUp always returns CatchingUp")
    };

    // Add status comment update for recovery (if we have a comment ID)
    if let Some(comment_id) = train.status_comment_id {
        result
            .effects
            .push(Effect::GitHub(GitHubEffect::UpdateComment {
                comment_id,
                body: format_phase_comment(&result.train),
            }));
    }

    result
}

/// Transition from CatchingUp to Retargeting.
///
/// After transitioning, immediately executes the Retargeting phase to produce
/// effects (rather than returning empty effects which would cause the train to stall).
/// Also emits a status comment update for GitHub-based recovery.
fn transition_to_retargeting(
    train: &mut TrainRecord,
    progress: DescendantProgress,
    squash_sha: Sha,
    prs: &HashMap<PrNumber, CachedPr>,
    ctx: &StepContext,
) -> StepResult {
    let current_phase = CascadePhase::CatchingUp {
        progress: progress.clone(),
        squash_sha: squash_sha.clone(),
    };

    let new_phase = next_phase(&current_phase, PhaseOutcome::AllComplete)
        .expect("CatchingUp -> Retargeting is valid");

    train.cascade_phase = new_phase.clone();
    train.increment_seq();

    // Immediately execute Retargeting to produce effects
    let mut result = if let CascadePhase::Retargeting {
        progress: new_progress,
        squash_sha: new_squash_sha,
    } = new_phase
    {
        execute_retargeting(train, new_progress, new_squash_sha, prs, ctx)
    } else {
        unreachable!("next_phase for CatchingUp -> Retargeting always returns Retargeting")
    };

    // Add status comment update for recovery (if we have a comment ID)
    if let Some(comment_id) = train.status_comment_id {
        result
            .effects
            .push(Effect::GitHub(GitHubEffect::UpdateComment {
                comment_id,
                body: format_phase_comment(&result.train),
            }));
    }

    result
}

/// Complete the cascade (Retargeting -> Idle).
///
/// After all descendants have been retargeted, determine the next step:
/// - If no descendants were successfully processed, the train is truly complete
/// - If one descendant was processed, advance `current_pr` and continue
/// - If multiple descendants were processed, emit FanOut for parallel trains
fn complete_cascade(train: &mut TrainRecord, progress: DescendantProgress) -> StepResult {
    train.cascade_phase = CascadePhase::Idle;
    train.increment_seq();

    // Get descendants that were successfully processed (not skipped)
    let completed: Vec<PrNumber> = progress.completed.iter().copied().collect();

    match completed.len() {
        0 => {
            // No descendants were successfully processed - truly complete
            StepResult {
                train: train.clone(),
                outcome: CascadeStepOutcome::Complete,
                effects: vec![],
            }
        }
        1 => {
            // Single descendant - advance current_pr and continue the train
            train.current_pr = completed[0];
            train.increment_seq();
            StepResult {
                train: train.clone(),
                outcome: CascadeStepOutcome::WaitingOnCi {
                    pr_number: completed[0],
                },
                effects: vec![],
            }
        }
        _ => {
            // Multiple descendants - fan out into separate trains
            StepResult {
                train: train.clone(),
                outcome: CascadeStepOutcome::FanOut {
                    descendants: completed,
                },
                effects: vec![],
            }
        }
    }
}

/// Skip a descendant that can't be processed.
fn skip_descendant(
    train: &mut TrainRecord,
    mut progress: DescendantProgress,
    descendant: PrNumber,
    _reason: &str,
) -> StepResult {
    progress.mark_skipped(descendant);

    // Update the phase with the new progress
    train.cascade_phase = match &train.cascade_phase {
        CascadePhase::Preparing { .. } => CascadePhase::Preparing { progress },
        CascadePhase::SquashPending { .. } => CascadePhase::SquashPending { progress },
        CascadePhase::Reconciling { squash_sha, .. } => CascadePhase::Reconciling {
            progress,
            squash_sha: squash_sha.clone(),
        },
        CascadePhase::CatchingUp { squash_sha, .. } => CascadePhase::CatchingUp {
            progress,
            squash_sha: squash_sha.clone(),
        },
        CascadePhase::Retargeting { squash_sha, .. } => CascadePhase::Retargeting {
            progress,
            squash_sha: squash_sha.clone(),
        },
        CascadePhase::Idle => CascadePhase::Idle,
    };
    train.increment_seq();

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::WaitingOnCi {
            pr_number: train.current_pr,
        },
        effects: vec![],
    }
}

/// Compute open descendants of a PR.
fn compute_open_descendants(pr: PrNumber, prs: &HashMap<PrNumber, CachedPr>) -> Vec<PrNumber> {
    let descendants_index = crate::state::build_descendants_index(prs);

    descendants_index
        .get(&pr)
        .map(|descs| {
            descs
                .iter()
                .filter(|d| prs.get(d).map(|p| p.state.is_open()).unwrap_or(false))
                .copied()
                .collect()
        })
        .unwrap_or_default()
}

/// Process the result of an operation and update the train state.
///
/// This is called after effects have been executed to record the outcome
/// and potentially advance to the next phase.
pub fn process_operation_result(
    mut train: TrainRecord,
    operation: OperationResult,
) -> (TrainRecord, Option<CascadeStepOutcome>) {
    match operation {
        OperationResult::DescendantPrepared { pr } => {
            if let Some(progress) = train.cascade_phase.progress_mut() {
                progress.mark_completed(pr);
            }
            train.increment_seq();
            (train, None)
        }
        OperationResult::DescendantReconciled { pr, squash_sha: _ } => {
            if let Some(progress) = train.cascade_phase.progress_mut() {
                progress.mark_completed(pr);
            }
            // Record the squash SHA for this descendant (for late-addition tracking)
            // This would update the PR cache in the full implementation
            train.increment_seq();
            (train, None)
        }
        OperationResult::DescendantCaughtUp { pr } => {
            if let Some(progress) = train.cascade_phase.progress_mut() {
                progress.mark_completed(pr);
            }
            train.increment_seq();
            (train, None)
        }
        OperationResult::DescendantRetargeted { pr } => {
            if let Some(progress) = train.cascade_phase.progress_mut() {
                progress.mark_completed(pr);
            }
            train.increment_seq();
            (train, None)
        }
        OperationResult::SquashMerged { pr, squash_sha } => {
            train.last_squash_sha = Some(squash_sha.clone());

            // Transition to Reconciling
            if let CascadePhase::SquashPending { progress: _ } = &train.cascade_phase {
                let result = next_phase(
                    &train.cascade_phase,
                    PhaseOutcome::SquashComplete {
                        squash_sha: squash_sha.clone(),
                    },
                );
                if let Ok(new_phase) = result {
                    train.cascade_phase = new_phase;
                }
            }
            train.increment_seq();
            (train, Some(CascadeStepOutcome::Merged { pr_number: pr }))
        }
        OperationResult::OperationFailed { pr, error } => {
            train.abort(TrainError::new(error.error_type(), error.description()));
            (
                train,
                Some(CascadeStepOutcome::Aborted {
                    pr_number: pr,
                    reason: error,
                }),
            )
        }
        OperationResult::DescendantSkipped { pr, reason: _ } => {
            if let Some(progress) = train.cascade_phase.progress_mut() {
                progress.mark_skipped(pr);
            }
            train.increment_seq();
            (train, None)
        }
    }
}

/// Result of executing an operation.
#[derive(Debug, Clone)]
pub enum OperationResult {
    /// A descendant was successfully prepared.
    DescendantPrepared { pr: PrNumber },

    /// A descendant was successfully reconciled.
    DescendantReconciled { pr: PrNumber, squash_sha: Sha },

    /// A descendant was successfully caught up.
    DescendantCaughtUp { pr: PrNumber },

    /// A descendant was successfully retargeted.
    DescendantRetargeted { pr: PrNumber },

    /// The PR was squash-merged.
    SquashMerged { pr: PrNumber, squash_sha: Sha },

    /// An operation failed.
    OperationFailed { pr: PrNumber, error: AbortReason },

    /// A descendant was skipped (PR closed, etc.).
    DescendantSkipped { pr: PrNumber, reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MergeStateStatus, Sha};

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
    fn execute_from_idle_with_no_descendants() {
        let train = TrainRecord::new(PrNumber(1));
        let pr = make_open_pr(1, "main", None);
        let prs = HashMap::from([(PrNumber(1), pr)]);
        let ctx = StepContext::new("main");

        let result = execute_cascade_step(train, &prs, &ctx);

        assert!(matches!(
            result.train.cascade_phase,
            CascadePhase::SquashPending { .. }
        ));
    }

    #[test]
    fn execute_from_idle_with_descendants() {
        let train = TrainRecord::new(PrNumber(1));
        let pr1 = make_open_pr(1, "main", None);
        let mut pr2 = make_open_pr(2, "branch-1", Some(1));
        pr2.head_ref = "branch-2".to_string();

        let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);
        let ctx = StepContext::new("main");

        let result = execute_cascade_step(train, &prs, &ctx);

        assert!(matches!(
            result.train.cascade_phase,
            CascadePhase::Preparing { .. }
        ));
        if let CascadePhase::Preparing { progress } = &result.train.cascade_phase {
            assert_eq!(progress.frozen_descendants.len(), 1);
            assert!(progress.frozen_descendants.contains(&PrNumber(2)));
        }
    }

    #[test]
    fn execute_preparing_generates_merge_effects() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };

        let pr1 = make_open_pr(1, "main", None);
        let mut pr2 = make_open_pr(2, "branch-1", Some(1));
        pr2.head_ref = "branch-2".to_string();

        let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);
        let ctx = StepContext::new("main");

        let result = execute_cascade_step(train, &prs, &ctx);

        // Should have merge and push effects
        assert!(!result.effects.is_empty());
        assert!(
            result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::Git(GitEffect::Merge { .. })))
        );
        assert!(
            result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::Git(GitEffect::Push { .. })))
        );
    }

    #[test]
    fn execute_squash_pending_generates_squash_effect() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::SquashPending {
            progress: DescendantProgress::new(vec![]),
        };

        let pr = make_open_pr(1, "main", None);
        let prs = HashMap::from([(PrNumber(1), pr)]);
        let ctx = StepContext::new("main");

        let result = execute_cascade_step(train, &prs, &ctx);

        assert!(
            result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::GitHub(GitHubEffect::SquashMerge { .. })))
        );
    }

    #[test]
    fn process_squash_merged_transitions_to_reconciling() {
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::SquashPending {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };

        let squash_sha = make_sha(0x123);
        let (updated_train, outcome) = process_operation_result(
            train,
            OperationResult::SquashMerged {
                pr: PrNumber(1),
                squash_sha: squash_sha.clone(),
            },
        );

        assert!(matches!(
            updated_train.cascade_phase,
            CascadePhase::Reconciling { .. }
        ));
        assert!(matches!(outcome, Some(CascadeStepOutcome::Merged { .. })));
    }

    #[test]
    fn skip_descendant_marks_as_skipped() {
        let progress = DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]);
        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::Preparing { progress };

        // Simulate skipping PR #2
        let (updated_train, _) = process_operation_result(
            train,
            OperationResult::DescendantSkipped {
                pr: PrNumber(2),
                reason: "PR closed".to_string(),
            },
        );

        if let CascadePhase::Preparing { progress } = &updated_train.cascade_phase {
            assert!(progress.skipped.contains(&PrNumber(2)));
        } else {
            panic!("Expected Preparing phase");
        }
    }

    mod property_tests {
        use super::*;
        use proptest::prelude::*;

        fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
            (2u64..100).prop_map(PrNumber) // Start from 2 to avoid conflict with root PR (1)
        }

        fn arb_sha() -> impl Strategy<Value = Sha> {
            "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
        }

        /// Generate a vector of unique PR numbers (no duplicates).
        fn arb_unique_descendants(min: usize, max: usize) -> impl Strategy<Value = Vec<PrNumber>> {
            prop::collection::hash_set(arb_pr_number(), min..max)
                .prop_map(|set| set.into_iter().collect())
        }

        /// Generate a valid open PR with the given parameters.
        fn make_pr_with_predecessor(
            number: u64,
            head_sha: Sha,
            base_ref: &str,
            predecessor: Option<PrNumber>,
        ) -> CachedPr {
            CachedPr::new(
                PrNumber(number),
                head_sha,
                format!("branch-{}", number),
                base_ref.to_string(),
                predecessor,
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            )
        }

        /// Simulates executing effects and returns the appropriate OperationResult.
        /// This is used to drive the cascade forward in tests.
        fn simulate_effect_execution(
            train: &TrainRecord,
            effects: &[Effect],
            squash_sha: &Sha,
        ) -> Option<OperationResult> {
            // Find the primary effect and determine what result to return
            for effect in effects {
                match effect {
                    Effect::Git(GitEffect::Push { refspec, .. }) => {
                        // Extract the branch name to determine which descendant was pushed
                        // refspec is like "HEAD:refs/heads/branch-N"
                        if let Some(branch) = refspec.strip_prefix("HEAD:refs/heads/branch-") {
                            if let Ok(n) = branch.parse::<u64>() {
                                let pr = PrNumber(n);
                                // Determine the operation based on current phase
                                return match &train.cascade_phase {
                                    CascadePhase::Preparing { .. } => {
                                        Some(OperationResult::DescendantPrepared { pr })
                                    }
                                    CascadePhase::Reconciling { .. } => {
                                        Some(OperationResult::DescendantReconciled {
                                            pr,
                                            squash_sha: squash_sha.clone(),
                                        })
                                    }
                                    CascadePhase::CatchingUp { .. } => {
                                        Some(OperationResult::DescendantCaughtUp { pr })
                                    }
                                    _ => None,
                                };
                            }
                        }
                    }
                    Effect::GitHub(GitHubEffect::SquashMerge { pr, .. }) => {
                        return Some(OperationResult::SquashMerged {
                            pr: *pr,
                            squash_sha: squash_sha.clone(),
                        });
                    }
                    Effect::GitHub(GitHubEffect::RetargetPr { pr, .. }) => {
                        return Some(OperationResult::DescendantRetargeted { pr: *pr });
                    }
                    _ => {}
                }
            }
            None
        }

        proptest! {
            /// Property: execute_cascade_step never skips phases.
            ///
            /// This tests that repeated calls to execute_cascade_step transition
            /// through phases in the correct order: Idle -> Preparing -> SquashPending
            /// -> Reconciling -> CatchingUp -> Retargeting -> Idle.
            ///
            /// Unlike the previous version that only tested next_phase(), this test
            /// actually exercises the full cascade step execution path.
            #[test]
            fn cascade_step_never_skips_phases(
                descendants in arb_unique_descendants(1, 4),
                sha in arb_sha()
            ) {
                let ctx = StepContext::new("main");

                // Build PR map with root and descendants
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha.clone(), "main", None);
                prs.insert(PrNumber(1), root_pr);

                for (i, &pr_num) in descendants.iter().enumerate() {
                    let predecessor = if i == 0 {
                        PrNumber(1)
                    } else {
                        descendants[i - 1]
                    };
                    let base_ref = format!("branch-{}", predecessor.0);
                    let pr = make_pr_with_predecessor(pr_num.0, sha.clone(), &base_ref, Some(predecessor));
                    prs.insert(pr_num, pr);
                }

                // Start from Idle
                let mut train = TrainRecord::new(PrNumber(1));
                prop_assert!(matches!(train.cascade_phase, CascadePhase::Idle));

                // Track phase sequence
                let mut phase_sequence = vec!["idle".to_string()];
                let mut last_phase_name = "idle".to_string();

                // Run the cascade until complete or max iterations
                let max_iterations = 100;
                for _ in 0..max_iterations {
                    let result = execute_cascade_step(train.clone(), &prs, &ctx);
                    train = result.train;

                    // Record phase transitions (only when phase changes)
                    let current_phase = train.cascade_phase.name().to_string();
                    if current_phase != last_phase_name {
                        phase_sequence.push(current_phase.clone());
                        last_phase_name = current_phase;
                    }

                    // Check for completion or abort
                    if matches!(result.outcome, CascadeStepOutcome::Complete)
                        || matches!(result.outcome, CascadeStepOutcome::FanOut { .. })
                        || matches!(result.outcome, CascadeStepOutcome::Aborted { .. })
                    {
                        break;
                    }

                    // Simulate effect execution to drive the cascade forward
                    if !result.effects.is_empty() {
                        if let Some(op_result) = simulate_effect_execution(&train, &result.effects, &sha) {
                            let (updated_train, _) = process_operation_result(train, op_result);
                            train = updated_train;
                        }
                    }
                }

                // Verify the phase sequence follows the correct order
                // Valid order: idle -> preparing -> squash_pending -> reconciling -> catching_up -> retargeting -> idle
                let valid_order = ["idle", "preparing", "squash_pending", "reconciling", "catching_up", "retargeting"];

                let mut last_order_idx = 0;
                for (i, phase) in phase_sequence.iter().enumerate() {
                    // Find this phase in the valid order
                    if let Some(order_idx) = valid_order.iter().position(|&p| p == phase) {
                        // Phase must be >= last seen phase (can't go backwards, can repeat idle at end)
                        prop_assert!(
                            order_idx >= last_order_idx || (phase == "idle" && i > 0),
                            "Phase sequence went backwards: {:?} (phase {} at index {} came after phase at order {})",
                            phase_sequence, phase, order_idx, last_order_idx
                        );
                        last_order_idx = order_idx;
                    } else {
                        prop_assert!(false, "Unknown phase in sequence: {}", phase);
                    }
                }

                // For non-empty descendants, we should see all phases
                if !descendants.is_empty() {
                    prop_assert!(
                        phase_sequence.contains(&"preparing".to_string()),
                        "With descendants, should have visited Preparing phase. Sequence: {:?}",
                        phase_sequence
                    );
                }
            }

            /// Property: Late additions don't corrupt ongoing cascades.
            ///
            /// This test verifies that when a new descendant PR is added AFTER the cascade
            /// has started (and frozen its descendants), the late addition is NEVER processed
            /// in any phase. This tests the full cascade execution path using execute_cascade_step.
            #[test]
            fn late_additions_dont_corrupt_cascade(
                initial_descendants in arb_unique_descendants(1, 3),
                late_addition in arb_pr_number(),
                sha in arb_sha()
            ) {
                // Ensure late_addition is distinct from initial_descendants and root
                prop_assume!(!initial_descendants.contains(&late_addition));
                prop_assume!(late_addition != PrNumber(1));

                let ctx = StepContext::new("main");

                // Build initial PR map (WITHOUT the late addition)
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha.clone(), "main", None);
                prs.insert(PrNumber(1), root_pr);

                for (i, &pr_num) in initial_descendants.iter().enumerate() {
                    let predecessor = if i == 0 {
                        PrNumber(1)
                    } else {
                        initial_descendants[i - 1]
                    };
                    let base_ref = format!("branch-{}", predecessor.0);
                    let pr = make_pr_with_predecessor(pr_num.0, sha.clone(), &base_ref, Some(predecessor));
                    prs.insert(pr_num, pr);
                }

                // Start cascade - this freezes the descendants
                let mut train = TrainRecord::new(PrNumber(1));
                let result = execute_cascade_step(train.clone(), &prs, &ctx);
                train = result.train;

                // Capture frozen set
                let frozen_set: Vec<PrNumber> = match &train.cascade_phase {
                    CascadePhase::Preparing { progress } => progress.frozen_descendants.clone(),
                    CascadePhase::SquashPending { progress } => progress.frozen_descendants.clone(),
                    _ => vec![],
                };

                // NOW add the "late" descendant to the PR map (AFTER freeze)
                let late_pr = make_pr_with_predecessor(
                    late_addition.0,
                    sha.clone(),
                    "branch-1",
                    Some(PrNumber(1)),
                );
                prs.insert(late_addition, late_pr);

                // The frozen set should NOT contain the late addition
                prop_assert!(
                    !frozen_set.contains(&late_addition),
                    "Late addition {} should not be in frozen set {:?}",
                    late_addition,
                    frozen_set
                );

                // Track all PRs that get processed (via effects targeting them)
                let mut processed_prs: std::collections::HashSet<PrNumber> = std::collections::HashSet::new();

                // Run the cascade to completion, tracking which PRs are processed
                let max_iterations = 100;
                for _ in 0..max_iterations {
                    let result = execute_cascade_step(train.clone(), &prs, &ctx);
                    train = result.train;

                    // Track which PRs are targeted by effects
                    for effect in &result.effects {
                        match effect {
                            Effect::Git(GitEffect::Push { refspec, .. }) => {
                                // refspec is like "HEAD:refs/heads/branch-N"
                                if let Some(branch) = refspec.strip_prefix("HEAD:refs/heads/branch-") {
                                    if let Ok(n) = branch.parse::<u64>() {
                                        processed_prs.insert(PrNumber(n));
                                    }
                                }
                            }
                            Effect::GitHub(GitHubEffect::RetargetPr { pr, .. }) => {
                                processed_prs.insert(*pr);
                            }
                            _ => {}
                        }
                    }

                    // Check for completion or abort
                    if matches!(result.outcome, CascadeStepOutcome::Complete)
                        || matches!(result.outcome, CascadeStepOutcome::FanOut { .. })
                        || matches!(result.outcome, CascadeStepOutcome::Aborted { .. })
                    {
                        break;
                    }

                    // Simulate effect execution to drive the cascade forward
                    if !result.effects.is_empty() {
                        if let Some(op_result) = simulate_effect_execution(&train, &result.effects, &sha) {
                            let (updated_train, _) = process_operation_result(train, op_result);
                            train = updated_train;
                        }
                    }
                }

                // CRITICAL ASSERTION: The late addition should NEVER have been processed
                prop_assert!(
                    !processed_prs.contains(&late_addition),
                    "Late addition {} was processed but should have been ignored! Processed: {:?}, Frozen: {:?}",
                    late_addition,
                    processed_prs,
                    frozen_set
                );

                // Verify frozen_descendants was preserved throughout (check final state)
                if let Some(progress) = train.cascade_phase.progress() {
                    prop_assert!(
                        !progress.frozen_descendants.contains(&late_addition),
                        "Late addition {} ended up in frozen_descendants!",
                        late_addition
                    );
                }
            }

            /// Property: Fan-out detection works correctly.
            ///
            /// When determining the outcome for a train with multiple descendants,
            /// create_step_outcome should return FanOut with all descendants.
            #[test]
            fn fan_out_detection_correct(
                descendants in arb_unique_descendants(2, 5),
                sha in arb_sha()
            ) {
                // Build PR map with multiple descendants of root (fan-out)
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha.clone(), "main", None);
                prs.insert(PrNumber(1), root_pr);

                // All descendants point directly to root (fan-out)
                for &pr_num in &descendants {
                    let pr = make_pr_with_predecessor(
                        pr_num.0,
                        sha.clone(),
                        "branch-1",
                        Some(PrNumber(1)),
                    );
                    prs.insert(pr_num, pr);
                }

                // Create a train in Idle (simulating after one PR completes)
                let train = TrainRecord::new(PrNumber(1));

                // Test create_step_outcome
                use crate::cascade::engine::CascadeEngine;
                let engine = CascadeEngine::new("main");
                let outcome = engine.create_step_outcome(&train, &prs);

                prop_assert!(
                    matches!(outcome, CascadeStepOutcome::FanOut { .. }),
                    "With multiple descendants, outcome should be FanOut, got: {:?}",
                    outcome
                );

                if let CascadeStepOutcome::FanOut {
                    descendants: outcome_descendants,
                } = outcome
                {
                    prop_assert_eq!(
                        outcome_descendants.len(),
                        descendants.len(),
                        "FanOut should contain all descendants"
                    );
                    for &desc in &descendants {
                        prop_assert!(
                            outcome_descendants.contains(&desc),
                            "FanOut should contain descendant {}",
                            desc
                        );
                    }
                }
            }

            /// Property: Single descendant returns WaitingOnCi, not FanOut.
            #[test]
            fn single_descendant_returns_waiting_on_ci(
                descendant in arb_pr_number(),
                sha in arb_sha()
            ) {
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha.clone(), "main", None);
                prs.insert(PrNumber(1), root_pr);

                let desc_pr = make_pr_with_predecessor(
                    descendant.0,
                    sha.clone(),
                    "branch-1",
                    Some(PrNumber(1)),
                );
                prs.insert(descendant, desc_pr);

                let train = TrainRecord::new(PrNumber(1));

                use crate::cascade::engine::CascadeEngine;
                let engine = CascadeEngine::new("main");
                let outcome = engine.create_step_outcome(&train, &prs);

                prop_assert!(
                    matches!(outcome, CascadeStepOutcome::WaitingOnCi { .. }),
                    "With single descendant, outcome should be WaitingOnCi, got: {:?}",
                    outcome
                );

                if let CascadeStepOutcome::WaitingOnCi { pr_number } = outcome {
                    prop_assert_eq!(
                        pr_number, descendant,
                        "WaitingOnCi should contain the single descendant"
                    );
                }
            }

            /// Property: No descendants returns Complete.
            #[test]
            fn no_descendants_returns_complete(sha in arb_sha()) {
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha, "main", None);
                prs.insert(PrNumber(1), root_pr);

                let train = TrainRecord::new(PrNumber(1));

                use crate::cascade::engine::CascadeEngine;
                let engine = CascadeEngine::new("main");
                let outcome = engine.create_step_outcome(&train, &prs);

                prop_assert!(
                    matches!(outcome, CascadeStepOutcome::Complete),
                    "With no descendants, outcome should be Complete, got: {:?}",
                    outcome
                );
            }

            // ─── Bug Fix Tests ───────────────────────────────────────────────────────
            //
            // These tests expose bugs described in review comments. They should FAIL
            // before the fix is applied and PASS after.

            /// Property: After completing Retargeting, cascade advances current_pr or emits FanOut.
            ///
            /// BUG: complete_cascade just sets Idle and returns Complete, never advancing
            /// current_pr or emitting FanOut. The train stops at the root and never
            /// processes descendants.
            #[test]
            fn complete_cascade_advances_or_fans_out(
                frozen_descendants in arb_unique_descendants(1, 4),
                sha in arb_sha()
            ) {
                let ctx = StepContext::new("main");

                // Build PR map
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha.clone(), "main", None);
                prs.insert(PrNumber(1), root_pr);

                for &pr_num in &frozen_descendants {
                    let pr = make_pr_with_predecessor(
                        pr_num.0,
                        sha.clone(),
                        "branch-1",
                        Some(PrNumber(1)),
                    );
                    prs.insert(pr_num, pr);
                }

                // Create a train in Retargeting phase with all descendants completed
                let mut train = TrainRecord::new(PrNumber(1));
                let mut progress = DescendantProgress::new(frozen_descendants.clone());
                for &desc in &frozen_descendants {
                    progress.mark_completed(desc);
                }
                train.cascade_phase = CascadePhase::Retargeting {
                    progress,
                    squash_sha: sha.clone(),
                };

                // Execute step - should complete cascade
                let result = execute_cascade_step(train, &prs, &ctx);

                // After Retargeting completes with N descendants:
                // - N=0: Complete (but we have at least 1)
                // - N=1: should advance current_pr and return WaitingOnCi
                // - N>1: should return FanOut with all completed descendants
                match frozen_descendants.len() {
                    1 => {
                        // Single descendant: should advance current_pr
                        prop_assert_eq!(
                            result.train.current_pr,
                            frozen_descendants[0],
                            "With single completed descendant, current_pr should advance to {}",
                            frozen_descendants[0]
                        );
                        prop_assert!(
                            matches!(result.outcome, CascadeStepOutcome::WaitingOnCi { pr_number } if pr_number == frozen_descendants[0]),
                            "With single completed descendant, outcome should be WaitingOnCi for that PR, got: {:?}",
                            result.outcome
                        );
                    }
                    n if n > 1 => {
                        // Multiple descendants: should fan out
                        prop_assert!(
                            matches!(&result.outcome, CascadeStepOutcome::FanOut { descendants } if descendants.len() == n),
                            "With {} completed descendants, outcome should be FanOut with all of them, got: {:?}",
                            n, result.outcome
                        );
                        if let CascadeStepOutcome::FanOut { descendants } = &result.outcome {
                            for &desc in &frozen_descendants {
                                prop_assert!(
                                    descendants.contains(&desc),
                                    "FanOut should contain completed descendant {}", desc
                                );
                            }
                        }
                    }
                    _ => unreachable!("arb_unique_descendants(1, 4) guarantees at least 1"),
                }
            }

            /// Property: External merge uses frozen descendants, not recomputed ones.
            ///
            /// BUG: handle_external_merge calls compute_open_descendants() which
            /// recomputes descendants, ignoring the frozen set and any late additions.
            #[test]
            fn external_merge_uses_frozen_descendants(
                frozen_descendants in arb_unique_descendants(1, 3),
                late_addition in arb_pr_number(),
                sha in arb_sha()
            ) {
                prop_assume!(!frozen_descendants.contains(&late_addition));

                let ctx = StepContext::new("main");

                // Build PR map with frozen descendants
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha.clone(), "main", None);
                prs.insert(PrNumber(1), root_pr);

                for &pr_num in &frozen_descendants {
                    let pr = make_pr_with_predecessor(
                        pr_num.0,
                        sha.clone(),
                        "branch-1",
                        Some(PrNumber(1)),
                    );
                    prs.insert(pr_num, pr);
                }

                // Create train in Preparing phase (descendants frozen)
                let mut train = TrainRecord::new(PrNumber(1));
                train.cascade_phase = CascadePhase::Preparing {
                    progress: DescendantProgress::new(frozen_descendants.clone()),
                };

                // Now add a late descendant AFTER the freeze
                let late_pr = make_pr_with_predecessor(
                    late_addition.0,
                    sha.clone(),
                    "branch-1",
                    Some(PrNumber(1)),
                );
                prs.insert(late_addition, late_pr);

                // Simulate external merge by setting PR state to merged
                let merge_sha = Sha::parse("f".repeat(40)).unwrap();
                let mut merged_root = make_pr_with_predecessor(1, sha.clone(), "main", None);
                merged_root.state = PrState::Merged { merge_commit_sha: merge_sha.clone() };
                prs.insert(PrNumber(1), merged_root);

                // Execute step - should handle external merge
                let result = execute_cascade_step(train, &prs, &ctx);

                // The result should transition to Reconciling with frozen descendants only
                prop_assert!(
                    matches!(&result.train.cascade_phase, CascadePhase::Reconciling { progress, .. }
                        if progress.frozen_descendants == frozen_descendants),
                    "External merge should use frozen descendants {:?}, got phase: {:?}",
                    frozen_descendants, result.train.cascade_phase
                );

                // Late addition should NOT be in the frozen set
                if let CascadePhase::Reconciling { progress, .. } = &result.train.cascade_phase {
                    prop_assert!(
                        !progress.frozen_descendants.contains(&late_addition),
                        "Late addition {} should not be in frozen descendants after external merge",
                        late_addition
                    );
                }
            }

            /// Property: Idle transition produces effects, not just WaitingOnCi.
            ///
            /// BUG: execute_from_idle transitions to Preparing/SquashPending but returns
            /// no effects, causing the train to stall until something else triggers it.
            #[test]
            fn idle_transition_produces_effects(
                descendants in arb_unique_descendants(0, 3),
                sha in arb_sha()
            ) {
                let ctx = StepContext::new("main");

                // Build PR map
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha.clone(), "main", None);
                prs.insert(PrNumber(1), root_pr);

                for &pr_num in &descendants {
                    let pr = make_pr_with_predecessor(
                        pr_num.0,
                        sha.clone(),
                        "branch-1",
                        Some(PrNumber(1)),
                    );
                    prs.insert(pr_num, pr);
                }

                // Start from Idle
                let train = TrainRecord::new(PrNumber(1));
                prop_assert!(matches!(train.cascade_phase, CascadePhase::Idle));

                // Execute step from Idle
                let result = execute_cascade_step(train, &prs, &ctx);

                // The transition should produce effects to drive the next operation
                if descendants.is_empty() {
                    // No descendants: should go to SquashPending with squash effect
                    prop_assert!(
                        matches!(result.train.cascade_phase, CascadePhase::SquashPending { .. }),
                        "With no descendants, should transition to SquashPending"
                    );
                    prop_assert!(
                        result.effects.iter().any(|e| matches!(e, Effect::GitHub(GitHubEffect::SquashMerge { .. }))),
                        "Transition to SquashPending should produce SquashMerge effect, got: {:?}",
                        result.effects
                    );
                } else {
                    // Has descendants: should go to Preparing with merge effects
                    prop_assert!(
                        matches!(result.train.cascade_phase, CascadePhase::Preparing { .. }),
                        "With descendants, should transition to Preparing"
                    );
                    prop_assert!(
                        !result.effects.is_empty(),
                        "Transition to Preparing should produce effects to start preparation, got empty"
                    );
                }
            }

            /// Property: Preparation includes Fetch effect for PR refs.
            ///
            /// BUG: execute_preparing uses cached head_sha directly without fetching
            /// via refs/pull/<n>/head, which may be stale.
            #[test]
            fn preparation_fetches_pr_refs(
                descendant in arb_pr_number(),
                sha in arb_sha()
            ) {
                let ctx = StepContext::new("main");

                // Build PR map
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha.clone(), "main", None);
                prs.insert(PrNumber(1), root_pr);

                let desc_pr = make_pr_with_predecessor(
                    descendant.0,
                    sha.clone(),
                    "branch-1",
                    Some(PrNumber(1)),
                );
                prs.insert(descendant, desc_pr);

                // Create train in Preparing phase
                let mut train = TrainRecord::new(PrNumber(1));
                train.cascade_phase = CascadePhase::Preparing {
                    progress: DescendantProgress::new(vec![descendant]),
                };

                let result = execute_cascade_step(train, &prs, &ctx);

                // Should include a Fetch effect to get latest PR refs
                let has_fetch = result.effects.iter().any(|e| {
                    matches!(e, Effect::Git(GitEffect::Fetch { refspecs })
                        if refspecs.iter().any(|r| r.contains("refs/pull/")))
                });

                prop_assert!(
                    has_fetch,
                    "Preparation should fetch PR refs before merging, effects: {:?}",
                    result.effects
                );
            }

            /// Property: Fan-out creates independent trains correctly.
            ///
            /// When a cascade completes for a root PR with multiple descendants (fan-out),
            /// the system should be able to start independent trains from each descendant.
            /// Each new train should have its own isolated state.
            #[test]
            fn fan_out_creates_independent_trains(
                descendants in arb_unique_descendants(2, 4),
                sha in arb_sha()
            ) {
                use crate::cascade::engine::CascadeEngine;

                let ctx = StepContext::new("main");
                let engine = CascadeEngine::new("main");

                // Build PR map with root and multiple direct descendants (fan-out topology)
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha.clone(), "main", None);
                prs.insert(PrNumber(1), root_pr);

                // All descendants point directly to root (fan-out topology)
                for &pr_num in &descendants {
                    let pr = make_pr_with_predecessor(
                        pr_num.0,
                        sha.clone(),
                        "branch-1",
                        Some(PrNumber(1)),
                    );
                    prs.insert(pr_num, pr);
                }

                // Simulate completing the root cascade up to fan-out point
                // Start train for root
                let mut train = TrainRecord::new(PrNumber(1));

                // Execute until we get FanOut outcome
                let max_iterations = 50;
                let mut fan_out_descendants: Option<Vec<PrNumber>> = None;

                for _ in 0..max_iterations {
                    let result = execute_cascade_step(train.clone(), &prs, &ctx);
                    train = result.train;

                    if let CascadeStepOutcome::FanOut { descendants: fan_out } = result.outcome {
                        fan_out_descendants = Some(fan_out);
                        break;
                    }

                    if matches!(result.outcome, CascadeStepOutcome::Complete)
                        || matches!(result.outcome, CascadeStepOutcome::Aborted { .. })
                    {
                        break;
                    }

                    // Simulate effect execution
                    if !result.effects.is_empty() {
                        if let Some(op_result) = simulate_effect_execution(&train, &result.effects, &sha) {
                            let (updated_train, _) = process_operation_result(train, op_result);
                            train = updated_train;
                        }
                    }
                }

                // If we got a fan-out, verify we can start independent trains
                if let Some(fan_out) = fan_out_descendants {
                    prop_assert!(
                        fan_out.len() >= 2,
                        "Fan-out should have at least 2 descendants, got: {:?}",
                        fan_out
                    );

                    // Try to start independent trains for each descendant
                    let mut independent_trains = Vec::new();
                    let mut active_trains = HashMap::new();

                    // After fan-out, the original root is merged. Update the prs map to reflect this.
                    // is_root requires: targets default_branch AND (no predecessor OR merged predecessor
                    // with predecessor_squash_reconciled set).
                    let mut merged_root = prs.get(&PrNumber(1)).unwrap().clone();
                    merged_root.state = crate::types::PrState::Merged {
                        merge_commit_sha: sha.clone(),
                    };
                    prs.insert(PrNumber(1), merged_root);

                    for &desc_pr in &fan_out {
                        // Update the PR to be a root:
                        // 1. Retarget to main (simulating GitHub retarget API call)
                        // 2. Set predecessor_squash_reconciled (marking reconciliation complete)
                        // Note: predecessor is NOT cleared - is_root checks if predecessor is merged
                        // and predecessor_squash_reconciled is set.
                        let mut updated_desc = prs.get(&desc_pr).unwrap().clone();
                        updated_desc.base_ref = "main".to_string();
                        updated_desc.predecessor_squash_reconciled = Some(sha.clone());
                        prs.insert(desc_pr, updated_desc);

                        // Try to start a train
                        let result = engine.start_train(desc_pr, &prs, &active_trains);

                        prop_assert!(
                            result.is_ok(),
                            "Should be able to start independent train for {}, got error: {:?}",
                            desc_pr,
                            result.err()
                        );

                        let start_result = result.unwrap();
                        prop_assert_eq!(
                            start_result.train.original_root_pr, desc_pr,
                            "New train should have {} as root",
                            desc_pr
                        );

                        // Add to active trains to prevent duplicates
                        active_trains.insert(desc_pr, start_result.train.clone());
                        independent_trains.push(start_result.train);
                    }

                    // Verify all trains are independent (different root PRs)
                    let roots: std::collections::HashSet<PrNumber> = independent_trains
                        .iter()
                        .map(|t| t.original_root_pr)
                        .collect();
                    prop_assert_eq!(
                        roots.len(),
                        independent_trains.len(),
                        "Each train should have a unique root PR"
                    );
                }
            }

            /// Property: End-to-end cascade completes successfully.
            ///
            /// This is an integration test that runs a complete cascade from start to finish,
            /// simulating effect execution between steps. It verifies the entire cascade
            /// machinery works together correctly.
            #[test]
            fn end_to_end_cascade_completes(
                descendants in arb_unique_descendants(1, 3),
                sha in arb_sha()
            ) {
                let ctx = StepContext::new("main");

                // Build PR map with a chain of descendants
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha.clone(), "main", None);
                prs.insert(PrNumber(1), root_pr);

                let descendants_vec: Vec<PrNumber> = descendants.iter().copied().collect();
                for (i, &pr_num) in descendants_vec.iter().enumerate() {
                    let predecessor = if i == 0 {
                        PrNumber(1)
                    } else {
                        descendants_vec[i - 1]
                    };
                    let base_ref = format!("branch-{}", predecessor.0);
                    let pr = make_pr_with_predecessor(pr_num.0, sha.clone(), &base_ref, Some(predecessor));
                    prs.insert(pr_num, pr);
                }

                // Start cascade from Idle
                let mut train = TrainRecord::new(PrNumber(1));
                prop_assert!(matches!(train.cascade_phase, CascadePhase::Idle));

                // Track visited phases and processed PRs
                let mut visited_phases: Vec<String> = vec![];
                let mut processed_prs: std::collections::HashSet<PrNumber> = std::collections::HashSet::new();
                let mut completed = false;

                // Run cascade to completion
                let max_iterations = 200;
                for iteration in 0..max_iterations {
                    let result = execute_cascade_step(train.clone(), &prs, &ctx);
                    train = result.train;

                    // Track phase
                    let phase_name = train.cascade_phase.name().to_string();
                    if visited_phases.last() != Some(&phase_name) {
                        visited_phases.push(phase_name);
                    }

                    // Track processed PRs from effects
                    for effect in &result.effects {
                        if let Effect::Git(GitEffect::Push { refspec, .. }) = effect {
                            if let Some(branch) = refspec.strip_prefix("HEAD:refs/heads/branch-") {
                                if let Ok(n) = branch.parse::<u64>() {
                                    processed_prs.insert(PrNumber(n));
                                }
                            }
                        }
                        if let Effect::GitHub(GitHubEffect::RetargetPr { pr, .. }) = effect {
                            processed_prs.insert(*pr);
                        }
                    }

                    // Check for completion
                    match &result.outcome {
                        CascadeStepOutcome::Complete => {
                            completed = true;
                            break;
                        }
                        CascadeStepOutcome::FanOut { .. } => {
                            // Fan-out is also a valid completion for this PR
                            completed = true;
                            break;
                        }
                        CascadeStepOutcome::Aborted { reason, .. } => {
                            prop_assert!(
                                false,
                                "Cascade aborted unexpectedly at iteration {}: {:?}",
                                iteration,
                                reason
                            );
                        }
                        _ => {}
                    }

                    // Simulate effect execution
                    if !result.effects.is_empty() {
                        if let Some(op_result) = simulate_effect_execution(&train, &result.effects, &sha) {
                            let (updated_train, _) = process_operation_result(train, op_result);
                            train = updated_train;
                        }
                    }
                }

                // Verify completion
                prop_assert!(
                    completed,
                    "Cascade should complete within {} iterations. Final phase: {}, visited: {:?}",
                    max_iterations,
                    train.cascade_phase.name(),
                    visited_phases
                );

                // Verify all phases were visited (for non-empty descendants)
                if !descendants.is_empty() {
                    let required_phases = ["idle", "preparing", "squash_pending", "reconciling", "catching_up", "retargeting"];
                    for required in required_phases {
                        prop_assert!(
                            visited_phases.iter().any(|p| p == required),
                            "Cascade should visit {} phase. Visited: {:?}",
                            required,
                            visited_phases
                        );
                    }

                    // Verify all descendants were processed
                    for &desc in &descendants {
                        prop_assert!(
                            processed_prs.contains(&desc),
                            "Descendant {} should have been processed. Processed: {:?}",
                            desc,
                            processed_prs
                        );
                    }
                }
            }

            /// Property: Active trains always produce effects or reach a terminal state.
            ///
            /// This property catches bugs where phase transitions return empty effects,
            /// causing the cascade to stall. For any active, non-blocked train, either:
            /// 1. The step produces non-empty effects (work is being done)
            /// 2. The outcome is terminal (Complete or Aborted)
            /// 3. The train is waiting (WaitingOnCi or Blocked)
            ///
            /// The key insight: if a train is active and the outcome suggests work should
            /// continue (WaitingOnCi with same phase), there MUST be effects to execute.
            /// Empty effects + WaitingOnCi = stall.
            #[test]
            fn active_train_produces_progress_or_terminates(
                descendants in arb_unique_descendants(0, 4),
                sha in arb_sha(),
                phase_index in 0usize..6
            ) {
                let ctx = StepContext::new("main");

                // Build PR map
                let mut prs = HashMap::new();
                let root_pr = make_pr_with_predecessor(1, sha.clone(), "main", None);
                prs.insert(PrNumber(1), root_pr);

                let descendants_vec: Vec<PrNumber> = descendants.iter().copied().collect();
                for (i, &pr_num) in descendants_vec.iter().enumerate() {
                    let predecessor = if i == 0 {
                        PrNumber(1)
                    } else {
                        descendants_vec[i - 1]
                    };
                    let base_ref = format!("branch-{}", predecessor.0);
                    let pr = make_pr_with_predecessor(pr_num.0, sha.clone(), &base_ref, Some(predecessor));
                    prs.insert(pr_num, pr);
                }

                // Create train in various phases
                let mut train = TrainRecord::new(PrNumber(1));
                let progress = DescendantProgress::new(descendants_vec.clone());

                // Set up train in different phases to test each transition
                train.cascade_phase = match phase_index {
                    0 => CascadePhase::Idle,
                    1 => CascadePhase::Preparing { progress: progress.clone() },
                    2 => CascadePhase::SquashPending { progress: progress.clone() },
                    3 => CascadePhase::Reconciling { progress: progress.clone(), squash_sha: sha.clone() },
                    4 => CascadePhase::CatchingUp { progress: progress.clone(), squash_sha: sha.clone() },
                    _ => CascadePhase::Retargeting { progress, squash_sha: sha.clone() },
                };

                // Set required fields for later phases
                if phase_index >= 3 {
                    train.last_squash_sha = Some(sha.clone());
                    train.predecessor_head_sha = Some(sha.clone());
                }

                let result = execute_cascade_step(train, &prs, &ctx);

                // The key property: active trains must make progress
                let is_terminal = matches!(
                    result.outcome,
                    CascadeStepOutcome::Complete | CascadeStepOutcome::Aborted { .. }
                );
                let is_waiting = matches!(
                    result.outcome,
                    CascadeStepOutcome::WaitingOnCi { .. } | CascadeStepOutcome::Blocked { .. }
                );
                let is_fan_out = matches!(result.outcome, CascadeStepOutcome::FanOut { .. });
                let has_effects = !result.effects.is_empty();

                // If the train is still active and waiting, it must have produced effects
                // (otherwise it's stalled). Terminal states and fan-out are allowed to have
                // empty effects since they represent completion.
                prop_assert!(
                    is_terminal || is_fan_out || has_effects || !is_waiting,
                    "Active train in waiting state must produce effects to avoid stall. \
                     Outcome: {:?}, Effects: {}, Phase: {}",
                    result.outcome,
                    result.effects.len(),
                    result.train.cascade_phase.name()
                );
            }
        }
    }
}
