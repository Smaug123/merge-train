//! Cascade step execution.
//!
//! This module handles executing a single step of the cascade state machine.
//! A "step" processes one descendant through the current phase, potentially
//! triggering a phase transition when all descendants are processed.

use std::collections::HashMap;

use crate::cascade::engine::{CascadeError, MAX_TRAIN_SIZE, format_phase_comment};
use crate::effects::{Effect, GitEffect, GitHubEffect, MergeStrategy};
use crate::state::transitions::{PhaseOutcome, next_phase};
use crate::types::{
    AbortReason, CachedPr, CascadePhase, CascadeStepOutcome, DescendantProgress, PrNumber, PrState,
    Sha, TrainError, TrainRecord, TrainState,
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

    // Resume from WaitingCi if needed - we're actively processing now
    if train.state == TrainState::WaitingCi {
        train.resume();
    }

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
    // Compute DIRECT descendants for the frozen set.
    // Per DESIGN.md, this is a "per-PR freeze" - we freeze only the descendants that
    // will be prepared before THIS PR squashes. Transitive descendants (children of
    // children) will be handled when their predecessor becomes the current PR.
    //
    // The "traversing through open PRs" in DESIGN.md refers to how we compute which
    // PRs are descendants (closed PRs block traversal in the index), not that we
    // freeze all transitive descendants.
    let direct_descendants = compute_direct_descendants(train.current_pr, prs);

    // Compute ALL transitive descendants for size check.
    // The train size limit should consider the entire reachable chain, not just
    // direct children. This catches deep chains that could exceed 50 PRs.
    let total_descendants = compute_all_descendants(train.current_pr, prs);

    // Check max-train-size when entering Preparing phase.
    // This catches late additions that could exceed the limit even though
    // the initial train was within bounds.
    // The +1 accounts for the current PR itself.
    if total_descendants.len() + 1 > MAX_TRAIN_SIZE {
        train.abort(TrainError::new(
            "train_too_large",
            format!(
                "Train size {} exceeds maximum {}. Too many descendants were added.",
                total_descendants.len() + 1,
                MAX_TRAIN_SIZE
            ),
        ));
        // Try to update status comment (don't fail if oversize - we're already aborting)
        let effects = train
            .status_comment_id
            .and_then(|comment_id| {
                format_phase_comment(train)
                    .ok()
                    .map(|body| Effect::GitHub(GitHubEffect::UpdateComment { comment_id, body }))
            })
            .into_iter()
            .collect();
        return StepResult {
            train: train.clone(),
            outcome: CascadeStepOutcome::Aborted {
                pr_number: train.current_pr,
                reason: AbortReason::TrainTooLarge {
                    pr_count: total_descendants.len() + 1,
                    max_allowed: MAX_TRAIN_SIZE,
                },
            },
            effects,
        };
    }

    // Transition to next phase using DIRECT descendants for the frozen set
    let new_phase = crate::state::transitions::start_preparing(direct_descendants.clone());
    train.cascade_phase = new_phase.clone();
    train.increment_seq();

    // Immediately execute the new phase to produce effects
    let mut result = match new_phase {
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

            // Record head SHA for the squash-time guard (same as in execute_preparing).
            // Without this, trains with no descendants skip the guard entirely because
            // predecessor_head_sha remains None.
            train.predecessor_pr = Some(train.current_pr);
            if train.predecessor_head_sha.is_none() {
                train.predecessor_head_sha = Some(current_pr.head_sha.clone());
            }

            execute_squash_pending(train, DescendantProgress::new(vec![]), current_pr, ctx)
        }
        _ => unreachable!("start_preparing only returns Preparing or SquashPending"),
    };

    // CRITICAL: Emit status comment update for Idle → Preparing/SquashPending transition.
    // This ensures GitHub-based recovery can see the phase transition and frozen descendants
    // immediately, not just after the first descendant is processed.
    //
    // CRASH SAFETY: Prepend (insert at 0) rather than append. If the executor crashes
    // between executing effects, the comment should already reflect the new phase.
    // Ordering: [UpdateComment, ...operational effects]
    if let Some(comment_id) = result.train.status_comment_id {
        match format_phase_comment(&result.train) {
            Ok(body) => {
                result.effects.insert(
                    0,
                    Effect::GitHub(GitHubEffect::UpdateComment { comment_id, body }),
                );
            }
            Err(CascadeError::StatusCommentOversize { .. }) => {
                result.train.abort(TrainError::new(
                    "status_comment_oversize",
                    "Status comment exceeds size limit",
                ));
                return StepResult {
                    train: result.train,
                    outcome: CascadeStepOutcome::Aborted {
                        pr_number: train.current_pr,
                        reason: AbortReason::StatusCommentTooLarge,
                    },
                    effects: vec![],
                };
            }
            Err(e) => unreachable!("Unexpected error formatting comment: {:?}", e),
        }
    }

    result
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
        return skip_descendant(
            train,
            progress,
            next_descendant,
            "PR not found in cache",
            prs,
            ctx,
        );
    };

    // Check if descendant is still open
    if !desc_pr.state.is_open() {
        return skip_descendant(
            train,
            progress,
            next_descendant,
            "PR is no longer open",
            prs,
            ctx,
        );
    }

    // Draft PRs should wait, not be prepared - per DESIGN.md lines 90-92
    if desc_pr.is_draft {
        train.wait_for_ci();
        return StepResult {
            train: train.clone(),
            outcome: CascadeStepOutcome::WaitingOnCi {
                pr_number: next_descendant,
            },
            effects: vec![],
        };
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

    // Store predecessor info for recovery (only on first preparation).
    // CRITICAL: Do NOT overwrite predecessor_head_sha on subsequent preparations.
    // The squash-time guard (line 406) must compare against the head SHA from
    // cascade START, not from the most recent preparation. Otherwise, if the root
    // PR is pushed between preparing descendants, the guard would compare the new
    // SHA against itself and incorrectly pass.
    train.predecessor_pr = Some(train.current_pr);
    if train.predecessor_head_sha.is_none() {
        train.predecessor_head_sha = Some(current_pr.head_sha.clone());
    }
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
                    "+refs/pull/{}/head:refs/remotes/origin/pr/{}",
                    train.current_pr, train.current_pr
                ),
                format!(
                    "+refs/pull/{}/head:refs/remotes/origin/pr/{}",
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

    // Update train state to WaitingCi for consistency with how abort/stop work.
    // This prevents callers from forgetting to update state and potentially
    // re-running steps prematurely.
    train.wait_for_ci();

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
    // CRITICAL: Verify the PR head hasn't changed since preparation.
    // If someone pushed new commits after we prepared descendants, those commits
    // would be squashed without proper preparation of descendants (data loss risk).
    if let Some(recorded_sha) = train.predecessor_head_sha.clone()
        && current_pr.head_sha != recorded_sha
    {
        train.abort(TrainError::new(
            "head_sha_changed",
            format!(
                "PR head changed since preparation (was {}, now {}). \
                 Aborting to prevent merging unreviewed/unprepared commits.",
                recorded_sha, current_pr.head_sha
            ),
        ));
        return StepResult {
            train: train.clone(),
            outcome: CascadeStepOutcome::Aborted {
                pr_number: train.current_pr,
                reason: AbortReason::HeadShaChanged {
                    expected: recorded_sha,
                    actual: current_pr.head_sha.clone(),
                },
            },
            effects: vec![],
        };
    }

    // Generate squash-merge effect
    let effects = vec![Effect::GitHub(GitHubEffect::SquashMerge {
        pr: train.current_pr,
        expected_sha: current_pr.head_sha.clone(),
    })];

    // Update train state to WaitingCi for consistency.
    train.wait_for_ci();

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
        return skip_descendant(
            train,
            progress,
            next_descendant,
            "PR not found in cache",
            prs,
            ctx,
        );
    };

    if !desc_pr.state.is_open() {
        return skip_descendant(
            train,
            progress,
            next_descendant,
            "PR is no longer open",
            prs,
            ctx,
        );
    }

    // Draft PRs should wait, not be reconciled - per DESIGN.md lines 90-92
    if desc_pr.is_draft {
        train.wait_for_ci();
        return StepResult {
            train: train.clone(),
            outcome: CascadeStepOutcome::WaitingOnCi {
                pr_number: next_descendant,
            },
            effects: vec![],
        };
    }

    // Generate reconciliation effects
    // CRITICAL: Must fetch and checkout the descendant branch before merging.
    // The worktree may still be on a different branch from a prior operation.
    let effects = vec![
        // Fetch the descendant's latest state and the default branch
        // (default branch needed to ensure squash_sha is available locally)
        Effect::Git(GitEffect::Fetch {
            refspecs: vec![
                format!(
                    "+refs/pull/{}/head:refs/remotes/origin/pr/{}",
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
        // Perform the two-step reconciliation merge.
        // CRITICAL: The interpreter MUST validate squash_sha even though
        // expected_squash_parent is None. See MergeReconcile docs for requirements:
        // 1. Verify squash_sha has exactly one parent (is actually a squash)
        // 2. Verify that parent is on origin/{default_branch} history
        Effect::Git(GitEffect::MergeReconcile {
            squash_sha: squash_sha.clone(),
            // Note: expected_squash_parent is None because we don't have the parent
            // SHA at this point. The interpreter computes $SQUASH_SHA^ itself and
            // MUST validate it's a valid squash (single parent, on default branch).
            expected_squash_parent: None,
            default_branch: ctx.default_branch.clone(),
            target_branch: desc_pr.head_ref.clone(),
        }),
        // Push the result
        Effect::Git(GitEffect::Push {
            refspec: format!("HEAD:refs/heads/{}", desc_pr.head_ref),
            force: false,
        }),
    ];

    // Update train state to WaitingCi for consistency.
    train.wait_for_ci();

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
        return skip_descendant(
            train,
            progress,
            next_descendant,
            "PR not found in cache",
            prs,
            ctx,
        );
    };

    if !desc_pr.state.is_open() {
        return skip_descendant(
            train,
            progress,
            next_descendant,
            "PR is no longer open",
            prs,
            ctx,
        );
    }

    // Draft PRs should wait, not be caught up - per DESIGN.md lines 90-92
    if desc_pr.is_draft {
        train.wait_for_ci();
        return StepResult {
            train: train.clone(),
            outcome: CascadeStepOutcome::WaitingOnCi {
                pr_number: next_descendant,
            },
            effects: vec![],
        };
    }

    // Generate catch-up effects
    // CRITICAL: Must fetch and checkout the descendant branch before merging.
    // The worktree may still be on a different branch from a prior operation.
    let effects = vec![
        // Fetch the descendant's latest state and the default branch
        Effect::Git(GitEffect::Fetch {
            refspecs: vec![
                format!(
                    "+refs/pull/{}/head:refs/remotes/origin/pr/{}",
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

    // Update train state to WaitingCi for consistency.
    train.wait_for_ci();

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
        return skip_descendant(
            train,
            progress,
            next_descendant,
            "PR not found in cache",
            prs,
            ctx,
        );
    };

    if !desc_pr.state.is_open() {
        return skip_descendant(
            train,
            progress,
            next_descendant,
            "PR is no longer open",
            prs,
            ctx,
        );
    }

    // Draft PRs should wait, not be retargeted - per DESIGN.md lines 90-92
    if desc_pr.is_draft {
        train.wait_for_ci();
        return StepResult {
            train: train.clone(),
            outcome: CascadeStepOutcome::WaitingOnCi {
                pr_number: next_descendant,
            },
            effects: vec![],
        };
    }

    // Generate retarget effect
    let effects = vec![Effect::GitHub(GitHubEffect::RetargetPr {
        pr: next_descendant,
        new_base: ctx.default_branch.clone(),
    })];

    // Update train state to WaitingCi for consistency.
    train.wait_for_ci();

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
/// CRITICAL: External merges bypass our controlled cascade flow and can drop
/// predecessor content if descendants weren't prepared. This function handles
/// all phases, aborting when necessary to prevent data loss.
///
/// Phase-specific handling:
/// - **Idle**: Has unprepared descendants (none were promised yet). Must abort
///   to prevent skipping preparation entirely.
/// - **Preparing**: Has unprepared descendants (some remain). Must abort.
/// - **SquashPending**: All descendants are prepared but the squash was expected
///   to be done by us. Progress must be RESET (completed cleared) before
///   entering Reconciling, since the external merge bypassed our squash.
/// - **Reconciling/CatchingUp/Retargeting**: Already past squash; can continue
///   with existing progress.
///
/// Also emits a status comment update to record the merge SHA and phase
/// transition for GitHub-based recovery.
fn handle_external_merge(
    train: &mut TrainRecord,
    merge_sha: Sha,
    prs: &HashMap<PrNumber, CachedPr>,
    _ctx: &StepContext,
) -> StepResult {
    // Store the squash SHA
    train.last_squash_sha = Some(merge_sha.clone());

    // CRITICAL: Handle each phase appropriately.
    // Idle and SquashPending are problematic because descendants weren't
    // prepared yet OR the completed set doesn't apply to reconciliation.
    let (progress, needs_abort) = match &train.cascade_phase {
        CascadePhase::Idle => {
            // External merge in Idle = preparation was never started.
            // ALL descendants (direct and transitive) are unprepared. Must abort if
            // there are any. Per DESIGN.md, we freeze all descendants at once, so
            // an external merge bypassing preparation is unsafe for the entire tree.
            let descendants = compute_all_descendants(train.current_pr, prs);
            if descendants.is_empty() {
                // No descendants - can complete safely
                (DescendantProgress::new(vec![]), false)
            } else {
                // Has descendants that weren't prepared
                (DescendantProgress::new(descendants), true)
            }
        }
        CascadePhase::Preparing { progress } => {
            // External merge in Preparing phase.
            // If any descendants are unprepared, abort (they don't have predecessor content).
            // If all are prepared, reset progress for reconciliation - the "completed" set
            // tracks which descendants were PREPARED, not reconciled. All must be reconciled.
            let has_unprepared = progress.remaining().next().is_some();
            if has_unprepared {
                (progress.clone(), true)
            } else {
                // All prepared - reset for reconciliation (same logic as SquashPending).
                // CRITICAL: Preserve skipped set - closed/failed descendants should remain
                // skipped through subsequent phases.
                let mut fresh_progress =
                    DescendantProgress::new(progress.frozen_descendants.clone());
                fresh_progress.skipped = progress.skipped.clone();
                (fresh_progress, false)
            }
        }
        CascadePhase::SquashPending { progress } => {
            // External merge in SquashPending = squash bypassed our control.
            // The `completed` set tracks which descendants were PREPARED, but
            // reconciliation needs a fresh start (none are reconciled yet).
            // CRITICAL: Preserve skipped set - closed/failed descendants should remain
            // skipped through subsequent phases (not reintroduced for reconciliation).
            let mut fresh_progress = DescendantProgress::new(progress.frozen_descendants.clone());
            fresh_progress.skipped = progress.skipped.clone();
            (fresh_progress, false)
        }
        CascadePhase::Reconciling { progress, .. }
        | CascadePhase::CatchingUp { progress, .. }
        | CascadePhase::Retargeting { progress, .. } => {
            // Already past squash - use existing progress
            (progress.clone(), false)
        }
    };

    // CRITICAL: If we have unprepared descendants, abort.
    // These descendants don't have the predecessor's content merged into them,
    // so reconciliation would drop changes.
    if needs_abort {
        let unprepared: Vec<PrNumber> = progress.remaining().copied().collect();
        let reason = AbortReason::PreparationIncomplete {
            unprepared_descendants: unprepared.clone(),
        };
        train.abort(TrainError::new(
            "preparation_incomplete",
            format!(
                "PR was merged externally before preparation completed. \
                 {} descendant(s) were not prepared: {:?}. \
                 Manual intervention required: merge the predecessor's content \
                 into these branches or rebase them.",
                unprepared.len(),
                unprepared
            ),
        ));
        // Try to update status comment, but don't panic if it's too large
        // (we're already aborting for another reason)
        let effects = train
            .status_comment_id
            .and_then(|comment_id| {
                format_phase_comment(train)
                    .ok()
                    .map(|body| Effect::GitHub(GitHubEffect::UpdateComment { comment_id, body }))
            })
            .into_iter()
            .collect();
        return StepResult {
            train: train.clone(),
            outcome: CascadeStepOutcome::Aborted {
                pr_number: train.current_pr,
                reason,
            },
            effects,
        };
    }

    // Check remaining (frozen - completed - skipped)
    let remaining: Vec<PrNumber> = progress.remaining().copied().collect();

    if remaining.is_empty() {
        // No descendants to process - cascade complete.
        // CRITICAL: Still validate the squash commit to enforce squash-only requirement.
        // Even with no descendants, a non-squash merge violates linear history.
        let mut result = complete_cascade(train, progress);

        // Add validation effect to ensure the external merge was a squash
        result.effects.insert(
            0,
            Effect::Git(GitEffect::ValidateSquashCommit {
                squash_sha: merge_sha,
                default_branch: _ctx.default_branch.clone(),
            }),
        );
        return result;
    }

    // Transition to Reconciling phase with frozen descendants
    train.cascade_phase = CascadePhase::Reconciling {
        progress,
        squash_sha: merge_sha,
    };
    train.increment_seq();

    // CRITICAL: Emit status comment update to record last_squash_sha and
    // Reconciling phase for GitHub-based recovery
    let effects = if let Some(comment_id) = train.status_comment_id {
        match format_phase_comment(train) {
            Ok(body) => {
                vec![Effect::GitHub(GitHubEffect::UpdateComment {
                    comment_id,
                    body,
                })]
            }
            Err(CascadeError::StatusCommentOversize { .. }) => {
                train.abort(TrainError::new(
                    "status_comment_oversize",
                    "Status comment exceeds size limit",
                ));
                return StepResult {
                    train: train.clone(),
                    outcome: CascadeStepOutcome::Aborted {
                        pr_number: train.current_pr,
                        reason: AbortReason::StatusCommentTooLarge,
                    },
                    effects: vec![],
                };
            }
            Err(e) => unreachable!("Unexpected error formatting comment: {:?}", e),
        }
    } else {
        vec![]
    };

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::Merged {
            pr_number: train.current_pr,
        },
        effects,
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

    // Add status comment update for recovery (if we have a comment ID).
    // CRASH SAFETY: Prepend (insert at 0) rather than append. If the executor crashes
    // between executing effects, the comment should already reflect the new phase.
    if let Some(comment_id) = train.status_comment_id {
        match format_phase_comment(&result.train) {
            Ok(body) => {
                result.effects.insert(
                    0,
                    Effect::GitHub(GitHubEffect::UpdateComment { comment_id, body }),
                );
            }
            Err(CascadeError::StatusCommentOversize { .. }) => {
                result.train.abort(TrainError::new(
                    "status_comment_oversize",
                    "Status comment exceeds size limit",
                ));
                return StepResult {
                    train: result.train,
                    outcome: CascadeStepOutcome::Aborted {
                        pr_number: train.current_pr,
                        reason: AbortReason::StatusCommentTooLarge,
                    },
                    effects: vec![],
                };
            }
            Err(e) => unreachable!("Unexpected error formatting comment: {:?}", e),
        }
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

    // Add status comment update for recovery (if we have a comment ID).
    // CRASH SAFETY: Prepend (insert at 0) rather than append. If the executor crashes
    // between executing effects, the comment should already reflect the new phase.
    if let Some(comment_id) = train.status_comment_id {
        match format_phase_comment(&result.train) {
            Ok(body) => {
                result.effects.insert(
                    0,
                    Effect::GitHub(GitHubEffect::UpdateComment { comment_id, body }),
                );
            }
            Err(CascadeError::StatusCommentOversize { .. }) => {
                result.train.abort(TrainError::new(
                    "status_comment_oversize",
                    "Status comment exceeds size limit",
                ));
                return StepResult {
                    train: result.train,
                    outcome: CascadeStepOutcome::Aborted {
                        pr_number: train.current_pr,
                        reason: AbortReason::StatusCommentTooLarge,
                    },
                    effects: vec![],
                };
            }
            Err(e) => unreachable!("Unexpected error formatting comment: {:?}", e),
        }
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

    // Add status comment update for recovery (if we have a comment ID).
    // CRASH SAFETY: Prepend (insert at 0) rather than append. If the executor crashes
    // between executing effects, the comment should already reflect the new phase.
    if let Some(comment_id) = train.status_comment_id {
        match format_phase_comment(&result.train) {
            Ok(body) => {
                result.effects.insert(
                    0,
                    Effect::GitHub(GitHubEffect::UpdateComment { comment_id, body }),
                );
            }
            Err(CascadeError::StatusCommentOversize { .. }) => {
                result.train.abort(TrainError::new(
                    "status_comment_oversize",
                    "Status comment exceeds size limit",
                ));
                return StepResult {
                    train: result.train,
                    outcome: CascadeStepOutcome::Aborted {
                        pr_number: train.current_pr,
                        reason: AbortReason::StatusCommentTooLarge,
                    },
                    effects: vec![],
                };
            }
            Err(e) => unreachable!("Unexpected error formatting comment: {:?}", e),
        }
    }

    result
}

/// Complete the cascade (Retargeting -> Idle).
///
/// After all descendants have been retargeted, determine the next step:
/// - If no descendants were successfully processed, the train is truly complete
/// - If one descendant was processed, advance `current_pr` and continue
/// - If multiple descendants were processed, emit FanOut for parallel trains
///
/// CRITICAL: Always emits a status comment update to ensure GitHub-based recovery
/// can see the new current_pr or completion state.
fn complete_cascade(train: &mut TrainRecord, progress: DescendantProgress) -> StepResult {
    train.cascade_phase = CascadePhase::Idle;
    train.predecessor_head_sha = None; // Clear stale SHA from previous cascade cycle
    train.predecessor_pr = None; // Clear stale PR from previous cascade cycle
    train.increment_seq();

    // Get descendants that were successfully processed (not skipped)
    let completed: Vec<PrNumber> = progress.completed.iter().copied().collect();

    // Helper to abort on oversize comment
    let abort_oversize = |train: &mut TrainRecord| -> StepResult {
        train.abort(TrainError::new(
            "status_comment_oversize",
            "Status comment exceeds size limit",
        ));
        StepResult {
            train: train.clone(),
            outcome: CascadeStepOutcome::Aborted {
                pr_number: train.current_pr,
                reason: AbortReason::StatusCommentTooLarge,
            },
            effects: vec![],
        }
    };

    // Build status comment update effect if we have a comment ID
    let status_update_effect = if let Some(comment_id) = train.status_comment_id {
        match format_phase_comment(train) {
            Ok(body) => Some(Effect::GitHub(GitHubEffect::UpdateComment {
                comment_id,
                body,
            })),
            Err(CascadeError::StatusCommentOversize { .. }) => {
                return abort_oversize(train);
            }
            Err(e) => unreachable!("Unexpected error formatting comment: {:?}", e),
        }
    } else {
        None
    };

    match completed.len() {
        0 => {
            // No descendants were successfully processed - truly complete
            StepResult {
                train: train.clone(),
                outcome: CascadeStepOutcome::Complete,
                effects: status_update_effect.into_iter().collect(),
            }
        }
        1 => {
            // Single descendant - advance current_pr and continue the train
            train.current_pr = completed[0];
            train.predecessor_head_sha = None; // Clear - was for the old current_pr
            train.predecessor_pr = None; // Clear - was for the old current_pr
            train.increment_seq();

            // Update status comment again after advancing current_pr
            let effects = if let Some(comment_id) = train.status_comment_id {
                match format_phase_comment(train) {
                    Ok(body) => {
                        vec![Effect::GitHub(GitHubEffect::UpdateComment {
                            comment_id,
                            body,
                        })]
                    }
                    Err(CascadeError::StatusCommentOversize { .. }) => {
                        return abort_oversize(train);
                    }
                    Err(e) => unreachable!("Unexpected error formatting comment: {:?}", e),
                }
            } else {
                vec![]
            };

            // Update train state to WaitingCi for consistency.
            train.wait_for_ci();

            StepResult {
                train: train.clone(),
                outcome: CascadeStepOutcome::WaitingOnCi {
                    pr_number: completed[0],
                },
                effects,
            }
        }
        _ => {
            // Multiple descendants - fan out into separate trains
            StepResult {
                train: train.clone(),
                outcome: CascadeStepOutcome::FanOut {
                    descendants: completed,
                },
                effects: status_update_effect.into_iter().collect(),
            }
        }
    }
}

/// Skip a descendant that can't be processed, then continue the cascade.
///
/// After marking the descendant as skipped, this function recursively calls
/// `execute_cascade_step` to process the next descendant, preventing cascade stalls.
fn skip_descendant(
    train: &mut TrainRecord,
    mut progress: DescendantProgress,
    descendant: PrNumber,
    _reason: &str,
    prs: &HashMap<PrNumber, CachedPr>,
    ctx: &StepContext,
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

    // Recursively call execute_cascade_step to process the next descendant.
    // This prevents the cascade from stalling when descendants are skipped.
    execute_cascade_step(train.clone(), prs, ctx)
}

/// Compute ALL open descendants of a PR (transitive, not just direct).
///
/// Returns ALL descendants transitively reachable from the given PR, not just
/// immediate children. This is important for accurate train size calculations.
///
/// For example, if main <- #1 <- #2 <- #3, compute_all_descendants(#1) will
/// return [#2, #3], not just [#2].
///
/// Note: Closed PRs are excluded AND block traversal (so descendants of a closed
/// PR are also excluded).
fn compute_all_descendants(pr: PrNumber, prs: &HashMap<PrNumber, CachedPr>) -> Vec<PrNumber> {
    let descendants_index = crate::state::build_descendants_index(prs);
    crate::state::descendants::collect_all_descendants(pr, &descendants_index, prs)
}

/// Compute DIRECT (immediate) open descendants of a PR.
///
/// Returns only PRs whose predecessor field points directly to the given PR.
/// Used for the frozen set during cascade, since we only prepare direct children.
/// Transitive descendants are handled when their predecessor becomes the current PR.
///
/// For example, if main <- #1 <- #2 <- #3, compute_direct_descendants(#1)
/// returns only [#2], not [#2, #3].
fn compute_direct_descendants(pr: PrNumber, prs: &HashMap<PrNumber, CachedPr>) -> Vec<PrNumber> {
    let descendants_index = crate::state::build_descendants_index(prs);
    crate::state::descendants::collect_direct_descendants(pr, &descendants_index, prs)
}

/// Process the result of an operation and update the train state.
///
/// This is called after effects have been executed to record the outcome
/// and potentially advance to the next phase.
///
/// Returns (updated_train, optional_outcome, effects_to_execute).
/// The effects include status comment updates for phase transitions that
/// are critical for GitHub-based recovery.
pub fn process_operation_result(
    mut train: TrainRecord,
    operation: OperationResult,
) -> (TrainRecord, Option<CascadeStepOutcome>, Vec<Effect>) {
    match operation {
        OperationResult::DescendantPrepared { pr } => {
            if let Some(progress) = train.cascade_phase.progress_mut() {
                progress.mark_completed(pr);
            }
            train.increment_seq();
            (train, None, vec![])
        }
        OperationResult::DescendantReconciled { pr, squash_sha: _ } => {
            if let Some(progress) = train.cascade_phase.progress_mut() {
                progress.mark_completed(pr);
            }
            // Record the squash SHA for this descendant (for late-addition tracking)
            // This would update the PR cache in the full implementation
            train.increment_seq();
            (train, None, vec![])
        }
        OperationResult::DescendantCaughtUp { pr } => {
            if let Some(progress) = train.cascade_phase.progress_mut() {
                progress.mark_completed(pr);
            }
            train.increment_seq();
            (train, None, vec![])
        }
        OperationResult::DescendantRetargeted { pr } => {
            if let Some(progress) = train.cascade_phase.progress_mut() {
                progress.mark_completed(pr);
            }
            train.increment_seq();

            // CRITICAL: Emit RecordReconciliation effect so is_root() will recognize
            // this descendant as a valid new root after the cascade completes.
            // Without this, fan-out descendants cannot start new trains.
            // DESIGN.md: "Record that reconciliation completed — CRITICAL for is_root()"
            let effects = if let Some(squash_sha) = train.last_squash_sha.clone() {
                vec![Effect::RecordReconciliation { pr, squash_sha }]
            } else {
                // INVARIANT VIOLATION: Retargeting should only occur after squash merge.
                // If last_squash_sha is None here, something has gone wrong.
                // Abort to prevent silent fan-out breakage.
                train.abort(TrainError::new(
                    "internal_invariant_violation",
                    format!(
                        "DescendantRetargeted for PR #{} but last_squash_sha is None. \
                         This indicates a bug - retargeting should only occur after squash merge.",
                        pr
                    ),
                ));
                return (
                    train,
                    Some(CascadeStepOutcome::Aborted {
                        pr_number: pr,
                        reason: AbortReason::InternalInvariantViolation {
                            details: format!(
                                "DescendantRetargeted without last_squash_sha for PR #{}",
                                pr
                            ),
                        },
                    }),
                    vec![],
                );
            };

            (train, None, effects)
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

            // CRITICAL: Emit status comment update after squash transition.
            // This ensures GitHub-based recovery can see `last_squash_sha` and the
            // Reconciling phase, preventing re-squash or recovery failures if the
            // bot crashes before the next phase transition.
            let effects = if let Some(comment_id) = train.status_comment_id {
                match format_phase_comment(&train) {
                    Ok(body) => {
                        vec![Effect::GitHub(GitHubEffect::UpdateComment {
                            comment_id,
                            body,
                        })]
                    }
                    Err(CascadeError::StatusCommentOversize { .. }) => {
                        train.abort(TrainError::new(
                            "status_comment_oversize",
                            "Status comment exceeds size limit",
                        ));
                        return (
                            train,
                            Some(CascadeStepOutcome::Aborted {
                                pr_number: pr,
                                reason: AbortReason::StatusCommentTooLarge,
                            }),
                            vec![],
                        );
                    }
                    Err(e) => unreachable!("Unexpected error formatting comment: {:?}", e),
                }
            } else {
                vec![]
            };

            (
                train,
                Some(CascadeStepOutcome::Merged { pr_number: pr }),
                effects,
            )
        }
        OperationResult::OperationFailed { pr, error } => {
            train.abort(TrainError::new(error.error_type(), error.description()));
            (
                train,
                Some(CascadeStepOutcome::Aborted {
                    pr_number: pr,
                    reason: error,
                }),
                vec![],
            )
        }
        OperationResult::DescendantSkipped { pr, reason: _ } => {
            if let Some(progress) = train.cascade_phase.progress_mut() {
                progress.mark_skipped(pr);
            }
            train.increment_seq();
            (train, None, vec![])
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
        let (updated_train, outcome, _effects) = process_operation_result(
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
        let (updated_train, _, _effects) = process_operation_result(
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
                        if let Some(branch) = refspec.strip_prefix("HEAD:refs/heads/branch-")
                            && let Ok(n) = branch.parse::<u64>()
                        {
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
                    if !result.effects.is_empty()
                        && let Some(op_result) = simulate_effect_execution(&train, &result.effects, &sha) {
                            let (updated_train, _, _effects) = process_operation_result(train, op_result);
                            train = updated_train;
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

                // For non-empty descendants, we should see ALL required phases
                // (not just preparing). This ensures the cascade doesn't skip any phase.
                if !descendants.is_empty() {
                    let required_phases = ["preparing", "squash_pending", "reconciling", "catching_up", "retargeting"];
                    for required_phase in required_phases {
                        prop_assert!(
                            phase_sequence.contains(&required_phase.to_string()),
                            "With descendants, should have visited {} phase. Sequence: {:?}",
                            required_phase,
                            phase_sequence
                        );
                    }
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

                // Track phases visited to verify cascade actually runs to completion
                let mut phase_sequence: Vec<String> = vec![];

                // Run the cascade to completion, tracking which PRs are processed
                let max_iterations = 100;
                for _ in 0..max_iterations {
                    let result = execute_cascade_step(train.clone(), &prs, &ctx);
                    train = result.train;

                    // Track phase transitions
                    let phase_name = match &train.cascade_phase {
                        CascadePhase::Idle => "idle",
                        CascadePhase::Preparing { .. } => "preparing",
                        CascadePhase::SquashPending { .. } => "squash_pending",
                        CascadePhase::Reconciling { .. } => "reconciling",
                        CascadePhase::CatchingUp { .. } => "catching_up",
                        CascadePhase::Retargeting { .. } => "retargeting",
                    };
                    if phase_sequence.last().is_none_or(|last| last != phase_name) {
                        phase_sequence.push(phase_name.to_string());
                    }

                    // Track which PRs are targeted by effects
                    for effect in &result.effects {
                        match effect {
                            Effect::Git(GitEffect::Push { refspec, .. }) => {
                                // refspec is like "HEAD:refs/heads/branch-N"
                                if let Some(branch) = refspec.strip_prefix("HEAD:refs/heads/branch-")
                                    && let Ok(n) = branch.parse::<u64>() {
                                        processed_prs.insert(PrNumber(n));
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
                    if !result.effects.is_empty()
                        && let Some(op_result) = simulate_effect_execution(&train, &result.effects, &sha) {
                            let (updated_train, _, _effects) = process_operation_result(train, op_result);
                            train = updated_train;
                        }
                }

                // Verify cascade actually ran through phases (not just first-step)
                if !initial_descendants.is_empty() {
                    let required_phases = ["preparing", "squash_pending", "reconciling", "catching_up", "retargeting"];
                    for required_phase in required_phases {
                        prop_assert!(
                            phase_sequence.contains(&required_phase.to_string()),
                            "Cascade should have visited {} phase but only visited: {:?}",
                            required_phase,
                            phase_sequence
                        );
                    }
                }

                // Verify we actually completed (not just hit max_iterations)
                prop_assert!(
                    phase_sequence.last() == Some(&"idle".to_string()),
                    "Cascade should have completed (returned to Idle), but ended in: {:?}",
                    phase_sequence.last()
                );

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

                // Create train in SquashPending phase (after preparation completes).
                // We use SquashPending because:
                // 1. It means all preparation is done (frozen descendants were prepared)
                // 2. External merge is expected to happen in this phase
                // 3. Transition to Reconciling preserves the frozen set (not late additions)
                let mut train = TrainRecord::new(PrNumber(1));
                let progress = DescendantProgress::new(frozen_descendants.clone());
                train.cascade_phase = CascadePhase::SquashPending { progress };

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
                    if !result.effects.is_empty()
                        && let Some(op_result) = simulate_effect_execution(&train, &result.effects, &sha) {
                            let (updated_train, _, _effects) = process_operation_result(train, op_result);
                            train = updated_train;
                        }
                }

                // Fan-out MUST occur for this topology (multiple direct descendants of root).
                // This prevents the test from passing vacuously if the cascade exits early.
                prop_assert!(
                    fan_out_descendants.is_some(),
                    "Fan-out should occur with multiple direct descendants. \
                     Test topology has {} descendants all pointing to root.",
                    descendants.len()
                );

                // Verify fan-out contains all descendants
                let fan_out = fan_out_descendants.unwrap();
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

                let descendants_vec: Vec<PrNumber> = descendants.to_vec();
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
                        if let Effect::Git(GitEffect::Push { refspec, .. }) = effect
                            && let Some(branch) = refspec.strip_prefix("HEAD:refs/heads/branch-")
                                && let Ok(n) = branch.parse::<u64>() {
                                    processed_prs.insert(PrNumber(n));
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
                    if !result.effects.is_empty()
                        && let Some(op_result) = simulate_effect_execution(&train, &result.effects, &sha) {
                            let (updated_train, _, _effects) = process_operation_result(train, op_result);
                            train = updated_train;
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

                let descendants_vec: Vec<PrNumber> = descendants.to_vec();
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

    // ─── Bug Regression Tests ─────────────────────────────────────────────────
    //
    // These tests expose specific bugs from review comments. Each test should
    // FAIL before the corresponding fix is applied and PASS after.

    mod bug_regression_tests {
        use super::*;
        use crate::types::{CommentId, TrainState};

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

        /// Regression test: process_operation_result must emit a status comment update
        /// when transitioning to Reconciling after squash. This ensures GitHub-based
        /// recovery can see last_squash_sha, preventing re-squash or recovery failures
        /// if the bot crashes.
        #[test]
        fn squash_merged_emits_status_comment_update() {
            let mut train = TrainRecord::new(PrNumber(1));
            train.cascade_phase = CascadePhase::SquashPending {
                progress: DescendantProgress::new(vec![PrNumber(2)]),
            };
            // CRITICAL: Train has a status comment ID
            train.status_comment_id = Some(CommentId(12345));

            let squash_sha = make_sha(0xabc);
            let (updated_train, outcome, effects) = process_operation_result(
                train,
                OperationResult::SquashMerged {
                    pr: PrNumber(1),
                    squash_sha: squash_sha.clone(),
                },
            );

            // Verify the transition happened
            assert!(matches!(
                updated_train.cascade_phase,
                CascadePhase::Reconciling { .. }
            ));
            assert!(matches!(outcome, Some(CascadeStepOutcome::Merged { .. })));
            assert_eq!(updated_train.last_squash_sha, Some(squash_sha));

            // Verify status comment update effect is emitted
            let has_comment_update = effects.iter().any(|e| {
                matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. })
                    if *comment_id == CommentId(12345))
            });
            assert!(
                has_comment_update,
                "process_operation_result must emit UpdateComment effect when transitioning \
                 to Reconciling. Effects: {:?}",
                effects
            );
        }

        /// Regression test: complete_cascade must emit a status comment update when
        /// changing phase to Idle and advancing current_pr.
        #[test]
        fn complete_cascade_emits_status_comment_update() {
            let mut train = TrainRecord::new(PrNumber(1));
            // Status comment exists
            train.status_comment_id = Some(CommentId(12345));

            // Set up Retargeting phase with one completed descendant
            let mut progress = DescendantProgress::new(vec![PrNumber(2)]);
            progress.mark_completed(PrNumber(2));
            train.cascade_phase = CascadePhase::Retargeting {
                progress,
                squash_sha: make_sha(0xabc),
            };

            // Build PR map
            let pr1 = make_open_pr(1, "main", None);
            let mut pr2 = make_open_pr(2, "branch-1", Some(1));
            pr2.head_ref = "branch-2".to_string();
            let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);
            let ctx = StepContext::new("main");

            // Execute cascade step - should complete and advance current_pr
            let result = execute_cascade_step(train, &prs, &ctx);

            // Should have advanced current_pr to the completed descendant
            assert_eq!(result.train.current_pr, PrNumber(2));

            // Verify status comment update is emitted
            let has_comment_update = result.effects.iter().any(|e| {
                matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. })
                    if *comment_id == CommentId(12345))
            });

            assert!(
                has_comment_update,
                "complete_cascade must emit status comment update when advancing current_pr. \
                 Effects: {:?}",
                result.effects
            );
        }

        /// Regression test: handle_external_merge must emit a status comment update
        /// when changing phase to Reconciling and storing squash_sha.
        ///
        /// CRITICAL: External merge in Preparing with all descendants prepared must
        /// transition to Reconciling (not complete the cascade). The "completed" set
        /// in Preparing tracks "prepared" not "reconciled" - all descendants still
        /// need reconciliation.
        #[test]
        fn external_merge_emits_status_comment_update() {
            let mut train = TrainRecord::new(PrNumber(1));
            // Status comment exists
            train.status_comment_id = Some(CommentId(12345));

            // In Preparing phase with descendants - but ALL COMPLETED (prepared)
            // This is required because handle_external_merge now aborts if
            // descendants weren't prepared.
            let mut progress = DescendantProgress::new(vec![PrNumber(2)]);
            progress.mark_completed(PrNumber(2)); // Descendant was prepared
            train.cascade_phase = CascadePhase::Preparing { progress };

            // Build PR map with root PR merged externally
            let merge_sha = make_sha(0xfff);
            let mut pr1 = make_open_pr(1, "main", None);
            pr1.state = PrState::Merged {
                merge_commit_sha: merge_sha.clone(),
            };
            let mut pr2 = make_open_pr(2, "branch-1", Some(1));
            pr2.head_ref = "branch-2".to_string();

            let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2)]);
            let ctx = StepContext::new("main");

            // Execute cascade step - should transition to Reconciling with reset progress
            let result = execute_cascade_step(train, &prs, &ctx);

            // CRITICAL: Must transition to Reconciling (not complete cascade).
            // Current PR stays at #1 because we're reconciling, not advancing.
            assert_eq!(result.train.current_pr, PrNumber(1));
            assert_eq!(result.train.last_squash_sha, Some(merge_sha.clone()));

            // Phase must be Reconciling with reset progress (descendant NOT in completed set)
            match &result.train.cascade_phase {
                CascadePhase::Reconciling {
                    progress,
                    squash_sha,
                } => {
                    assert_eq!(*squash_sha, merge_sha);
                    // Progress must be reset - descendant should be in frozen set but NOT completed
                    assert!(
                        progress.frozen_descendants.contains(&PrNumber(2)),
                        "Descendant must be in frozen_descendants"
                    );
                    assert!(
                        !progress.completed.contains(&PrNumber(2)),
                        "Descendant must NOT be in completed set (needs reconciliation)"
                    );
                }
                other => panic!(
                    "Expected Reconciling phase, got {:?}",
                    std::mem::discriminant(other)
                ),
            }

            // Verify status comment update is emitted
            let has_comment_update = result.effects.iter().any(|e| {
                matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. })
                    if *comment_id == CommentId(12345))
            });

            assert!(
                has_comment_update,
                "handle_external_merge must emit status comment update. \
                 Effects: {:?}",
                result.effects
            );
        }

        /// Regression test: handle_external_merge must abort if descendants weren't
        /// prepared. Moving to Reconciling with unprepared descendants violates the
        /// "prepare before squash" invariant and can drop predecessor content.
        #[test]
        fn external_merge_verifies_descendants_prepared() {
            let mut train = TrainRecord::new(PrNumber(1));
            // In Preparing phase with TWO descendants, but NONE completed
            train.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
            };

            // Build PR map with root PR merged externally BEFORE preparation completed
            let merge_sha = make_sha(0xfff);
            let mut pr1 = make_open_pr(1, "main", None);
            pr1.state = PrState::Merged {
                merge_commit_sha: merge_sha.clone(),
            };
            let mut pr2 = make_open_pr(2, "branch-1", Some(1));
            pr2.head_ref = "branch-2".to_string();
            let mut pr3 = make_open_pr(3, "branch-2", Some(2));
            pr3.head_ref = "branch-3".to_string();

            let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2), (PrNumber(3), pr3)]);
            let ctx = StepContext::new("main");

            // Execute cascade step
            let result = execute_cascade_step(train, &prs, &ctx);

            // Should abort because descendants weren't prepared
            assert!(
                matches!(result.outcome, CascadeStepOutcome::Aborted { .. }),
                "handle_external_merge must abort when descendants are unprepared. \
                 Got outcome: {:?}",
                result.outcome
            );

            // Verify it's a PreparationIncomplete error
            if let CascadeStepOutcome::Aborted { reason, .. } = &result.outcome {
                assert!(
                    matches!(reason, AbortReason::PreparationIncomplete { .. }),
                    "Abort reason should be PreparationIncomplete, got: {:?}",
                    reason
                );
            }
        }

        /// Regression test: external merge with NO descendants must still emit
        /// ValidateSquashCommit effect to enforce squash-only requirement.
        /// Even with no descendants to corrupt, a non-squash merge violates linear history.
        #[test]
        fn external_merge_no_descendants_validates_squash() {
            let mut train = TrainRecord::new(PrNumber(1));
            train.status_comment_id = Some(CommentId(12345));
            // In Idle phase (no descendants)
            train.cascade_phase = CascadePhase::Idle;

            // Build PR map with root PR merged externally
            let merge_sha = make_sha(0xfff);
            let mut pr1 = make_open_pr(1, "main", None);
            pr1.state = PrState::Merged {
                merge_commit_sha: merge_sha.clone(),
            };

            let prs = HashMap::from([(PrNumber(1), pr1)]);
            let ctx = StepContext::new("main");

            // Execute cascade step
            let result = execute_cascade_step(train, &prs, &ctx);

            // Should complete (no descendants to process)
            assert!(
                matches!(result.outcome, CascadeStepOutcome::Complete),
                "Expected Complete outcome, got: {:?}",
                result.outcome
            );

            // CRITICAL: Must emit ValidateSquashCommit effect
            let has_validation = result.effects.iter().any(|e| {
                matches!(
                    e,
                    Effect::Git(GitEffect::ValidateSquashCommit {
                        squash_sha,
                        default_branch
                    }) if squash_sha == &merge_sha && default_branch == "main"
                )
            });
            assert!(
                has_validation,
                "External merge with no descendants must emit ValidateSquashCommit. \
                 Effects: {:?}",
                result.effects
            );

            // Validation should be first effect (before status update)
            assert!(
                matches!(
                    &result.effects[0],
                    Effect::Git(GitEffect::ValidateSquashCommit { .. })
                ),
                "ValidateSquashCommit must be first effect. Effects: {:?}",
                result.effects
            );
        }

        /// BUG: skip_descendant returns WaitingOnCi with no effects. Without a poll tick,
        /// the cascade can stall after a closed/missing descendant.
        #[test]
        fn skip_descendant_continues_cascade() {
            let mut train = TrainRecord::new(PrNumber(1));
            // In Preparing phase with two descendants
            train.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
            };

            // Build PR map - PR #2 is CLOSED (will be skipped), PR #3 is open
            let pr1 = make_open_pr(1, "main", None);
            let mut pr2 = CachedPr::new(
                PrNumber(2),
                make_sha(2),
                "branch-2".to_string(),
                "branch-1".to_string(),
                Some(PrNumber(1)),
                PrState::Closed, // CLOSED - will be skipped
                MergeStateStatus::Clean,
                false,
            );
            pr2.head_ref = "branch-2".to_string();
            let mut pr3 = make_open_pr(3, "branch-1", Some(1));
            pr3.head_ref = "branch-3".to_string();

            let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2), (PrNumber(3), pr3)]);
            let ctx = StepContext::new("main");

            // Execute cascade step - should skip PR #2 and continue to PR #3
            let result = execute_cascade_step(train, &prs, &ctx);

            // PR #2 should be skipped
            if let CascadePhase::Preparing { progress } = &result.train.cascade_phase {
                assert!(
                    progress.skipped.contains(&PrNumber(2)),
                    "PR #2 should be marked as skipped"
                );
            }

            // BUG: skip_descendant returns WaitingOnCi with empty effects.
            // The cascade will stall because there's nothing to trigger the next step.
            //
            // The fix should either:
            // 1. Immediately invoke the next step after skipping (tail recursion), OR
            // 2. Return effects to process the next descendant
            //
            // Option 1 is cleaner: after skipping, call execute_cascade_step again
            // to process the next descendant.

            // Check that we either have effects to continue OR moved past the skipped PR
            let has_effects = !result.effects.is_empty();
            let skipped_and_continued =
                if let CascadePhase::Preparing { progress } = &result.train.cascade_phase {
                    // If we skipped PR #2, we should have moved on to process PR #3
                    progress.skipped.contains(&PrNumber(2))
                        && (has_effects || progress.completed.contains(&PrNumber(3)))
                } else {
                    false
                };

            assert!(
                has_effects || skipped_and_continued,
                "BUG: skip_descendant returns empty effects, causing cascade to stall. \
                 Outcome: {:?}, Effects: {:?}",
                result.outcome,
                result.effects
            );
        }

        /// Regression test: predecessor_head_sha must NOT be overwritten when preparing
        /// subsequent descendants. The squash-time guard (line 406) must compare against
        /// the head SHA from cascade START, not from the most recent preparation.
        ///
        /// Bug scenario:
        /// 1. Start cascade with root PR at SHA_A, prepare descendant #2 → stores SHA_A
        /// 2. External push changes root PR to SHA_B
        /// 3. Prepare descendant #3 → INCORRECTLY overwrites predecessor_head_sha to SHA_B
        /// 4. Squash guard compares SHA_B vs SHA_B → passes (should fail!)
        /// 5. Result: Descendant #2 was prepared with SHA_A but squash includes SHA_B content
        #[test]
        fn predecessor_head_sha_not_overwritten_on_subsequent_preparations() {
            let original_sha = make_sha(0x111);
            let new_sha = make_sha(0x222);

            // Setup: train with 2 DIRECT descendants of PR#1 (both based on branch-1)
            // This is the key setup - both descendants have the same predecessor
            let mut train = TrainRecord::new(PrNumber(1));
            train.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
            };

            // PR map with root PR at original SHA
            let mut pr1 = make_open_pr(1, "main", None);
            pr1.head_sha = original_sha.clone();
            // Both PR#2 and PR#3 are direct descendants of PR#1 (base_ref = "branch-1")
            let mut pr2 = make_open_pr(2, "branch-1", Some(1));
            pr2.head_ref = "branch-2".to_string();
            let mut pr3 = make_open_pr(3, "branch-1", Some(1)); // Direct child of PR#1
            pr3.head_ref = "branch-3".to_string();

            let prs = HashMap::from([
                (PrNumber(1), pr1.clone()),
                (PrNumber(2), pr2),
                (PrNumber(3), pr3),
            ]);
            let ctx = StepContext::new("main");

            // Step 1: Prepare first descendant (#2)
            let result = execute_cascade_step(train, &prs, &ctx);
            assert_eq!(
                result.train.predecessor_head_sha,
                Some(original_sha.clone()),
                "First preparation should record original SHA"
            );

            // Step 2: Mark first descendant completed, resume train
            let mut train = result.train;
            if let CascadePhase::Preparing { ref mut progress } = train.cascade_phase {
                progress.mark_completed(PrNumber(2));
            }
            train.state = TrainState::Running; // Resume after CI

            // Step 3: Simulate root PR head change (external push)
            let mut pr1_updated = pr1.clone();
            pr1_updated.head_sha = new_sha.clone();
            let mut pr2_updated = make_open_pr(2, "branch-1", Some(1));
            pr2_updated.head_ref = "branch-2".to_string();
            let mut pr3_updated = make_open_pr(3, "branch-1", Some(1));
            pr3_updated.head_ref = "branch-3".to_string();
            let prs_updated = HashMap::from([
                (PrNumber(1), pr1_updated),
                (PrNumber(2), pr2_updated),
                (PrNumber(3), pr3_updated),
            ]);

            // Step 4: Prepare second descendant (#3) - BUG: overwrites predecessor_head_sha
            let result = execute_cascade_step(train, &prs_updated, &ctx);

            // CRITICAL ASSERTION (currently fails due to bug):
            // predecessor_head_sha should STILL be original_sha, not new_sha
            assert_eq!(
                result.train.predecessor_head_sha,
                Some(original_sha.clone()),
                "predecessor_head_sha must NOT be overwritten on subsequent preparations. \
                 Was {}, expected {} (original). Bug: squash guard would compare {} vs {} \
                 and incorrectly pass, allowing unprepared commits to be squashed.",
                result
                    .train
                    .predecessor_head_sha
                    .as_ref()
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                original_sha,
                new_sha,
                new_sha
            );
        }

        /// Regression test: trains with NO descendants must record predecessor_head_sha
        /// so the squash-time guard can detect post-start pushes.
        ///
        /// Bug scenario (before fix):
        /// 1. Train starts with no descendants, head SHA is X
        /// 2. Someone pushes to PR, head SHA becomes Y
        /// 3. Bot squash-merges - guard doesn't fire because predecessor_head_sha was never set
        /// 4. Result: Unreviewed commits Y are merged
        #[test]
        fn no_descendants_train_records_head_sha() {
            let original_sha = make_sha(0x111);

            // Create train in Idle phase (no descendants)
            let train = TrainRecord::new(PrNumber(1));

            // Build PR map with single PR (no descendants)
            let pr1 = CachedPr::new(
                PrNumber(1),
                original_sha.clone(),
                "branch-1".to_string(),
                "main".to_string(),
                None,
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            );
            let prs = HashMap::from([(PrNumber(1), pr1)]);
            let ctx = StepContext::new("main");

            // Execute cascade step - should transition to SquashPending
            let result = execute_cascade_step(train, &prs, &ctx);

            // Phase should be SquashPending (no descendants to prepare)
            assert!(
                matches!(
                    result.train.cascade_phase,
                    CascadePhase::SquashPending { .. }
                ),
                "Should transition to SquashPending with no descendants. Got: {:?}",
                result.train.cascade_phase
            );

            // CRITICAL: predecessor_head_sha must be recorded
            assert_eq!(
                result.train.predecessor_head_sha,
                Some(original_sha.clone()),
                "BUG: Trains with no descendants must record predecessor_head_sha \
                 for the squash-time guard. Without this, post-start pushes go undetected."
            );

            // predecessor_pr should also be set
            assert_eq!(
                result.train.predecessor_pr,
                Some(PrNumber(1)),
                "predecessor_pr should be set to current_pr"
            );
        }

        /// Regression test: trains with NO descendants must abort on head SHA change.
        ///
        /// This verifies the guard actually fires for no-descendant trains after the fix.
        #[test]
        fn no_descendants_train_aborts_on_head_change() {
            let original_sha = make_sha(0x111);
            let new_sha = make_sha(0x222);

            // Setup: train already in SquashPending with recorded SHA
            let mut train = TrainRecord::new(PrNumber(1));
            train.cascade_phase = CascadePhase::SquashPending {
                progress: DescendantProgress::new(vec![]),
            };
            train.predecessor_pr = Some(PrNumber(1));
            train.predecessor_head_sha = Some(original_sha.clone());
            train.state = TrainState::Running;

            // Build PR map with CHANGED head SHA (simulates post-start push)
            let pr1 = CachedPr::new(
                PrNumber(1),
                new_sha.clone(), // HEAD CHANGED
                "branch-1".to_string(),
                "main".to_string(),
                None,
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            );
            let prs = HashMap::from([(PrNumber(1), pr1)]);
            let ctx = StepContext::new("main");

            // Execute cascade step - should abort
            let result = execute_cascade_step(train, &prs, &ctx);

            // Must abort due to head SHA change
            assert!(
                matches!(
                    result.outcome,
                    CascadeStepOutcome::Aborted {
                        reason: AbortReason::HeadShaChanged { .. },
                        ..
                    }
                ),
                "Train with no descendants must abort on head SHA change. \
                 Original: {}, New: {}, Outcome: {:?}",
                original_sha,
                new_sha,
                result.outcome
            );
        }

        /// Regression test: external merge must preserve the skipped set.
        ///
        /// Bug scenario (before fix):
        /// 1. Train in Preparing phase with descendants #2 (skipped/closed) and #3
        /// 2. Descendant #3 is prepared, #2 was skipped (closed PR)
        /// 3. External merge happens
        /// 4. BUG: DescendantProgress::new() clears skipped set
        /// 5. Result: #2 is reintroduced for reconciliation, causing errors
        #[test]
        fn external_merge_preserves_skipped_descendants() {
            let merge_sha = make_sha(0xfff);

            // Setup: train in SquashPending with one descendant skipped
            let mut train = TrainRecord::new(PrNumber(1));
            let mut progress = DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]);
            progress.mark_completed(PrNumber(3)); // #3 was prepared
            progress.mark_skipped(PrNumber(2)); // #2 was skipped (closed)
            train.cascade_phase = CascadePhase::SquashPending { progress };

            // Build PR map with root PR merged externally
            let mut pr1 = make_open_pr(1, "main", None);
            pr1.state = PrState::Merged {
                merge_commit_sha: merge_sha.clone(),
            };
            // #2 is closed (skipped)
            let mut pr2 = CachedPr::new(
                PrNumber(2),
                make_sha(2),
                "branch-2".to_string(),
                "branch-1".to_string(),
                Some(PrNumber(1)),
                PrState::Closed,
                MergeStateStatus::Clean,
                false,
            );
            pr2.head_ref = "branch-2".to_string();
            // #3 is open
            let mut pr3 = make_open_pr(3, "branch-1", Some(1));
            pr3.head_ref = "branch-3".to_string();

            let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2), (PrNumber(3), pr3)]);
            let ctx = StepContext::new("main");

            // Execute cascade step - should transition to Reconciling
            let result = execute_cascade_step(train, &prs, &ctx);

            // Phase must be Reconciling
            match &result.train.cascade_phase {
                CascadePhase::Reconciling { progress, .. } => {
                    // CRITICAL: Skipped set must be preserved
                    assert!(
                        progress.skipped.contains(&PrNumber(2)),
                        "BUG: Skipped descendant #2 was dropped during external merge handling. \
                         Skipped set: {:?}",
                        progress.skipped
                    );

                    // Completed set should be reset (prepared != reconciled)
                    assert!(
                        !progress.completed.contains(&PrNumber(3)),
                        "Completed set should be reset for reconciliation"
                    );

                    // Only #3 should be in remaining() (not #2 which is skipped)
                    let remaining: Vec<_> = progress.remaining().copied().collect();
                    assert_eq!(
                        remaining,
                        vec![PrNumber(3)],
                        "Only unskipped descendants should be in remaining(). \
                         Got: {:?}",
                        remaining
                    );
                }
                other => panic!(
                    "Expected Reconciling phase, got {:?}",
                    std::mem::discriminant(other)
                ),
            }
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // Property-based tests that would have caught review comment bugs
        // ─────────────────────────────────────────────────────────────────────────────

        mod property_based_bug_detection {
            use super::*;
            use crate::types::TrainState;
            use proptest::prelude::*;

            fn arb_sha() -> impl Strategy<Value = Sha> {
                "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
            }

            fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
                (2u64..100).prop_map(PrNumber)
            }

            fn arb_unique_descendants(
                min: usize,
                max: usize,
            ) -> impl Strategy<Value = Vec<PrNumber>> {
                prop::collection::hash_set(arb_pr_number(), min..max)
                    .prop_map(|set| set.into_iter().collect())
            }

            /// BUG #1: handle_external_merge only aborts when phase is Preparing.
            /// External merge in Idle or SquashPending can skip preparation entirely.
            ///
            /// Property: For ANY phase with unprepared direct descendants, an external
            /// merge MUST abort (not silently proceed to Reconciling).
            #[test]
            fn external_merge_in_idle_with_descendants_must_abort() {
                proptest!(|(
                    descendants in arb_unique_descendants(1, 3),
                    merge_sha in arb_sha()
                )| {
                    // Create train in Idle phase (no preparation started)
                    let train = TrainRecord::new(PrNumber(1));
                    prop_assert!(matches!(train.cascade_phase, CascadePhase::Idle));

                    // Build PR map - root is merged externally, descendants exist
                    let pr1 = CachedPr::new(
                        PrNumber(1),
                        merge_sha.clone(),
                        "branch-1".to_string(),
                        "main".to_string(),
                        None,
                        PrState::Merged { merge_commit_sha: merge_sha.clone() },
                        MergeStateStatus::Clean,
                        false,
                    );
                    let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                    // Add descendants (all are DIRECT children of #1 - unprepared)
                    for &desc in &descendants {
                        let desc_pr = CachedPr::new(
                            desc,
                            merge_sha.clone(),
                            format!("branch-{}", desc.0),
                            "branch-1".to_string(), // base is root's head branch
                            Some(PrNumber(1)),
                            PrState::Open,
                            MergeStateStatus::Clean,
                            false,
                        );
                        prs.insert(desc, desc_pr);
                    }

                    let ctx = StepContext::new("main");
                    let result = execute_cascade_step(train, &prs, &ctx);

                    // MUST abort - descendants were never prepared
                    prop_assert!(
                        matches!(result.outcome, CascadeStepOutcome::Aborted { .. }),
                        "External merge in Idle with descendants MUST abort. \
                         Descendants: {:?}, Outcome: {:?}",
                        descendants, result.outcome
                    );

                    if let CascadeStepOutcome::Aborted { reason, .. } = &result.outcome {
                        prop_assert!(
                            matches!(reason, AbortReason::PreparationIncomplete { .. }),
                            "Should be PreparationIncomplete, got: {:?}",
                            reason
                        );
                    }
                });
            }

            /// BUG #1 (continued): External merge in SquashPending must reset progress.
            /// The completed set tracks PREPARED descendants, not RECONCILED ones.
            ///
            /// Property: External merge in SquashPending must start Reconciling with
            /// empty completed set (all descendants need reconciliation).
            #[test]
            fn external_merge_in_squash_pending_resets_completed() {
                proptest!(|(
                    descendants in arb_unique_descendants(1, 3),
                    merge_sha in arb_sha()
                )| {
                    // Create train in SquashPending with ALL descendants "prepared"
                    let mut progress = DescendantProgress::new(descendants.clone());
                    for &desc in &descendants {
                        progress.mark_completed(desc);
                    }
                    let mut train = TrainRecord::new(PrNumber(1));
                    train.cascade_phase = CascadePhase::SquashPending { progress };

                    // Build PR map - root is merged externally
                    let pr1 = CachedPr::new(
                        PrNumber(1),
                        merge_sha.clone(),
                        "branch-1".to_string(),
                        "main".to_string(),
                        None,
                        PrState::Merged { merge_commit_sha: merge_sha.clone() },
                        MergeStateStatus::Clean,
                        false,
                    );
                    let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                    for &desc in &descendants {
                        let desc_pr = CachedPr::new(
                            desc,
                            merge_sha.clone(),
                            format!("branch-{}", desc.0),
                            "branch-1".to_string(),
                            Some(PrNumber(1)),
                            PrState::Open,
                            MergeStateStatus::Clean,
                            false,
                        );
                        prs.insert(desc, desc_pr);
                    }

                    let ctx = StepContext::new("main");
                    let result = execute_cascade_step(train, &prs, &ctx);

                    // Should transition to Reconciling (not abort - prep is complete)
                    prop_assert!(
                        matches!(result.train.cascade_phase, CascadePhase::Reconciling { .. }),
                        "External merge in SquashPending should transition to Reconciling. \
                         Got: {:?}",
                        result.train.cascade_phase
                    );

                    // CRITICAL: The completed set must be RESET, not carried over
                    if let CascadePhase::Reconciling { progress, .. } = &result.train.cascade_phase {
                        prop_assert!(
                            progress.completed.is_empty(),
                            "BUG: completed set was carried over from SquashPending! \
                             Completed: {:?}. These descendants were PREPARED, not RECONCILED.",
                            progress.completed
                        );
                        prop_assert_eq!(
                            progress.remaining().count(),
                            descendants.len(),
                            "All descendants should need reconciliation"
                        );
                    }
                });
            }

            /// BUG #1b: External merge in Preparing with ALL descendants prepared skips reconciliation.
            /// The completed set in Preparing tracks "prepared" not "reconciled".
            ///
            /// Property: External merge in Preparing (with all prepared) must transition to
            /// Reconciling with reset progress, not skip directly to complete_cascade.
            #[test]
            fn external_merge_in_preparing_all_prepared_resets_completed() {
                proptest!(|(
                    descendants in arb_unique_descendants(1, 3),
                    merge_sha in arb_sha()
                )| {
                    // Create train in Preparing with ALL descendants already "prepared" (completed)
                    let mut progress = DescendantProgress::new(descendants.clone());
                    for &desc in &descendants {
                        progress.mark_completed(desc);
                    }
                    let mut train = TrainRecord::new(PrNumber(1));
                    train.cascade_phase = CascadePhase::Preparing { progress };

                    // Build PR map - root is merged externally
                    let pr1 = CachedPr::new(
                        PrNumber(1),
                        merge_sha.clone(),
                        "branch-1".to_string(),
                        "main".to_string(),
                        None,
                        PrState::Merged { merge_commit_sha: merge_sha.clone() },
                        MergeStateStatus::Clean,
                        false,
                    );
                    let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                    for &desc in &descendants {
                        let desc_pr = CachedPr::new(
                            desc,
                            merge_sha.clone(),
                            format!("branch-{}", desc.0),
                            "branch-1".to_string(),
                            Some(PrNumber(1)),
                            PrState::Open,
                            MergeStateStatus::Clean,
                            false,
                        );
                        prs.insert(desc, desc_pr);
                    }

                    let ctx = StepContext::new("main");
                    let result = execute_cascade_step(train, &prs, &ctx);

                    // CRITICAL: Must transition to Reconciling (not Idle/complete)
                    prop_assert!(
                        matches!(result.train.cascade_phase, CascadePhase::Reconciling { .. }),
                        "External merge in Preparing (all prepared) must transition to Reconciling. \
                         Got: {:?}. BUG: skipped reconciliation!",
                        result.train.cascade_phase
                    );

                    // The completed set must be RESET - "prepared" != "reconciled"
                    if let CascadePhase::Reconciling { progress, .. } = &result.train.cascade_phase {
                        prop_assert!(
                            progress.completed.is_empty(),
                            "BUG: completed set was carried over from Preparing! \
                             Completed: {:?}. These descendants were PREPARED, not RECONCILED.",
                            progress.completed
                        );
                        prop_assert_eq!(
                            progress.remaining().count(),
                            descendants.len(),
                            "All descendants should need reconciliation"
                        );
                    }
                });
            }

            /// BUG #2: External merges with NO descendants don't validate squash semantics.
            /// Non-squash merges (merge commits, rebase) violate the squash-only requirement.
            ///
            /// Property: External merge with NO descendants must emit ValidateSquashCommit.
            /// (With descendants, validation happens during reconciliation in the next step.)
            #[test]
            fn external_merge_no_descendants_emits_validate_squash_effect() {
                proptest!(|(merge_sha in arb_sha())| {
                    let mut train = TrainRecord::new(PrNumber(1));
                    train.status_comment_id = Some(CommentId(12345));
                    train.cascade_phase = CascadePhase::Idle; // No descendants

                    // Build PR map - root is merged externally, no descendants
                    let pr1 = CachedPr::new(
                        PrNumber(1),
                        merge_sha.clone(),
                        "branch-1".to_string(),
                        "main".to_string(),
                        None,
                        PrState::Merged { merge_commit_sha: merge_sha.clone() },
                        MergeStateStatus::Clean,
                        false,
                    );
                    let prs = HashMap::from([(PrNumber(1), pr1)]);

                    let ctx = StepContext::new("main");
                    let result = execute_cascade_step(train, &prs, &ctx);

                    // Should complete (no descendants to process)
                    prop_assert!(
                        matches!(result.outcome, CascadeStepOutcome::Complete),
                        "External merge with no descendants should Complete. Got: {:?}",
                        result.outcome
                    );

                    // MUST have ValidateSquashCommit effect
                    let has_validate_effect = result.effects.iter().any(|e| {
                        matches!(e, Effect::Git(GitEffect::ValidateSquashCommit { .. }))
                    });

                    prop_assert!(
                        has_validate_effect,
                        "BUG: External merge with no descendants must emit ValidateSquashCommit. \
                         Effects: {:?}",
                        result.effects
                    );
                });
            }

            /// Property: External merge WITH descendants transitions to Reconciling.
            /// Validation then happens during reconciliation (MergeReconcile validates).
            #[test]
            fn external_merge_with_descendants_transitions_to_reconciling() {
                proptest!(|(
                    descendants in arb_unique_descendants(1, 3),
                    merge_sha in arb_sha()
                )| {
                    let mut train = TrainRecord::new(PrNumber(1));
                    train.status_comment_id = Some(CommentId(12345));

                    // SquashPending with all descendants "prepared"
                    let mut progress = DescendantProgress::new(descendants.clone());
                    for &desc in &descendants {
                        progress.mark_completed(desc);
                    }
                    train.cascade_phase = CascadePhase::SquashPending { progress };

                    // Build PR map - root is merged externally
                    let pr1 = CachedPr::new(
                        PrNumber(1),
                        merge_sha.clone(),
                        "branch-1".to_string(),
                        "main".to_string(),
                        None,
                        PrState::Merged { merge_commit_sha: merge_sha.clone() },
                        MergeStateStatus::Clean,
                        false,
                    );
                    let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                    for &desc in &descendants {
                        let desc_pr = CachedPr::new(
                            desc,
                            merge_sha.clone(),
                            format!("branch-{}", desc.0),
                            "branch-1".to_string(),
                            Some(PrNumber(1)),
                            PrState::Open,
                            MergeStateStatus::Clean,
                            false,
                        );
                        prs.insert(desc, desc_pr);
                    }

                    let ctx = StepContext::new("main");
                    let result = execute_cascade_step(train, &prs, &ctx);

                    // Must transition to Reconciling (not complete)
                    prop_assert!(
                        matches!(result.train.cascade_phase, CascadePhase::Reconciling { .. }),
                        "External merge with descendants must transition to Reconciling. \
                         Got: {:?}",
                        result.train.cascade_phase
                    );

                    // The reconciliation step will validate via MergeReconcile
                    // (but that's in the NEXT execute_cascade_step call, not this one)
                });
            }

            /// BUG #3: Idle → Preparing/SquashPending transitions don't update status comment.
            ///
            /// Property: Every phase transition MUST emit a status comment update
            /// (when status_comment_id is set).
            #[test]
            fn idle_transition_emits_status_comment_update() {
                proptest!(|(
                    descendants in arb_unique_descendants(0, 3),
                    sha in arb_sha()
                )| {
                    let mut train = TrainRecord::new(PrNumber(1));
                    train.status_comment_id = Some(CommentId(12345));

                    // Build PR map
                    let pr1 = CachedPr::new(
                        PrNumber(1),
                        sha.clone(),
                        "branch-1".to_string(),
                        "main".to_string(),
                        None,
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                    for &desc in &descendants {
                        let desc_pr = CachedPr::new(
                            desc,
                            sha.clone(),
                            format!("branch-{}", desc.0),
                            "branch-1".to_string(),
                            Some(PrNumber(1)),
                            PrState::Open,
                            MergeStateStatus::Clean,
                            false,
                        );
                        prs.insert(desc, desc_pr);
                    }

                    let ctx = StepContext::new("main");
                    let result = execute_cascade_step(train, &prs, &ctx);

                    // Should have transitioned from Idle
                    let transitioned = !matches!(result.train.cascade_phase, CascadePhase::Idle);

                    if transitioned {
                        // MUST emit status comment update for GitHub-based recovery
                        let has_comment_update = result.effects.iter().any(|e| {
                            matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. })
                                if *comment_id == CommentId(12345))
                        });
                        prop_assert!(
                            has_comment_update,
                            "BUG: Idle → {:?} transition did not emit status comment update! \
                             Effects: {:?}",
                            result.train.cascade_phase.name(),
                            result.effects
                        );
                    }
                });
            }

            /// BUG #6: Late-addition size check only counts direct descendants.
            /// Deep chains can exceed the 50-PR limit without triggering abort.
            ///
            /// Property: Train size check must count ALL transitive descendants,
            /// not just immediate children.
            #[test]
            fn train_size_check_counts_transitive_descendants() {
                // Create a deep linear chain that exceeds limit
                // Root <- D1 <- D2 <- ... <- DN where N > MAX_TRAIN_SIZE
                const CHAIN_LENGTH: usize = 55; // Exceeds MAX_TRAIN_SIZE (50)

                let sha = Sha::parse("abcd1234abcd1234abcd1234abcd1234abcd1234").unwrap();
                let train = TrainRecord::new(PrNumber(1));

                // Build a DEEP chain: 1 <- 2 <- 3 <- ... <- 55
                let mut prs = HashMap::new();
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                prs.insert(PrNumber(1), pr1);

                for i in 2..=(CHAIN_LENGTH as u64) {
                    let pr = CachedPr::new(
                        PrNumber(i),
                        sha.clone(),
                        format!("branch-{}", i),
                        format!("branch-{}", i - 1), // base is predecessor's head
                        Some(PrNumber(i - 1)),       // predecessor is previous in chain
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    prs.insert(PrNumber(i), pr);
                }

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // MUST abort due to train too large
                // BUG: If only counting direct descendants, would only see 1 (PR #2)
                // and proceed, but the full chain is 55 PRs.
                assert!(
                    matches!(
                        result.outcome,
                        CascadeStepOutcome::Aborted {
                            reason: AbortReason::TrainTooLarge { .. },
                            ..
                        }
                    ),
                    "BUG: Deep chain of {} PRs should trigger TrainTooLarge abort. \
                     Only direct descendants counted? Outcome: {:?}",
                    CHAIN_LENGTH,
                    result.outcome
                );

                if let CascadeStepOutcome::Aborted {
                    reason: AbortReason::TrainTooLarge { pr_count, .. },
                    ..
                } = &result.outcome
                {
                    assert_eq!(
                        *pr_count, CHAIN_LENGTH,
                        "BUG: pr_count should be {} (full chain), not just direct descendants",
                        CHAIN_LENGTH
                    );
                }
            }

            /// BUG #6 (property version): For any chain structure, size check must
            /// count all reachable PRs.
            #[test]
            fn transitive_descendant_count_is_correct() {
                proptest!(|(chain_length in 2usize..10)| {
                    let sha = Sha::parse("abcd1234abcd1234abcd1234abcd1234abcd1234").unwrap();

                    // Build a linear chain
                    let mut prs = HashMap::new();
                    let pr1 = CachedPr::new(
                        PrNumber(1),
                        sha.clone(),
                        "branch-1".to_string(),
                        "main".to_string(),
                        None,
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    prs.insert(PrNumber(1), pr1);

                    for i in 2..=(chain_length as u64) {
                        let pr = CachedPr::new(
                            PrNumber(i),
                            sha.clone(),
                            format!("branch-{}", i),
                            format!("branch-{}", i - 1),
                            Some(PrNumber(i - 1)),
                            PrState::Open,
                            MergeStateStatus::Clean,
                            false,
                        );
                        prs.insert(PrNumber(i), pr);
                    }

                    // compute_all_descendants should return chain_length - 1 descendants
                    let all_desc = compute_all_descendants(PrNumber(1), &prs);
                    prop_assert_eq!(
                        all_desc.len(),
                        chain_length - 1,
                        "compute_all_descendants should find {} transitive descendants, found {}",
                        chain_length - 1,
                        all_desc.len()
                    );

                    // compute_direct_descendants should return only 1 (PR #2)
                    let direct_desc = compute_direct_descendants(PrNumber(1), &prs);
                    prop_assert_eq!(
                        direct_desc.len(),
                        1,
                        "compute_direct_descendants should find exactly 1 direct child"
                    );
                    prop_assert!(direct_desc.contains(&PrNumber(2)));
                });
            }

            // ─────────────────────────────────────────────────────────────────────────────
            // Tests for review comment fixes (WaitingOnCi state, RecordReconciliation)
            // ─────────────────────────────────────────────────────────────────────────────

            /// Review Comment #6 fix: WaitingOnCi outcomes MUST set train.state to WaitingCi.
            ///
            /// Property: Whenever execute_cascade_step returns CascadeStepOutcome::WaitingOnCi,
            /// the returned train.state MUST be TrainState::WaitingCi.
            #[test]
            fn waiting_on_ci_outcome_sets_train_state() {
                proptest!(|(
                    descendants in arb_unique_descendants(1, 3),
                    sha in arb_sha()
                )| {
                    // Create train in Preparing phase with multiple descendants
                    let mut train = TrainRecord::new(PrNumber(1));
                    train.status_comment_id = Some(CommentId(12345));
                    train.predecessor_head_sha = Some(sha.clone());

                    // Set up Preparing phase
                    let progress = DescendantProgress::new(descendants.clone());
                    train.cascade_phase = CascadePhase::Preparing { progress };

                    // Build PR map with root open, descendants exist
                    let pr1 = CachedPr::new(
                        PrNumber(1),
                        sha.clone(),
                        "branch-1".to_string(),
                        "main".to_string(),
                        None,
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    let mut prs = HashMap::from([(PrNumber(1), pr1)]);

                    // Add descendants as open PRs based on branch-1
                    for &desc in &descendants {
                        let desc_pr = CachedPr::new(
                            desc,
                            sha.clone(),
                            format!("branch-{}", desc.0),
                            "branch-1".to_string(),
                            Some(PrNumber(1)),
                            PrState::Open,
                            MergeStateStatus::Clean,
                            false,
                        );
                        prs.insert(desc, desc_pr);
                    }

                    let ctx = StepContext::new("main");
                    let result = execute_cascade_step(train, &prs, &ctx);

                    // If outcome is WaitingOnCi, train.state MUST be WaitingCi
                    if matches!(result.outcome, CascadeStepOutcome::WaitingOnCi { .. }) {
                        prop_assert!(
                            matches!(result.train.state, TrainState::WaitingCi),
                            "BUG (Review Comment #6): WaitingOnCi outcome returned but \
                             train.state is {:?}, not WaitingCi. \
                             Outcome: {:?}",
                            result.train.state,
                            result.outcome
                        );
                    }
                });
            }

            /// Review Comment #2 fix: DescendantRetargeted MUST emit RecordReconciliation.
            ///
            /// Property: When processing OperationResult::DescendantRetargeted with
            /// last_squash_sha set, the step MUST emit Effect::RecordReconciliation.
            #[test]
            fn descendant_retargeted_emits_record_reconciliation() {
                proptest!(|(
                    descendant_pr in arb_pr_number(),
                    squash_sha in arb_sha(),
                    head_sha in arb_sha()
                )| {
                    // Create train in Retargeting phase with a descendant to retarget
                    let mut train = TrainRecord::new(PrNumber(1));
                    train.status_comment_id = Some(CommentId(12345));
                    train.last_squash_sha = Some(squash_sha.clone());

                    // Set up Retargeting phase with descendant pending
                    let progress = DescendantProgress::new(vec![descendant_pr]);
                    train.cascade_phase = CascadePhase::Retargeting {
                        progress,
                        squash_sha: squash_sha.clone(),
                    };

                    // Build PR map
                    let pr1 = CachedPr::new(
                        PrNumber(1),
                        head_sha.clone(),
                        "branch-1".to_string(),
                        "main".to_string(),
                        None,
                        PrState::Merged { merge_commit_sha: squash_sha.clone() },
                        MergeStateStatus::Clean,
                        false,
                    );
                    let desc_pr = CachedPr::new(
                        descendant_pr,
                        head_sha.clone(),
                        format!("branch-{}", descendant_pr.0),
                        "main".to_string(), // Already retargeted to main
                        None,               // Predecessor cleared
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    let _prs = HashMap::from([(PrNumber(1), pr1), (descendant_pr, desc_pr)]);

                    // Simulate retarget completion via operation result
                    let op_result = OperationResult::DescendantRetargeted { pr: descendant_pr };
                    let (updated_train, _, effects) =
                        process_operation_result(train.clone(), op_result);

                    // MUST emit RecordReconciliation for the retargeted descendant
                    let has_record_reconciliation = effects.iter().any(|e| {
                        match e {
                            Effect::RecordReconciliation { pr, squash_sha: s } =>
                                *pr == descendant_pr && *s == squash_sha,
                            _ => false,
                        }
                    });

                    prop_assert!(
                        has_record_reconciliation,
                        "BUG (Review Comment #2): DescendantRetargeted with last_squash_sha \
                         MUST emit RecordReconciliation. \
                         descendant_pr: {:?}, last_squash_sha: {:?}, effects: {:?}",
                        descendant_pr,
                        train.last_squash_sha,
                        effects
                    );

                    // Verify the descendant is marked completed in progress
                    if let CascadePhase::Retargeting { progress, .. } = &updated_train.cascade_phase {
                        prop_assert!(
                            progress.completed.contains(&descendant_pr),
                            "Descendant should be marked completed after retarget"
                        );
                    }
                });
            }

            /// Review Comment #2 (continued): Abort on missing last_squash_sha.
            ///
            /// Property: When last_squash_sha is None, DescendantRetargeted MUST abort
            /// with InternalInvariantViolation. This prevents silent fan-out breakage.
            #[test]
            fn descendant_retargeted_aborts_without_squash_sha() {
                proptest!(|(
                    descendant_pr in arb_pr_number(),
                    head_sha in arb_sha()
                )| {
                    // Create train WITHOUT last_squash_sha set
                    let mut train = TrainRecord::new(PrNumber(1));
                    train.last_squash_sha = None; // Critical: no squash SHA

                    // Set up Retargeting phase (squash_sha is in phase but last_squash_sha is None)
                    let progress = DescendantProgress::new(vec![descendant_pr]);
                    train.cascade_phase = CascadePhase::Retargeting {
                        progress,
                        squash_sha: head_sha.clone(), // Phase has it, but train.last_squash_sha doesn't
                    };

                    // Simulate retarget completion
                    let op_result = OperationResult::DescendantRetargeted { pr: descendant_pr };
                    let (updated_train, outcome, effects) = process_operation_result(train, op_result);

                    // MUST abort with InternalInvariantViolation
                    let is_invariant_violation_abort = matches!(
                        &outcome,
                        Some(CascadeStepOutcome::Aborted {
                            reason: AbortReason::InternalInvariantViolation { .. },
                            ..
                        })
                    );

                    prop_assert!(
                        is_invariant_violation_abort,
                        "DescendantRetargeted WITHOUT train.last_squash_sha MUST abort with \
                         InternalInvariantViolation. Got outcome: {:?}",
                        outcome
                    );

                    // Train should be aborted
                    prop_assert!(
                        updated_train.state == TrainState::Aborted,
                        "Train state should be Aborted, got: {:?}",
                        updated_train.state
                    );

                    // Should NOT emit RecordReconciliation (we're aborting, not continuing)
                    let has_record_reconciliation = effects.iter().any(|e| {
                        matches!(e, Effect::RecordReconciliation { .. })
                    });

                    prop_assert!(
                        !has_record_reconciliation,
                        "Aborting train should NOT emit RecordReconciliation. Effects: {:?}",
                        effects
                    );
                });
            }

            /// Draft descendants in Preparing phase should return WaitingOnCi with empty effects.
            ///
            /// Property: When a descendant is a draft (is_draft=true) during Preparing phase,
            /// the cascade step should return WaitingOnCi with no effects, and set train.state
            /// to WaitingCi.
            #[test]
            fn draft_descendant_waits_in_preparing() {
                proptest!(|(
                    descendant_pr in arb_pr_number(),
                    sha in arb_sha()
                )| {
                    // Create train in Preparing phase
                    let mut train = TrainRecord::new(PrNumber(1));
                    train.status_comment_id = Some(CommentId(12345));
                    train.predecessor_head_sha = Some(sha.clone());

                    let progress = DescendantProgress::new(vec![descendant_pr]);
                    train.cascade_phase = CascadePhase::Preparing { progress };

                    // Build PR map with OPEN root and draft descendant
                    let pr1 = CachedPr::new(
                        PrNumber(1),
                        sha.clone(),
                        "branch-1".to_string(),
                        "main".to_string(),
                        None,
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    // Descendant is a DRAFT
                    let desc_pr = CachedPr::new(
                        descendant_pr,
                        sha.clone(),
                        format!("branch-{}", descendant_pr.0),
                        "branch-1".to_string(),
                        Some(PrNumber(1)),
                        PrState::Open,
                        MergeStateStatus::Draft,
                        true,
                    );
                    let prs = HashMap::from([
                        (PrNumber(1), pr1),
                        (descendant_pr, desc_pr),
                    ]);

                    let ctx = StepContext::new("main");
                    let result = execute_cascade_step(train, &prs, &ctx);

                    // Draft descendants should return WaitingOnCi
                    prop_assert!(
                        matches!(result.outcome, CascadeStepOutcome::WaitingOnCi { pr_number } if pr_number == descendant_pr),
                        "Draft descendant should return WaitingOnCi, got {:?}",
                        result.outcome
                    );

                    // Effects should be empty (no modifications to draft PRs)
                    prop_assert!(
                        result.effects.is_empty(),
                        "Draft descendant should have empty effects, got {:?}",
                        result.effects
                    );

                    // Train state should be WaitingCi
                    prop_assert!(
                        matches!(result.train.state, TrainState::WaitingCi),
                        "Draft descendant should set train.state to WaitingCi, got {:?}",
                        result.train.state
                    );
                });
            }

            /// Draft descendants in post-squash phases should also wait.
            ///
            /// Tests Reconciling, CatchingUp, and Retargeting phases. These require special
            /// setup since the root PR must be Open (Merged roots trigger external merge handling).
            #[test]
            fn draft_descendant_waits_in_post_squash_phases() {
                proptest!(|(
                    descendant_pr in arb_pr_number(),
                    sha in arb_sha(),
                    phase_selector in 0u8..3
                )| {
                    // Create train in a post-squash phase
                    let mut train = TrainRecord::new(PrNumber(1));
                    train.status_comment_id = Some(CommentId(12345));
                    train.last_squash_sha = Some(sha.clone());

                    let progress = DescendantProgress::new(vec![descendant_pr]);
                    train.cascade_phase = match phase_selector {
                        0 => CascadePhase::Reconciling {
                            progress,
                            squash_sha: sha.clone(),
                        },
                        1 => CascadePhase::CatchingUp {
                            progress,
                            squash_sha: sha.clone(),
                        },
                        _ => CascadePhase::Retargeting {
                            progress,
                            squash_sha: sha.clone(),
                        },
                    };

                    // Root must be OPEN to avoid external merge handling path.
                    // In practice, after squash the root is Merged and external merge
                    // handling transitions us to Reconciling. But for testing the phase
                    // handlers directly, we use Open to bypass that logic.
                    let pr1 = CachedPr::new(
                        PrNumber(1),
                        sha.clone(),
                        "branch-1".to_string(),
                        "main".to_string(),
                        None,
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    // Descendant is a DRAFT
                    let desc_pr = CachedPr::new(
                        descendant_pr,
                        sha.clone(),
                        format!("branch-{}", descendant_pr.0),
                        "branch-1".to_string(),
                        Some(PrNumber(1)),
                        PrState::Open,
                        MergeStateStatus::Draft,
                        true,
                    );
                    let prs = HashMap::from([
                        (PrNumber(1), pr1),
                        (descendant_pr, desc_pr),
                    ]);

                    let ctx = StepContext::new("main");
                    let result = execute_cascade_step(train, &prs, &ctx);

                    // Draft descendants should return WaitingOnCi
                    prop_assert!(
                        matches!(result.outcome, CascadeStepOutcome::WaitingOnCi { pr_number } if pr_number == descendant_pr),
                        "Draft descendant in phase {} should return WaitingOnCi, got {:?}",
                        phase_selector,
                        result.outcome
                    );

                    // Effects should be empty (no modifications to draft PRs)
                    prop_assert!(
                        result.effects.is_empty(),
                        "Draft descendant should have empty effects, got {:?}",
                        result.effects
                    );

                    // Train state should be WaitingCi
                    prop_assert!(
                        matches!(result.train.state, TrainState::WaitingCi),
                        "Draft descendant should set train.state to WaitingCi, got {:?}",
                        result.train.state
                    );
                });
            }

            /// TrainTooLarge abort MUST emit status comment update effect.
            ///
            /// Property: When aborting due to TrainTooLarge, if status_comment_id is set,
            /// the effects MUST include an UpdateComment to inform GitHub-based recovery.
            #[test]
            fn train_too_large_emits_status_comment_update() {
                // Create a chain exceeding the limit (needs >50 PRs for MAX_TRAIN_SIZE)
                const CHAIN_LENGTH: usize = 55;

                let sha = Sha::parse("abcd1234abcd1234abcd1234abcd1234abcd1234").unwrap();
                let mut train = TrainRecord::new(PrNumber(1));
                // CRITICAL: Set status_comment_id to test comment update
                train.status_comment_id = Some(CommentId(12345));

                // Build a deep chain
                let mut prs = HashMap::new();
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                prs.insert(PrNumber(1), pr1);

                for i in 2..=(CHAIN_LENGTH as u64) {
                    let pr = CachedPr::new(
                        PrNumber(i),
                        sha.clone(),
                        format!("branch-{}", i),
                        format!("branch-{}", i - 1),
                        Some(PrNumber(i - 1)),
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    prs.insert(PrNumber(i), pr);
                }

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // Should abort due to TrainTooLarge
                assert!(
                    matches!(
                        result.outcome,
                        CascadeStepOutcome::Aborted {
                            reason: AbortReason::TrainTooLarge { .. },
                            ..
                        }
                    ),
                    "Should abort with TrainTooLarge, got: {:?}",
                    result.outcome
                );

                // MUST emit status comment update
                let has_update_comment = result
                    .effects
                    .iter()
                    .any(|e| matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { .. })));

                assert!(
                    has_update_comment,
                    "TrainTooLarge abort with status_comment_id MUST emit UpdateComment effect. \
                     Effects: {:?}",
                    result.effects
                );
            }

            /// TrainTooLarge abort without status_comment_id should NOT emit update effect.
            ///
            /// Property: When aborting due to TrainTooLarge without a status comment,
            /// no UpdateComment effect should be emitted (nothing to update).
            #[test]
            fn train_too_large_no_update_without_comment_id() {
                const CHAIN_LENGTH: usize = 55;

                let sha = Sha::parse("abcd1234abcd1234abcd1234abcd1234abcd1234").unwrap();
                let mut train = TrainRecord::new(PrNumber(1));
                // NO status_comment_id set
                train.status_comment_id = None;

                let mut prs = HashMap::new();
                let pr1 = CachedPr::new(
                    PrNumber(1),
                    sha.clone(),
                    "branch-1".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );
                prs.insert(PrNumber(1), pr1);

                for i in 2..=(CHAIN_LENGTH as u64) {
                    let pr = CachedPr::new(
                        PrNumber(i),
                        sha.clone(),
                        format!("branch-{}", i),
                        format!("branch-{}", i - 1),
                        Some(PrNumber(i - 1)),
                        PrState::Open,
                        MergeStateStatus::Clean,
                        false,
                    );
                    prs.insert(PrNumber(i), pr);
                }

                let ctx = StepContext::new("main");
                let result = execute_cascade_step(train, &prs, &ctx);

                // Should abort due to TrainTooLarge
                assert!(
                    matches!(
                        result.outcome,
                        CascadeStepOutcome::Aborted {
                            reason: AbortReason::TrainTooLarge { .. },
                            ..
                        }
                    ),
                    "Should abort with TrainTooLarge, got: {:?}",
                    result.outcome
                );

                // Should NOT emit any UpdateComment (no comment to update)
                let has_update_comment = result
                    .effects
                    .iter()
                    .any(|e| matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { .. })));

                assert!(
                    !has_update_comment,
                    "TrainTooLarge abort WITHOUT status_comment_id should NOT emit UpdateComment. \
                     Effects: {:?}",
                    result.effects
                );
            }
        }
    }
}
