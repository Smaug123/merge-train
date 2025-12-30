//! Cascade step execution.
//!
//! This module handles executing a single step of the cascade state machine.
//! A "step" processes one descendant through the current phase, potentially
//! triggering a phase transition when all descendants are processed.

use std::collections::HashMap;

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
fn execute_from_idle(
    train: &mut TrainRecord,
    prs: &HashMap<PrNumber, CachedPr>,
    _ctx: &StepContext,
) -> StepResult {
    // Compute frozen descendants
    let descendants = compute_open_descendants(train.current_pr, prs);

    // Transition to next phase
    let new_phase = crate::state::transitions::start_preparing(descendants.clone());
    train.cascade_phase = new_phase;
    train.increment_seq();

    // If we have descendants, we'll start preparing them
    // If not, we'll be in SquashPending and ready to merge
    let (outcome, effects) = if descendants.is_empty() {
        // No descendants - ready to squash
        (
            CascadeStepOutcome::WaitingOnCi {
                pr_number: train.current_pr,
            },
            vec![],
        )
    } else {
        // Has descendants - will prepare them first
        (
            CascadeStepOutcome::WaitingOnCi {
                pr_number: train.current_pr,
            },
            vec![],
        )
    };

    StepResult {
        train: train.clone(),
        outcome,
        effects,
    }
}

/// Execute Preparing phase - merge predecessor head into the next descendant.
fn execute_preparing(
    train: &mut TrainRecord,
    progress: DescendantProgress,
    prs: &HashMap<PrNumber, CachedPr>,
    _ctx: &StepContext,
) -> StepResult {
    // Find the next descendant to prepare
    let Some(&next_descendant) = progress.remaining().next() else {
        // All descendants prepared - transition to SquashPending
        return transition_to_squash_pending(train, progress);
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

    // Get predecessor head SHA (the current PR's head)
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

    let predecessor_head = current_pr.head_sha.clone();

    // Store predecessor info for recovery
    train.predecessor_pr = Some(train.current_pr);
    train.predecessor_head_sha = Some(predecessor_head.clone());
    train.increment_seq();

    // Generate effects for preparation
    let effects = vec![
        // Merge predecessor head into descendant
        Effect::Git(GitEffect::Merge {
            target: predecessor_head.as_str().to_string(),
            strategy: MergeStrategy::Default,
            message: format!(
                "Merge predecessor into {} (merge train preparation)",
                desc_pr.head_ref
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
    _ctx: &StepContext,
) -> StepResult {
    // Find the next descendant to reconcile
    let Some(&next_descendant) = progress.remaining().next() else {
        // All descendants reconciled - transition to CatchingUp
        return transition_to_catching_up(train, progress, squash_sha);
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
        return transition_to_retargeting(train, progress, squash_sha);
    };

    // Get descendant PR info
    let Some(desc_pr) = prs.get(&next_descendant) else {
        return skip_descendant(train, progress, next_descendant, "PR not found in cache");
    };

    if !desc_pr.state.is_open() {
        return skip_descendant(train, progress, next_descendant, "PR is no longer open");
    }

    // Generate catch-up effects
    let effects = vec![
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
        return complete_cascade(train);
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
fn handle_external_merge(
    train: &mut TrainRecord,
    merge_sha: Sha,
    prs: &HashMap<PrNumber, CachedPr>,
    _ctx: &StepContext,
) -> StepResult {
    // Store the squash SHA
    train.last_squash_sha = Some(merge_sha.clone());

    // Compute descendants for the merged PR
    let descendants = compute_open_descendants(train.current_pr, prs);

    if descendants.is_empty() {
        // No descendants - cascade complete
        return complete_cascade(train);
    }

    // Transition to Reconciling phase
    let progress = DescendantProgress::new(descendants);
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
fn transition_to_squash_pending(
    train: &mut TrainRecord,
    progress: DescendantProgress,
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

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::WaitingOnCi {
            pr_number: train.current_pr,
        },
        effects: vec![],
    }
}

/// Transition from Reconciling to CatchingUp.
fn transition_to_catching_up(
    train: &mut TrainRecord,
    progress: DescendantProgress,
    squash_sha: Sha,
) -> StepResult {
    let current_phase = CascadePhase::Reconciling {
        progress: progress.clone(),
        squash_sha: squash_sha.clone(),
    };

    let new_phase = next_phase(&current_phase, PhaseOutcome::AllComplete)
        .expect("Reconciling -> CatchingUp is valid");

    train.cascade_phase = new_phase;
    train.increment_seq();

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::WaitingOnCi {
            pr_number: train.current_pr,
        },
        effects: vec![],
    }
}

/// Transition from CatchingUp to Retargeting.
fn transition_to_retargeting(
    train: &mut TrainRecord,
    progress: DescendantProgress,
    squash_sha: Sha,
) -> StepResult {
    let current_phase = CascadePhase::CatchingUp {
        progress: progress.clone(),
        squash_sha: squash_sha.clone(),
    };

    let new_phase = next_phase(&current_phase, PhaseOutcome::AllComplete)
        .expect("CatchingUp -> Retargeting is valid");

    train.cascade_phase = new_phase;
    train.increment_seq();

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::WaitingOnCi {
            pr_number: train.current_pr,
        },
        effects: vec![],
    }
}

/// Complete the cascade (Retargeting -> Idle).
fn complete_cascade(train: &mut TrainRecord) -> StepResult {
    train.cascade_phase = CascadePhase::Idle;
    train.increment_seq();

    StepResult {
        train: train.clone(),
        outcome: CascadeStepOutcome::Complete,
        effects: vec![],
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
}
