//! Cascade engine for orchestrating merge train operations.
//!
//! The `CascadeEngine` is a stateless orchestrator that computes state transitions
//! and returns effects to be executed by interpreters. It does not perform I/O directly.

use std::collections::HashMap;

use thiserror::Error;

use crate::effects::{Effect, GitEffect, GitHubEffect};
use crate::state::topology::{MergeStack, is_root};
use crate::state::{build_descendants_index, compute_stacks};
use crate::types::{
    AbortReason, BlockReason, CachedPr, CascadePhase, CascadeStepOutcome, MergeStateStatus,
    PrNumber, Sha, TrainError, TrainRecord,
};

/// Maximum number of PRs allowed in a single train.
pub const MAX_TRAIN_SIZE: usize = 50;

/// Errors that can occur in cascade operations.
#[derive(Debug, Error)]
pub enum CascadeError {
    /// The PR is not a valid root for starting a train.
    #[error("PR #{0} is not a valid root: {1}")]
    NotARoot(PrNumber, String),

    /// A train already exists for this root.
    #[error("Train already exists for PR #{0}")]
    TrainAlreadyExists(PrNumber),

    /// No train exists for this root.
    #[error("No train exists for PR #{0}")]
    NoTrainExists(PrNumber),

    /// The train is in an invalid state for this operation.
    #[error("Train for PR #{0} is in invalid state {1} for operation {2}")]
    InvalidState(PrNumber, String, String),

    /// Cycle detected in predecessor graph.
    #[error("Cycle detected in predecessor graph: {0:?}")]
    CycleDetected(Vec<PrNumber>),

    /// Train exceeds maximum size.
    #[error("Train size {0} exceeds maximum {1}")]
    TrainTooLarge(usize, usize),

    /// PR not found in cache.
    #[error("PR #{0} not found in cache")]
    PrNotFound(PrNumber),

    /// Phase transition error.
    #[error("Invalid phase transition: {0}")]
    InvalidTransition(String),
}

/// Result of starting a train.
#[derive(Debug)]
pub struct StartTrainResult {
    /// The newly created train record.
    pub train: TrainRecord,

    /// The computed stack from root to tip.
    pub stack: MergeStack,

    /// Effects to execute (e.g., post status comment).
    pub effects: Vec<Effect>,
}

/// Result of stopping a train.
#[derive(Debug)]
pub struct StopTrainResult {
    /// The modified train record.
    pub train: TrainRecord,

    /// Effects to execute (e.g., update status comment, remove worktree).
    pub effects: Vec<Effect>,
}

/// The cascade engine orchestrates merge train operations.
///
/// This is a stateless object that computes state transitions and returns
/// effects. All state is passed in and returned explicitly.
#[derive(Debug, Clone)]
pub struct CascadeEngine {
    /// The default branch name (e.g., "main").
    pub default_branch: String,
}

impl CascadeEngine {
    /// Creates a new cascade engine.
    pub fn new(default_branch: impl Into<String>) -> Self {
        CascadeEngine {
            default_branch: default_branch.into(),
        }
    }

    /// Starts a new train for the given root PR.
    ///
    /// # Arguments
    ///
    /// * `root_pr` - The PR number to start the train from
    /// * `prs` - The cached PR information
    /// * `active_trains` - Currently active trains (to check for conflicts)
    ///
    /// # Returns
    ///
    /// A `StartTrainResult` containing the new train record, computed stack,
    /// and effects to execute.
    ///
    /// # Errors
    ///
    /// - `NotARoot`: The PR is not a valid root (doesn't target default branch,
    ///   has unmerged predecessor, etc.)
    /// - `TrainAlreadyExists`: A train already exists for this root
    /// - `CycleDetected`: The predecessor graph contains a cycle
    /// - `TrainTooLarge`: The stack exceeds the maximum size
    pub fn start_train(
        &self,
        root_pr: PrNumber,
        prs: &HashMap<PrNumber, CachedPr>,
        active_trains: &HashMap<PrNumber, TrainRecord>,
    ) -> Result<StartTrainResult, CascadeError> {
        // Check if PR exists
        let pr = prs.get(&root_pr).ok_or(CascadeError::PrNotFound(root_pr))?;

        // Check if PR is open
        if !pr.state.is_open() {
            return Err(CascadeError::NotARoot(
                root_pr,
                "PR is not open".to_string(),
            ));
        }

        // Check if PR is a draft - DESIGN.md requires explicit draft check.
        // GitHub's mergeStateStatus can be CLEAN for drafts (if CI passes), but
        // the merge API will reject drafts. Check explicitly via is_draft field.
        if pr.is_draft {
            return Err(CascadeError::NotARoot(
                root_pr,
                "PR is a draft. Please mark it as ready for review first".to_string(),
            ));
        }

        // Check if PR is a valid root
        if !is_root(pr, &self.default_branch, prs) {
            return Err(CascadeError::NotARoot(
                root_pr,
                format!(
                    "PR targets {} (expected {}) or has unmerged predecessor",
                    pr.base_ref, self.default_branch
                ),
            ));
        }

        // Check for existing train keyed by this root
        if active_trains.contains_key(&root_pr) {
            return Err(CascadeError::TrainAlreadyExists(root_pr));
        }

        // Check for cycles
        if let Some(cycle) = crate::state::topology::detect_cycle(prs) {
            return Err(CascadeError::CycleDetected(cycle));
        }

        // Compute the stack
        let descendants_index = build_descendants_index(prs);
        let stacks = compute_stacks(prs, &self.default_branch, &descendants_index);

        // Find our stack
        let stack = stacks
            .into_iter()
            .find(|s| s.root() == Some(root_pr))
            .ok_or_else(|| {
                CascadeError::NotARoot(root_pr, "PR not found in computed stacks".to_string())
            })?;

        // Check for overlapping trains: ensure no PR in this stack is already part of another train.
        // This prevents starting a train when any PR is already being processed by another train.
        for pr_in_stack in &stack.prs {
            for (train_root, train) in active_trains {
                // Check if this PR is the current PR being processed by another train
                if train.current_pr == *pr_in_stack {
                    return Err(CascadeError::NotARoot(
                        root_pr,
                        format!(
                            "PR #{} is currently being processed by train #{}",
                            pr_in_stack, train_root
                        ),
                    ));
                }

                // Check if this PR is in the frozen descendants of another train
                if let Some(progress) = train.cascade_phase.progress()
                    && progress.frozen_descendants.contains(pr_in_stack)
                {
                    return Err(CascadeError::NotARoot(
                        root_pr,
                        format!(
                            "PR #{} is a descendant in active train #{}",
                            pr_in_stack, train_root
                        ),
                    ));
                }
            }
        }

        // Check stack size
        if stack.len() > MAX_TRAIN_SIZE {
            return Err(CascadeError::TrainTooLarge(stack.len(), MAX_TRAIN_SIZE));
        }

        // Create the train record
        let train = TrainRecord::new(root_pr);

        // Generate effects for starting the train
        let effects = vec![
            // Post initial status comment
            Effect::GitHub(GitHubEffect::PostComment {
                pr: root_pr,
                body: format_start_comment(&train, &stack),
            }),
            // Add reaction to the start command (if we had the comment ID)
            // This would be handled by the webhook handler
        ];

        Ok(StartTrainResult {
            train,
            stack,
            effects,
        })
    }

    /// Stops an existing train.
    ///
    /// # Arguments
    ///
    /// * `train` - The train record to stop
    /// * `force` - If true, stop even if the train is in the middle of an operation
    ///
    /// # Returns
    ///
    /// A `StopTrainResult` containing the modified train record and effects.
    pub fn stop_train(
        &self,
        mut train: TrainRecord,
        force: bool,
    ) -> Result<StopTrainResult, CascadeError> {
        // Check if train can be stopped
        if !train.state.is_active() && !force {
            return Err(CascadeError::InvalidState(
                train.original_root_pr,
                format!("{:?}", train.state),
                "stop".to_string(),
            ));
        }

        // Stop the train
        train.stop();

        // Generate effects
        let mut effects = vec![];

        // Remove the stack's worktree.
        // DESIGN.md: "Remove the stack's dedicated worktree (abort any in-progress merge first,
        // then remove via `git worktree remove --force`)"
        // The worktree name is keyed by original_root_pr for stability during retargeting.
        effects.push(Effect::Git(GitEffect::RemoveWorktree {
            name: format!("stack-{}", train.original_root_pr),
        }));

        // Update status comment if one exists
        if let Some(comment_id) = train.status_comment_id {
            effects.push(Effect::GitHub(GitHubEffect::UpdateComment {
                comment_id,
                body: format_stop_comment(&train),
            }));
        }

        Ok(StopTrainResult { train, effects })
    }

    /// Evaluates whether a train can proceed and what action to take.
    ///
    /// This is the main decision function called when:
    /// - A train is first started
    /// - A webhook indicates a relevant event occurred
    /// - Periodic polling checks for missed events
    ///
    /// # Arguments
    ///
    /// * `train` - The train record
    /// * `prs` - The cached PR information
    ///
    /// # Returns
    ///
    /// A `TrainAction` indicating what to do next.
    pub fn evaluate_train(
        &self,
        train: &TrainRecord,
        prs: &HashMap<PrNumber, CachedPr>,
    ) -> TrainAction {
        // If train is not running, nothing to do
        if !train.state.is_active() {
            return TrainAction::Idle;
        }

        // Get the current PR being processed
        let Some(current_pr) = prs.get(&train.current_pr) else {
            return TrainAction::Abort {
                reason: AbortReason::PrClosed,
            };
        };

        // Check if PR is still open
        if !current_pr.state.is_open() {
            if current_pr.state.is_merged() {
                // PR was merged externally - advance the train
                return TrainAction::AdvanceAfterExternalMerge {
                    merge_sha: current_pr
                        .state
                        .merge_commit_sha()
                        .cloned()
                        .expect("merged PR has merge_commit_sha"),
                };
            }
            return TrainAction::Abort {
                reason: AbortReason::PrClosed,
            };
        }

        // Evaluate merge state status
        match current_pr.merge_state_status {
            MergeStateStatus::Clean | MergeStateStatus::Unstable => {
                // Ready to proceed with cascade
                TrainAction::Proceed
            }
            MergeStateStatus::Blocked => TrainAction::Block {
                reason: BlockReason::Blocked,
            },
            MergeStateStatus::Behind => {
                // If we're in Idle phase and the current PR is a root (targets the default branch),
                // we can update the branch. This handles both:
                // - The original root (train.current_pr == train.original_root_pr)
                // - Retargeted descendants that became new roots after fan-out
                //
                // After fan-out, descendants become independent trains with their own TrainRecord.
                // These PRs now target the default branch, so they should also be able to handle
                // BEHIND by re-running catch-up to merge in the latest changes from main.
                if matches!(train.cascade_phase, CascadePhase::Idle)
                    && current_pr.base_ref == self.default_branch
                {
                    TrainAction::UpdateBranch
                } else {
                    TrainAction::Block {
                        reason: BlockReason::Behind,
                    }
                }
            }
            MergeStateStatus::Dirty => TrainAction::Abort {
                reason: AbortReason::MergeConflict {
                    details: "GitHub reports merge conflict".to_string(),
                },
            },
            MergeStateStatus::Unknown => TrainAction::Block {
                reason: BlockReason::Unknown,
            },
            MergeStateStatus::Draft => TrainAction::Block {
                reason: BlockReason::Draft,
            },
            MergeStateStatus::HasHooks => TrainAction::Abort {
                reason: AbortReason::MergeHooksEnabled,
            },
        }
    }

    /// Computes the descendant set for entering the Preparing phase.
    ///
    /// This captures the "frozen" set of descendants that will be processed
    /// through all subsequent phases. Descendants added after this point
    /// are NOT included (late additions).
    pub fn compute_frozen_descendants(
        &self,
        current_pr: PrNumber,
        prs: &HashMap<PrNumber, CachedPr>,
    ) -> Vec<PrNumber> {
        let descendants_index = build_descendants_index(prs);

        descendants_index
            .get(&current_pr)
            .map(|descendants| {
                descendants
                    .iter()
                    .filter(|pr| prs.get(pr).map(|p| p.state.is_open()).unwrap_or(false))
                    .copied()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Computes the initial phase when starting to process a PR.
    ///
    /// If there are descendants, we start with `Preparing`. Otherwise,
    /// we go directly to `SquashPending`.
    pub fn compute_initial_phase(&self, descendants: Vec<PrNumber>) -> CascadePhase {
        crate::state::transitions::start_preparing(descendants)
    }

    /// Applies an abort to a train.
    pub fn abort_train(&self, mut train: TrainRecord, reason: AbortReason) -> TrainRecord {
        train.abort(TrainError::new(reason.error_type(), reason.description()));
        train
    }

    /// Creates the outcome for the current cascade step.
    ///
    /// CRITICAL: Uses frozen progress from the current phase when available.
    /// This prevents late additions from leaking into outcomes. Only compute
    /// descendants fresh when in Idle phase (no frozen set yet).
    pub fn create_step_outcome(
        &self,
        train: &TrainRecord,
        prs: &HashMap<PrNumber, CachedPr>,
    ) -> CascadeStepOutcome {
        // Use frozen descendants from the current phase if available.
        // This ensures late additions don't leak into outcomes.
        let descendants: Vec<PrNumber> = match train.cascade_phase.progress() {
            Some(progress) => {
                // Use the frozen set, filtered by what's completed (remaining)
                progress.remaining().copied().collect()
            }
            None => {
                // Idle phase - no frozen set yet, compute fresh
                self.compute_frozen_descendants(train.current_pr, prs)
            }
        };

        match descendants.len() {
            0 => CascadeStepOutcome::Complete,
            1 => CascadeStepOutcome::WaitingOnCi {
                pr_number: descendants[0],
            },
            _ => CascadeStepOutcome::FanOut { descendants },
        }
    }
}

/// Action to take for a train after evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrainAction {
    /// Nothing to do (train not active or waiting).
    Idle,

    /// Train can proceed with the cascade.
    Proceed,

    /// Train is blocked waiting for something.
    Block { reason: BlockReason },

    /// Train must be aborted due to an error.
    Abort { reason: AbortReason },

    /// The root branch needs to be updated (BEHIND status).
    UpdateBranch,

    /// The PR was merged externally; advance the train.
    AdvanceAfterExternalMerge { merge_sha: Sha },
}

/// Format the initial status comment when starting a train.
///
/// CRITICAL: Includes machine-readable JSON payload in HTML comment for
/// GitHub-based recovery. See DESIGN.md "Status comments" section.
fn format_start_comment(train: &TrainRecord, stack: &MergeStack) -> String {
    // Serialize train state as JSON for recovery
    let json_payload = format_train_json(train);

    let mut lines = vec![
        format!("<!-- merge-train-state\n{}\n-->", json_payload),
        "## Merge Train Status".to_string(),
        String::new(),
        "**Status:** Running".to_string(),
        format!("**Root PR:** #{}", train.original_root_pr),
        format!("**Current PR:** #{}", train.current_pr),
        format!("**Phase:** {}", train.cascade_phase.name()),
    ];

    if stack.len() > 1 {
        lines.push(String::new());
        lines.push("**Stack:**".to_string());
        for (i, pr) in stack.prs.iter().enumerate() {
            let marker = if *pr == train.current_pr { "→" } else { " " };
            lines.push(format!("{} {}. #{}", marker, i + 1, pr));
        }
    }

    lines.push(String::new());
    lines.push("---".to_string());
    lines.push("*Use `@merge-train stop` to cancel.*".to_string());

    lines.join("\n")
}

/// Format the status comment when a train is stopped.
///
/// CRITICAL: Includes machine-readable JSON payload in HTML comment for
/// GitHub-based recovery. See DESIGN.md "Status comments" section.
fn format_stop_comment(train: &TrainRecord) -> String {
    // Serialize train state as JSON for recovery
    let json_payload = format_train_json(train);

    [
        format!("<!-- merge-train-state\n{}\n-->", json_payload),
        "## Merge Train Status".to_string(),
        String::new(),
        "**Status:** Stopped".to_string(),
        format!("**Root PR:** #{}", train.original_root_pr),
        String::new(),
        "The merge train was stopped by user request.".to_string(),
        String::new(),
        "---".to_string(),
        "*Use `@merge-train start` to restart.*".to_string(),
    ]
    .join("\n")
}

/// Format the status comment for a phase update.
///
/// CRITICAL: Includes machine-readable JSON payload in HTML comment for
/// GitHub-based recovery. This should be called when transitioning between
/// phases to keep the recovery state in sync.
pub fn format_phase_comment(train: &TrainRecord) -> String {
    let json_payload = format_train_json(train);

    let phase_name = match &train.cascade_phase {
        CascadePhase::Idle => "Idle",
        CascadePhase::Preparing { .. } => "Preparing",
        CascadePhase::SquashPending { .. } => "Squash Pending",
        CascadePhase::Reconciling { .. } => "Reconciling",
        CascadePhase::CatchingUp { .. } => "Catching Up",
        CascadePhase::Retargeting { .. } => "Retargeting",
    };

    let progress_info = train
        .cascade_phase
        .progress()
        .map_or_else(String::new, |p| {
            let completed = p.completed.len();
            let total = p.frozen_descendants.len();
            format!(" ({}/{} descendants)", completed, total)
        });

    [
        format!("<!-- merge-train-state\n{}\n-->", json_payload),
        "## Merge Train Status".to_string(),
        String::new(),
        format!("**Status:** Running ({}{})", phase_name, progress_info),
        format!("**Current PR:** #{}", train.current_pr),
        format!("**Root PR:** #{}", train.original_root_pr),
        String::new(),
        "---".to_string(),
        "*Use `@merge-train stop` to stop the train.*".to_string(),
    ]
    .join("\n")
}

/// Format train record as JSON for status comment recovery payload.
///
/// This produces the machine-readable JSON that enables GitHub-based recovery
/// when local state is lost. The format matches DESIGN.md specifications.
fn format_train_json(train: &TrainRecord) -> String {
    // Use serde to serialize the train record, which produces the correct
    // structure including cascade_phase with frozen_descendants
    serde_json::to_string_pretty(train).unwrap_or_else(|e| {
        // This should never fail for a valid TrainRecord, but handle it gracefully
        format!("{{\"error\": \"serialization failed: {}\"}}", e)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PrState, Sha, TrainState};

    fn make_sha(n: u64) -> Sha {
        Sha::parse(format!("{:0>40x}", n)).unwrap()
    }

    fn make_pr(
        number: u64,
        base_ref: &str,
        predecessor: Option<u64>,
        state: PrState,
        merge_state_status: MergeStateStatus,
    ) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            make_sha(number),
            format!("branch-{}", number),
            base_ref.to_string(),
            predecessor.map(PrNumber),
            state,
            merge_state_status,
            false,
        )
    }

    fn make_open_pr(number: u64, base_ref: &str, predecessor: Option<u64>) -> CachedPr {
        make_pr(
            number,
            base_ref,
            predecessor,
            PrState::Open,
            MergeStateStatus::Clean,
        )
    }

    #[test]
    fn start_train_succeeds_for_valid_root() {
        let engine = CascadeEngine::new("main");

        let pr = make_open_pr(1, "main", None);
        let prs = HashMap::from([(PrNumber(1), pr)]);
        let active_trains = HashMap::new();

        let result = engine.start_train(PrNumber(1), &prs, &active_trains);

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.train.original_root_pr, PrNumber(1));
        assert_eq!(result.train.current_pr, PrNumber(1));
        assert_eq!(result.train.state, TrainState::Running);
        assert_eq!(result.stack.root(), Some(PrNumber(1)));
    }

    #[test]
    fn start_train_fails_for_non_root() {
        let engine = CascadeEngine::new("main");

        // PR targets feature branch, not main
        let pr = make_open_pr(1, "feature", None);
        let prs = HashMap::from([(PrNumber(1), pr)]);
        let active_trains = HashMap::new();

        let result = engine.start_train(PrNumber(1), &prs, &active_trains);

        assert!(matches!(result, Err(CascadeError::NotARoot(_, _))));
    }

    #[test]
    fn start_train_fails_for_existing_train() {
        let engine = CascadeEngine::new("main");

        let pr = make_open_pr(1, "main", None);
        let prs = HashMap::from([(PrNumber(1), pr)]);

        let existing_train = TrainRecord::new(PrNumber(1));
        let active_trains = HashMap::from([(PrNumber(1), existing_train)]);

        let result = engine.start_train(PrNumber(1), &prs, &active_trains);

        assert!(matches!(result, Err(CascadeError::TrainAlreadyExists(_))));
    }

    #[test]
    fn start_train_includes_descendants_in_stack() {
        let engine = CascadeEngine::new("main");

        // main <- #1 <- #2 <- #3
        let pr1 = make_open_pr(1, "main", None);
        let mut pr2 = make_open_pr(2, "branch-1", Some(1));
        pr2.head_ref = "branch-2".to_string();
        let mut pr3 = make_open_pr(3, "branch-2", Some(2));
        pr3.head_ref = "branch-3".to_string();

        let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2), (PrNumber(3), pr3)]);
        let active_trains = HashMap::new();

        let result = engine.start_train(PrNumber(1), &prs, &active_trains);

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(
            result.stack.prs,
            vec![PrNumber(1), PrNumber(2), PrNumber(3)]
        );
    }

    #[test]
    fn stop_train_succeeds_for_active_train() {
        let engine = CascadeEngine::new("main");

        let train = TrainRecord::new(PrNumber(1));
        let result = engine.stop_train(train, false);

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.train.state, TrainState::Stopped);
        assert!(result.train.ended_at.is_some());
    }

    #[test]
    fn stop_train_fails_for_inactive_train_without_force() {
        let engine = CascadeEngine::new("main");

        let mut train = TrainRecord::new(PrNumber(1));
        train.stop();

        let result = engine.stop_train(train, false);

        assert!(matches!(result, Err(CascadeError::InvalidState(_, _, _))));
    }

    #[test]
    fn stop_train_succeeds_for_inactive_train_with_force() {
        let engine = CascadeEngine::new("main");

        let mut train = TrainRecord::new(PrNumber(1));
        train.stop();

        let result = engine.stop_train(train, true);

        assert!(result.is_ok());
    }

    #[test]
    fn evaluate_train_returns_proceed_for_clean_pr() {
        let engine = CascadeEngine::new("main");

        let train = TrainRecord::new(PrNumber(1));
        let pr = make_open_pr(1, "main", None);
        let prs = HashMap::from([(PrNumber(1), pr)]);

        let action = engine.evaluate_train(&train, &prs);

        assert_eq!(action, TrainAction::Proceed);
    }

    #[test]
    fn evaluate_train_returns_block_for_blocked_pr() {
        let engine = CascadeEngine::new("main");

        let train = TrainRecord::new(PrNumber(1));
        let pr = make_pr(1, "main", None, PrState::Open, MergeStateStatus::Blocked);
        let prs = HashMap::from([(PrNumber(1), pr)]);

        let action = engine.evaluate_train(&train, &prs);

        assert!(matches!(
            action,
            TrainAction::Block {
                reason: BlockReason::Blocked
            }
        ));
    }

    #[test]
    fn evaluate_train_returns_abort_for_closed_pr() {
        let engine = CascadeEngine::new("main");

        let train = TrainRecord::new(PrNumber(1));
        let pr = make_pr(1, "main", None, PrState::Closed, MergeStateStatus::Clean);
        let prs = HashMap::from([(PrNumber(1), pr)]);

        let action = engine.evaluate_train(&train, &prs);

        assert!(matches!(
            action,
            TrainAction::Abort {
                reason: AbortReason::PrClosed
            }
        ));
    }

    #[test]
    fn evaluate_train_returns_abort_for_dirty_pr() {
        let engine = CascadeEngine::new("main");

        let train = TrainRecord::new(PrNumber(1));
        let pr = make_pr(1, "main", None, PrState::Open, MergeStateStatus::Dirty);
        let prs = HashMap::from([(PrNumber(1), pr)]);

        let action = engine.evaluate_train(&train, &prs);

        assert!(matches!(
            action,
            TrainAction::Abort {
                reason: AbortReason::MergeConflict { .. }
            }
        ));
    }

    #[test]
    fn evaluate_train_returns_update_branch_for_behind_root() {
        let engine = CascadeEngine::new("main");

        let train = TrainRecord::new(PrNumber(1));
        let pr = make_pr(1, "main", None, PrState::Open, MergeStateStatus::Behind);
        let prs = HashMap::from([(PrNumber(1), pr)]);

        let action = engine.evaluate_train(&train, &prs);

        assert_eq!(action, TrainAction::UpdateBranch);
    }

    #[test]
    fn compute_frozen_descendants_returns_open_descendants() {
        let engine = CascadeEngine::new("main");

        let pr1 = make_open_pr(1, "main", None);
        let mut pr2 = make_open_pr(2, "branch-1", Some(1));
        pr2.head_ref = "branch-2".to_string();
        // pr3 is closed, should not be included
        let mut pr3 = make_pr(
            3,
            "branch-1",
            Some(1),
            PrState::Closed,
            MergeStateStatus::Clean,
        );
        pr3.head_ref = "branch-3".to_string();

        let prs = HashMap::from([(PrNumber(1), pr1), (PrNumber(2), pr2), (PrNumber(3), pr3)]);

        let descendants = engine.compute_frozen_descendants(PrNumber(1), &prs);

        assert_eq!(descendants.len(), 1);
        assert!(descendants.contains(&PrNumber(2)));
        assert!(!descendants.contains(&PrNumber(3)));
    }

    #[test]
    fn compute_initial_phase_with_descendants() {
        let engine = CascadeEngine::new("main");

        let phase = engine.compute_initial_phase(vec![PrNumber(2), PrNumber(3)]);

        assert!(matches!(phase, CascadePhase::Preparing { .. }));
        if let CascadePhase::Preparing { progress } = phase {
            assert_eq!(progress.frozen_descendants, vec![PrNumber(2), PrNumber(3)]);
        }
    }

    #[test]
    fn compute_initial_phase_without_descendants() {
        let engine = CascadeEngine::new("main");

        let phase = engine.compute_initial_phase(vec![]);

        assert!(matches!(phase, CascadePhase::SquashPending { .. }));
    }

    mod property_tests {
        use super::*;
        use proptest::prelude::*;

        fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
            (1u64..100).prop_map(PrNumber)
        }

        fn arb_sha() -> impl Strategy<Value = Sha> {
            "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
        }

        /// Generate a vector of unique PR numbers (no duplicates).
        fn arb_unique_prs(min: usize, max: usize) -> impl Strategy<Value = Vec<PrNumber>> {
            prop::collection::hash_set(arb_pr_number(), min..max)
                .prop_map(|set| set.into_iter().collect())
        }

        fn make_test_pr(number: u64, base_ref: &str, predecessor: Option<PrNumber>) -> CachedPr {
            CachedPr::new(
                PrNumber(number),
                Sha::parse(format!("{:0>40x}", number)).unwrap(),
                format!("branch-{}", number),
                base_ref.to_string(),
                predecessor,
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            )
        }

        proptest! {
            /// Property: No PR appears in multiple active trains.
            ///
            /// This invariant ensures that overlapping trains are never created.
            /// For any set of active trains, the union of (current_pr ∪ frozen_descendants)
            /// across all trains must have no duplicates.
            ///
            /// This property would have caught the bug where start_train only checked
            /// if the root PR had an existing train, not if any descendant was already
            /// part of another train.
            #[test]
            fn no_overlapping_trains_invariant(
                train_roots in arb_unique_prs(1, 4),
                extra_descendants in arb_unique_prs(0, 6),
            ) {
                let engine = CascadeEngine::new("main");

                // Build a set of PRs where each root has a subset of descendants
                let mut prs = HashMap::new();
                let mut active_trains: HashMap<PrNumber, TrainRecord> = HashMap::new();

                // Create root PRs targeting main
                for (i, &root) in train_roots.iter().enumerate() {
                    let pr = make_test_pr(root.0, "main", None);
                    prs.insert(root, pr);

                    // Only start trains for odd-indexed roots to test mixed state
                    if i % 2 == 0 {
                        // Attempt to start a train
                        if let Ok(result) = engine.start_train(root, &prs, &active_trains) {
                            active_trains.insert(root, result.train);
                        }
                    }
                }

                // Try adding descendants to existing trains (simulating cascades with descendants)
                for (i, &desc) in extra_descendants.iter().enumerate() {
                    // Skip if this PR number conflicts with a root
                    if train_roots.contains(&desc) {
                        continue;
                    }

                    // Pick a "parent" from the roots
                    let parent_idx = i % train_roots.len();
                    let parent = train_roots[parent_idx];
                    let parent_head = format!("branch-{}", parent.0);

                    let pr = make_test_pr(desc.0, &parent_head, Some(parent));
                    prs.insert(desc, pr);
                }

                // Now verify the invariant: no PR appears in multiple trains
                let mut seen_prs: std::collections::HashSet<PrNumber> = std::collections::HashSet::new();
                let mut duplicates: Vec<PrNumber> = Vec::new();

                for (_, train) in &active_trains {
                    // Check current_pr
                    if seen_prs.contains(&train.current_pr) {
                        duplicates.push(train.current_pr);
                    }
                    seen_prs.insert(train.current_pr);

                    // Check frozen_descendants
                    if let Some(progress) = train.cascade_phase.progress() {
                        for &desc in &progress.frozen_descendants {
                            if seen_prs.contains(&desc) {
                                duplicates.push(desc);
                            }
                            seen_prs.insert(desc);
                        }
                    }
                }

                prop_assert!(
                    duplicates.is_empty(),
                    "Found PRs appearing in multiple trains: {:?}",
                    duplicates
                );
            }

            /// Property: stop_train always emits a worktree removal effect.
            ///
            /// When stopping a train, the worktree must be cleaned up to avoid
            /// leaving stale worktrees on disk. This property verifies that
            /// stop_train always includes a RemoveWorktree effect.
            #[test]
            fn stop_train_emits_worktree_removal(
                root_pr in arb_pr_number(),
                has_status_comment in any::<bool>(),
            ) {
                let engine = CascadeEngine::new("main");

                let mut train = TrainRecord::new(root_pr);
                if has_status_comment {
                    train.status_comment_id = Some(crate::types::CommentId(12345));
                }

                let result = engine.stop_train(train.clone(), false);

                prop_assert!(result.is_ok(), "stop_train should succeed for active train");

                let stop_result = result.unwrap();

                // Check that RemoveWorktree effect is present
                let has_worktree_removal = stop_result.effects.iter().any(|effect| {
                    matches!(effect, Effect::Git(GitEffect::RemoveWorktree { name })
                        if name == &format!("stack-{}", root_pr))
                });

                prop_assert!(
                    has_worktree_removal,
                    "stop_train must emit RemoveWorktree effect for stack-{}. Effects: {:?}",
                    root_pr,
                    stop_result.effects
                );

                // If there's a status comment, also verify UpdateComment is emitted
                if has_status_comment {
                    let has_comment_update = stop_result.effects.iter().any(|effect| {
                        matches!(effect, Effect::GitHub(GitHubEffect::UpdateComment { .. }))
                    });

                    prop_assert!(
                        has_comment_update,
                        "stop_train must emit UpdateComment when status_comment_id is set"
                    );
                }
            }

            /// Property: Starting a train for a PR that's a descendant in another train fails.
            ///
            /// This directly tests the fix for overlapping trains - you cannot start
            /// a new train if any PR in your stack is already part of an active train.
            #[test]
            fn start_train_rejects_overlapping_descendants(
                sha in arb_sha(),
            ) {
                let engine = CascadeEngine::new("main");

                // Create a chain: main <- #1 <- #2 <- #3
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

                let pr2 = CachedPr::new(
                    PrNumber(2),
                    sha.clone(),
                    "branch-2".to_string(),
                    "branch-1".to_string(),
                    Some(PrNumber(1)),
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );

                // PR3 targets main directly (would be a root), but claims PR2 as predecessor
                // This simulates a fan-out scenario where #3 was retargeted
                let pr3 = CachedPr::new(
                    PrNumber(3),
                    sha.clone(),
                    "branch-3".to_string(),
                    "main".to_string(),
                    None,
                    PrState::Open,
                    MergeStateStatus::Clean,
                    false,
                );

                let prs = HashMap::from([
                    (PrNumber(1), pr1),
                    (PrNumber(2), pr2),
                    (PrNumber(3), pr3),
                ]);

                // Start train for #1 (which includes #2 as descendant)
                let mut active_trains = HashMap::new();
                let result1 = engine.start_train(PrNumber(1), &prs, &active_trains);
                prop_assert!(result1.is_ok(), "Should be able to start train for #1");
                active_trains.insert(PrNumber(1), result1.unwrap().train);

                // Now try to start train for #3 - this should succeed since #3 doesn't
                // overlap with train #1's descendants
                let result3 = engine.start_train(PrNumber(3), &prs, &active_trains);
                prop_assert!(
                    result3.is_ok(),
                    "Should be able to start independent train for #3: {:?}",
                    result3.err()
                );

                // But trying to start train for #2 (which is a descendant of #1) should fail
                // because #2 is already in train #1's frozen_descendants
                // Note: #2 isn't a root so this will fail with NotARoot, which is fine
                let result2 = engine.start_train(PrNumber(2), &prs, &active_trains);
                prop_assert!(
                    result2.is_err(),
                    "Starting train for descendant #2 should fail"
                );
            }
        }
    }
}
