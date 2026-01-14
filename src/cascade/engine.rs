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
    PrNumber, Sha, TrainError, TrainRecord, TrainState,
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

    /// External merge occurred before preparation completed.
    /// This violates the "prepare before squash" invariant and can drop content.
    #[error("External merge with {} unprepared descendant(s): {:?}", unprepared_descendants.len(), unprepared_descendants)]
    PreparationIncomplete {
        unprepared_descendants: Vec<PrNumber>,
    },

    /// Status comment size limit exceeded after truncation.
    ///
    /// Per DESIGN.md: "If STILL too large after aggressive truncation, this indicates
    /// a bug in the size estimation (the 50 PR limit with truncation should always fit).
    /// The bot MUST NOT post a minimal comment without JSON, as this would silently
    /// disable GitHub-based recovery."
    #[error(
        "Status comment size ({actual_size} bytes) exceeded {max_size} byte limit. This is a bug — please report it. Train aborted to prevent recovery data loss."
    )]
    StatusCommentOversize { actual_size: usize, max_size: usize },

    /// Failed to serialize train record to JSON.
    ///
    /// This should never happen for a valid TrainRecord, but if it does,
    /// we propagate the error rather than producing invalid JSON that would
    /// break GitHub-based recovery.
    #[error("Failed to serialize train record: {0}")]
    SerializationFailed(String),
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

        // Check stack size using ALL transitive descendants, not just linear stack.
        // This correctly handles fan-out trees where a root may have many direct descendants.
        // For example: main <- #1 <- {#2, #3, #4, ...} would have stack.len() = 1 but
        // could have 50+ transitive descendants that all need processing.
        let descendants_index = build_descendants_index(prs);
        let all_descendants =
            crate::state::descendants::collect_all_descendants(root_pr, &descendants_index, prs);
        let total_train_size = all_descendants.len() + 1; // +1 for root PR

        if total_train_size > MAX_TRAIN_SIZE {
            return Err(CascadeError::TrainTooLarge(
                total_train_size,
                MAX_TRAIN_SIZE,
            ));
        }

        // Check for overlapping trains: ensure no PR in this train (root + all transitive
        // descendants) is already part of another active train. This is critical for fan-out
        // scenarios where stack.prs might only contain the linear portion while transitive
        // descendants include all fan-out branches.
        //
        // For example: main <- #1 <- {#2, #3, #4} - if train for #1 is active with frozen
        // descendants [#2, #3, #4], we must prevent starting a new train whose descendants
        // overlap with any of these.
        //
        // IMPORTANT: For trains in Idle phase, progress() returns None so frozen_descendants
        // is not available. We must compute the *potential* descendants of Idle trains
        // dynamically to prevent overlap.
        let prs_in_new_train: Vec<PrNumber> = std::iter::once(root_pr)
            .chain(all_descendants.iter().copied())
            .collect();

        for pr_in_train in &prs_in_new_train {
            for (train_root, train) in active_trains {
                // Check if this PR is the current PR being processed by another train
                if train.current_pr == *pr_in_train {
                    return Err(CascadeError::NotARoot(
                        root_pr,
                        format!(
                            "PR #{} is currently being processed by train #{}",
                            pr_in_train, train_root
                        ),
                    ));
                }

                // Check against the train's descendants.
                // For non-Idle phases: use frozen_descendants from progress()
                // For Idle phase: compute potential descendants dynamically
                let is_in_train_descendants = match train.cascade_phase.progress() {
                    Some(progress) => progress.frozen_descendants.contains(pr_in_train),
                    None => {
                        // Idle phase: compute potential descendants from current_pr.
                        // This prevents race where a new train could overlap with an Idle
                        // train's soon-to-be-frozen descendants.
                        let potential_descendants =
                            crate::state::descendants::collect_all_descendants(
                                train.current_pr,
                                &descendants_index,
                                prs,
                            );
                        potential_descendants.contains(pr_in_train)
                    }
                };

                if is_in_train_descendants {
                    return Err(CascadeError::NotARoot(
                        root_pr,
                        format!(
                            "PR #{} is a descendant in active train #{}",
                            pr_in_train, train_root
                        ),
                    ));
                }
            }
        }

        // Create the train record
        let train = TrainRecord::new(root_pr);

        // Generate effects for starting the train
        let effects = vec![
            // Create worktree for this stack.
            // DESIGN.md: "Each stack has its own isolated worktree"
            // The worktree name is keyed by original_root_pr for stability during retargeting.
            Effect::Git(GitEffect::CreateWorktree {
                name: format!("stack-{}", root_pr),
            }),
            // Post initial status comment
            Effect::GitHub(GitHubEffect::PostComment {
                pr: root_pr,
                body: format_start_comment(&train, &stack)?,
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
                body: format_stop_comment(&train)?,
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

        // Check if PR is still open.
        // In post-squash phases (Reconciling, CatchingUp, Retargeting), the current PR
        // being merged is expected - the cascade performed the squash and we're now
        // processing descendants. Only treat merged as "external" in pre-squash phases.
        if !current_pr.state.is_open() {
            let is_post_squash = matches!(
                train.cascade_phase,
                CascadePhase::Reconciling { .. }
                    | CascadePhase::CatchingUp { .. }
                    | CascadePhase::Retargeting { .. }
            );

            if current_pr.state.is_merged() {
                if is_post_squash {
                    // Expected state - proceed with descendant processing
                    return TrainAction::Proceed;
                }

                // Pre-squash phases: external merge needs special handling.
                // If we're in Preparing phase with unprepared descendants, this violates
                // the "prepare before squash" invariant and we must abort.
                //
                // Filter to open PRs only: closed/missing descendants would be skipped
                // anyway (per DESIGN.md "Descendant PR closed during cascade" -> skip).
                // Without this filter, we'd incorrectly abort if a descendant closed
                // between the freeze point and when we mark it skipped.
                if let Some(progress) = train.cascade_phase.progress() {
                    let unprepared: Vec<PrNumber> = progress
                        .remaining()
                        .filter(|pr_num| {
                            prs.get(pr_num)
                                .map(|pr| pr.state.is_open())
                                .unwrap_or(false)
                        })
                        .copied()
                        .collect();
                    if !unprepared.is_empty() {
                        return TrainAction::Abort {
                            reason: AbortReason::PreparationIncomplete {
                                unprepared_descendants: unprepared,
                            },
                        };
                    }
                }

                // Either Idle (no progress) or Preparing with all descendants complete.
                // merge_commit_sha should always be present for merged PRs. If missing,
                // it indicates corrupted cached data - abort instead of panicking.
                //
                // IMPORTANT: The handler for AdvanceAfterExternalMerge MUST verify
                // preparation before reconciliation per DESIGN.md §"Verifying
                // preparation before reconciliation". For each descendant, use
                // `git merge-base --is-ancestor <predecessor_head> <descendant_head>`
                // to confirm the predecessor's content was merged in. If not, abort
                // with an error requiring manual intervention. This check catches
                // cases where someone manually merged the PR via GitHub UI before
                // preparation completed.
                return match current_pr.state.merge_commit_sha() {
                    Some(sha) => TrainAction::AdvanceAfterExternalMerge {
                        merge_sha: sha.clone(),
                    },
                    None => TrainAction::Abort {
                        reason: AbortReason::InternalInvariantViolation {
                            details: format!(
                                "Merged PR #{} has no merge_commit_sha in cached data",
                                train.current_pr
                            ),
                        },
                    },
                };
            }
            // PR closed without merge
            if is_post_squash {
                // Shouldn't happen in post-squash phases, but let phase execution handle it
                return TrainAction::Proceed;
            }
            return TrainAction::Abort {
                reason: AbortReason::PrClosed,
            };
        }

        // Check if PR is a draft. GitHub's mergeStateStatus can be CLEAN for a draft
        // PR (if CI passes), but the merge API will reject it. Check is_draft explicitly.
        if current_pr.is_draft {
            return TrainAction::Block {
                reason: BlockReason::Draft,
            };
        }

        // During Reconciling/CatchingUp/Retargeting phases, the descendant's base branch
        // no longer exists (deleted after predecessor's squash-merge). GitHub's
        // mergeStateStatus is unreliable in this state (may report BEHIND, BLOCKED,
        // or UNKNOWN). We ignore it and proceed - actual conflicts will be detected
        // when we attempt the git operations.
        //
        // Per DESIGN.md: "Ignore mergeStateStatus entirely during these phases —
        // it's meaningless when the base branch doesn't exist"
        let ignore_merge_status = matches!(
            train.cascade_phase,
            CascadePhase::Reconciling { .. }
                | CascadePhase::CatchingUp { .. }
                | CascadePhase::Retargeting { .. }
        );
        if ignore_merge_status {
            return TrainAction::Proceed;
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
                // TODO: BEHIND roots (Idle phase, targets default branch) should be updated
                // by merging main and pushing, rather than blocked. This requires implementing
                // merge+push effects in the cascade step machinery. For now, treat all BEHIND
                // PRs the same way.
                TrainAction::Block {
                    reason: BlockReason::Behind,
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
    ///
    /// Returns ALL transitive descendants (not just direct), filtered to open PRs.
    /// This aligns with `start_train`'s use of `collect_all_descendants` for size
    /// checking and overlap detection.
    pub fn compute_frozen_descendants(
        &self,
        current_pr: PrNumber,
        prs: &HashMap<PrNumber, CachedPr>,
    ) -> Vec<PrNumber> {
        let descendants_index = build_descendants_index(prs);

        // Use collect_all_descendants for transitive closure, not just direct descendants.
        // This function already filters to open PRs only.
        crate::state::descendants::collect_all_descendants(current_pr, &descendants_index, prs)
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
    /// Uses frozen progress from the current phase when available. This prevents
    /// late additions from leaking into outcomes. Only computes descendants fresh
    /// when in Idle phase (no frozen set yet).
    ///
    /// Note: This filters out closed/missing descendants from the outcome, even if
    /// they're still in `progress.remaining()`. Callers should use
    /// `mark_closed_descendants_skipped()` from the phases module to properly update
    /// the progress tracking.
    pub fn create_step_outcome(
        &self,
        train: &TrainRecord,
        prs: &HashMap<PrNumber, CachedPr>,
    ) -> CascadeStepOutcome {
        // Use frozen descendants from the current phase if available.
        // This ensures late additions don't leak into outcomes.
        let descendants: Vec<PrNumber> = match train.cascade_phase.progress() {
            Some(progress) => {
                // Use the frozen set, filtered by what's completed (remaining).
                // Additionally filter out any PRs that are closed or missing from
                // the cache, as they can't be processed and shouldn't appear in outcomes.
                progress
                    .remaining()
                    .filter(|pr_num| {
                        prs.get(pr_num)
                            .map(|pr| pr.state.is_open())
                            .unwrap_or(false)
                    })
                    .copied()
                    .collect()
            }
            None => {
                // Idle phase - no frozen set yet, compute fresh
                // compute_frozen_descendants already filters to open PRs only
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

    /// The PR was merged externally; advance the train.
    AdvanceAfterExternalMerge { merge_sha: Sha },
}

/// Format the initial status comment when starting a train.
///
/// Includes machine-readable JSON payload in HTML comment for GitHub-based
/// recovery. See DESIGN.md "Status comments" section.
///
/// Returns an error if the status comment would exceed size limits, which
/// should cause the caller to abort the train.
fn format_start_comment(train: &TrainRecord, stack: &MergeStack) -> Result<String, CascadeError> {
    // Serialize train state as JSON for recovery
    let json_payload = format_train_json(train)?;

    let mut lines = vec![
        format!(
            "<!-- merge-train-state\n{}\n-->",
            escape_for_html_comment(&json_payload)
        ),
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

    let body = lines.join("\n");
    check_final_comment_size(&body)?;
    Ok(body)
}

/// Format the status comment when a train is stopped.
///
/// Includes machine-readable JSON payload in HTML comment for GitHub-based
/// recovery. See DESIGN.md "Status comments" section.
///
/// Returns an error if the status comment would exceed size limits, which
/// should cause the caller to abort the train.
fn format_stop_comment(train: &TrainRecord) -> Result<String, CascadeError> {
    // Serialize train state as JSON for recovery
    let json_payload = format_train_json(train)?;

    let body = [
        format!(
            "<!-- merge-train-state\n{}\n-->",
            escape_for_html_comment(&json_payload)
        ),
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
    .join("\n");
    check_final_comment_size(&body)?;
    Ok(body)
}

/// Format the status comment for a phase update.
///
/// Includes machine-readable JSON payload in HTML comment for GitHub-based
/// recovery. Called when transitioning between phases to keep the recovery
/// state in sync.
///
/// Returns an error if the status comment would exceed size limits. Per DESIGN.md,
/// callers MUST abort the train in this case to prevent recovery data loss.
pub fn format_phase_comment(train: &TrainRecord) -> Result<String, CascadeError> {
    let json_payload = format_train_json(train)?;

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

    // Derive status text from train.state for consistency with JSON payload.
    // Previously this hardcoded "Running" even when the train was aborted.
    let status_text = match train.state {
        TrainState::Running => format!("Running ({}{})", phase_name, progress_info),
        TrainState::WaitingCi => format!("Waiting for CI ({}{})", phase_name, progress_info),
        TrainState::Stopped => "Stopped".to_string(),
        TrainState::Aborted => train
            .error
            .as_ref()
            .map(|e| format!("Aborted: {}", e.error_type))
            .unwrap_or_else(|| "Aborted".to_string()),
        TrainState::NeedsManualReview => "Needs Manual Review".to_string(),
    };

    let body = [
        format!(
            "<!-- merge-train-state\n{}\n-->",
            escape_for_html_comment(&json_payload)
        ),
        "## Merge Train Status".to_string(),
        String::new(),
        format!("**Status:** {}", status_text),
        format!("**Current PR:** #{}", train.current_pr),
        format!("**Root PR:** #{}", train.original_root_pr),
        String::new(),
        "---".to_string(),
        "*Use `@merge-train stop` to stop the train.*".to_string(),
    ]
    .join("\n");
    check_final_comment_size(&body)?;
    Ok(body)
}

/// Format train record as JSON for status comment recovery payload.
///
/// This produces the machine-readable JSON that enables GitHub-based recovery
/// when local state is lost. The format matches DESIGN.md specifications.
/// Maximum error message size in JSON (4KB per DESIGN.md)
const MAX_ERROR_MESSAGE_SIZE: usize = 4 * 1024;

/// Maximum error stderr size in JSON (2KB per DESIGN.md)
const MAX_ERROR_STDERR_SIZE: usize = 2 * 1024;

/// Maximum status comment JSON size (60KB per DESIGN.md)
const MAX_STATUS_COMMENT_SIZE: usize = 60 * 1024;

/// GitHub's hard limit for comment body size (65536 bytes)
const GITHUB_COMMENT_SIZE_LIMIT: usize = 65536;

/// Truncate a string to a maximum length, adding "..." if truncated.
///
/// Handles UTF-8 correctly by finding a valid character boundary.
///
/// # Returns
///
/// - If `s.len() <= max_len`, returns `s` unchanged
/// - Otherwise, returns a truncated string with "..." suffix that is at most
///   `max_len` bytes (for `max_len >= 3`) or exactly 3 bytes (for `max_len < 3`)
///
/// # Edge case
///
/// When `max_len < 3` and truncation is needed, the result is `"..."` (3 bytes)
/// regardless of `max_len`. Callers requiring strict size limits should ensure
/// `max_len >= 3`.
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else if max_len < 3 {
        // Edge case: max_len is too small to fit any content plus "..."
        // Return just the ellipsis (minimum truncation indicator)
        "...".to_string()
    } else {
        // Reserve space for "..." suffix
        let truncate_at = max_len - 3;
        // Find a valid UTF-8 character boundary at or before truncate_at.
        // This avoids panicking when truncate_at falls in the middle of a
        // multi-byte UTF-8 character.
        let safe_truncate_at = s
            .char_indices()
            .take_while(|(i, _)| *i <= truncate_at)
            .last()
            .map(|(i, _)| i)
            .unwrap_or(0);
        format!("{}...", &s[..safe_truncate_at])
    }
}

fn format_train_json(train: &TrainRecord) -> Result<String, CascadeError> {
    // Create a copy with truncated error fields to avoid exceeding size limits
    let mut train_copy = train.clone();

    // Truncate error fields per DESIGN.md limits
    if let Some(ref mut error) = train_copy.error {
        error.message = truncate_string(&error.message, MAX_ERROR_MESSAGE_SIZE);
        if let Some(ref stderr) = error.stderr {
            error.stderr = Some(truncate_string(stderr, MAX_ERROR_STDERR_SIZE));
        }
    }

    // Exclude local-only fields from status comment JSON:
    // status_comment_id is redundant (the comment already knows its own ID)
    // and not needed for GitHub-based recovery
    train_copy.status_comment_id = None;

    // Use compact serialization (not pretty) to save space
    let json = serde_json::to_string(&train_copy)
        .map_err(|e| CascadeError::SerializationFailed(e.to_string()))?;

    // Enforce size limit (60KB per DESIGN.md) in release builds.
    // Per DESIGN.md: "If STILL too large after aggressive truncation, this indicates
    // a bug in the size estimation. The bot MUST NOT post a minimal comment without
    // JSON, as this would silently disable GitHub-based recovery."
    //
    // Note: We do NOT log here (no eprintln/tracing) because this is the pure engine
    // layer. The error is propagated to the caller who can log via the imperative shell.
    if json.len() >= MAX_STATUS_COMMENT_SIZE {
        return Err(CascadeError::StatusCommentOversize {
            actual_size: json.len(),
            max_size: MAX_STATUS_COMMENT_SIZE,
        });
    }

    Ok(json)
}

/// Escape JSON for safe embedding in HTML comments.
///
/// HTML comments terminate at `-->`, so if the JSON payload contains this
/// sequence (e.g., in error messages or stderr from git), it would corrupt
/// the recovery payload. We escape `-->` as `--\u003e`, which is valid JSON
/// (unicode escape for `>`). When parsed, serde_json automatically decodes
/// `\u003e` back to `>`, so the original string is recovered.
fn escape_for_html_comment(json: &str) -> String {
    json.replace("-->", r"--\u003e")
}

/// Validate that the final comment body doesn't exceed GitHub's size limit.
///
/// This check happens AFTER escaping and markdown wrapping, ensuring the
/// complete comment body fits within GitHub's 65536 byte limit. The earlier
/// JSON size check (60KB) is a conservative pre-check; this validates the
/// final result.
fn check_final_comment_size(body: &str) -> Result<(), CascadeError> {
    if body.len() > GITHUB_COMMENT_SIZE_LIMIT {
        return Err(CascadeError::StatusCommentOversize {
            actual_size: body.len(),
            max_size: GITHUB_COMMENT_SIZE_LIMIT,
        });
    }
    Ok(())
}

// Unit tests, regression tests, and property-based bug detection tests
// are in engine_tests.rs. Only specification-level property tests remain here.
#[cfg(test)]
#[path = "engine_tests.rs"]
mod engine_tests;

/// Property-based specification tests for the cascade engine.
///
/// These tests specify key invariants that must hold for correctness:
/// - No overlapping trains (a PR cannot be in multiple active trains)
/// - Worktree cleanup on stop
/// - Overlap detection works for fan-out structures
///
/// Unit tests, regression tests, and helper function tests are in `engine_tests.rs`.
#[cfg(test)]
mod property_tests {
    use super::*;
    use crate::types::{DescendantProgress, PrState};
    use proptest::prelude::*;

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        (1u64..100).prop_map(PrNumber)
    }

    fn arb_sha() -> impl Strategy<Value = Sha> {
        "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
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
        /// This test attempts to start multiple trains that would have overlapping
        /// descendants and verifies the engine rejects them.
        #[test]
        fn no_overlapping_trains_invariant(
            root1 in 1u64..50,
            root2 in 51u64..100,
            shared_desc in 101u64..150,
        ) {
            let engine = CascadeEngine::new("main");

            // Create two roots that would both claim the same descendant:
            // main <- #root1 <- #shared_desc
            // main <- #root2 (independent root)
            //
            // Train #root1 starts first and freezes #shared_desc.
            // Then we manually set train #root1's frozen_descendants to include #shared_desc.
            // Then we create a PR structure where #root2's descendants also include #shared_desc
            // and verify start_train rejects it.

            let pr_root1 = make_test_pr(root1, "main", None);
            let pr_shared = make_test_pr(shared_desc, &format!("branch-{}", root1), Some(PrNumber(root1)));
            let pr_root2 = make_test_pr(root2, "main", None);

            // Initial state: just root1 and its descendant
            let mut prs = HashMap::from([
                (PrNumber(root1), pr_root1),
                (PrNumber(shared_desc), pr_shared),
            ]);

            let mut active_trains = HashMap::new();

            // Start train for root1
            let result1 = engine.start_train(PrNumber(root1), &prs, &active_trains);
            prop_assert!(result1.is_ok(), "Should start train for root1");
            let mut train1 = result1.unwrap().train;

            // Transition train1 to Preparing with shared_desc frozen
            train1.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(vec![PrNumber(shared_desc)]),
            };
            active_trains.insert(PrNumber(root1), train1);

            // Now add root2 and make shared_desc also a descendant of root2
            // by changing its predecessor
            prs.insert(PrNumber(root2), pr_root2);

            // Modify shared_desc to now be a descendant of root2
            let pr_shared_for_root2 = make_test_pr(shared_desc, &format!("branch-{}", root2), Some(PrNumber(root2)));
            prs.insert(PrNumber(shared_desc), pr_shared_for_root2);

            // Attempt to start train for root2 - should fail because shared_desc
            // is already in train1's frozen_descendants
            let result2 = engine.start_train(PrNumber(root2), &prs, &active_trains);
            prop_assert!(
                result2.is_err(),
                "Starting train for root2 should fail - its descendant {} is in train {}'s frozen_descendants",
                shared_desc, root1
            );

            // Verify the error mentions the overlap
            if let Err(CascadeError::NotARoot(pr, msg)) = result2 {
                prop_assert_eq!(pr, PrNumber(root2));
                prop_assert!(
                    msg.contains(&format!("#{}", shared_desc)) || msg.contains("descendant"),
                    "Error message should mention the overlapping PR: {}",
                    msg
                );
            }
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

        /// Property: Starting a train for a PR whose descendants overlap with an active train fails.
        ///
        /// This tests the overlap guard - you cannot start a new train if any PR
        /// in your descendant tree is already in an active train's frozen_descendants.
        ///
        /// Scenario:
        /// - main <- #1 <- #2 (train for #1 is active with #2 frozen)
        /// - main <- #3 <- #2 (new root #3 wants #2 as descendant)
        /// - Starting train for #3 should fail due to overlap
        #[test]
        fn start_train_rejects_overlapping_descendants(
            sha in arb_sha(),
        ) {
            let engine = CascadeEngine::new("main");

            // Phase 1: Create initial structure and start train for #1
            // main <- #1 <- #2
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

            let mut prs = HashMap::from([
                (PrNumber(1), pr1),
                (PrNumber(2), pr2),
            ]);

            // Start train for #1
            let mut active_trains = HashMap::new();
            let result1 = engine.start_train(PrNumber(1), &prs, &active_trains);
            prop_assert!(result1.is_ok(), "Should be able to start train for #1");
            let mut train1 = result1.unwrap().train;

            // Transition train #1 to Preparing phase with #2 frozen
            train1.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(vec![PrNumber(2)]),
            };
            active_trains.insert(PrNumber(1), train1);

            // Phase 2: Add a new root #3 that would have #2 as its descendant
            // main <- #3 <- #2 (we change #2's predecessor)
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
            prs.insert(PrNumber(3), pr3);

            // Modify #2 to now be a descendant of #3
            let pr2_modified = CachedPr::new(
                PrNumber(2),
                sha.clone(),
                "branch-2".to_string(),
                "branch-3".to_string(),
                Some(PrNumber(3)),
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            );
            prs.insert(PrNumber(2), pr2_modified);

            // Starting train for #3 should FAIL because #2 is in train #1's frozen_descendants
            let result3 = engine.start_train(PrNumber(3), &prs, &active_trains);
            prop_assert!(
                result3.is_err(),
                "Starting train for #3 should fail - its descendant #2 is in train #1's frozen_descendants"
            );

            // Verify the error is specifically about overlap (not some other error)
            match result3 {
                Err(CascadeError::NotARoot(pr, msg)) => {
                    prop_assert_eq!(pr, PrNumber(3));
                    prop_assert!(
                        msg.contains("#2") && msg.contains("descendant"),
                        "Error should mention that #2 is a descendant in another train: {}",
                        msg
                    );
                }
                Err(other) => {
                    prop_assert!(false, "Expected NotARoot error for overlap, got: {:?}", other);
                }
                Ok(_) => {
                    prop_assert!(false, "Should have rejected overlapping train");
                }
            }
        }

        /// Property: Fan-out overlap detection works correctly.
        ///
        /// Tests that the overlap guard checks ALL transitive descendants,
        /// not just the linear stack. This catches the bug where fan-out
        /// branches could be missed.
        ///
        /// Scenario:
        /// - main <- #1 <- {#2, #3, #4} (fan-out from #1)
        /// - Train for #1 starts with all fan-out descendants frozen
        /// - main <- #5 <- #4 (new root #5 wants #4 as descendant)
        /// - Starting train for #5 should fail due to overlap on #4
        #[test]
        fn start_train_rejects_fanout_overlap(
            sha in arb_sha(),
        ) {
            let engine = CascadeEngine::new("main");

            // Create fan-out structure: main <- #1 <- {#2, #3, #4}
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

            // Fan-out descendants all target branch-1
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

            let pr3 = CachedPr::new(
                PrNumber(3),
                sha.clone(),
                "branch-3".to_string(),
                "branch-1".to_string(),
                Some(PrNumber(1)),
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            );

            let pr4 = CachedPr::new(
                PrNumber(4),
                sha.clone(),
                "branch-4".to_string(),
                "branch-1".to_string(),
                Some(PrNumber(1)),
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            );

            let mut prs = HashMap::from([
                (PrNumber(1), pr1),
                (PrNumber(2), pr2),
                (PrNumber(3), pr3),
                (PrNumber(4), pr4),
            ]);

            // Start train for #1
            let mut active_trains = HashMap::new();
            let result1 = engine.start_train(PrNumber(1), &prs, &active_trains);
            prop_assert!(result1.is_ok(), "Should be able to start train for #1");
            let mut train1 = result1.unwrap().train;

            // Transition to Preparing with ALL fan-out descendants frozen
            train1.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3), PrNumber(4)]),
            };
            active_trains.insert(PrNumber(1), train1);

            // Add a new root #5 whose descendant is #4 (one of the fan-out PRs)
            let pr5 = CachedPr::new(
                PrNumber(5),
                sha.clone(),
                "branch-5".to_string(),
                "main".to_string(),
                None,
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            );
            prs.insert(PrNumber(5), pr5);

            // Modify #4 to be a descendant of #5
            let pr4_modified = CachedPr::new(
                PrNumber(4),
                sha.clone(),
                "branch-4".to_string(),
                "branch-5".to_string(),
                Some(PrNumber(5)),
                PrState::Open,
                MergeStateStatus::Clean,
                false,
            );
            prs.insert(PrNumber(4), pr4_modified);

            // Starting train for #5 should fail - #4 is in train #1's frozen_descendants
            let result5 = engine.start_train(PrNumber(5), &prs, &active_trains);
            prop_assert!(
                result5.is_err(),
                "Starting train for #5 should fail - its descendant #4 is in train #1's frozen_descendants (fan-out case)"
            );

            // Verify the error mentions the specific overlap
            if let Err(CascadeError::NotARoot(pr, msg)) = result5 {
                prop_assert_eq!(pr, PrNumber(5));
                prop_assert!(
                    msg.contains("#4"),
                    "Error should mention the overlapping PR #4: {}",
                    msg
                );
            }
        }
    }
}
