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
                // Pre-squash phases: external merge needs advancing
                return TrainAction::AdvanceAfterExternalMerge {
                    merge_sha: current_pr
                        .state
                        .merge_commit_sha()
                        .cloned()
                        .expect("merged PR has merge_commit_sha"),
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
    /// Uses frozen progress from the current phase when available. This prevents
    /// late additions from leaking into outcomes. Only computes descendants fresh
    /// when in Idle phase (no frozen set yet).
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

    Ok(lines.join("\n"))
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

    Ok([
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
    .join("\n"))
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

    Ok([
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
    .join("\n"))
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

/// Truncate a string to a maximum length, adding "..." if truncated.
///
/// Handles UTF-8 correctly by finding a valid character boundary. The result
/// will be at most `max_len` bytes, including the "..." suffix if truncated.
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        // Reserve space for "..." suffix
        let truncate_at = max_len.saturating_sub(3);
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
    if json.len() >= MAX_STATUS_COMMENT_SIZE {
        // Log the issue (eprintln for now; in production this would use tracing)
        eprintln!(
            "ERROR: Status comment JSON ({} bytes) exceeds {} byte limit. \
             Aborting train to prevent recovery data loss.",
            json.len(),
            MAX_STATUS_COMMENT_SIZE
        );

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
    fn evaluate_train_returns_block_for_behind_root() {
        // TODO: BEHIND roots should eventually be updated (merge main, push) instead of blocked.
        // For now, we treat them the same as non-root BEHIND PRs.
        let engine = CascadeEngine::new("main");

        let train = TrainRecord::new(PrNumber(1));
        let pr = make_pr(1, "main", None, PrState::Open, MergeStateStatus::Behind);
        let prs = HashMap::from([(PrNumber(1), pr)]);

        let action = engine.evaluate_train(&train, &prs);

        assert_eq!(
            action,
            TrainAction::Block {
                reason: BlockReason::Behind
            }
        );
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
        use crate::types::DescendantProgress;
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

                // Transition trains to Preparing phase so frozen_descendants gets populated.
                // Without this, trains stay in Idle and progress() returns None.
                for (&root, train) in active_trains.iter_mut() {
                    // Collect descendants for this root
                    let descendants: Vec<PrNumber> = extra_descendants
                        .iter()
                        .filter(|&&d| {
                            prs.get(&d)
                                .and_then(|p| p.predecessor)
                                .map(|pred| pred == root)
                                .unwrap_or(false)
                        })
                        .copied()
                        .collect();

                    if !descendants.is_empty() {
                        train.cascade_phase = CascadePhase::Preparing {
                            progress: DescendantProgress::new(descendants),
                        };
                    }
                }

                // Now verify the invariant: no PR appears in multiple trains
                let mut seen_prs: std::collections::HashSet<PrNumber> = std::collections::HashSet::new();
                let mut duplicates: Vec<PrNumber> = Vec::new();

                for train in active_trains.values() {
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
            /// This tests the overlap guard - you cannot start a new train if any PR
            /// in your stack is already part of an active train's current_pr or frozen_descendants.
            ///
            /// Scenario: Train #1 is in Preparing phase with #2 frozen. A new PR #4 targets
            /// branch-2 (making #2 its predecessor). When we try to start a train for a
            /// separate root #5, that should succeed. But trying to start #2 fails (NotARoot).
            #[test]
            fn start_train_rejects_overlapping_descendants(
                sha in arb_sha(),
            ) {
                let engine = CascadeEngine::new("main");

                // Create a chain: main <- #1 <- #2
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

                // PR3 is an independent root (targets main, no predecessor)
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

                // Start train for #1
                let mut active_trains = HashMap::new();
                let result1 = engine.start_train(PrNumber(1), &prs, &active_trains);
                prop_assert!(result1.is_ok(), "Should be able to start train for #1");
                let mut train1 = result1.unwrap().train;

                // Transition train #1 to Preparing phase so frozen_descendants is populated.
                // Without this, the train stays in Idle and progress() returns None.
                train1.cascade_phase = CascadePhase::Preparing {
                    progress: DescendantProgress::new(vec![PrNumber(2)]),
                };
                active_trains.insert(PrNumber(1), train1);

                // Starting train for #3 should succeed - it's independent (no overlap)
                let result3 = engine.start_train(PrNumber(3), &prs, &active_trains);
                prop_assert!(
                    result3.is_ok(),
                    "Should be able to start independent train for #3: {:?}",
                    result3.err()
                );

                // Starting train for #2 should fail.
                // Note: #2 isn't a root (has predecessor #1), so this fails with NotARoot.
                // The overlap guard (checking frozen_descendants) would fire if #2 were a root.
                let result2 = engine.start_train(PrNumber(2), &prs, &active_trains);
                prop_assert!(
                    result2.is_err(),
                    "Starting train for descendant #2 should fail"
                );

                // Verify that the overlap is actually detectable: train #1's frozen_descendants
                // contains #2, so any root whose stack includes #2 would be rejected.
                let train1 = active_trains.get(&PrNumber(1)).unwrap();
                if let Some(progress) = train1.cascade_phase.progress() {
                    prop_assert!(
                        progress.frozen_descendants.contains(&PrNumber(2)),
                        "Train #1 should have #2 in frozen_descendants"
                    );
                } else {
                    prop_assert!(false, "Train #1 should be in a phase with progress");
                }
            }
        }
    }

    // ─── Regression Tests ─────────────────────────────────────────────────────

    mod bug_regression_tests {
        use super::*;
        use crate::types::{CommentId, DescendantProgress};

        fn make_sha(n: u64) -> Sha {
            Sha::parse(format!("{:0>40x}", n)).unwrap()
        }

        /// Regression test: evaluate_train must block draft PRs even if they have CLEAN
        /// mergeStateStatus. GitHub's API can report CLEAN for drafts (if CI passes),
        /// but the merge API will reject them.
        #[test]
        fn evaluate_train_blocks_draft_prs() {
            let engine = CascadeEngine::new("main");

            let train = TrainRecord::new(PrNumber(1));

            // Create a draft PR that reports CLEAN status (this can happen per DESIGN.md).
            // We construct directly because CachedPr::new() has a debug_assert preventing
            // this combination, but GitHub's API can actually return it.
            let draft_pr = CachedPr {
                number: PrNumber(1),
                head_sha: make_sha(1),
                head_ref: "branch-1".to_string(),
                base_ref: "main".to_string(),
                predecessor: None,
                state: PrState::Open,
                merge_state_status: MergeStateStatus::Clean, // Draft can still be CLEAN!
                is_draft: true,                              // This is the key: it's a draft
                closed_at: None,
                predecessor_squash_reconciled: None,
            };

            let prs = HashMap::from([(PrNumber(1), draft_pr)]);

            let action = engine.evaluate_train(&train, &prs);

            assert!(
                matches!(
                    action,
                    TrainAction::Block {
                        reason: BlockReason::Draft
                    }
                ),
                "evaluate_train must return Block {{ reason: Draft }} for draft PRs. \
                 Got: {:?}",
                action
            );
        }

        /// Regression test: Status comment JSON must use compact serialization,
        /// omit local-only fields, and respect the 60KB size limit.
        #[test]
        fn status_comment_json_has_size_guard() {
            // Create a train with a large number of descendants (within the 50 PR limit)
            let mut train = TrainRecord::new(PrNumber(1));
            train.status_comment_id = Some(CommentId(12345)); // LOCAL-ONLY field

            // Add many descendants to approach size limits
            let descendants: Vec<PrNumber> = (2..=45).map(PrNumber).collect();
            train.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(descendants),
            };

            // Generate the status comment JSON
            let json = format_train_json(&train).expect("Should not overflow with moderate input");

            // status_comment_id should be omitted from JSON (it's local-only)
            assert!(
                !json.contains("status_comment_id"),
                "status_comment_id should be omitted from recovery JSON. \
                 JSON: {}",
                json
            );

            // Should use compact serialization, not pretty-printed
            let has_excessive_whitespace = json.lines().count() > 10;
            assert!(
                !has_excessive_whitespace,
                "Status comment JSON should use compact format. Lines: {}",
                json.lines().count()
            );

            // Verify size is under 60KB
            assert!(
                json.len() < 60 * 1024,
                "Status comment JSON ({} bytes) exceeds 60KB limit",
                json.len()
            );
        }

        /// Regression test: format_train_json must truncate error fields per DESIGN.md.
        /// error.message: Maximum 4KB, error.stderr: Maximum 2KB
        #[test]
        fn status_comment_truncates_error_fields() {
            let mut train = TrainRecord::new(PrNumber(1));

            // Create an error with very long message and stderr
            let long_message = "x".repeat(10_000); // 10KB, over 4KB limit
            let long_stderr = "y".repeat(5_000); // 5KB, over 2KB limit

            train.abort(
                TrainError::new("test_error", long_message.clone())
                    .with_stderr(long_stderr.clone()),
            );

            let json = format_train_json(&train).expect("Should not overflow with moderate input");

            // Check that message was truncated (should not contain full 10KB string)
            assert!(
                !json.contains(&long_message),
                "error.message ({} bytes) should be truncated to 4KB limit",
                long_message.len()
            );

            // Check that stderr was truncated (should not contain full 5KB string)
            assert!(
                !json.contains(&long_stderr),
                "error.stderr ({} bytes) should be truncated to 2KB limit",
                long_stderr.len()
            );

            // Verify truncation indicator is present
            assert!(
                json.contains("..."),
                "Truncated fields should have '...' indicator"
            );
        }

        // ─────────────────────────────────────────────────────────────────────────────
        // Property-based invariant tests
        // ─────────────────────────────────────────────────────────────────────────────

        mod property_based_bug_detection {
            use super::*;
            use proptest::prelude::*;

            /// truncate_string handles non-ASCII boundaries safely.
            ///
            /// Property: truncate_string NEVER panics, regardless of input string
            /// content or max_len value. The output is always valid UTF-8.
            #[test]
            fn truncate_string_never_panics() {
                proptest!(|(
                    s in "\\PC{0,200}",  // Any Unicode string up to 200 chars
                    max_len in 0usize..100
                )| {
                    // This should never panic
                    let result = truncate_string(&s, max_len);

                    // Result should be valid UTF-8 (it's a String, so it is)
                    prop_assert!(result.len() <= max_len.max(3), // at least "..."
                        "Result length {} exceeds max_len {}",
                        result.len(), max_len
                    );

                    // If truncated, should end with "..."
                    if s.len() > max_len && max_len >= 3 {
                        prop_assert!(
                            result.ends_with("..."),
                            "Truncated string should end with '...'"
                        );
                    }
                });
            }

            /// BUG #5 (specific cases): truncate_string with multi-byte UTF-8.
            ///
            /// Property: Truncation at any position within multi-byte characters
            /// should find a valid boundary and not panic.
            #[test]
            fn truncate_string_handles_multibyte_chars() {
                // Test with various multi-byte UTF-8 strings
                let test_cases = [
                    "Hello 世界!", // Chinese
                    "Привет мир",  // Russian
                    "🎉🎊🎁🎈",    // Emojis (4-byte UTF-8)
                    "café résumé", // Latin with diacritics
                    "αβγδε",       // Greek
                    "こんにちは",  // Japanese
                ];

                for s in &test_cases {
                    // Try truncating at every possible byte position
                    for max_len in 0..=s.len() + 5 {
                        let result = truncate_string(s, max_len);

                        // Should never panic, and result should be valid UTF-8
                        assert!(
                            result.len() <= max_len.max(3),
                            "truncate_string({:?}, {}) = {:?} exceeds max",
                            s,
                            max_len,
                            result
                        );
                    }
                }
            }

            /// Status comment size is enforced in release builds.
            ///
            /// Property: format_train_json ALWAYS returns a string under 60KB,
            /// even in release builds, even with pathologically large input.
            #[test]
            fn format_train_json_enforces_size_limit() {
                proptest!(|(
                    desc_count in 0usize..50,
                    error_msg_len in 0usize..20000,
                    stderr_len in 0usize..10000
                )| {
                    let mut train = TrainRecord::new(PrNumber(1));

                    // Add many descendants
                    let descendants: Vec<PrNumber> = (2..=(desc_count as u64 + 1))
                        .map(PrNumber)
                        .collect();
                    if !descendants.is_empty() {
                        train.cascade_phase = CascadePhase::Preparing {
                            progress: DescendantProgress::new(descendants),
                        };
                    }

                    // Add a large error
                    if error_msg_len > 0 {
                        let msg = "E".repeat(error_msg_len);
                        let mut error = TrainError::new("test", msg);
                        if stderr_len > 0 {
                            error = error.with_stderr("S".repeat(stderr_len));
                        }
                        train.abort(error);
                    }

                    // format_train_json now returns Result - it should either succeed
                    // with a reasonably-sized JSON, or error if oversize.
                    match format_train_json(&train) {
                        Ok(json) => {
                            // If it succeeds, MUST be under 60KB
                            prop_assert!(
                                json.len() < 60 * 1024,
                                "BUG: format_train_json returned {} bytes, exceeding 60KB limit",
                                json.len()
                            );

                            // Must be valid JSON
                            prop_assert!(
                                serde_json::from_str::<serde_json::Value>(&json).is_ok(),
                                "format_train_json must return valid JSON"
                            );
                        }
                        Err(CascadeError::StatusCommentOversize { .. }) => {
                            // Expected for extreme inputs - this is correct behavior per DESIGN.md
                        }
                        Err(e) => {
                            prop_assert!(false, "Unexpected error: {:?}", e);
                        }
                    }
                });
            }

            /// BUG #4 (extreme case): Even with max-size input, should either succeed
            /// under 60KB OR return StatusCommentOversize error.
            #[test]
            fn format_train_json_handles_extreme_input() {
                let mut train = TrainRecord::new(PrNumber(1));

                // Max descendants (50 is the limit per MAX_TRAIN_SIZE)
                let descendants: Vec<PrNumber> = (2..=50).map(PrNumber).collect();
                train.cascade_phase = CascadePhase::Preparing {
                    progress: DescendantProgress::new(descendants),
                };

                // Max error sizes
                let huge_msg = "X".repeat(100_000); // 100KB message
                let huge_stderr = "Y".repeat(50_000); // 50KB stderr
                train.abort(TrainError::new("huge_error", huge_msg).with_stderr(huge_stderr));

                match format_train_json(&train) {
                    Ok(json) => {
                        // If it succeeds, must stay under 60KB
                        assert!(
                            json.len() < 60 * 1024,
                            "BUG: Extreme input produced {} byte JSON, exceeding 60KB limit",
                            json.len()
                        );

                        // Must still be valid JSON
                        assert!(
                            serde_json::from_str::<serde_json::Value>(&json).is_ok(),
                            "Extreme input must still produce valid JSON"
                        );
                    }
                    Err(CascadeError::StatusCommentOversize {
                        actual_size,
                        max_size,
                    }) => {
                        // Expected for extreme inputs - verify the error is sensible
                        assert!(
                            actual_size > max_size,
                            "Oversize error should report actual > max"
                        );
                    }
                    Err(e) => panic!("Unexpected error: {:?}", e),
                }
            }

            /// Property: escape_for_html_comment prevents --> from terminating comments,
            /// and the escaped JSON still parses correctly, recovering the original value.
            #[test]
            fn escape_for_html_comment_preserves_json_semantics() {
                proptest!(|(
                    prefix in "[a-zA-Z0-9 ]{0,20}",
                    suffix in "[a-zA-Z0-9 ]{0,20}",
                    arrow_count in 1usize..5
                )| {
                    // Create a string with --> embedded
                    let dangerous_content = format!("{}{}{}", prefix, "-->".repeat(arrow_count), suffix);

                    // Create a TrainRecord with this dangerous content in an error
                    let mut train = TrainRecord::new(PrNumber(1));
                    let error = TrainError::new("git", dangerous_content.clone())
                        .with_stderr(format!("stderr also has --> in it: {}", dangerous_content));
                    train.abort(error);

                    // Serialize and escape
                    let json = format_train_json(&train).expect("should serialize");
                    let escaped = escape_for_html_comment(&json);

                    // Property 1: No --> in escaped output
                    prop_assert!(
                        !escaped.contains("-->"),
                        "Escaped JSON must not contain -->, but found: {}",
                        escaped.chars().take(200).collect::<String>()
                    );

                    // Property 2: Escaped JSON is still valid JSON
                    let parsed: serde_json::Value = serde_json::from_str(&escaped)
                        .expect("Escaped JSON must still be valid JSON");

                    // Property 3: The original content is recoverable after parsing
                    let error_obj = parsed.get("error").expect("should have error field");
                    let message = error_obj.get("message").and_then(|v| v.as_str()).expect("should have message");
                    let stderr = error_obj.get("stderr").and_then(|v| v.as_str()).expect("should have stderr");

                    prop_assert!(
                        message.contains("-->"),
                        "Original --> should be recovered in message after parsing"
                    );
                    prop_assert!(
                        stderr.contains("-->"),
                        "Original --> should be recovered in stderr after parsing"
                    );
                });
            }

            /// Edge case: Multiple overlapping --> sequences
            #[test]
            fn escape_handles_overlapping_arrows() {
                // Test case: "-->" appears multiple times, including "---->>" (overlapping)
                let input = r#"{"msg":"a-->b---->c"}"#;
                let escaped = escape_for_html_comment(input);

                assert!(
                    !escaped.contains("-->"),
                    "Should escape all --> occurrences"
                );
                assert_eq!(
                    escaped, r#"{"msg":"a--\u003eb----\u003ec"}"#,
                    "Each --> should be escaped independently"
                );

                // Verify it round-trips correctly
                let parsed: serde_json::Value = serde_json::from_str(&escaped).unwrap();
                assert_eq!(
                    parsed.get("msg").unwrap().as_str().unwrap(),
                    "a-->b---->c",
                    "Original string should be recovered after parsing"
                );
            }
        }
    }
}
