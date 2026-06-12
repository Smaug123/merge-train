//! Handler for `pull_request` webhook events.
//!
//! This handler processes PR lifecycle events:
//! - `opened` - Add new PR to cache
//! - `closed` (merged) - Update PR state to Merged
//! - `closed` (not merged) - Update PR state to Closed
//! - `edited` (base changed) - Update base branch, re-validate predecessor
//! - `synchronize` - Update head SHA, set merge state to Unknown
//! - `converted_to_draft` / `ready_for_review` - Update draft status

use crate::effects::{Effect, GitHubEffect};
use crate::persistence::event::StateEventPayload;
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::types::{PrNumber, TrainError};
use crate::webhooks::events::{PrAction, PullRequestEvent};

use super::{HandlerError, HandlerResult};

/// Handles a pull request event.
///
/// # Event Actions
///
/// | Action | State Update |
/// |--------|--------------|
/// | `opened` | Add PR to cache |
/// | `closed` (merged) | `pr.state = Merged { sha }` |
/// | `closed` (not merged) | `pr.state = Closed` |
/// | `reopened` | `pr.state = Open` |
/// | `edited` (base changed) | Update `base_ref`, re-validate predecessor |
/// | `synchronize` | Update `head_sha`, `merge_state = Unknown` |
/// | `converted_to_draft` | `is_draft = true` |
/// | `ready_for_review` | `is_draft = false`, trigger re-evaluation |
pub fn handle_pull_request(
    event: &PullRequestEvent,
    state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    match event.action {
        PrAction::Opened => handle_opened(event, state),
        PrAction::Closed => handle_closed(event, state),
        PrAction::Reopened => handle_reopened(event, state),
        PrAction::Edited => handle_edited(event, state),
        PrAction::Synchronize => handle_synchronize(event, state),
        PrAction::ConvertedToDraft => handle_converted_to_draft(event, state),
        PrAction::ReadyForReview => handle_ready_for_review(event, state),
    }
}

/// Handles a newly opened PR.
fn handle_opened(
    event: &PullRequestEvent,
    _state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    // Add the PR to cache
    // The actual CachedPr creation happens during event application
    let state_events = vec![StateEventPayload::PrOpened {
        pr: event.pr_number,
        head_sha: event.head_sha.clone(),
        head_ref: event.head_branch.clone(),
        base_ref: event.base_branch.clone(),
        is_draft: event.is_draft,
    }];

    Ok(HandlerResult::with_events(state_events))
}

/// Handles a closed PR (merged or not merged).
fn handle_closed(
    event: &PullRequestEvent,
    state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    let mut state_events = Vec::new();
    let effects = Vec::new();

    if event.merged {
        let merge_sha = event.merge_commit_sha.clone().ok_or_else(|| {
            HandlerError::InvalidState(format!(
                "PR #{} is merged but has no merge_commit_sha",
                event.pr_number
            ))
        })?;

        state_events.push(StateEventPayload::PrMerged {
            pr: event.pr_number,
            merge_sha: merge_sha.clone(),
        });

        // Check if this PR is in an active train
        if let Some(train_root) = find_train_containing_pr(event.pr_number, state) {
            let train = state.active_trains.get(&train_root).unwrap();

            // If this is the current PR being processed, the cascade engine
            // handles advancement. But we need to check for external merges
            // during preparation phase.
            if train.current_pr == event.pr_number {
                // The cascade engine's evaluate_train handles this via
                // AdvanceAfterExternalMerge action
                // No additional state events needed here
            }
        }
    } else {
        state_events.push(StateEventPayload::PrClosed {
            pr: event.pr_number,
        });

        // If this PR is in an active train, we may need to abort or skip
        if let Some(train_root) = find_train_containing_pr(event.pr_number, state) {
            let train = state.active_trains.get(&train_root).unwrap();

            if train.current_pr == event.pr_number {
                // Current PR closed without merge - abort the train
                state_events.push(StateEventPayload::TrainAborted {
                    root_pr: train_root,
                    error: TrainError::new(
                        "pr_closed",
                        format!("PR #{} was closed without being merged", event.pr_number),
                    ),
                });
            } else {
                // A descendant was closed - mark it as skipped
                state_events.push(StateEventPayload::DescendantSkipped {
                    root_pr: train_root,
                    descendant_pr: event.pr_number,
                    reason: "PR closed without merge".to_string(),
                });
            }
        }
    }

    Ok(HandlerResult::new(state_events, effects))
}

/// Handles a reopened PR.
fn handle_reopened(
    event: &PullRequestEvent,
    _state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    let state_events = vec![StateEventPayload::PrReopened {
        pr: event.pr_number,
    }];

    Ok(HandlerResult::with_events(state_events))
}

/// Handles an edited PR (title, body, or base branch changed).
fn handle_edited(
    event: &PullRequestEvent,
    state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    let mut state_events = Vec::new();
    let mut effects = Vec::new();

    // Check if base branch changed
    if let Some(cached_pr) = state.prs.get(&event.pr_number)
        && cached_pr.base_ref != event.base_branch
    {
        // Base branch changed
        state_events.push(StateEventPayload::PrBaseChanged {
            pr: event.pr_number,
            old_base: cached_pr.base_ref.clone(),
            new_base: event.base_branch.clone(),
        });

        // Per DESIGN.md "Continuous base branch validation":
        // If the PR has a predecessor declaration, validate it still makes sense
        if let Some(predecessor) = cached_pr.predecessor {
            // Check if the new base matches the predecessor's head branch
            if let Some(pred_pr) = state.prs.get(&predecessor)
                && event.base_branch != pred_pr.head_ref
            {
                // Base no longer matches predecessor's head - invalid stack
                effects.push(Effect::GitHub(GitHubEffect::PostComment {
                    pr: event.pr_number,
                    body: format!(
                        "Warning: PR #{} was retargeted to `{}`, but its declared \
                         predecessor #{} has head branch `{}`. The stack relationship \
                         may be broken.",
                        event.pr_number, event.base_branch, predecessor, pred_pr.head_ref
                    ),
                }));
            }
        }

        // If the PR is in an active train, check for issues
        if let Some(train_root) = find_train_containing_pr(event.pr_number, state) {
            let train = state.active_trains.get(&train_root).unwrap();

            // If this is a descendant that was retargeted away from the predecessor,
            // we may need to handle it
            if train.current_pr != event.pr_number {
                // Descendant was retargeted - may break the cascade
                effects.push(Effect::GitHub(GitHubEffect::PostComment {
                    pr: event.pr_number,
                    body: format!(
                        "Warning: This PR is part of active train #{}. Changing the \
                         base branch may cause issues with the cascade.",
                        train_root
                    ),
                }));
            }
        }
    }

    // If no significant changes, return empty
    if state_events.is_empty() && effects.is_empty() {
        return Ok(HandlerResult::empty());
    }

    Ok(HandlerResult::new(state_events, effects))
}

/// Handles a synchronized PR (new commits pushed).
fn handle_synchronize(
    event: &PullRequestEvent,
    state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    let state_events = vec![StateEventPayload::PrSynchronized {
        pr: event.pr_number,
        new_head_sha: event.head_sha.clone(),
    }];

    // Check if this PR is in an active train
    if let Some(train_root) = find_train_containing_pr(event.pr_number, state) {
        let _train = state.active_trains.get(&train_root).unwrap();

        // Per DESIGN.md: "If `head_sha` changes during cascade (force push by user),
        // abort with explicit error"
        //
        // However, we need to distinguish between:
        // 1. Force push by user (unexpected) - abort
        // 2. Push by the bot during cascade operations (expected)
        //
        // The bot pushes during:
        // - Preparation (merging predecessor head)
        // - Reconciliation (ours-merge of squash commit)
        // - Catch-up (merging main)
        //
        // For now, we record the sync and let the cascade engine evaluate.
        // A more sophisticated implementation would track expected SHAs.
        //
        // TODO: Implement expected SHA tracking to distinguish bot vs user pushes
    }

    Ok(HandlerResult::with_events(state_events))
}

/// Handles conversion to draft.
fn handle_converted_to_draft(
    event: &PullRequestEvent,
    _state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    let state_events = vec![StateEventPayload::PrConvertedToDraft {
        pr: event.pr_number,
    }];

    // If this PR is the current PR in an active train, the train becomes blocked
    // The cascade engine handles this via the BlockReason::Draft check
    // No additional handling needed here

    Ok(HandlerResult::with_events(state_events))
}

/// Handles a PR becoming ready for review.
fn handle_ready_for_review(
    event: &PullRequestEvent,
    state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    let state_events = vec![StateEventPayload::PrReadyForReview {
        pr: event.pr_number,
    }];

    let mut effects = Vec::new();

    // If this PR is in an active train that was blocked on draft, trigger re-evaluation
    if let Some(train_root) = find_train_containing_pr(event.pr_number, state) {
        let train = state.active_trains.get(&train_root).unwrap();

        if train.current_pr == event.pr_number {
            // Trigger re-evaluation of the train
            // This is done by the caller when processing effects
            effects.push(Effect::GitHub(GitHubEffect::RefetchPr {
                pr: event.pr_number,
            }));
        }
    }

    Ok(HandlerResult::new(state_events, effects))
}

/// Finds the train that contains a given PR.
///
/// Returns the root PR number of the train, or None if not in any train.
fn find_train_containing_pr(
    pr_number: PrNumber,
    state: &PersistedRepoSnapshot,
) -> Option<PrNumber> {
    // Check if PR is a train root
    if state.active_trains.contains_key(&pr_number) {
        return Some(pr_number);
    }

    // Check if PR is current_pr or a frozen descendant in any train
    for (root_pr, train) in &state.active_trains {
        if train.current_pr == pr_number {
            return Some(*root_pr);
        }
        if let Some(progress) = train.cascade_phase.progress()
            && progress.frozen_descendants.contains(&pr_number)
        {
            return Some(*root_pr);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        CachedPr, CascadePhase, DescendantProgress, MergeStateStatus, PrState, RepoId, Sha,
        TrainRecord,
    };

    fn make_state() -> PersistedRepoSnapshot {
        PersistedRepoSnapshot::new("main")
    }

    fn make_pr_event(action: PrAction, pr_number: u64) -> PullRequestEvent {
        PullRequestEvent {
            repo: RepoId::new("owner", "repo"),
            action,
            pr_number: PrNumber(pr_number),
            head_sha: Sha::parse("a".repeat(40)).unwrap(),
            head_branch: format!("branch-{}", pr_number),
            base_branch: "main".to_string(),
            is_draft: false,
            merged: false,
            merge_commit_sha: None,
            author_id: 1,
        }
    }

    fn make_cached_pr(number: u64, base_ref: &str, predecessor: Option<u64>) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::parse("a".repeat(40)).unwrap(),
            format!("branch-{}", number),
            base_ref.to_string(),
            predecessor.map(PrNumber),
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        )
    }

    #[test]
    fn opened_creates_pr_opened_event() {
        let event = make_pr_event(PrAction::Opened, 1);
        let state = make_state();

        let result = handle_pull_request(&event, &state).unwrap();

        assert_eq!(result.state_events.len(), 1);
        assert!(matches!(
            &result.state_events[0],
            StateEventPayload::PrOpened { pr, .. }
            if *pr == PrNumber(1)
        ));
    }

    #[test]
    fn closed_merged_creates_pr_merged_event() {
        let mut event = make_pr_event(PrAction::Closed, 1);
        event.merged = true;
        event.merge_commit_sha = Some(Sha::parse("b".repeat(40)).unwrap());

        let state = make_state();

        let result = handle_pull_request(&event, &state).unwrap();

        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::PrMerged { pr, .. }
            if *pr == PrNumber(1)
        )));
    }

    #[test]
    fn closed_not_merged_creates_pr_closed_event() {
        let event = make_pr_event(PrAction::Closed, 1);
        let state = make_state();

        let result = handle_pull_request(&event, &state).unwrap();

        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::PrClosed { pr }
            if *pr == PrNumber(1)
        )));
    }

    #[test]
    fn closed_current_pr_aborts_train() {
        let event = make_pr_event(PrAction::Closed, 1);
        let mut state = make_state();

        // Add an active train where #1 is the current PR
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        state
            .active_trains
            .insert(PrNumber(1), TrainRecord::new(PrNumber(1)));

        let result = handle_pull_request(&event, &state).unwrap();

        // Should have TrainAborted event
        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::TrainAborted { root_pr, .. }
            if *root_pr == PrNumber(1)
        )));
    }

    #[test]
    fn closed_descendant_pr_marks_skipped() {
        let event = make_pr_event(PrAction::Closed, 2);
        let mut state = make_state();

        // Train for #1 with #2 as a frozen descendant
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        state
            .prs
            .insert(PrNumber(2), make_cached_pr(2, "branch-1", Some(1)));

        let mut train = TrainRecord::new(PrNumber(1));
        train.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        state.active_trains.insert(PrNumber(1), train);

        let result = handle_pull_request(&event, &state).unwrap();

        // Should have DescendantSkipped event
        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::DescendantSkipped { root_pr, descendant_pr, .. }
            if *root_pr == PrNumber(1) && *descendant_pr == PrNumber(2)
        )));
    }

    #[test]
    fn synchronize_creates_pr_synchronized_event() {
        let event = make_pr_event(PrAction::Synchronize, 1);
        let state = make_state();

        let result = handle_pull_request(&event, &state).unwrap();

        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::PrSynchronized { pr, .. }
            if *pr == PrNumber(1)
        )));
    }

    #[test]
    fn base_changed_creates_pr_base_changed_event() {
        let mut event = make_pr_event(PrAction::Edited, 1);
        event.base_branch = "feature".to_string(); // Changed from main

        let mut state = make_state();
        state.prs.insert(
            PrNumber(1),
            make_cached_pr(1, "main", None), // Original base was main
        );

        let result = handle_pull_request(&event, &state).unwrap();

        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::PrBaseChanged { pr, old_base, new_base }
            if *pr == PrNumber(1) && old_base == "main" && new_base == "feature"
        )));
    }

    #[test]
    fn converted_to_draft_creates_event() {
        let event = make_pr_event(PrAction::ConvertedToDraft, 1);
        let state = make_state();

        let result = handle_pull_request(&event, &state).unwrap();

        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::PrConvertedToDraft { pr }
            if *pr == PrNumber(1)
        )));
    }

    #[test]
    fn ready_for_review_creates_event() {
        let event = make_pr_event(PrAction::ReadyForReview, 1);
        let state = make_state();

        let result = handle_pull_request(&event, &state).unwrap();

        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::PrReadyForReview { pr }
            if *pr == PrNumber(1)
        )));
    }
}
