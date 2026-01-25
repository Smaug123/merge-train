//! Handler for `pull_request_review` webhook events.
//!
//! This handler processes PR review events to trigger re-evaluation
//! of merge state when reviews are submitted or dismissed.
//!
//! Per DESIGN.md §"Incremental updates":
//! - `pull_request_review` → Re-fetch `merge_state` via GraphQL

use crate::effects::{Effect, GitHubEffect};
use crate::persistence::event::StateEventPayload;
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::types::{PrNumber, TrainError};
use crate::webhooks::events::{PullRequestReviewEvent, ReviewAction, ReviewState};

use super::{HandlerError, HandlerResult};

/// Handles a pull request review event.
///
/// # Event Actions
///
/// | Action | Behavior |
/// |--------|----------|
/// | `submitted` (approved) | Re-fetch merge state, may unblock train |
/// | `submitted` (changes_requested) | Re-fetch merge state, may block train |
/// | `dismissed` | Re-fetch merge state |
/// | `edited` | No action (content changes don't affect merge state) |
pub fn handle_review(
    event: &PullRequestReviewEvent,
    state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    match event.action {
        ReviewAction::Submitted => handle_submitted(event, state),
        ReviewAction::Dismissed => handle_dismissed(event, state),
        ReviewAction::Edited => Ok(HandlerResult::empty()),
    }
}

/// Handles a submitted review.
fn handle_submitted(
    event: &PullRequestReviewEvent,
    _state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    let mut state_events = Vec::new();
    let mut effects = Vec::new();

    // Record the review submission
    state_events.push(StateEventPayload::ReviewSubmitted {
        pr: event.pr_number,
        reviewer: event.reviewer_login.clone(),
        state: review_state_to_string(&event.state),
    });

    // Always trigger a merge state re-fetch for the affected PR
    // The merge state depends on required reviews, which may have changed
    effects.push(Effect::GitHub(GitHubEffect::RefetchPr {
        pr: event.pr_number,
    }));

    // If this PR is in an active train and the review requests changes,
    // the train may need to be blocked (the engine handles this via
    // mergeStateStatus after refetch)

    Ok(HandlerResult::new(state_events, effects))
}

/// Handles a dismissed review.
///
/// Per DESIGN.md §"Review dismissal behaviour":
/// "If a review is dismissed [...] the cascade aborts immediately.
/// Unlike CI failure, review dismissal does not auto-resume"
fn handle_dismissed(
    event: &PullRequestReviewEvent,
    state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    let mut state_events = Vec::new();
    let mut effects = Vec::new();

    // Record the review dismissal
    state_events.push(StateEventPayload::ReviewDismissed {
        pr: event.pr_number,
        reviewer: event.reviewer_login.clone(),
    });

    // If this PR is in an active train, abort immediately.
    // Review dismissal is treated as a deliberate human action that should
    // not auto-resume (unlike CI failures which may be transient).
    if let Some(train_root) = find_train_containing_pr(event.pr_number, state) {
        state_events.push(StateEventPayload::TrainAborted {
            root_pr: train_root,
            error: TrainError::new(
                "review_dismissed",
                format!(
                    "Review by {} was dismissed on PR #{}. Re-approve and issue `@merge-train start` to resume.",
                    event.reviewer_login, event.pr_number
                ),
            ),
        });
    }

    // Trigger merge state re-fetch (even after abort, to update cached state)
    effects.push(Effect::GitHub(GitHubEffect::RefetchPr {
        pr: event.pr_number,
    }));

    Ok(HandlerResult::new(state_events, effects))
}

/// Finds the train that contains a given PR.
///
/// Returns the root PR number of the train, or None if the PR isn't in any train.
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

/// Converts ReviewState to string for event logging.
fn review_state_to_string(state: &ReviewState) -> String {
    match state {
        ReviewState::Approved => "approved".to_string(),
        ReviewState::ChangesRequested => "changes_requested".to_string(),
        ReviewState::Commented => "commented".to_string(),
        ReviewState::Dismissed => "dismissed".to_string(),
        ReviewState::Pending => "pending".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CachedPr, MergeStateStatus, PrNumber, PrState, RepoId, Sha, TrainRecord};

    fn make_state() -> PersistedRepoSnapshot {
        PersistedRepoSnapshot::new("main")
    }

    fn make_review_event(
        action: ReviewAction,
        pr_number: u64,
        state: ReviewState,
    ) -> PullRequestReviewEvent {
        PullRequestReviewEvent {
            repo: RepoId::new("owner", "repo"),
            action,
            pr_number: PrNumber(pr_number),
            review_id: 67890,
            state,
            reviewer_id: 12345,
            reviewer_login: "reviewer".to_string(),
            body: "LGTM!".to_string(),
        }
    }

    #[test]
    fn submitted_approved_triggers_refetch() {
        let event = make_review_event(ReviewAction::Submitted, 1, ReviewState::Approved);
        let state = make_state();

        let result = handle_review(&event, &state).unwrap();

        // Should have ReviewSubmitted event
        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::ReviewSubmitted { pr, state, .. }
            if *pr == PrNumber(1) && state == "approved"
        )));

        // Should trigger refetch
        assert!(result.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::RefetchPr { pr })
            if *pr == PrNumber(1)
        )));
    }

    #[test]
    fn submitted_changes_requested_triggers_refetch() {
        let event = make_review_event(ReviewAction::Submitted, 1, ReviewState::ChangesRequested);
        let state = make_state();

        let result = handle_review(&event, &state).unwrap();

        // Should have ReviewSubmitted event
        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::ReviewSubmitted { state, .. }
            if state == "changes_requested"
        )));

        // Should trigger refetch
        assert!(
            result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::GitHub(GitHubEffect::RefetchPr { .. })))
        );
    }

    #[test]
    fn dismissed_triggers_refetch() {
        let event = make_review_event(ReviewAction::Dismissed, 1, ReviewState::Dismissed);
        let state = make_state();

        let result = handle_review(&event, &state).unwrap();

        // Should have ReviewDismissed event
        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::ReviewDismissed { pr, .. }
            if *pr == PrNumber(1)
        )));

        // Should trigger refetch
        assert!(
            result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::GitHub(GitHubEffect::RefetchPr { .. })))
        );
    }

    #[test]
    fn edited_returns_empty() {
        let event = make_review_event(ReviewAction::Edited, 1, ReviewState::Commented);
        let state = make_state();

        let result = handle_review(&event, &state).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn commented_review_still_recorded() {
        let event = make_review_event(ReviewAction::Submitted, 1, ReviewState::Commented);
        let state = make_state();

        let result = handle_review(&event, &state).unwrap();

        // Should have ReviewSubmitted event even for comments
        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::ReviewSubmitted { state, .. }
            if state == "commented"
        )));
    }

    fn make_cached_pr(number: u64) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::parse("a".repeat(40)).unwrap(),
            format!("branch-{}", number),
            "main".to_string(),
            None,
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        )
    }

    /// Per DESIGN.md §"Review dismissal behaviour":
    /// "If a review is dismissed [...] the cascade aborts immediately.
    /// Unlike CI failure, review dismissal does not auto-resume"
    #[test]
    fn dismissed_aborts_active_train() {
        let event = make_review_event(ReviewAction::Dismissed, 1, ReviewState::Dismissed);
        let mut state = make_state();

        // Set up an active train where PR #1 is the current PR
        state.prs.insert(PrNumber(1), make_cached_pr(1));
        state
            .active_trains
            .insert(PrNumber(1), TrainRecord::new(PrNumber(1)));

        let result = handle_review(&event, &state).unwrap();

        // Should have TrainAborted event (not just ReviewDismissed)
        assert!(
            result.state_events.iter().any(|e| matches!(
                e,
                StateEventPayload::TrainAborted { root_pr, .. }
                if *root_pr == PrNumber(1)
            )),
            "Expected TrainAborted event for PR #1, got: {:?}",
            result.state_events
        );
    }
}
