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
fn handle_dismissed(
    event: &PullRequestReviewEvent,
    _state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    let mut state_events = Vec::new();
    let mut effects = Vec::new();

    // Record the review dismissal
    state_events.push(StateEventPayload::ReviewDismissed {
        pr: event.pr_number,
        reviewer: event.reviewer_login.clone(),
    });

    // Trigger merge state re-fetch
    // A dismissed changes_requested review might unblock the PR
    effects.push(Effect::GitHub(GitHubEffect::RefetchPr {
        pr: event.pr_number,
    }));

    Ok(HandlerResult::new(state_events, effects))
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
    use crate::types::{PrNumber, RepoId};

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
}
