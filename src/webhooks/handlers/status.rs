//! Handler for `status` webhook events.
//!
//! This handler processes legacy commit status events (as opposed to
//! check_suite/check_run events). Some CI systems still use the older
//! status API.
//!
//! Per DESIGN.md, status events should trigger re-evaluation of merge state
//! similar to check_suite events.

use crate::effects::{Effect, GitHubEffect};
use crate::persistence::event::StateEventPayload;
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::types::PrNumber;
use crate::webhooks::events::{StatusEvent, StatusState};

use super::{HandlerError, HandlerResult};

/// Handles a commit status event.
///
/// Status events are sent for the legacy commit status API. We handle them
/// similarly to check_suite events: when a status reaches a terminal state,
/// we trigger merge state re-fetch for associated PRs.
///
/// # Event States
///
/// | State | Behavior |
/// |-------|----------|
/// | `success` / `failure` / `error` | Re-fetch merge state for PRs with this head SHA |
/// | `pending` | No action (wait for terminal state) |
pub fn handle_status(
    event: &StatusEvent,
    state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    // Only act on terminal states
    if !event.state.is_terminal() {
        return Ok(HandlerResult::empty());
    }

    let mut state_events = Vec::new();
    let mut effects = Vec::new();

    // Record the status event
    state_events.push(StateEventPayload::StatusReceived {
        sha: event.sha.clone(),
        context: event.context.clone(),
        state: status_state_to_string(&event.state),
    });

    // Find PRs with this head SHA and trigger refetch
    let matching_prs: Vec<PrNumber> = state
        .prs
        .iter()
        .filter(|(_, pr)| pr.head_sha == event.sha)
        .map(|(pr_num, _)| *pr_num)
        .collect();

    for pr_number in matching_prs {
        effects.push(Effect::GitHub(GitHubEffect::RefetchPr { pr: pr_number }));
    }

    Ok(HandlerResult::new(state_events, effects))
}

/// Converts StatusState to string for event logging.
fn status_state_to_string(state: &StatusState) -> String {
    match state {
        StatusState::Pending => "pending".to_string(),
        StatusState::Success => "success".to_string(),
        StatusState::Failure => "failure".to_string(),
        StatusState::Error => "error".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CachedPr, MergeStateStatus, PrState, RepoId, Sha};

    fn make_state() -> PersistedRepoSnapshot {
        PersistedRepoSnapshot::new("main")
    }

    fn make_status_event(sha: &str, state: StatusState) -> StatusEvent {
        StatusEvent {
            repo: RepoId::new("owner", "repo"),
            sha: Sha::parse(sha).unwrap(),
            state,
            context: "ci/test".to_string(),
            description: Some("All tests passed".to_string()),
            target_url: Some("https://ci.example.com/build/123".to_string()),
        }
    }

    fn make_cached_pr(number: u64, head_sha: &str) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::parse(head_sha).unwrap(),
            format!("branch-{}", number),
            "main".to_string(),
            None,
            PrState::Open,
            MergeStateStatus::Clean,
            false,
        )
    }

    #[test]
    fn pending_status_returns_empty() {
        let sha = "a".repeat(40);
        let event = make_status_event(&sha, StatusState::Pending);
        let state = make_state();

        let result = handle_status(&event, &state).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn success_status_creates_event_and_triggers_refetch() {
        let sha = "a".repeat(40);
        let event = make_status_event(&sha, StatusState::Success);

        let mut state = make_state();
        state.prs.insert(PrNumber(1), make_cached_pr(1, &sha));

        let result = handle_status(&event, &state).unwrap();

        // Should have StatusReceived event
        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::StatusReceived { context, state, .. }
            if context == "ci/test" && state == "success"
        )));

        // Should trigger refetch
        assert!(result.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::RefetchPr { pr })
            if *pr == PrNumber(1)
        )));
    }

    #[test]
    fn failure_status_triggers_refetch() {
        let sha = "a".repeat(40);
        let event = make_status_event(&sha, StatusState::Failure);

        let mut state = make_state();
        state.prs.insert(PrNumber(1), make_cached_pr(1, &sha));

        let result = handle_status(&event, &state).unwrap();

        // Should trigger refetch even on failure (merge state changes)
        assert!(
            result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::GitHub(GitHubEffect::RefetchPr { .. })))
        );
    }

    #[test]
    fn error_status_triggers_refetch() {
        let sha = "a".repeat(40);
        let event = make_status_event(&sha, StatusState::Error);

        let mut state = make_state();
        state.prs.insert(PrNumber(1), make_cached_pr(1, &sha));

        let result = handle_status(&event, &state).unwrap();

        // Should trigger refetch on error state
        assert!(
            result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::GitHub(GitHubEffect::RefetchPr { .. })))
        );
    }

    #[test]
    fn status_for_unknown_sha_creates_event_only() {
        let sha = "a".repeat(40);
        let event = make_status_event(&sha, StatusState::Success);
        let state = make_state(); // No PRs

        let result = handle_status(&event, &state).unwrap();

        // Should have StatusReceived event
        assert!(!result.state_events.is_empty());

        // No refetch (no matching PRs)
        assert!(result.effects.is_empty());
    }

    #[test]
    fn status_triggers_refetch_for_multiple_prs_with_same_sha() {
        let sha = "a".repeat(40);
        let event = make_status_event(&sha, StatusState::Success);

        let mut state = make_state();
        // Multiple PRs with the same head SHA (e.g., rebased onto same commit)
        state.prs.insert(PrNumber(1), make_cached_pr(1, &sha));
        state.prs.insert(PrNumber(2), make_cached_pr(2, &sha));

        let result = handle_status(&event, &state).unwrap();

        let refetch_prs: Vec<PrNumber> = result
            .effects
            .iter()
            .filter_map(|e| match e {
                Effect::GitHub(GitHubEffect::RefetchPr { pr }) => Some(*pr),
                _ => None,
            })
            .collect();

        assert!(refetch_prs.contains(&PrNumber(1)));
        assert!(refetch_prs.contains(&PrNumber(2)));
    }

    #[test]
    fn status_skips_prs_with_different_sha() {
        let sha = "a".repeat(40);
        let different_sha = "b".repeat(40);
        let event = make_status_event(&sha, StatusState::Success);

        let mut state = make_state();
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, &different_sha));

        let result = handle_status(&event, &state).unwrap();

        // Should not trigger refetch for PR with different SHA
        assert!(
            !result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::GitHub(GitHubEffect::RefetchPr { .. })))
        );
    }
}
