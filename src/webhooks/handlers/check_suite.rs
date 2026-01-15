//! Handler for `check_suite` webhook events.
//!
//! This handler processes CI check suite events to trigger re-evaluation
//! of merge state when CI completes.
//!
//! Per DESIGN.md §"Incremental updates":
//! - `check_suite.completed` → Re-fetch `merge_state` via GraphQL

use crate::effects::{Effect, GitHubEffect};
use crate::persistence::event::StateEventPayload;
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::types::PrNumber;
use crate::webhooks::events::{CheckSuiteAction, CheckSuiteEvent};

use super::{HandlerError, HandlerResult};

/// Handles a check suite event.
///
/// # Event Actions
///
/// | Action | Behavior |
/// |--------|----------|
/// | `completed` | Trigger merge state re-fetch for associated PRs |
/// | `created` / `requested` / `rerequested` | No action (wait for completion) |
pub fn handle_check_suite(
    event: &CheckSuiteEvent,
    state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    match event.action {
        CheckSuiteAction::Completed => handle_completed(event, state),
        // Other actions don't require immediate handling
        CheckSuiteAction::Created | CheckSuiteAction::Requested | CheckSuiteAction::Rerequested => {
            Ok(HandlerResult::empty())
        }
    }
}

/// Handles check suite completion.
///
/// When a check suite completes, we need to re-fetch the merge state for
/// all associated PRs to see if they're now ready for merging.
fn handle_completed(
    event: &CheckSuiteEvent,
    state: &PersistedRepoSnapshot,
) -> Result<HandlerResult, HandlerError> {
    let mut state_events = Vec::new();
    let mut effects = Vec::new();

    // Record the check suite completion
    let conclusion = event
        .conclusion
        .clone()
        .unwrap_or_else(|| "unknown".to_string());
    state_events.push(StateEventPayload::CheckSuiteCompleted {
        sha: event.head_sha.clone(),
        conclusion: conclusion.clone(),
    });

    // For each associated PR, trigger a merge state re-fetch
    for &pr_number in &event.pull_requests {
        // Only process PRs we know about
        if let Some(cached_pr) = state.prs.get(&pr_number) {
            // Only refetch if the SHA matches the PR's current head
            // This avoids unnecessary API calls for outdated check suites
            if cached_pr.head_sha == event.head_sha {
                effects.push(Effect::GitHub(GitHubEffect::RefetchPr { pr: pr_number }));

                // If this PR is in an active train waiting for CI, the refetch
                // will trigger re-evaluation
                if let Some(train_root) = find_train_containing_pr(pr_number, state) {
                    let train = state.active_trains.get(&train_root).unwrap();

                    // If this is the current PR and train is waiting for CI
                    if train.current_pr == pr_number && train.state.is_active() {
                        // The re-fetch effect above will cause re-evaluation
                        // No additional state events needed here
                    }
                }
            }
        }
    }

    Ok(HandlerResult::new(state_events, effects))
}

/// Finds the train that contains a given PR.
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
    use crate::types::{CachedPr, MergeStateStatus, PrState, RepoId, Sha};

    fn make_state() -> PersistedRepoSnapshot {
        PersistedRepoSnapshot::new("main")
    }

    fn make_check_suite_event(
        action: CheckSuiteAction,
        sha: &str,
        prs: Vec<u64>,
    ) -> CheckSuiteEvent {
        CheckSuiteEvent {
            repo: RepoId::new("owner", "repo"),
            action,
            head_sha: Sha::parse(sha).unwrap(),
            conclusion: if action == CheckSuiteAction::Completed {
                Some("success".to_string())
            } else {
                None
            },
            pull_requests: prs.into_iter().map(PrNumber).collect(),
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
    fn created_action_returns_empty() {
        let sha = "a".repeat(40);
        let event = make_check_suite_event(CheckSuiteAction::Created, &sha, vec![1]);
        let state = make_state();

        let result = handle_check_suite(&event, &state).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn completed_creates_check_suite_completed_event() {
        let sha = "a".repeat(40);
        let event = make_check_suite_event(CheckSuiteAction::Completed, &sha, vec![1]);
        let state = make_state();

        let result = handle_check_suite(&event, &state).unwrap();

        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::CheckSuiteCompleted { conclusion, .. }
            if conclusion == "success"
        )));
    }

    #[test]
    fn completed_triggers_refetch_for_matching_sha() {
        let sha = "a".repeat(40);
        let event = make_check_suite_event(CheckSuiteAction::Completed, &sha, vec![1]);

        let mut state = make_state();
        state.prs.insert(PrNumber(1), make_cached_pr(1, &sha));

        let result = handle_check_suite(&event, &state).unwrap();

        assert!(result.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::RefetchPr { pr })
            if *pr == PrNumber(1)
        )));
    }

    #[test]
    fn completed_skips_refetch_for_mismatched_sha() {
        let sha = "a".repeat(40);
        let different_sha = "b".repeat(40);
        let event = make_check_suite_event(CheckSuiteAction::Completed, &sha, vec![1]);

        let mut state = make_state();
        // PR has a different SHA
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, &different_sha));

        let result = handle_check_suite(&event, &state).unwrap();

        // Should not trigger refetch for mismatched SHA
        assert!(
            !result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::GitHub(GitHubEffect::RefetchPr { .. })))
        );
    }

    #[test]
    fn completed_skips_unknown_prs() {
        let sha = "a".repeat(40);
        let event = make_check_suite_event(CheckSuiteAction::Completed, &sha, vec![999]);
        let state = make_state();

        let result = handle_check_suite(&event, &state).unwrap();

        // Should not trigger refetch for unknown PRs
        assert!(
            !result
                .effects
                .iter()
                .any(|e| matches!(e, Effect::GitHub(GitHubEffect::RefetchPr { .. })))
        );
    }

    #[test]
    fn completed_handles_multiple_prs() {
        let sha = "a".repeat(40);
        let event = make_check_suite_event(CheckSuiteAction::Completed, &sha, vec![1, 2, 3]);

        let mut state = make_state();
        state.prs.insert(PrNumber(1), make_cached_pr(1, &sha));
        state.prs.insert(PrNumber(2), make_cached_pr(2, &sha));
        // PR 3 not in cache

        let result = handle_check_suite(&event, &state).unwrap();

        // Should trigger refetch for PR 1 and 2, but not 3
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
        assert!(!refetch_prs.contains(&PrNumber(3)));
    }
}
