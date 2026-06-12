//! Event handlers for GitHub webhook events.
//!
//! This module provides pure event handlers that map webhook events to state
//! mutations and effects. Handlers take current state and return state events
//! plus effects to execute, without performing any I/O.
//!
//! # Design
//!
//! Handlers are pure functions following the effects-as-data pattern:
//! - Input: Current state + webhook event
//! - Output: State events to persist + effects to execute
//!
//! This enables:
//! - Testing without I/O
//! - Replay and recovery
//! - Clear separation of concerns
//!
//! # Event Types
//!
//! | Event | Handler |
//! |-------|---------|
//! | `issue_comment` | `handle_issue_comment` - predecessor, start, stop commands |
//! | `pull_request` | `handle_pull_request` - opened, closed, edited, synchronize |
//! | `check_suite` | `handle_check_suite` - CI completion |
//! | `pull_request_review` | `handle_review` - approval, dismissal |
//! | `status` | `handle_status` - legacy CI status |

mod check_suite;
mod issue_comment;
mod pull_request;
mod review;
mod status;

use thiserror::Error;

use crate::effects::Effect;
use crate::persistence::event::StateEventPayload;
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::webhooks::GitHubEvent;

pub use check_suite::handle_check_suite;
pub use issue_comment::handle_issue_comment;
pub use pull_request::handle_pull_request;
pub use review::handle_review;
pub use status::handle_status;

/// Errors that can occur during event handling.
#[derive(Debug, Error)]
pub enum HandlerError {
    /// The event references a PR that doesn't exist in the cache.
    #[error("PR #{0} not found in cache")]
    PrNotFound(u64),

    /// The event is for a regular issue, not a PR.
    #[error("Event is for an issue, not a pull request")]
    NotAPullRequest,

    /// A train operation failed.
    #[error("Train operation failed: {0}")]
    TrainError(String),

    /// Invalid state for this operation.
    #[error("Invalid state: {0}")]
    InvalidState(String),
}

/// Result of handling an event.
///
/// Contains state events to persist and effects to execute.
#[derive(Debug, Clone)]
pub struct HandlerResult {
    /// State events to append to the event log.
    ///
    /// These represent state mutations. The caller wraps them with sequence
    /// numbers and timestamps to create `StateEvent` instances.
    pub state_events: Vec<StateEventPayload>,

    /// Effects to execute (GitHub API calls, git operations, etc.).
    pub effects: Vec<Effect>,
}

impl HandlerResult {
    /// Creates an empty result (no state changes, no effects).
    pub fn empty() -> Self {
        HandlerResult {
            state_events: Vec::new(),
            effects: Vec::new(),
        }
    }

    /// Creates a result with state events only.
    pub fn with_events(state_events: Vec<StateEventPayload>) -> Self {
        HandlerResult {
            state_events,
            effects: Vec::new(),
        }
    }

    /// Creates a result with effects only.
    pub fn with_effects(effects: Vec<Effect>) -> Self {
        HandlerResult {
            state_events: Vec::new(),
            effects,
        }
    }

    /// Creates a result with both state events and effects.
    pub fn new(state_events: Vec<StateEventPayload>, effects: Vec<Effect>) -> Self {
        HandlerResult {
            state_events,
            effects,
        }
    }

    /// Returns true if this result has no state events and no effects.
    pub fn is_empty(&self) -> bool {
        self.state_events.is_empty() && self.effects.is_empty()
    }

    /// Merges another result into this one.
    pub fn merge(&mut self, other: HandlerResult) {
        self.state_events.extend(other.state_events);
        self.effects.extend(other.effects);
    }
}

/// Handles a GitHub webhook event.
///
/// This is the main entry point for event handling. It dispatches to the
/// appropriate handler based on the event type.
///
/// # Arguments
///
/// * `event` - The parsed webhook event
/// * `state` - The current repository state (PR cache, active trains, etc.)
/// * `bot_name` - The bot name for command parsing (e.g., "merge-train")
///
/// # Returns
///
/// A `HandlerResult` containing state events to persist and effects to execute.
/// Returns an empty result for events that don't require any action.
///
/// # Errors
///
/// Returns an error if the event cannot be processed (e.g., missing PR in cache).
pub fn handle_event(
    event: &GitHubEvent,
    state: &PersistedRepoSnapshot,
    bot_name: &str,
) -> Result<HandlerResult, HandlerError> {
    match event {
        GitHubEvent::IssueComment(e) => handle_issue_comment(e, state, bot_name),
        GitHubEvent::PullRequest(e) => handle_pull_request(e, state),
        GitHubEvent::CheckSuite(e) => handle_check_suite(e, state),
        GitHubEvent::Status(e) => handle_status(e, state),
        GitHubEvent::PullRequestReview(e) => handle_review(e, state),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handler_result_empty() {
        let result = HandlerResult::empty();
        assert!(result.is_empty());
        assert!(result.state_events.is_empty());
        assert!(result.effects.is_empty());
    }

    #[test]
    fn handler_result_merge() {
        let mut result1 = HandlerResult::with_events(vec![StateEventPayload::TrainStopped {
            root_pr: crate::types::PrNumber(1),
        }]);
        let result2 = HandlerResult::with_effects(vec![Effect::GitHub(
            crate::effects::GitHubEffect::GetRepoSettings,
        )]);

        result1.merge(result2);

        assert_eq!(result1.state_events.len(), 1);
        assert_eq!(result1.effects.len(), 1);
    }
}
