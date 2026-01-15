//! Handler for `issue_comment` webhook events.
//!
//! This handler processes `@merge-train` commands from PR comments:
//! - `predecessor #N` - declares predecessor relationship
//! - `start` - starts the merge train cascade
//! - `stop` - stops the merge train
//! - `stop --force` - force stops the merge train

use crate::cascade::engine::CascadeEngine;
use crate::commands::{Command, parse_command};
use crate::effects::{Effect, GitHubEffect, Reaction};
use crate::persistence::event::StateEventPayload;
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::types::PrNumber;
use crate::webhooks::events::{CommentAction, IssueCommentEvent};

use super::{HandlerError, HandlerResult};

/// Handles an issue comment event.
///
/// Parses the comment for `@merge-train` commands and processes them.
///
/// # Event Actions
///
/// | Action | Behavior |
/// |--------|----------|
/// | `created` | Parse and execute command if present |
/// | `edited` | Re-parse and update predecessor if changed |
/// | `deleted` | Remove predecessor if authoritative comment was deleted |
pub fn handle_issue_comment(
    event: &IssueCommentEvent,
    state: &PersistedRepoSnapshot,
    bot_name: &str,
) -> Result<HandlerResult, HandlerError> {
    // Only handle comments on PRs, not regular issues
    let pr_number = event.pr_number.ok_or(HandlerError::NotAPullRequest)?;

    match event.action {
        CommentAction::Created => handle_created(event, state, bot_name, pr_number),
        CommentAction::Edited => handle_edited(event, state, bot_name, pr_number),
        CommentAction::Deleted => handle_deleted(event, state, pr_number),
    }
}

/// Handles a newly created comment.
fn handle_created(
    event: &IssueCommentEvent,
    state: &PersistedRepoSnapshot,
    bot_name: &str,
    pr_number: PrNumber,
) -> Result<HandlerResult, HandlerError> {
    // Parse command from comment
    let Some(command) = parse_command(&event.body, bot_name) else {
        return Ok(HandlerResult::empty());
    };

    match command {
        Command::Predecessor(predecessor) => {
            handle_predecessor_command(event, state, pr_number, predecessor)
        }
        Command::Start => handle_start_command(event, state, pr_number),
        Command::Stop => handle_stop_command(event, state, pr_number, false),
        Command::StopForce => handle_stop_command(event, state, pr_number, true),
    }
}

/// Handles an edited comment.
///
/// Re-parses the command and updates state if necessary.
fn handle_edited(
    event: &IssueCommentEvent,
    state: &PersistedRepoSnapshot,
    bot_name: &str,
    pr_number: PrNumber,
) -> Result<HandlerResult, HandlerError> {
    // For now, treat edits like new comments for predecessor declarations.
    // A more sophisticated implementation would track the authoritative comment ID
    // and handle command removal/changes accordingly.
    //
    // Per DESIGN.md: "When a predecessor declaration is edited, the bot handles
    // several cases: Command changed, Command removed, Command added to non-predecessor comment"
    //
    // TODO: Implement full edit handling with authoritative comment tracking
    let Some(command) = parse_command(&event.body, bot_name) else {
        // Command was removed from comment - this is a no-op for now
        // Full implementation would check if this was the authoritative predecessor comment
        return Ok(HandlerResult::empty());
    };

    match command {
        Command::Predecessor(predecessor) => {
            // Re-validate and update predecessor
            handle_predecessor_command(event, state, pr_number, predecessor)
        }
        // start/stop commands in edited comments are ignored
        // (they should be fresh commands)
        _ => Ok(HandlerResult::empty()),
    }
}

/// Handles a deleted comment.
///
/// If the deleted comment was the authoritative predecessor declaration,
/// removes the predecessor relationship.
fn handle_deleted(
    _event: &IssueCommentEvent,
    _state: &PersistedRepoSnapshot,
    _pr_number: PrNumber,
) -> Result<HandlerResult, HandlerError> {
    // Per DESIGN.md: "When a comment is deleted, the bot checks if it was the
    // authoritative predecessor declaration for a PR. If so:
    // 1. Remove the predecessor relationship from the cached state
    // 2. If a train is running that involves this PR, abort with error"
    //
    // TODO: Implement with authoritative comment ID tracking
    // For now, we don't track comment IDs, so deleted comments are no-ops
    Ok(HandlerResult::empty())
}

/// Handles the `@merge-train predecessor #N` command.
fn handle_predecessor_command(
    event: &IssueCommentEvent,
    state: &PersistedRepoSnapshot,
    pr_number: PrNumber,
    predecessor: PrNumber,
) -> Result<HandlerResult, HandlerError> {
    // Check if PR already has a predecessor declared
    if let Some(existing_pr) = state.prs.get(&pr_number)
        && let Some(existing_pred) = existing_pr.predecessor
    {
        if existing_pred == predecessor {
            // Same predecessor, just acknowledge
            return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
                GitHubEffect::AddReaction {
                    comment_id: event.comment_id,
                    reaction: Reaction::ThumbsUp,
                },
            )]));
        }
        // Different predecessor - reject per DESIGN.md
        // "Subsequent declarations are rejected with error"
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::PostComment {
                pr: pr_number,
                body: format!(
                    "Error: PR #{} already has a predecessor declaration pointing to #{}. \
                     Edit that comment to change predecessors, or delete it first.",
                    pr_number, existing_pred
                ),
            },
        )]));
    }

    // Validate the predecessor exists and is either:
    // 1. Targeting the default branch (root of a stack)
    // 2. Has its own predecessor declaration (part of a stack)
    // 3. Already merged (predecessor's content is in main)
    let predecessor_pr = state.prs.get(&predecessor);
    let predecessor_valid = predecessor_pr.is_some_and(|pr| {
        pr.base_ref == state.default_branch || pr.predecessor.is_some() || pr.state.is_merged()
    });

    if !predecessor_valid && predecessor_pr.is_some() {
        // Predecessor exists but isn't a valid stack member
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::PostComment {
                pr: pr_number,
                body: format!(
                    "Error: PR #{} cannot be a predecessor because it doesn't target {} \
                     and doesn't have its own predecessor declaration.",
                    predecessor, state.default_branch
                ),
            },
        )]));
    }

    // Record the predecessor declaration
    let state_events = vec![StateEventPayload::PredecessorDeclared {
        pr: pr_number,
        predecessor,
    }];

    // Acknowledge with thumbs up reaction
    let effects = vec![Effect::GitHub(GitHubEffect::AddReaction {
        comment_id: event.comment_id,
        reaction: Reaction::ThumbsUp,
    })];

    Ok(HandlerResult::new(state_events, effects))
}

/// Handles the `@merge-train start` command.
fn handle_start_command(
    event: &IssueCommentEvent,
    state: &PersistedRepoSnapshot,
    pr_number: PrNumber,
) -> Result<HandlerResult, HandlerError> {
    // Check if a train already exists for this PR
    if state.active_trains.contains_key(&pr_number) {
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::PostComment {
                pr: pr_number,
                body: format!(
                    "Error: A train is already active for PR #{}. \
                     Use `@merge-train stop` to stop it first.",
                    pr_number
                ),
            },
        )]));
    }

    // Use the cascade engine to start the train
    let engine = CascadeEngine::new(&state.default_branch);

    match engine.start_train(pr_number, &state.prs, &state.active_trains) {
        Ok(result) => {
            // Generate state event for train start
            let state_events = vec![StateEventPayload::TrainStarted {
                root_pr: pr_number,
                current_pr: pr_number,
            }];

            // Combine effects: ack reaction + cascade engine effects
            let mut effects = vec![Effect::GitHub(GitHubEffect::AddReaction {
                comment_id: event.comment_id,
                reaction: Reaction::Rocket,
            })];
            effects.extend(result.effects);

            Ok(HandlerResult::new(state_events, effects))
        }
        Err(e) => {
            // Post error comment
            Ok(HandlerResult::with_effects(vec![
                Effect::GitHub(GitHubEffect::AddReaction {
                    comment_id: event.comment_id,
                    reaction: Reaction::Confused,
                }),
                Effect::GitHub(GitHubEffect::PostComment {
                    pr: pr_number,
                    body: format!("Error starting train: {}", e),
                }),
            ]))
        }
    }
}

/// Handles the `@merge-train stop` command.
fn handle_stop_command(
    event: &IssueCommentEvent,
    state: &PersistedRepoSnapshot,
    pr_number: PrNumber,
    force: bool,
) -> Result<HandlerResult, HandlerError> {
    // Find the train for this PR
    // The PR might be the root, or it might be a descendant in an active train
    let train_root = find_train_for_pr(pr_number, state);

    let Some(root_pr) = train_root else {
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::PostComment {
                pr: pr_number,
                body: "No active train found for this PR.".to_string(),
            },
        )]));
    };

    let Some(train) = state.active_trains.get(&root_pr) else {
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::PostComment {
                pr: pr_number,
                body: "No active train found for this PR.".to_string(),
            },
        )]));
    };

    // Use the cascade engine to stop the train
    let engine = CascadeEngine::new(&state.default_branch);

    match engine.stop_train(train.clone(), force) {
        Ok(result) => {
            // Generate state event for train stop
            let state_events = vec![StateEventPayload::TrainStopped { root_pr }];

            // Combine effects: ack reaction + cascade engine effects
            let mut effects = vec![Effect::GitHub(GitHubEffect::AddReaction {
                comment_id: event.comment_id,
                reaction: Reaction::ThumbsUp,
            })];
            effects.extend(result.effects);

            Ok(HandlerResult::new(state_events, effects))
        }
        Err(e) => {
            // Post error comment
            Ok(HandlerResult::with_effects(vec![
                Effect::GitHub(GitHubEffect::AddReaction {
                    comment_id: event.comment_id,
                    reaction: Reaction::Confused,
                }),
                Effect::GitHub(GitHubEffect::PostComment {
                    pr: pr_number,
                    body: format!("Error stopping train: {}", e),
                }),
            ]))
        }
    }
}

/// Finds the train that contains a given PR.
///
/// Returns the root PR number of the train, or None if the PR isn't in any train.
fn find_train_for_pr(pr_number: PrNumber, state: &PersistedRepoSnapshot) -> Option<PrNumber> {
    // Check if PR is a train root
    if state.active_trains.contains_key(&pr_number) {
        return Some(pr_number);
    }

    // Check if PR is a descendant in any active train
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
    use crate::types::{CachedPr, CommentId, MergeStateStatus, PrState, RepoId, Sha};

    fn make_state() -> PersistedRepoSnapshot {
        PersistedRepoSnapshot::new("main")
    }

    fn make_comment_event(
        action: CommentAction,
        pr_number: Option<u64>,
        body: &str,
    ) -> IssueCommentEvent {
        IssueCommentEvent {
            repo: RepoId::new("owner", "repo"),
            action,
            pr_number: pr_number.map(PrNumber),
            comment_id: CommentId(12345),
            body: body.to_string(),
            author_id: 1,
            author_login: "user".to_string(),
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
    fn ignores_comment_on_issue_not_pr() {
        let event = make_comment_event(CommentAction::Created, None, "@merge-train start");
        let state = make_state();

        let result = handle_issue_comment(&event, &state, "merge-train");
        assert!(matches!(result, Err(HandlerError::NotAPullRequest)));
    }

    #[test]
    fn ignores_comment_without_command() {
        let event = make_comment_event(CommentAction::Created, Some(1), "Just a regular comment");
        let state = make_state();

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn predecessor_command_creates_state_event() {
        let event = make_comment_event(
            CommentAction::Created,
            Some(2),
            "@merge-train predecessor #1",
        );
        let mut state = make_state();
        // Add predecessor PR to cache (targets main, so valid as root)
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        state
            .prs
            .insert(PrNumber(2), make_cached_pr(2, "branch-1", None));

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        assert_eq!(result.state_events.len(), 1);
        assert!(matches!(
            &result.state_events[0],
            StateEventPayload::PredecessorDeclared { pr, predecessor }
            if *pr == PrNumber(2) && *predecessor == PrNumber(1)
        ));

        // Should have a thumbs up reaction
        assert!(result.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::AddReaction {
                reaction: Reaction::ThumbsUp,
                ..
            })
        )));
    }

    #[test]
    fn start_command_on_valid_root_creates_train() {
        let event = make_comment_event(CommentAction::Created, Some(1), "@merge-train start");
        let mut state = make_state();
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // Should have TrainStarted event
        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::TrainStarted { root_pr, current_pr }
            if *root_pr == PrNumber(1) && *current_pr == PrNumber(1)
        )));

        // Should have rocket reaction
        assert!(result.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::AddReaction {
                reaction: Reaction::Rocket,
                ..
            })
        )));
    }

    #[test]
    fn start_command_on_existing_train_returns_error() {
        let event = make_comment_event(CommentAction::Created, Some(1), "@merge-train start");
        let mut state = make_state();
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        // Add existing train
        state
            .active_trains
            .insert(PrNumber(1), crate::types::TrainRecord::new(PrNumber(1)));

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // Should have error comment, no state events
        assert!(result.state_events.is_empty());
        assert!(result.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::PostComment { body, .. })
            if body.contains("already active")
        )));
    }

    #[test]
    fn stop_command_on_active_train_stops_it() {
        let event = make_comment_event(CommentAction::Created, Some(1), "@merge-train stop");
        let mut state = make_state();
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        state
            .active_trains
            .insert(PrNumber(1), crate::types::TrainRecord::new(PrNumber(1)));

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // Should have TrainStopped event
        assert!(result.state_events.iter().any(|e| matches!(
            e,
            StateEventPayload::TrainStopped { root_pr }
            if *root_pr == PrNumber(1)
        )));

        // Should have thumbs up reaction
        assert!(result.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::AddReaction {
                reaction: Reaction::ThumbsUp,
                ..
            })
        )));
    }

    #[test]
    fn stop_command_on_no_train_returns_message() {
        let event = make_comment_event(CommentAction::Created, Some(1), "@merge-train stop");
        let state = make_state();

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // Should have "no active train" comment, no state events
        assert!(result.state_events.is_empty());
        assert!(result.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::PostComment { body, .. })
            if body.contains("No active train")
        )));
    }

    #[test]
    fn duplicate_predecessor_same_value_just_acks() {
        let event = make_comment_event(
            CommentAction::Created,
            Some(2),
            "@merge-train predecessor #1",
        );
        let mut state = make_state();
        // PR 2 already has predecessor #1
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        state
            .prs
            .insert(PrNumber(2), make_cached_pr(2, "branch-1", Some(1)));

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // No state events (already declared)
        assert!(result.state_events.is_empty());
        // Should still ack
        assert!(result.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::AddReaction {
                reaction: Reaction::ThumbsUp,
                ..
            })
        )));
    }

    #[test]
    fn duplicate_predecessor_different_value_rejects() {
        let event = make_comment_event(
            CommentAction::Created,
            Some(2),
            "@merge-train predecessor #3",
        );
        let mut state = make_state();
        // PR 2 already has predecessor #1
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        state
            .prs
            .insert(PrNumber(2), make_cached_pr(2, "branch-1", Some(1)));
        state
            .prs
            .insert(PrNumber(3), make_cached_pr(3, "main", None));

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // No state events (rejected)
        assert!(result.state_events.is_empty());
        // Should have error comment
        assert!(result.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::PostComment { body, .. })
            if body.contains("already has a predecessor")
        )));
    }
}
