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
use crate::types::{PrNumber, TrainError};
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
    // Only handle comments on PRs, not regular issues.
    // Per DESIGN.md: "Returns an empty result for events that don't require any action."
    // Comments on issues (not PRs) are silently ignored - they're not actionable.
    let Some(pr_number) = event.pr_number else {
        return Ok(HandlerResult::empty());
    };

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
///
/// Per DESIGN.md, when a predecessor declaration is edited, the bot handles:
/// 1. Command changed (e.g., `#123` to `#456`): Re-validate and update
/// 2. Command removed: Remove predecessor relationship if authoritative
/// 3. Command added to non-predecessor comment: Rejected if PR already has predecessor
fn handle_edited(
    event: &IssueCommentEvent,
    state: &PersistedRepoSnapshot,
    bot_name: &str,
    pr_number: PrNumber,
) -> Result<HandlerResult, HandlerError> {
    let command = parse_command(&event.body, bot_name);

    // Check if this was the authoritative predecessor comment
    let is_authoritative = state
        .prs
        .get(&pr_number)
        .is_some_and(|pr| pr.predecessor_comment_id == Some(event.comment_id));

    match command {
        Some(Command::Predecessor(predecessor)) => {
            if is_authoritative {
                // This is the authoritative comment being edited to a (possibly new) predecessor.
                // Per DESIGN.md: "Command changed (e.g., `#123` to `#456`): Re-run full
                // validation, update if passes, reject if fails"
                //
                // We handle this specially because handle_predecessor_command would reject
                // "PR already has predecessor" - but editing the authoritative comment is allowed.
                handle_authoritative_predecessor_edit(event, state, pr_number, predecessor)
            } else {
                // This is a non-authoritative comment being edited to add a predecessor.
                // Per DESIGN.md: "Command added to non-predecessor comment: Rejected if
                // the PR already has a predecessor declaration."
                // handle_predecessor_command already handles this case.
                handle_predecessor_command(event, state, pr_number, predecessor)
            }
        }
        Some(_) => {
            // start/stop commands in edited comments are ignored
            // (they should be fresh commands)
            Ok(HandlerResult::empty())
        }
        None => {
            // Command was removed from comment
            if is_authoritative {
                // Per DESIGN.md: "If this was the authoritative predecessor comment for
                // the PR (tracked via comment ID), remove the predecessor relationship."
                let train_involved = find_train_for_pr(pr_number, state);
                if let Some(train_root) = train_involved {
                    // Per DESIGN.md: "If a train is already started and the predecessor
                    // is changed or removed, the bot aborts with an error"
                    let error_message = format!(
                        "Predecessor declaration was removed from comment {} \
                         while a train is running",
                        event.comment_id
                    );
                    let state_events = vec![
                        StateEventPayload::TrainAborted {
                            root_pr: train_root,
                            error: TrainError::new("predecessor_removed", &error_message),
                        },
                        StateEventPayload::PredecessorRemoved {
                            pr: pr_number,
                            comment_id: event.comment_id,
                        },
                    ];
                    let effects = vec![Effect::GitHub(GitHubEffect::PostComment {
                        pr: pr_number,
                        body: format!(
                            "Error: {}. The train has been aborted. \
                             Please re-establish the predecessor relationship and \
                             restart the train.",
                            error_message
                        ),
                    })];
                    return Ok(HandlerResult::new(state_events, effects));
                }

                // Remove the predecessor relationship
                let state_events = vec![StateEventPayload::PredecessorRemoved {
                    pr: pr_number,
                    comment_id: event.comment_id,
                }];

                Ok(HandlerResult::new(state_events, vec![]))
            } else {
                // Not the authoritative comment, nothing to do
                Ok(HandlerResult::empty())
            }
        }
    }
}

/// Handles a deleted comment.
///
/// If the deleted comment was the authoritative predecessor declaration,
/// removes the predecessor relationship.
fn handle_deleted(
    event: &IssueCommentEvent,
    state: &PersistedRepoSnapshot,
    pr_number: PrNumber,
) -> Result<HandlerResult, HandlerError> {
    // Per DESIGN.md: "When a comment is deleted, the bot checks if it was the
    // authoritative predecessor declaration for a PR. If so:
    // 1. Remove the predecessor relationship from the cached state
    // 2. If a train is running that involves this PR, abort with error"
    //
    // Check if this comment was the authoritative predecessor declaration
    let Some(pr) = state.prs.get(&pr_number) else {
        return Ok(HandlerResult::empty());
    };

    // Only process if this was the authoritative predecessor comment
    if pr.predecessor_comment_id != Some(event.comment_id) {
        return Ok(HandlerResult::empty());
    }

    // Check if a train is running that involves this PR
    let train_involved = find_train_for_pr(pr_number, state);
    if let Some(train_root) = train_involved {
        // Per DESIGN.md: "If a train is running that involves this PR, abort with error"
        let error_message = format!(
            "Predecessor declaration (comment {}) was deleted while a train is running",
            event.comment_id
        );
        let state_events = vec![
            StateEventPayload::TrainAborted {
                root_pr: train_root,
                error: TrainError::new("predecessor_deleted", &error_message),
            },
            StateEventPayload::PredecessorRemoved {
                pr: pr_number,
                comment_id: event.comment_id,
            },
        ];
        let effects = vec![Effect::GitHub(GitHubEffect::PostComment {
            pr: pr_number,
            body: format!(
                "Error: {}. The train has been aborted. Please re-establish the \
                 predecessor relationship and restart the train.",
                error_message
            ),
        })];
        return Ok(HandlerResult::new(state_events, effects));
    }

    // Remove the predecessor relationship
    let state_events = vec![StateEventPayload::PredecessorRemoved {
        pr: pr_number,
        comment_id: event.comment_id,
    }];

    Ok(HandlerResult::new(state_events, vec![]))
}

/// Handles editing the authoritative predecessor comment to change the predecessor.
///
/// This is called when the user edits the comment that established the predecessor
/// relationship, changing it to point to a different PR.
fn handle_authoritative_predecessor_edit(
    event: &IssueCommentEvent,
    state: &PersistedRepoSnapshot,
    pr_number: PrNumber,
    new_predecessor: PrNumber,
) -> Result<HandlerResult, HandlerError> {
    // Check if the predecessor is the same (no-op, just acknowledge)
    if let Some(existing_pr) = state.prs.get(&pr_number)
        && existing_pr.predecessor == Some(new_predecessor)
    {
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::AddReaction {
                comment_id: event.comment_id,
                reaction: Reaction::ThumbsUp,
            },
        )]));
    }

    // Check if a train is running that involves this PR
    let train_involved = find_train_for_pr(pr_number, state);
    if train_involved.is_some() {
        // Per DESIGN.md: "If a train is already started and the predecessor is changed
        // or removed, the bot aborts with an error"
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::PostComment {
                pr: pr_number,
                body: format!(
                    "Error: Cannot change predecessor from #{} to #{} while a train is running. \
                     Stop the train first with `@merge-train stop`.",
                    state
                        .prs
                        .get(&pr_number)
                        .and_then(|pr| pr.predecessor)
                        .map_or("?".to_string(), |p| p.0.to_string()),
                    new_predecessor
                ),
            },
        )]));
    }

    // Validate the new predecessor exists in cache
    let Some(pred_pr) = state.prs.get(&new_predecessor) else {
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::PostComment {
                pr: pr_number,
                body: format!(
                    "Error: PR #{} not found. The predecessor must be an open PR in this repository.",
                    new_predecessor
                ),
            },
        )]));
    };

    // Validate the predecessor is valid (same validation as handle_predecessor_command):
    // 1. Open AND (targeting the default branch OR has its own predecessor declaration)
    // 2. Already merged (predecessor's content is in main)
    //
    // Per DESIGN.md: "A PR whose predecessor was closed without merge is **orphaned**"
    let predecessor_valid = if pred_pr.state.is_merged() {
        true
    } else if pred_pr.state.is_open() {
        // Open predecessor must target default branch or have its own predecessor
        pred_pr.base_ref == state.default_branch || pred_pr.predecessor.is_some()
    } else {
        // Closed without merge - not valid
        false
    };

    if !predecessor_valid {
        // Predecessor exists but isn't a valid stack member
        let error_msg = if !pred_pr.state.is_open() && !pred_pr.state.is_merged() {
            format!(
                "Error: PR #{} was closed without being merged. \
                 The predecessor must be an open PR or a merged PR.",
                new_predecessor
            )
        } else {
            format!(
                "Error: PR #{} cannot be a predecessor because it doesn't target {} \
                 and doesn't have its own predecessor declaration.",
                new_predecessor, state.default_branch
            )
        };
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::PostComment {
                pr: pr_number,
                body: error_msg,
            },
        )]));
    }

    // Emit the updated predecessor declaration
    let state_events = vec![StateEventPayload::PredecessorDeclared {
        pr: pr_number,
        predecessor: new_predecessor,
        comment_id: Some(event.comment_id),
    }];

    let effects = vec![Effect::GitHub(GitHubEffect::AddReaction {
        comment_id: event.comment_id,
        reaction: Reaction::ThumbsUp,
    })];

    Ok(HandlerResult::new(state_events, effects))
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
        // "Subsequent declarations are rejected with error: PR already has predecessor
        // declaration in comment #C pointing to #N. Edit that comment to change
        // predecessors, or delete it first."
        let comment_ref = match existing_pr.predecessor_comment_id {
            Some(cid) => format!(" in comment {}", cid),
            None => String::new(),
        };
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::PostComment {
                pr: pr_number,
                body: format!(
                    "Error: PR #{} already has a predecessor declaration{} pointing to #{}. \
                     Edit that comment to change predecessors, or delete it first.",
                    pr_number, comment_ref, existing_pred
                ),
            },
        )]));
    }

    // Validate the target PR exists in cache
    if !state.prs.contains_key(&pr_number) {
        return Err(HandlerError::PrNotFound(pr_number.0));
    }

    // Validate the predecessor exists and is either:
    // 1. Open AND (targeting the default branch OR has its own predecessor declaration)
    // 2. Already merged (predecessor's content is in main)
    //
    // Per DESIGN.md: "A PR whose predecessor was closed without merge is **orphaned**"
    let Some(predecessor_pr) = state.prs.get(&predecessor) else {
        // Predecessor PR not found in cache
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::PostComment {
                pr: pr_number,
                body: format!(
                    "Error: PR #{} not found. The predecessor must be an open PR in this repository.",
                    predecessor
                ),
            },
        )]));
    };

    // Check if predecessor is merged (always valid)
    let predecessor_valid = if predecessor_pr.state.is_merged() {
        true
    } else if predecessor_pr.state.is_open() {
        // Open predecessor must target default branch or have its own predecessor
        predecessor_pr.base_ref == state.default_branch || predecessor_pr.predecessor.is_some()
    } else {
        // Closed without merge - not valid
        false
    };

    if !predecessor_valid {
        // Predecessor exists but isn't a valid stack member
        let error_msg = if !predecessor_pr.state.is_open() && !predecessor_pr.state.is_merged() {
            format!(
                "Error: PR #{} was closed without being merged. \
                 The predecessor must be an open PR or a merged PR.",
                predecessor
            )
        } else {
            format!(
                "Error: PR #{} cannot be a predecessor because it doesn't target {} \
                 and doesn't have its own predecessor declaration.",
                predecessor, state.default_branch
            )
        };
        return Ok(HandlerResult::with_effects(vec![Effect::GitHub(
            GitHubEffect::PostComment {
                pr: pr_number,
                body: error_msg,
            },
        )]));
    }

    // Record the predecessor declaration with the authoritative comment ID
    let state_events = vec![StateEventPayload::PredecessorDeclared {
        pr: pr_number,
        predecessor,
        comment_id: Some(event.comment_id),
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

    fn make_cached_pr_with_comment(
        number: u64,
        base_ref: &str,
        predecessor: Option<u64>,
        predecessor_comment_id: Option<u64>,
    ) -> CachedPr {
        let mut pr = make_cached_pr(number, base_ref, predecessor);
        pr.predecessor_comment_id = predecessor_comment_id.map(CommentId);
        pr
    }

    #[test]
    fn ignores_comment_on_issue_not_pr() {
        let event = make_comment_event(CommentAction::Created, None, "@merge-train start");
        let state = make_state();

        // Comments on issues (not PRs) should be silently ignored, not an error.
        // Per DESIGN.md: "Returns an empty result for events that don't require any action."
        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();
        assert!(result.is_empty());
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
            StateEventPayload::PredecessorDeclared { pr, predecessor, comment_id }
            if *pr == PrNumber(2) && *predecessor == PrNumber(1) && *comment_id == Some(CommentId(12345))
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

    #[test]
    fn predecessor_not_found_rejects() {
        let event = make_comment_event(
            CommentAction::Created,
            Some(2),
            "@merge-train predecessor #99",
        );
        let mut state = make_state();
        // Only PR 2 exists, predecessor #99 does not
        state
            .prs
            .insert(PrNumber(2), make_cached_pr(2, "main", None));

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // No state events (rejected)
        assert!(result.state_events.is_empty());
        // Should have error comment about PR not found
        assert!(
            result.effects.iter().any(|e| matches!(
                e,
                Effect::GitHub(GitHubEffect::PostComment { body, .. })
                if body.contains("#99 not found")
            )),
            "Expected error about PR not found, got: {:?}",
            result.effects
        );
    }

    fn make_cached_pr_with_state(
        number: u64,
        base_ref: &str,
        predecessor: Option<u64>,
        state: PrState,
    ) -> CachedPr {
        CachedPr::new(
            PrNumber(number),
            Sha::parse("a".repeat(40)).unwrap(),
            format!("branch-{}", number),
            base_ref.to_string(),
            predecessor.map(PrNumber),
            state,
            MergeStateStatus::Clean,
            false,
        )
    }

    #[test]
    fn predecessor_closed_without_merge_rejects() {
        let event = make_comment_event(
            CommentAction::Created,
            Some(2),
            "@merge-train predecessor #1",
        );
        let mut state = make_state();
        // PR 1 is closed without merge (not a valid predecessor)
        state.prs.insert(
            PrNumber(1),
            make_cached_pr_with_state(1, "main", None, PrState::Closed),
        );
        state.prs.insert(
            PrNumber(2),
            make_cached_pr_with_state(2, "branch-1", None, PrState::Open),
        );

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // No state events (rejected)
        assert!(result.state_events.is_empty());
        // Should have error comment about closed PR
        assert!(
            result.effects.iter().any(|e| matches!(
                e,
                Effect::GitHub(GitHubEffect::PostComment { body, .. })
                if body.contains("closed without being merged")
            )),
            "Expected error about closed PR, got: {:?}",
            result.effects
        );
    }

    #[test]
    fn predecessor_merged_is_valid() {
        let event = make_comment_event(
            CommentAction::Created,
            Some(2),
            "@merge-train predecessor #1",
        );
        let mut state = make_state();
        // PR 1 was merged (valid as predecessor per late-addition handling)
        state.prs.insert(
            PrNumber(1),
            make_cached_pr_with_state(
                1,
                "main",
                None,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
                },
            ),
        );
        state.prs.insert(
            PrNumber(2),
            make_cached_pr_with_state(2, "branch-1", None, PrState::Open),
        );

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // Should have PredecessorDeclared event
        assert_eq!(result.state_events.len(), 1);
        assert!(matches!(
            &result.state_events[0],
            StateEventPayload::PredecessorDeclared { pr, predecessor, comment_id }
            if *pr == PrNumber(2) && *predecessor == PrNumber(1) && *comment_id == Some(CommentId(12345))
        ));
    }

    #[test]
    fn target_pr_not_in_cache_returns_error() {
        let event = make_comment_event(
            CommentAction::Created,
            Some(2),
            "@merge-train predecessor #1",
        );
        let mut state = make_state();
        // PR 1 exists but PR 2 (the target) does not
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));

        let result = handle_issue_comment(&event, &state, "merge-train");

        // Should return PrNotFound error
        assert!(matches!(result, Err(HandlerError::PrNotFound(2))));
    }

    // ─── Delete handling tests ───────────────────────────────────────────────

    #[test]
    fn delete_authoritative_comment_removes_predecessor() {
        let event = make_comment_event(CommentAction::Deleted, Some(2), "");
        let mut state = make_state();
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        // PR 2 has predecessor #1 via comment 12345 (matching the event's comment_id)
        state.prs.insert(
            PrNumber(2),
            make_cached_pr_with_comment(2, "branch-1", Some(1), Some(12345)),
        );

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // Should emit PredecessorRemoved event
        assert_eq!(result.state_events.len(), 1);
        assert!(matches!(
            &result.state_events[0],
            StateEventPayload::PredecessorRemoved { pr, comment_id }
            if *pr == PrNumber(2) && *comment_id == CommentId(12345)
        ));
    }

    #[test]
    fn delete_non_authoritative_comment_is_noop() {
        let event = make_comment_event(CommentAction::Deleted, Some(2), "");
        let mut state = make_state();
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        // PR 2 has predecessor #1 via a DIFFERENT comment (99999, not 12345)
        state.prs.insert(
            PrNumber(2),
            make_cached_pr_with_comment(2, "branch-1", Some(1), Some(99999)),
        );

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // Should be no-op (not the authoritative comment)
        assert!(result.is_empty());
    }

    #[test]
    fn delete_comment_unknown_pr_is_noop() {
        let event = make_comment_event(CommentAction::Deleted, Some(99), "");
        let state = make_state();

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // PR not in cache, should be no-op
        assert!(result.is_empty());
    }

    // ─── Edit handling tests ─────────────────────────────────────────────────

    #[test]
    fn edit_remove_command_from_authoritative_removes_predecessor() {
        // Edit the authoritative comment to remove the command
        let event = make_comment_event(CommentAction::Edited, Some(2), "Just some text now");
        let mut state = make_state();
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        // PR 2 has predecessor #1 via comment 12345
        state.prs.insert(
            PrNumber(2),
            make_cached_pr_with_comment(2, "branch-1", Some(1), Some(12345)),
        );

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // Should emit PredecessorRemoved event
        assert_eq!(result.state_events.len(), 1);
        assert!(matches!(
            &result.state_events[0],
            StateEventPayload::PredecessorRemoved { pr, comment_id }
            if *pr == PrNumber(2) && *comment_id == CommentId(12345)
        ));
    }

    #[test]
    fn edit_remove_command_from_non_authoritative_is_noop() {
        // Edit a non-authoritative comment (comment_id doesn't match)
        let event = make_comment_event(CommentAction::Edited, Some(2), "Just some text now");
        let mut state = make_state();
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        // PR 2 has predecessor #1 via a DIFFERENT comment (99999)
        state.prs.insert(
            PrNumber(2),
            make_cached_pr_with_comment(2, "branch-1", Some(1), Some(99999)),
        );

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // Should be no-op
        assert!(result.is_empty());
    }

    #[test]
    fn edit_change_predecessor_in_authoritative_updates() {
        // Edit the authoritative comment to change predecessor from #1 to #3
        let event = make_comment_event(
            CommentAction::Edited,
            Some(2),
            "@merge-train predecessor #3",
        );
        let mut state = make_state();
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        state
            .prs
            .insert(PrNumber(3), make_cached_pr(3, "main", None));
        // PR 2 has predecessor #1 via comment 12345 (the authoritative comment)
        state.prs.insert(
            PrNumber(2),
            make_cached_pr_with_comment(2, "branch-1", Some(1), Some(12345)),
        );

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // Should emit new PredecessorDeclared event (updating the predecessor)
        assert_eq!(result.state_events.len(), 1);
        assert!(matches!(
            &result.state_events[0],
            StateEventPayload::PredecessorDeclared { pr, predecessor, comment_id }
            if *pr == PrNumber(2) && *predecessor == PrNumber(3) && *comment_id == Some(CommentId(12345))
        ));
    }

    #[test]
    fn edit_add_command_to_non_predecessor_comment_rejected_if_already_has_predecessor() {
        // Try to add a predecessor command to a different comment when PR already has one
        let event = make_comment_event(
            CommentAction::Edited,
            Some(2),
            "@merge-train predecessor #3",
        );
        let mut state = make_state();
        state
            .prs
            .insert(PrNumber(1), make_cached_pr(1, "main", None));
        state
            .prs
            .insert(PrNumber(3), make_cached_pr(3, "main", None));
        // PR 2 already has predecessor #1 via a DIFFERENT comment (99999)
        state.prs.insert(
            PrNumber(2),
            make_cached_pr_with_comment(2, "branch-1", Some(1), Some(99999)),
        );

        let result = handle_issue_comment(&event, &state, "merge-train").unwrap();

        // Should be rejected with error comment
        assert!(result.state_events.is_empty());
        assert!(result.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::PostComment { body, .. })
            if body.contains("already has a predecessor")
        )));
    }
}
