//! Event priority classification for webhook processing.
//!
//! The per-repo worker uses a priority queue to process events. Stop commands
//! get higher priority to allow humans to halt cascades promptly, even when
//! there are many pending events.
//!
//! # Priority Levels
//!
//! - `High`: Stop commands (`@merge-train stop`, `@merge-train stop --force`)
//! - `Normal`: All other events
//!
//! From DESIGN.md:
//! > "priority: stop > others"

use serde::{Deserialize, Serialize};

use crate::commands::{Command, parse_command};

use super::events::{CommentAction, GitHubEvent, IssueCommentEvent};

/// Default bot name for command parsing.
const DEFAULT_BOT_NAME: &str = "merge-train";

/// Event priority level.
///
/// Higher-priority events are processed before lower-priority events in the
/// per-repo worker queue.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub enum EventPriority {
    /// Normal priority for most events.
    #[default]
    Normal = 0,
    /// High priority for stop commands (allows humans to halt cascades promptly).
    High = 1,
}

/// Classifies the priority of a GitHub event.
///
/// Stop commands (`@merge-train stop` and `@merge-train stop --force`) get
/// `High` priority to ensure they are processed promptly, allowing humans to
/// halt cascades even when there are many pending events.
///
/// All other events get `Normal` priority.
///
/// # Arguments
///
/// * `event` - The parsed GitHub event
///
/// # Examples
///
/// ```
/// use merge_train::webhooks::events::{GitHubEvent, IssueCommentEvent, CommentAction};
/// use merge_train::webhooks::priority::{classify_priority, EventPriority};
/// use merge_train::types::{CommentId, PrNumber, RepoId};
///
/// let stop_event = IssueCommentEvent {
///     repo: RepoId::new("owner", "repo"),
///     action: CommentAction::Created,
///     pr_number: Some(PrNumber(42)),
///     comment_id: CommentId(123),
///     body: "@merge-train stop".to_string(),
///     author_id: 1,
///     author_login: "user".to_string(),
/// };
///
/// assert_eq!(
///     classify_priority(&GitHubEvent::IssueComment(stop_event)),
///     EventPriority::High
/// );
/// ```
pub fn classify_priority(event: &GitHubEvent) -> EventPriority {
    classify_priority_with_bot_name(event, DEFAULT_BOT_NAME)
}

/// Classifies priority with a custom bot name.
///
/// This is useful for testing or deployments with different bot names.
pub fn classify_priority_with_bot_name(event: &GitHubEvent, bot_name: &str) -> EventPriority {
    match event {
        GitHubEvent::IssueComment(comment) => classify_comment_priority(comment, bot_name),
        // All other events are normal priority
        GitHubEvent::PullRequest(_)
        | GitHubEvent::CheckSuite(_)
        | GitHubEvent::Status(_)
        | GitHubEvent::PullRequestReview(_) => EventPriority::Normal,
    }
}

/// Checks if an issue comment contains a stop command.
fn classify_comment_priority(comment: &IssueCommentEvent, bot_name: &str) -> EventPriority {
    // Commands are only valid on PRs, not regular issues.
    // Don't elevate priority for comments on non-PR issues.
    if comment.pr_number.is_none() {
        return EventPriority::Normal;
    }

    // Only check created/edited comments for commands
    // Deleted comments can't contain commands (body is empty)
    match comment.action {
        CommentAction::Created | CommentAction::Edited => {
            if is_stop_command(&comment.body, bot_name) {
                EventPriority::High
            } else {
                EventPriority::Normal
            }
        }
        CommentAction::Deleted => EventPriority::Normal,
    }
}

/// Returns true if the text contains a stop command.
fn is_stop_command(text: &str, bot_name: &str) -> bool {
    match parse_command(text, bot_name) {
        Some(Command::Stop) | Some(Command::StopForce) => true,
        Some(Command::Start) | Some(Command::Predecessor(_)) | None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CommentId, PrNumber, RepoId, Sha};
    use crate::webhooks::events::{
        CheckSuiteAction, CheckSuiteEvent, PrAction, PullRequestEvent, PullRequestReviewEvent,
        ReviewAction, ReviewState, StatusEvent, StatusState,
    };
    use proptest::prelude::*;

    fn make_comment(body: &str, action: CommentAction) -> GitHubEvent {
        GitHubEvent::IssueComment(IssueCommentEvent {
            repo: RepoId::new("owner", "repo"),
            action,
            pr_number: Some(PrNumber(42)),
            comment_id: CommentId(123),
            body: body.to_string(),
            author_id: 1,
            author_login: "user".to_string(),
        })
    }

    fn make_pr_event() -> GitHubEvent {
        GitHubEvent::PullRequest(PullRequestEvent {
            repo: RepoId::new("owner", "repo"),
            action: PrAction::Closed,
            pr_number: PrNumber(42),
            merged: true,
            merge_commit_sha: Some(Sha::parse("a".repeat(40)).unwrap()),
            head_sha: Sha::parse("b".repeat(40)).unwrap(),
            base_branch: "main".to_string(),
            head_branch: "feature".to_string(),
            is_draft: false,
            author_id: 1,
        })
    }

    fn make_check_suite_event() -> GitHubEvent {
        GitHubEvent::CheckSuite(CheckSuiteEvent {
            repo: RepoId::new("owner", "repo"),
            action: CheckSuiteAction::Completed,
            suite_id: 12345,
            head_sha: Sha::parse("c".repeat(40)).unwrap(),
            conclusion: Some("success".to_string()),
            pull_requests: vec![PrNumber(42)],
            updated_at: chrono::Utc::now(),
        })
    }

    fn make_status_event() -> GitHubEvent {
        GitHubEvent::Status(StatusEvent {
            repo: RepoId::new("owner", "repo"),
            sha: Sha::parse("d".repeat(40)).unwrap(),
            state: StatusState::Success,
            context: "ci/test".to_string(),
            description: None,
            target_url: None,
        })
    }

    fn make_review_event() -> GitHubEvent {
        GitHubEvent::PullRequestReview(PullRequestReviewEvent {
            repo: RepoId::new("owner", "repo"),
            action: ReviewAction::Submitted,
            pr_number: PrNumber(42),
            review_id: 67890,
            state: ReviewState::Approved,
            reviewer_id: 100,
            reviewer_login: "reviewer".to_string(),
            body: "LGTM".to_string(),
        })
    }

    // ========================================================================
    // Stop commands should be High priority
    // ========================================================================

    #[test]
    fn stop_command_is_high_priority() {
        let event = make_comment("@merge-train stop", CommentAction::Created);
        assert_eq!(classify_priority(&event), EventPriority::High);
    }

    #[test]
    fn stop_force_command_is_high_priority() {
        let event = make_comment("@merge-train stop --force", CommentAction::Created);
        assert_eq!(classify_priority(&event), EventPriority::High);
    }

    #[test]
    fn stop_in_edited_comment_is_high_priority() {
        let event = make_comment("@merge-train stop", CommentAction::Edited);
        assert_eq!(classify_priority(&event), EventPriority::High);
    }

    #[test]
    fn stop_with_surrounding_text_is_high_priority() {
        let event = make_comment(
            "Please @merge-train stop this cascade, there's an issue",
            CommentAction::Created,
        );
        assert_eq!(classify_priority(&event), EventPriority::High);
    }

    #[test]
    fn stop_case_insensitive_is_high_priority() {
        let event = make_comment("@Merge-Train STOP", CommentAction::Created);
        assert_eq!(classify_priority(&event), EventPriority::High);
    }

    // ========================================================================
    // Non-stop commands should be Normal priority
    // ========================================================================

    #[test]
    fn start_command_is_normal_priority() {
        let event = make_comment("@merge-train start", CommentAction::Created);
        assert_eq!(classify_priority(&event), EventPriority::Normal);
    }

    #[test]
    fn predecessor_command_is_normal_priority() {
        let event = make_comment("@merge-train predecessor #123", CommentAction::Created);
        assert_eq!(classify_priority(&event), EventPriority::Normal);
    }

    #[test]
    fn no_command_is_normal_priority() {
        let event = make_comment("LGTM! Merging.", CommentAction::Created);
        assert_eq!(classify_priority(&event), EventPriority::Normal);
    }

    #[test]
    fn deleted_comment_is_normal_priority() {
        // Deleted comments have empty body, no command to parse
        let event = make_comment("", CommentAction::Deleted);
        assert_eq!(classify_priority(&event), EventPriority::Normal);
    }

    #[test]
    fn stop_command_on_issue_not_pr_is_normal_priority() {
        // Commands are only valid on PRs, not regular issues
        let event = GitHubEvent::IssueComment(IssueCommentEvent {
            repo: RepoId::new("owner", "repo"),
            action: CommentAction::Created,
            pr_number: None, // Not a PR
            comment_id: CommentId(123),
            body: "@merge-train stop".to_string(),
            author_id: 1,
            author_login: "user".to_string(),
        });
        assert_eq!(classify_priority(&event), EventPriority::Normal);
    }

    // ========================================================================
    // All other event types should be Normal priority
    // ========================================================================

    #[test]
    fn pull_request_event_is_normal_priority() {
        let event = make_pr_event();
        assert_eq!(classify_priority(&event), EventPriority::Normal);
    }

    #[test]
    fn check_suite_event_is_normal_priority() {
        let event = make_check_suite_event();
        assert_eq!(classify_priority(&event), EventPriority::Normal);
    }

    #[test]
    fn status_event_is_normal_priority() {
        let event = make_status_event();
        assert_eq!(classify_priority(&event), EventPriority::Normal);
    }

    #[test]
    fn review_event_is_normal_priority() {
        let event = make_review_event();
        assert_eq!(classify_priority(&event), EventPriority::Normal);
    }

    // ========================================================================
    // Custom bot name
    // ========================================================================

    #[test]
    fn custom_bot_name_stop_is_high_priority() {
        let event = GitHubEvent::IssueComment(IssueCommentEvent {
            repo: RepoId::new("owner", "repo"),
            action: CommentAction::Created,
            pr_number: Some(PrNumber(42)),
            comment_id: CommentId(123),
            body: "@my-custom-bot stop".to_string(),
            author_id: 1,
            author_login: "user".to_string(),
        });
        assert_eq!(
            classify_priority_with_bot_name(&event, "my-custom-bot"),
            EventPriority::High
        );
    }

    #[test]
    fn custom_bot_name_does_not_match_default() {
        // Using custom bot name, default trigger shouldn't match
        let event = make_comment("@merge-train stop", CommentAction::Created);
        assert_eq!(
            classify_priority_with_bot_name(&event, "other-bot"),
            EventPriority::Normal
        );
    }

    // ========================================================================
    // Priority ordering
    // ========================================================================

    #[test]
    fn high_priority_greater_than_normal() {
        assert!(EventPriority::High > EventPriority::Normal);
    }

    #[test]
    fn priority_ordering() {
        let mut priorities = vec![
            EventPriority::Normal,
            EventPriority::High,
            EventPriority::Normal,
        ];
        priorities.sort();
        assert_eq!(
            priorities,
            vec![
                EventPriority::Normal,
                EventPriority::Normal,
                EventPriority::High
            ]
        );
    }

    // ========================================================================
    // Property tests
    // ========================================================================

    proptest! {
        /// Valid stop commands always get High priority.
        ///
        /// Note: Commands are only recognized when the command word is followed by
        /// whitespace or end-of-string. Text like `@merge-train stop.` (punctuation
        /// immediately after the command) is NOT a valid command—the parser extracts
        /// `stop.` as the command word, which doesn't match `"stop"`.
        #[test]
        fn prop_stop_commands_are_high_priority(
            prefix in "[a-zA-Z0-9 !?.,:;]{0,50}",
            // Suffix must start with whitespace (if non-empty) to produce valid commands.
            // Punctuation immediately after the command (e.g., "stop.") is invalid.
            suffix in "( [a-zA-Z0-9!?.,:;]{0,49})?",
            force in proptest::bool::ANY,
        ) {
            let stop_cmd = if force { "stop --force" } else { "stop" };

            // Need to ensure there's a word boundary before @
            // If prefix ends with alphanumeric, insert a space before the command
            let body = if prefix.chars().last().is_none_or(|c| !c.is_alphanumeric()) {
                format!("{}@merge-train {}{}", prefix, stop_cmd, suffix)
            } else {
                format!("{} @merge-train {}{}", prefix, stop_cmd, suffix)
            };

            let event = make_comment(&body, CommentAction::Created);
            prop_assert_eq!(classify_priority(&event), EventPriority::High);
        }

        /// Non-stop issue comments get Normal priority.
        ///
        /// Uses a regex that CAN generate strings with `@` and `-` so the
        /// prop_assume actually filters something. This ensures we test
        /// realistic comment text including @ mentions that aren't commands.
        #[test]
        fn prop_non_stop_comments_are_normal_priority(
            text in "[a-zA-Z0-9 @!?.,:;-]{0,100}",
        ) {
            // Use parse_command to check for stop commands, which handles all
            // whitespace variations (tabs, multiple spaces, etc.)
            prop_assume!(!matches!(
                parse_command(&text, DEFAULT_BOT_NAME),
                Some(Command::Stop) | Some(Command::StopForce)
            ));

            let event = make_comment(&text, CommentAction::Created);
            prop_assert_eq!(classify_priority(&event), EventPriority::Normal);
        }

        /// Priority serde roundtrip.
        #[test]
        fn prop_priority_serde_roundtrip(high in proptest::bool::ANY) {
            let priority = if high { EventPriority::High } else { EventPriority::Normal };
            let json = serde_json::to_string(&priority).unwrap();
            let parsed: EventPriority = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(priority, parsed);
        }
    }
}
