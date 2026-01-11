//! GitHub webhook event types.
//!
//! This module defines typed representations of GitHub webhook events that the
//! merge train bot handles. Each event type corresponds to a GitHub webhook event
//! with the fields we need for processing.
//!
//! # Event Types
//!
//! The bot processes these webhook events (from DESIGN.md Event Handling table):
//!
//! - `issue_comment` - Command parsing (`@merge-train predecessor/start/stop`)
//! - `pull_request` - PR lifecycle (opened, closed, edited, synchronize)
//! - `check_suite` - CI completion (Checks API)
//! - `status` - CI completion (legacy Status API)
//! - `pull_request_review` - Review submitted/dismissed

use serde::{Deserialize, Serialize};

use crate::types::{CommentId, PrNumber, RepoId, Sha};

/// A parsed GitHub webhook event.
///
/// This enum contains only the event types the bot cares about. Unknown or
/// irrelevant events are represented by returning `None` from the parser.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GitHubEvent {
    /// An issue or PR comment was created, edited, or deleted.
    ///
    /// Note: In GitHub's API, PR comments on the conversation tab are delivered
    /// as `issue_comment` events, not `pull_request_review_comment` events.
    IssueComment(IssueCommentEvent),

    /// A pull request was opened, closed, edited, or synchronized.
    PullRequest(PullRequestEvent),

    /// A check suite completed (GitHub Checks API).
    ///
    /// This is the modern CI mechanism. Most CI systems (GitHub Actions, etc.)
    /// use this API.
    CheckSuite(CheckSuiteEvent),

    /// A commit status was updated (legacy Status API).
    ///
    /// Some CI systems still use the Status API instead of Checks. The bot
    /// tracks both mechanisms per-PR to avoid deadlocks.
    Status(StatusEvent),

    /// A pull request review was submitted or dismissed.
    PullRequestReview(PullRequestReviewEvent),
}

impl GitHubEvent {
    /// Returns the repository this event belongs to.
    pub fn repo_id(&self) -> &RepoId {
        match self {
            GitHubEvent::IssueComment(e) => &e.repo,
            GitHubEvent::PullRequest(e) => &e.repo,
            GitHubEvent::CheckSuite(e) => &e.repo,
            GitHubEvent::Status(e) => &e.repo,
            GitHubEvent::PullRequestReview(e) => &e.repo,
        }
    }
}

/// Action performed on an issue comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommentAction {
    /// Comment was created.
    Created,
    /// Comment was edited.
    Edited,
    /// Comment was deleted.
    Deleted,
}

/// An issue/PR comment event.
///
/// In GitHub's model, comments on the PR conversation tab are "issue comments"
/// even when they're on a PR. This is where `@merge-train` commands appear.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueCommentEvent {
    /// The repository.
    pub repo: RepoId,

    /// The action that triggered this event.
    pub action: CommentAction,

    /// The PR number (issue number for PRs).
    ///
    /// This is only set if the comment is on a pull request, not a regular issue.
    /// Commands are only valid on PRs.
    pub pr_number: Option<PrNumber>,

    /// The comment ID.
    pub comment_id: CommentId,

    /// The comment body text.
    ///
    /// For `deleted` actions, this will be empty.
    pub body: String,

    /// The comment author's user ID.
    pub author_id: u64,

    /// The comment author's login name.
    pub author_login: String,
}

/// Action performed on a pull request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrAction {
    /// PR was opened.
    Opened,
    /// PR was closed (merged or not).
    Closed,
    /// PR was edited (title, body, or base branch changed).
    Edited,
    /// PR head was updated (new commits pushed).
    Synchronize,
    /// PR was reopened.
    Reopened,
    /// PR was converted to draft.
    ConvertedToDraft,
    /// PR was marked ready for review.
    ReadyForReview,
}

/// A pull request event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestEvent {
    /// The repository.
    pub repo: RepoId,

    /// The action that triggered this event.
    pub action: PrAction,

    /// The PR number.
    pub pr_number: PrNumber,

    /// Whether the PR was merged (only meaningful for `closed` action).
    pub merged: bool,

    /// The merge commit SHA (only set if merged).
    pub merge_commit_sha: Option<Sha>,

    /// The current head SHA of the PR branch.
    pub head_sha: Sha,

    /// The base branch name (e.g., "main" or the predecessor's branch).
    pub base_branch: String,

    /// The head branch name (the PR's source branch).
    pub head_branch: String,

    /// Whether the PR is a draft.
    pub is_draft: bool,

    /// The PR author's user ID.
    pub author_id: u64,
}

/// A check suite event (GitHub Checks API).
///
/// Check suites group multiple check runs. When all checks in a suite complete,
/// GitHub emits a `check_suite.completed` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckSuiteEvent {
    /// The repository.
    pub repo: RepoId,

    /// The action that triggered this event.
    pub action: CheckSuiteAction,

    /// The head SHA this check suite is for.
    pub head_sha: Sha,

    /// The conclusion of the check suite (only set for `completed` action).
    ///
    /// Possible values: success, failure, neutral, cancelled, timed_out,
    /// action_required, stale, skipped.
    pub conclusion: Option<String>,

    /// Pull request numbers associated with this check suite.
    ///
    /// A check suite can be associated with multiple PRs if the same commit
    /// is the head of multiple PRs.
    pub pull_requests: Vec<PrNumber>,
}

/// Action for check suite events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckSuiteAction {
    /// Check suite was created.
    Created,
    /// Check suite was requested to run.
    Requested,
    /// Check suite was re-requested.
    Rerequested,
    /// Check suite completed.
    Completed,
}

/// State of a commit status (legacy Status API).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusState {
    /// Check is pending.
    Pending,
    /// Check succeeded.
    Success,
    /// Check failed.
    Failure,
    /// Check errored.
    Error,
}

impl StatusState {
    /// Returns true if this is a terminal state (not pending).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            StatusState::Success | StatusState::Failure | StatusState::Error
        )
    }
}

/// A commit status event (legacy Status API).
///
/// Some CI systems use the Status API instead of Checks. The bot tracks both
/// mechanisms per-PR to avoid deadlocks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusEvent {
    /// The repository.
    pub repo: RepoId,

    /// The commit SHA this status is for.
    pub sha: Sha,

    /// The state of the status.
    pub state: StatusState,

    /// The context (name) of the status check.
    ///
    /// E.g., "ci/jenkins", "codecov/patch".
    pub context: String,

    /// Optional description.
    pub description: Option<String>,

    /// Optional target URL for details.
    pub target_url: Option<String>,
}

/// Action for pull request review events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewAction {
    /// Review was submitted.
    Submitted,
    /// Review was dismissed.
    Dismissed,
    /// Review was edited.
    Edited,
}

/// State of a pull request review.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReviewState {
    /// Review approved the PR.
    Approved,
    /// Review requested changes.
    ChangesRequested,
    /// Review was just a comment (no approval/rejection).
    Commented,
    /// Review was dismissed.
    Dismissed,
    /// Review is pending (not submitted yet).
    Pending,
}

/// A pull request review event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequestReviewEvent {
    /// The repository.
    pub repo: RepoId,

    /// The action that triggered this event.
    pub action: ReviewAction,

    /// The PR number.
    pub pr_number: PrNumber,

    /// The state of the review.
    pub state: ReviewState,

    /// The reviewer's user ID.
    pub reviewer_id: u64,

    /// The reviewer's login name.
    pub reviewer_login: String,

    /// The review body (may be empty).
    pub body: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ========================================================================
    // Arbitrary generators for property tests
    // ========================================================================

    fn arb_repo_id() -> impl Strategy<Value = RepoId> {
        ("[a-z][a-z0-9]{0,9}", "[a-z][a-z0-9]{0,9}")
            .prop_map(|(owner, repo)| RepoId::new(owner, repo))
    }

    fn arb_sha() -> impl Strategy<Value = Sha> {
        "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
    }

    fn arb_comment_action() -> impl Strategy<Value = CommentAction> {
        prop_oneof![
            Just(CommentAction::Created),
            Just(CommentAction::Edited),
            Just(CommentAction::Deleted),
        ]
    }

    fn arb_pr_action() -> impl Strategy<Value = PrAction> {
        prop_oneof![
            Just(PrAction::Opened),
            Just(PrAction::Closed),
            Just(PrAction::Edited),
            Just(PrAction::Synchronize),
            Just(PrAction::Reopened),
            Just(PrAction::ConvertedToDraft),
            Just(PrAction::ReadyForReview),
        ]
    }

    fn arb_check_suite_action() -> impl Strategy<Value = CheckSuiteAction> {
        prop_oneof![
            Just(CheckSuiteAction::Created),
            Just(CheckSuiteAction::Requested),
            Just(CheckSuiteAction::Rerequested),
            Just(CheckSuiteAction::Completed),
        ]
    }

    fn arb_status_state() -> impl Strategy<Value = StatusState> {
        prop_oneof![
            Just(StatusState::Pending),
            Just(StatusState::Success),
            Just(StatusState::Failure),
            Just(StatusState::Error),
        ]
    }

    fn arb_review_action() -> impl Strategy<Value = ReviewAction> {
        prop_oneof![
            Just(ReviewAction::Submitted),
            Just(ReviewAction::Dismissed),
            Just(ReviewAction::Edited),
        ]
    }

    fn arb_review_state() -> impl Strategy<Value = ReviewState> {
        prop_oneof![
            Just(ReviewState::Approved),
            Just(ReviewState::ChangesRequested),
            Just(ReviewState::Commented),
            Just(ReviewState::Dismissed),
            Just(ReviewState::Pending),
        ]
    }

    fn arb_issue_comment_event() -> impl Strategy<Value = IssueCommentEvent> {
        (
            arb_repo_id(),
            arb_comment_action(),
            proptest::option::of(1u64..10000u64),
            1u64..10000u64,
            "[a-zA-Z0-9 @#]{0,100}",
            1u64..1000000u64,
            "[a-z][a-z0-9]{0,15}",
        )
            .prop_map(
                |(repo, action, pr_number, comment_id, body, author_id, author_login)| {
                    IssueCommentEvent {
                        repo,
                        action,
                        pr_number: pr_number.map(PrNumber),
                        comment_id: CommentId(comment_id),
                        body,
                        author_id,
                        author_login,
                    }
                },
            )
    }

    fn arb_pull_request_event() -> impl Strategy<Value = PullRequestEvent> {
        (
            arb_repo_id(),
            arb_pr_action(),
            1u64..10000u64,
            proptest::bool::ANY,
            proptest::option::of(arb_sha()),
            arb_sha(),
            "[a-z][a-z0-9/-]{0,20}",
            "[a-z][a-z0-9/-]{0,20}",
            proptest::bool::ANY,
            1u64..1000000u64,
        )
            .prop_map(
                |(
                    repo,
                    action,
                    pr_number,
                    merged,
                    merge_commit_sha,
                    head_sha,
                    base_branch,
                    head_branch,
                    is_draft,
                    author_id,
                )| {
                    PullRequestEvent {
                        repo,
                        action,
                        pr_number: PrNumber(pr_number),
                        merged,
                        merge_commit_sha,
                        head_sha,
                        base_branch,
                        head_branch,
                        is_draft,
                        author_id,
                    }
                },
            )
    }

    fn arb_check_suite_event() -> impl Strategy<Value = CheckSuiteEvent> {
        (
            arb_repo_id(),
            arb_check_suite_action(),
            arb_sha(),
            proptest::option::of("[a-z_]{1,20}"),
            proptest::collection::vec(1u64..10000u64, 0..3),
        )
            .prop_map(
                |(repo, action, head_sha, conclusion, prs)| CheckSuiteEvent {
                    repo,
                    action,
                    head_sha,
                    conclusion,
                    pull_requests: prs.into_iter().map(PrNumber).collect(),
                },
            )
    }

    fn arb_status_event() -> impl Strategy<Value = StatusEvent> {
        (
            arb_repo_id(),
            arb_sha(),
            arb_status_state(),
            "[a-z][a-z0-9/]{0,30}",
            proptest::option::of("[a-zA-Z0-9 ]{0,50}"),
            proptest::option::of("https://[a-z.]+/[a-z0-9/]{0,20}"),
        )
            .prop_map(
                |(repo, sha, state, context, description, target_url)| StatusEvent {
                    repo,
                    sha,
                    state,
                    context,
                    description,
                    target_url,
                },
            )
    }

    fn arb_review_event() -> impl Strategy<Value = PullRequestReviewEvent> {
        (
            arb_repo_id(),
            arb_review_action(),
            1u64..10000u64,
            arb_review_state(),
            1u64..1000000u64,
            "[a-z][a-z0-9]{0,15}",
            "[a-zA-Z0-9 ]{0,100}",
        )
            .prop_map(
                |(repo, action, pr_number, state, reviewer_id, reviewer_login, body)| {
                    PullRequestReviewEvent {
                        repo,
                        action,
                        pr_number: PrNumber(pr_number),
                        state,
                        reviewer_id,
                        reviewer_login,
                        body,
                    }
                },
            )
    }

    fn arb_github_event() -> impl Strategy<Value = GitHubEvent> {
        prop_oneof![
            arb_issue_comment_event().prop_map(GitHubEvent::IssueComment),
            arb_pull_request_event().prop_map(GitHubEvent::PullRequest),
            arb_check_suite_event().prop_map(GitHubEvent::CheckSuite),
            arb_status_event().prop_map(GitHubEvent::Status),
            arb_review_event().prop_map(GitHubEvent::PullRequestReview),
        ]
    }

    // ========================================================================
    // Property tests
    // ========================================================================

    proptest! {
        /// All event types should serialize and deserialize correctly.
        #[test]
        fn github_event_serde_roundtrip(event in arb_github_event()) {
            let json = serde_json::to_string(&event).unwrap();
            let parsed: GitHubEvent = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(event, parsed);
        }

        /// IssueCommentEvent serialization roundtrip.
        #[test]
        fn issue_comment_event_serde_roundtrip(event in arb_issue_comment_event()) {
            let json = serde_json::to_string(&event).unwrap();
            let parsed: IssueCommentEvent = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(event, parsed);
        }

        /// PullRequestEvent serialization roundtrip.
        #[test]
        fn pull_request_event_serde_roundtrip(event in arb_pull_request_event()) {
            let json = serde_json::to_string(&event).unwrap();
            let parsed: PullRequestEvent = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(event, parsed);
        }

        /// CheckSuiteEvent serialization roundtrip.
        #[test]
        fn check_suite_event_serde_roundtrip(event in arb_check_suite_event()) {
            let json = serde_json::to_string(&event).unwrap();
            let parsed: CheckSuiteEvent = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(event, parsed);
        }

        /// StatusEvent serialization roundtrip.
        #[test]
        fn status_event_serde_roundtrip(event in arb_status_event()) {
            let json = serde_json::to_string(&event).unwrap();
            let parsed: StatusEvent = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(event, parsed);
        }

        /// PullRequestReviewEvent serialization roundtrip.
        #[test]
        fn review_event_serde_roundtrip(event in arb_review_event()) {
            let json = serde_json::to_string(&event).unwrap();
            let parsed: PullRequestReviewEvent = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(event, parsed);
        }

        /// CommentAction serialization roundtrip.
        #[test]
        fn comment_action_serde_roundtrip(action in arb_comment_action()) {
            let json = serde_json::to_string(&action).unwrap();
            let parsed: CommentAction = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(action, parsed);
        }

        /// PrAction serialization roundtrip.
        #[test]
        fn pr_action_serde_roundtrip(action in arb_pr_action()) {
            let json = serde_json::to_string(&action).unwrap();
            let parsed: PrAction = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(action, parsed);
        }

        /// CheckSuiteAction serialization roundtrip.
        #[test]
        fn check_suite_action_serde_roundtrip(action in arb_check_suite_action()) {
            let json = serde_json::to_string(&action).unwrap();
            let parsed: CheckSuiteAction = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(action, parsed);
        }

        /// StatusState serialization roundtrip.
        #[test]
        fn status_state_serde_roundtrip(state in arb_status_state()) {
            let json = serde_json::to_string(&state).unwrap();
            let parsed: StatusState = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(state, parsed);
        }

        /// ReviewAction serialization roundtrip.
        #[test]
        fn review_action_serde_roundtrip(action in arb_review_action()) {
            let json = serde_json::to_string(&action).unwrap();
            let parsed: ReviewAction = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(action, parsed);
        }

        /// ReviewState serialization roundtrip.
        #[test]
        fn review_state_serde_roundtrip(state in arb_review_state()) {
            let json = serde_json::to_string(&state).unwrap();
            let parsed: ReviewState = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(state, parsed);
        }

        /// repo_id() returns the correct repo for all event types.
        #[test]
        fn repo_id_is_consistent(event in arb_github_event()) {
            let repo = event.repo_id();
            match &event {
                GitHubEvent::IssueComment(e) => prop_assert_eq!(repo, &e.repo),
                GitHubEvent::PullRequest(e) => prop_assert_eq!(repo, &e.repo),
                GitHubEvent::CheckSuite(e) => prop_assert_eq!(repo, &e.repo),
                GitHubEvent::Status(e) => prop_assert_eq!(repo, &e.repo),
                GitHubEvent::PullRequestReview(e) => prop_assert_eq!(repo, &e.repo),
            }
        }
    }

    // ========================================================================
    // Unit tests
    // ========================================================================

    #[test]
    fn status_state_is_terminal() {
        assert!(!StatusState::Pending.is_terminal());
        assert!(StatusState::Success.is_terminal());
        assert!(StatusState::Failure.is_terminal());
        assert!(StatusState::Error.is_terminal());
    }

    #[test]
    fn comment_action_json_format() {
        // Verify snake_case serialization
        assert_eq!(
            serde_json::to_string(&CommentAction::Created).unwrap(),
            "\"created\""
        );
        assert_eq!(
            serde_json::to_string(&CommentAction::Edited).unwrap(),
            "\"edited\""
        );
        assert_eq!(
            serde_json::to_string(&CommentAction::Deleted).unwrap(),
            "\"deleted\""
        );
    }

    #[test]
    fn pr_action_json_format() {
        // Verify snake_case serialization
        assert_eq!(
            serde_json::to_string(&PrAction::Opened).unwrap(),
            "\"opened\""
        );
        assert_eq!(
            serde_json::to_string(&PrAction::Closed).unwrap(),
            "\"closed\""
        );
        assert_eq!(
            serde_json::to_string(&PrAction::Synchronize).unwrap(),
            "\"synchronize\""
        );
        assert_eq!(
            serde_json::to_string(&PrAction::ConvertedToDraft).unwrap(),
            "\"converted_to_draft\""
        );
        assert_eq!(
            serde_json::to_string(&PrAction::ReadyForReview).unwrap(),
            "\"ready_for_review\""
        );
    }

    #[test]
    fn review_state_json_format() {
        // Verify SCREAMING_SNAKE_CASE serialization (GitHub's format)
        assert_eq!(
            serde_json::to_string(&ReviewState::Approved).unwrap(),
            "\"APPROVED\""
        );
        assert_eq!(
            serde_json::to_string(&ReviewState::ChangesRequested).unwrap(),
            "\"CHANGES_REQUESTED\""
        );
        assert_eq!(
            serde_json::to_string(&ReviewState::Commented).unwrap(),
            "\"COMMENTED\""
        );
        assert_eq!(
            serde_json::to_string(&ReviewState::Dismissed).unwrap(),
            "\"DISMISSED\""
        );
        assert_eq!(
            serde_json::to_string(&ReviewState::Pending).unwrap(),
            "\"PENDING\""
        );
    }

    #[test]
    fn check_suite_action_json_format() {
        assert_eq!(
            serde_json::to_string(&CheckSuiteAction::Completed).unwrap(),
            "\"completed\""
        );
        assert_eq!(
            serde_json::to_string(&CheckSuiteAction::Rerequested).unwrap(),
            "\"rerequested\""
        );
    }

    #[test]
    fn status_state_json_format() {
        assert_eq!(
            serde_json::to_string(&StatusState::Pending).unwrap(),
            "\"pending\""
        );
        assert_eq!(
            serde_json::to_string(&StatusState::Success).unwrap(),
            "\"success\""
        );
        assert_eq!(
            serde_json::to_string(&StatusState::Failure).unwrap(),
            "\"failure\""
        );
        assert_eq!(
            serde_json::to_string(&StatusState::Error).unwrap(),
            "\"error\""
        );
    }
}
