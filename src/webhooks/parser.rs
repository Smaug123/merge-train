//! GitHub webhook payload parser.
//!
//! This module parses raw webhook JSON payloads into typed [`GitHubEvent`] values.
//! The parser is designed to be robust against unknown fields and event types.
//!
//! # Parsing Strategy
//!
//! 1. The event type is determined from the `X-GitHub-Event` header
//! 2. The payload is parsed according to the event type
//! 3. Unknown event types return `Ok(None)` (ignored, not error)
//! 4. Malformed payloads return `Err` with details
//!
//! # Headers
//!
//! GitHub webhooks include these headers:
//! - `X-GitHub-Event` - Event type (e.g., "issue_comment")
//! - `X-GitHub-Delivery` - Unique delivery ID
//! - `X-Hub-Signature-256` - HMAC-SHA256 signature (verified elsewhere)

use serde::Deserialize;
use thiserror::Error;

use crate::types::{CommentId, PrNumber, RepoId, Sha};

use super::events::{
    CheckSuiteAction, CheckSuiteEvent, CommentAction, GitHubEvent, IssueCommentEvent, PrAction,
    PullRequestEvent, PullRequestReviewEvent, ReviewAction, ReviewState, StatusEvent, StatusState,
};

/// Error type for webhook parsing failures.
#[derive(Debug, Error)]
pub enum ParseError {
    /// JSON deserialization failed (includes missing required fields).
    #[error("JSON parse error: {0}")]
    JsonError(#[from] serde_json::Error),

    /// Field has invalid value (e.g., malformed SHA, unknown state).
    #[error("invalid field value for {field}: {value}")]
    InvalidField { field: &'static str, value: String },
}

/// Parses a webhook payload into a typed event.
///
/// # Arguments
///
/// * `event_type` - The value of the `X-GitHub-Event` header
/// * `payload` - The raw JSON payload bytes
///
/// # Returns
///
/// * `Ok(Some(event))` - Successfully parsed a known event type
/// * `Ok(None)` - Unknown event type (ignored, not an error)
/// * `Err(e)` - Malformed payload or missing required fields
///
/// # Examples
///
/// ```
/// use merge_train::webhooks::parse_webhook;
///
/// let payload = br#"{
///     "action": "created",
///     "comment": {
///         "id": 123,
///         "body": "@merge-train start",
///         "user": { "id": 456, "login": "octocat" }
///     },
///     "issue": {
///         "number": 42,
///         "pull_request": { "url": "..." }
///     },
///     "repository": {
///         "owner": { "login": "owner" },
///         "name": "repo"
///     }
/// }"#;
///
/// let result = parse_webhook("issue_comment", payload);
/// assert!(result.is_ok());
/// ```
pub fn parse_webhook(event_type: &str, payload: &[u8]) -> Result<Option<GitHubEvent>, ParseError> {
    match event_type {
        "issue_comment" => parse_issue_comment(payload).map(|e| Some(GitHubEvent::IssueComment(e))),
        "pull_request" => parse_pull_request(payload).map(|opt| opt.map(GitHubEvent::PullRequest)),
        "check_suite" => parse_check_suite(payload).map(|e| Some(GitHubEvent::CheckSuite(e))),
        "status" => parse_status(payload).map(|e| Some(GitHubEvent::Status(e))),
        "pull_request_review" => {
            parse_pull_request_review(payload).map(|e| Some(GitHubEvent::PullRequestReview(e)))
        }
        // Unknown event types are ignored (not an error)
        _ => Ok(None),
    }
}

// ============================================================================
// Raw payload structures for deserialization
//
// These match GitHub's webhook JSON structure. We use Option<T> liberally to
// handle missing fields gracefully, then validate required fields explicitly.
// ============================================================================

/// Minimal repository info present in all webhook payloads.
#[derive(Debug, Deserialize)]
struct RawRepository {
    owner: RawOwner,
    name: String,
}

#[derive(Debug, Deserialize)]
struct RawOwner {
    login: String,
}

/// Minimal user info.
#[derive(Debug, Deserialize)]
struct RawUser {
    id: u64,
    login: String,
}

// ============================================================================
// issue_comment event
// ============================================================================

#[derive(Debug, Deserialize)]
struct RawIssueCommentPayload {
    action: String,
    comment: RawComment,
    issue: RawIssue,
    repository: RawRepository,
}

#[derive(Debug, Deserialize)]
struct RawComment {
    id: u64,
    body: Option<String>,
    user: RawUser,
}

#[derive(Debug, Deserialize)]
struct RawIssue {
    number: u64,
    // If this field is present, the issue is actually a PR
    pull_request: Option<serde_json::Value>,
}

fn parse_issue_comment(payload: &[u8]) -> Result<IssueCommentEvent, ParseError> {
    let raw: RawIssueCommentPayload = serde_json::from_slice(payload)?;

    let action = match raw.action.as_str() {
        "created" => CommentAction::Created,
        "edited" => CommentAction::Edited,
        "deleted" => CommentAction::Deleted,
        other => {
            return Err(ParseError::InvalidField {
                field: "action",
                value: other.to_string(),
            });
        }
    };

    // Only set pr_number if this is a PR (has pull_request field)
    let pr_number = raw.issue.pull_request.map(|_| PrNumber(raw.issue.number));

    Ok(IssueCommentEvent {
        repo: RepoId::new(raw.repository.owner.login, raw.repository.name),
        action,
        pr_number,
        comment_id: CommentId(raw.comment.id),
        body: raw.comment.body.unwrap_or_default(),
        author_id: raw.comment.user.id,
        author_login: raw.comment.user.login,
    })
}

// ============================================================================
// pull_request event
// ============================================================================

#[derive(Debug, Deserialize)]
struct RawPullRequestPayload {
    action: String,
    pull_request: RawPullRequest,
    repository: RawRepository,
}

#[derive(Debug, Deserialize)]
struct RawPullRequest {
    number: u64,
    merged: Option<bool>,
    merge_commit_sha: Option<String>,
    head: RawRef,
    base: RawRef,
    draft: Option<bool>,
    user: RawUser,
}

#[derive(Debug, Deserialize)]
struct RawRef {
    sha: String,
    #[serde(rename = "ref")]
    ref_name: String,
}

fn parse_pull_request(payload: &[u8]) -> Result<Option<PullRequestEvent>, ParseError> {
    let raw: RawPullRequestPayload = serde_json::from_slice(payload)?;

    let action = match raw.action.as_str() {
        "opened" => PrAction::Opened,
        "closed" => PrAction::Closed,
        "edited" => PrAction::Edited,
        "synchronize" => PrAction::Synchronize,
        "reopened" => PrAction::Reopened,
        "converted_to_draft" => PrAction::ConvertedToDraft,
        "ready_for_review" => PrAction::ReadyForReview,
        // Other actions (assigned, labeled, etc.) are not relevant to us
        _ => return Ok(None),
    };

    let head_sha =
        Sha::parse(&raw.pull_request.head.sha).map_err(|_| ParseError::InvalidField {
            field: "pull_request.head.sha",
            value: raw.pull_request.head.sha.clone(),
        })?;

    let merge_commit_sha = raw
        .pull_request
        .merge_commit_sha
        .as_ref()
        .map(Sha::parse)
        .transpose()
        .map_err(|_| ParseError::InvalidField {
            field: "pull_request.merge_commit_sha",
            value: raw
                .pull_request
                .merge_commit_sha
                .clone()
                .unwrap_or_default(),
        })?;

    Ok(Some(PullRequestEvent {
        repo: RepoId::new(raw.repository.owner.login, raw.repository.name),
        action,
        pr_number: PrNumber(raw.pull_request.number),
        merged: raw.pull_request.merged.unwrap_or(false),
        merge_commit_sha,
        head_sha,
        base_branch: raw.pull_request.base.ref_name,
        head_branch: raw.pull_request.head.ref_name,
        is_draft: raw.pull_request.draft.unwrap_or(false),
        author_id: raw.pull_request.user.id,
    }))
}

// ============================================================================
// check_suite event
// ============================================================================

#[derive(Debug, Deserialize)]
struct RawCheckSuitePayload {
    action: String,
    check_suite: RawCheckSuite,
    repository: RawRepository,
}

#[derive(Debug, Deserialize)]
struct RawCheckSuite {
    head_sha: String,
    conclusion: Option<String>,
    pull_requests: Vec<RawCheckSuitePr>,
}

#[derive(Debug, Deserialize)]
struct RawCheckSuitePr {
    number: u64,
}

fn parse_check_suite(payload: &[u8]) -> Result<CheckSuiteEvent, ParseError> {
    let raw: RawCheckSuitePayload = serde_json::from_slice(payload)?;

    let action = match raw.action.as_str() {
        "created" => CheckSuiteAction::Created,
        "requested" => CheckSuiteAction::Requested,
        "rerequested" => CheckSuiteAction::Rerequested,
        "completed" => CheckSuiteAction::Completed,
        other => {
            return Err(ParseError::InvalidField {
                field: "action",
                value: other.to_string(),
            });
        }
    };

    let head_sha = Sha::parse(&raw.check_suite.head_sha).map_err(|_| ParseError::InvalidField {
        field: "check_suite.head_sha",
        value: raw.check_suite.head_sha.clone(),
    })?;

    Ok(CheckSuiteEvent {
        repo: RepoId::new(raw.repository.owner.login, raw.repository.name),
        action,
        head_sha,
        conclusion: raw.check_suite.conclusion,
        pull_requests: raw
            .check_suite
            .pull_requests
            .into_iter()
            .map(|pr| PrNumber(pr.number))
            .collect(),
    })
}

// ============================================================================
// status event (legacy Status API)
// ============================================================================

#[derive(Debug, Deserialize)]
struct RawStatusPayload {
    sha: String,
    state: String,
    context: String,
    description: Option<String>,
    target_url: Option<String>,
    repository: RawRepository,
}

fn parse_status(payload: &[u8]) -> Result<StatusEvent, ParseError> {
    let raw: RawStatusPayload = serde_json::from_slice(payload)?;

    let state = match raw.state.as_str() {
        "pending" => StatusState::Pending,
        "success" => StatusState::Success,
        "failure" => StatusState::Failure,
        "error" => StatusState::Error,
        other => {
            return Err(ParseError::InvalidField {
                field: "state",
                value: other.to_string(),
            });
        }
    };

    let sha = Sha::parse(&raw.sha).map_err(|_| ParseError::InvalidField {
        field: "sha",
        value: raw.sha.clone(),
    })?;

    Ok(StatusEvent {
        repo: RepoId::new(raw.repository.owner.login, raw.repository.name),
        sha,
        state,
        context: raw.context,
        description: raw.description,
        target_url: raw.target_url,
    })
}

// ============================================================================
// pull_request_review event
// ============================================================================

#[derive(Debug, Deserialize)]
struct RawPullRequestReviewPayload {
    action: String,
    review: RawReview,
    pull_request: RawPullRequestMinimal,
    repository: RawRepository,
}

#[derive(Debug, Deserialize)]
struct RawReview {
    user: RawUser,
    state: String,
    body: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawPullRequestMinimal {
    number: u64,
}

fn parse_pull_request_review(payload: &[u8]) -> Result<PullRequestReviewEvent, ParseError> {
    let raw: RawPullRequestReviewPayload = serde_json::from_slice(payload)?;

    let action = match raw.action.as_str() {
        "submitted" => ReviewAction::Submitted,
        "dismissed" => ReviewAction::Dismissed,
        "edited" => ReviewAction::Edited,
        other => {
            return Err(ParseError::InvalidField {
                field: "action",
                value: other.to_string(),
            });
        }
    };

    // GitHub uses SCREAMING_SNAKE_CASE for review states
    let state = match raw.review.state.to_uppercase().as_str() {
        "APPROVED" => ReviewState::Approved,
        "CHANGES_REQUESTED" => ReviewState::ChangesRequested,
        "COMMENTED" => ReviewState::Commented,
        "DISMISSED" => ReviewState::Dismissed,
        "PENDING" => ReviewState::Pending,
        other => {
            return Err(ParseError::InvalidField {
                field: "review.state",
                value: other.to_string(),
            });
        }
    };

    Ok(PullRequestReviewEvent {
        repo: RepoId::new(raw.repository.owner.login, raw.repository.name),
        action,
        pr_number: PrNumber(raw.pull_request.number),
        state,
        reviewer_id: raw.review.user.id,
        reviewer_login: raw.review.user.login,
        body: raw.review.body.unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Unit tests for each event type
    // ========================================================================

    #[test]
    fn parse_issue_comment_created() {
        let payload = r#"{
            "action": "created",
            "comment": {
                "id": 12345,
                "body": "@merge-train start",
                "user": { "id": 100, "login": "octocat" }
            },
            "issue": {
                "number": 42,
                "pull_request": { "url": "https://api.github.com/repos/owner/repo/pulls/42" }
            },
            "repository": {
                "owner": { "login": "myorg" },
                "name": "myrepo"
            }
        }"#;

        let result = parse_webhook("issue_comment", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::IssueComment(e) => {
                assert_eq!(e.repo, RepoId::new("myorg", "myrepo"));
                assert_eq!(e.action, CommentAction::Created);
                assert_eq!(e.pr_number, Some(PrNumber(42)));
                assert_eq!(e.comment_id, CommentId(12345));
                assert_eq!(e.body, "@merge-train start");
                assert_eq!(e.author_id, 100);
                assert_eq!(e.author_login, "octocat");
            }
            _ => panic!("expected IssueComment"),
        }
    }

    #[test]
    fn parse_issue_comment_on_issue_not_pr() {
        let payload = r#"{
            "action": "created",
            "comment": {
                "id": 999,
                "body": "test comment",
                "user": { "id": 1, "login": "user" }
            },
            "issue": {
                "number": 10
            },
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("issue_comment", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::IssueComment(e) => {
                // pr_number should be None for regular issues
                assert_eq!(e.pr_number, None);
            }
            _ => panic!("expected IssueComment"),
        }
    }

    #[test]
    fn parse_issue_comment_deleted() {
        let payload = r#"{
            "action": "deleted",
            "comment": {
                "id": 999,
                "user": { "id": 1, "login": "user" }
            },
            "issue": {
                "number": 10,
                "pull_request": {}
            },
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("issue_comment", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::IssueComment(e) => {
                assert_eq!(e.action, CommentAction::Deleted);
                // Body is empty when not present
                assert_eq!(e.body, "");
            }
            _ => panic!("expected IssueComment"),
        }
    }

    #[test]
    fn parse_pull_request_opened() {
        let payload = r#"{
            "action": "opened",
            "pull_request": {
                "number": 123,
                "head": {
                    "sha": "1234567890abcdef1234567890abcdef12345678",
                    "ref": "feature-branch"
                },
                "base": {
                    "sha": "abcdef1234567890abcdef1234567890abcdef12",
                    "ref": "main"
                },
                "draft": false,
                "user": { "id": 42, "login": "dev" }
            },
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("pull_request", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::PullRequest(e) => {
                assert_eq!(e.action, PrAction::Opened);
                assert_eq!(e.pr_number, PrNumber(123));
                assert_eq!(
                    e.head_sha,
                    Sha::parse("1234567890abcdef1234567890abcdef12345678").unwrap()
                );
                assert_eq!(e.base_branch, "main");
                assert_eq!(e.head_branch, "feature-branch");
                assert!(!e.is_draft);
                assert!(!e.merged);
                assert!(e.merge_commit_sha.is_none());
            }
            _ => panic!("expected PullRequest"),
        }
    }

    #[test]
    fn parse_pull_request_closed_merged() {
        let payload = r#"{
            "action": "closed",
            "pull_request": {
                "number": 99,
                "merged": true,
                "merge_commit_sha": "fedcba0987654321fedcba0987654321fedcba09",
                "head": {
                    "sha": "1234567890abcdef1234567890abcdef12345678",
                    "ref": "pr-branch"
                },
                "base": {
                    "sha": "0000000000000000000000000000000000000000",
                    "ref": "main"
                },
                "user": { "id": 1, "login": "author" }
            },
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("pull_request", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::PullRequest(e) => {
                assert_eq!(e.action, PrAction::Closed);
                assert!(e.merged);
                assert_eq!(
                    e.merge_commit_sha,
                    Some(Sha::parse("fedcba0987654321fedcba0987654321fedcba09").unwrap())
                );
            }
            _ => panic!("expected PullRequest"),
        }
    }

    #[test]
    fn parse_pull_request_synchronize() {
        let payload = r#"{
            "action": "synchronize",
            "pull_request": {
                "number": 50,
                "head": {
                    "sha": "abcdef1234567890abcdef1234567890abcdef12",
                    "ref": "branch"
                },
                "base": {
                    "sha": "1234567890abcdef1234567890abcdef12345678",
                    "ref": "main"
                },
                "user": { "id": 1, "login": "user" }
            },
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("pull_request", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::PullRequest(e) => {
                assert_eq!(e.action, PrAction::Synchronize);
            }
            _ => panic!("expected PullRequest"),
        }
    }

    #[test]
    fn parse_check_suite_completed() {
        let payload = r#"{
            "action": "completed",
            "check_suite": {
                "head_sha": "deadbeef1234567890abcdef1234567890abcdef",
                "conclusion": "success",
                "pull_requests": [
                    { "number": 10 },
                    { "number": 20 }
                ]
            },
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("check_suite", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::CheckSuite(e) => {
                assert_eq!(e.action, CheckSuiteAction::Completed);
                assert_eq!(
                    e.head_sha,
                    Sha::parse("deadbeef1234567890abcdef1234567890abcdef").unwrap()
                );
                assert_eq!(e.conclusion, Some("success".to_string()));
                assert_eq!(e.pull_requests, vec![PrNumber(10), PrNumber(20)]);
            }
            _ => panic!("expected CheckSuite"),
        }
    }

    #[test]
    fn parse_check_suite_requested_no_conclusion() {
        let payload = r#"{
            "action": "requested",
            "check_suite": {
                "head_sha": "1111111111111111111111111111111111111111",
                "pull_requests": []
            },
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("check_suite", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::CheckSuite(e) => {
                assert_eq!(e.action, CheckSuiteAction::Requested);
                assert!(e.conclusion.is_none());
                assert!(e.pull_requests.is_empty());
            }
            _ => panic!("expected CheckSuite"),
        }
    }

    #[test]
    fn parse_status_success() {
        let payload = r#"{
            "sha": "abcdef1234567890abcdef1234567890abcdef12",
            "state": "success",
            "context": "ci/jenkins",
            "description": "Build passed",
            "target_url": "https://ci.example.com/build/123",
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("status", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::Status(e) => {
                assert_eq!(
                    e.sha,
                    Sha::parse("abcdef1234567890abcdef1234567890abcdef12").unwrap()
                );
                assert_eq!(e.state, StatusState::Success);
                assert_eq!(e.context, "ci/jenkins");
                assert_eq!(e.description, Some("Build passed".to_string()));
                assert_eq!(
                    e.target_url,
                    Some("https://ci.example.com/build/123".to_string())
                );
            }
            _ => panic!("expected Status"),
        }
    }

    #[test]
    fn parse_status_pending_minimal() {
        let payload = r#"{
            "sha": "0000000000000000000000000000000000000000",
            "state": "pending",
            "context": "continuous-integration",
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("status", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::Status(e) => {
                assert_eq!(e.state, StatusState::Pending);
                assert!(e.description.is_none());
                assert!(e.target_url.is_none());
            }
            _ => panic!("expected Status"),
        }
    }

    #[test]
    fn parse_pull_request_review_approved() {
        let payload = r#"{
            "action": "submitted",
            "review": {
                "user": { "id": 555, "login": "reviewer" },
                "state": "approved",
                "body": "LGTM!"
            },
            "pull_request": {
                "number": 77
            },
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("pull_request_review", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::PullRequestReview(e) => {
                assert_eq!(e.action, ReviewAction::Submitted);
                assert_eq!(e.state, ReviewState::Approved);
                assert_eq!(e.pr_number, PrNumber(77));
                assert_eq!(e.reviewer_id, 555);
                assert_eq!(e.reviewer_login, "reviewer");
                assert_eq!(e.body, "LGTM!");
            }
            _ => panic!("expected PullRequestReview"),
        }
    }

    #[test]
    fn parse_pull_request_review_dismissed() {
        let payload = r#"{
            "action": "dismissed",
            "review": {
                "user": { "id": 1, "login": "admin" },
                "state": "dismissed"
            },
            "pull_request": {
                "number": 100
            },
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("pull_request_review", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::PullRequestReview(e) => {
                assert_eq!(e.action, ReviewAction::Dismissed);
                assert_eq!(e.state, ReviewState::Dismissed);
                assert_eq!(e.body, "");
            }
            _ => panic!("expected PullRequestReview"),
        }
    }

    #[test]
    fn parse_pull_request_review_changes_requested() {
        let payload = r#"{
            "action": "submitted",
            "review": {
                "user": { "id": 1, "login": "reviewer" },
                "state": "changes_requested",
                "body": "Please fix the bug"
            },
            "pull_request": {
                "number": 50
            },
            "repository": {
                "owner": { "login": "org" },
                "name": "repo"
            }
        }"#;

        let result = parse_webhook("pull_request_review", payload.as_bytes()).unwrap();
        let event = result.expect("should parse");

        match event {
            GitHubEvent::PullRequestReview(e) => {
                assert_eq!(e.state, ReviewState::ChangesRequested);
            }
            _ => panic!("expected PullRequestReview"),
        }
    }

    // ========================================================================
    // Unknown event types return Ok(None)
    // ========================================================================

    #[test]
    fn unknown_event_type_returns_none() {
        let payload = b"{}";

        assert!(parse_webhook("ping", payload).unwrap().is_none());
        assert!(parse_webhook("push", payload).unwrap().is_none());
        assert!(parse_webhook("deployment", payload).unwrap().is_none());
        assert!(parse_webhook("star", payload).unwrap().is_none());
        assert!(parse_webhook("fork", payload).unwrap().is_none());
        assert!(parse_webhook("unknown_event", payload).unwrap().is_none());
    }

    // ========================================================================
    // Error handling
    // ========================================================================

    #[test]
    fn malformed_json_returns_error() {
        let payload = b"not valid json";
        let result = parse_webhook("issue_comment", payload);
        assert!(matches!(result, Err(ParseError::JsonError(_))));
    }

    #[test]
    fn missing_required_field_returns_error() {
        // Missing repository
        let payload = r#"{
            "action": "created",
            "comment": { "id": 1, "body": "test", "user": { "id": 1, "login": "u" } },
            "issue": { "number": 1 }
        }"#;
        let result = parse_webhook("issue_comment", payload.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn invalid_action_returns_error() {
        let payload = r#"{
            "action": "invalid_action",
            "comment": { "id": 1, "body": "test", "user": { "id": 1, "login": "u" } },
            "issue": { "number": 1 },
            "repository": { "owner": { "login": "o" }, "name": "r" }
        }"#;
        let result = parse_webhook("issue_comment", payload.as_bytes());
        assert!(matches!(
            result,
            Err(ParseError::InvalidField {
                field: "action",
                ..
            })
        ));
    }

    #[test]
    fn invalid_sha_returns_error() {
        let payload = r#"{
            "sha": "not-a-valid-sha",
            "state": "success",
            "context": "ci",
            "repository": { "owner": { "login": "o" }, "name": "r" }
        }"#;
        let result = parse_webhook("status", payload.as_bytes());
        assert!(matches!(
            result,
            Err(ParseError::InvalidField { field: "sha", .. })
        ));
    }

    #[test]
    fn unhandled_pr_action_returns_none() {
        // "labeled" is a valid GitHub action but not one we handle - should be ignored
        let payload = r#"{
            "action": "labeled",
            "pull_request": {
                "number": 1,
                "head": { "sha": "1234567890abcdef1234567890abcdef12345678", "ref": "b" },
                "base": { "sha": "abcdef1234567890abcdef1234567890abcdef12", "ref": "main" },
                "user": { "id": 1, "login": "u" }
            },
            "repository": { "owner": { "login": "o" }, "name": "r" }
        }"#;
        let result = parse_webhook("pull_request", payload.as_bytes());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn other_unhandled_pr_actions_return_none() {
        // Various other PR actions that GitHub sends but we don't care about
        for action in [
            "assigned",
            "unlabeled",
            "review_requested",
            "locked",
            "milestoned",
        ] {
            let payload = format!(
                r#"{{
                "action": "{}",
                "pull_request": {{
                    "number": 1,
                    "head": {{ "sha": "1234567890abcdef1234567890abcdef12345678", "ref": "b" }},
                    "base": {{ "sha": "abcdef1234567890abcdef1234567890abcdef12", "ref": "main" }},
                    "user": {{ "id": 1, "login": "u" }}
                }},
                "repository": {{ "owner": {{ "login": "o" }}, "name": "r" }}
            }}"#,
                action
            );
            let result = parse_webhook("pull_request", payload.as_bytes());
            assert!(
                result.unwrap().is_none(),
                "action '{}' should return None",
                action
            );
        }
    }

    // ========================================================================
    // Case sensitivity tests
    // ========================================================================

    #[test]
    fn review_state_case_insensitive() {
        // GitHub sometimes sends lowercase, sometimes uppercase
        for state_str in ["approved", "APPROVED", "Approved"] {
            let payload = format!(
                r#"{{
                "action": "submitted",
                "review": {{
                    "user": {{ "id": 1, "login": "u" }},
                    "state": "{}"
                }},
                "pull_request": {{ "number": 1 }},
                "repository": {{ "owner": {{ "login": "o" }}, "name": "r" }}
            }}"#,
                state_str
            );
            let result = parse_webhook("pull_request_review", payload.as_bytes()).unwrap();
            match result {
                Some(GitHubEvent::PullRequestReview(e)) => {
                    assert_eq!(e.state, ReviewState::Approved);
                }
                _ => panic!("expected PullRequestReview"),
            }
        }
    }
}
