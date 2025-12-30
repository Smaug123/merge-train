//! Deduplication keys for webhook deliveries.
//!
//! GitHub may redeliver webhooks with different `X-GitHub-Delivery` IDs for the
//! same logical event. This module provides keys that identify logical events
//! for deduplication.
//!
//! # Key Formats by Event Type
//!
//! - `issue_comment.created`: `issue_comment:<pr>:<comment_id>:created`
//! - `issue_comment.edited`: `issue_comment:<pr>:<comment_id>:edited:<updated_at>`
//! - `issue_comment.deleted`: `issue_comment:<pr>:<comment_id>:deleted`
//! - `pull_request.<action>`: `pull_request:<pr>:<action>:<head_sha>`
//! - `pull_request.edited`: `pull_request:<pr>:edited:<updated_at>`
//! - `check_suite.<action>`: `check_suite:<suite_id>:<action>:<updated_at>`
//! - `status`: `status:<sha>:<context>:<state>`
//!
//! # TTL-based Expiration
//!
//! Seen dedupe keys are stored in the snapshot with timestamps. Keys older than
//! the retention period (default 24 hours) are pruned to prevent unbounded growth.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{CommentId, PrNumber, Sha};

/// A deduplication key that identifies a logical webhook event.
///
/// This is used to detect and skip duplicate deliveries of the same event.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DedupeKey(String);

impl DedupeKey {
    /// Creates a dedupe key for an `issue_comment.created` event.
    pub fn issue_comment_created(pr: PrNumber, comment_id: CommentId) -> Self {
        DedupeKey(format!("issue_comment:{}:{}:created", pr.0, comment_id.0))
    }

    /// Creates a dedupe key for an `issue_comment.edited` event.
    ///
    /// The `updated_at` timestamp distinguishes multiple edits to the same comment.
    pub fn issue_comment_edited(
        pr: PrNumber,
        comment_id: CommentId,
        updated_at: &DateTime<Utc>,
    ) -> Self {
        DedupeKey(format!(
            "issue_comment:{}:{}:edited:{}",
            pr.0,
            comment_id.0,
            updated_at.to_rfc3339()
        ))
    }

    /// Creates a dedupe key for an `issue_comment.deleted` event.
    pub fn issue_comment_deleted(pr: PrNumber, comment_id: CommentId) -> Self {
        DedupeKey(format!("issue_comment:{}:{}:deleted", pr.0, comment_id.0))
    }

    /// Creates a dedupe key for a `pull_request` event (non-edited).
    ///
    /// For most actions (opened, closed, synchronize, etc.), the head SHA
    /// makes the event unique.
    pub fn pull_request(pr: PrNumber, action: &str, head_sha: &Sha) -> Self {
        DedupeKey(format!(
            "pull_request:{}:{}:{}",
            pr.0,
            action,
            head_sha.as_str()
        ))
    }

    /// Creates a dedupe key for a `pull_request.edited` event.
    ///
    /// Edits don't necessarily change the head SHA (e.g., base retarget, title change),
    /// so we use `updated_at` instead.
    pub fn pull_request_edited(pr: PrNumber, updated_at: &DateTime<Utc>) -> Self {
        DedupeKey(format!(
            "pull_request:{}:edited:{}",
            pr.0,
            updated_at.to_rfc3339()
        ))
    }

    /// Creates a dedupe key for a `check_suite` event.
    ///
    /// Check suite reruns reuse the same suite ID, so `updated_at` distinguishes
    /// subsequent completions.
    pub fn check_suite(suite_id: u64, action: &str, updated_at: &DateTime<Utc>) -> Self {
        DedupeKey(format!(
            "check_suite:{}:{}:{}",
            suite_id,
            action,
            updated_at.to_rfc3339()
        ))
    }

    /// Creates a dedupe key for a `status` event.
    ///
    /// Status events are per-SHA and context, with state changes.
    pub fn status(sha: &Sha, context: &str, state: &str) -> Self {
        DedupeKey(format!("status:{}:{}:{}", sha.as_str(), context, state))
    }

    /// Creates a dedupe key for a `pull_request_review` event.
    pub fn pull_request_review(pr: PrNumber, review_id: u64, action: &str) -> Self {
        DedupeKey(format!(
            "pull_request_review:{}:{}:{}",
            pr.0, review_id, action
        ))
    }

    /// Returns the key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DedupeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for DedupeKey {
    fn from(s: String) -> Self {
        DedupeKey(s)
    }
}

/// Raw webhook payload data for dedupe key extraction.
///
/// This is a minimal representation of webhook fields needed for deduplication.
/// The full webhook parsing happens in the webhooks module (Stage 11).
#[derive(Debug, Clone)]
pub enum WebhookEventData {
    IssueCommentCreated {
        pr: PrNumber,
        comment_id: CommentId,
    },
    IssueCommentEdited {
        pr: PrNumber,
        comment_id: CommentId,
        updated_at: DateTime<Utc>,
    },
    IssueCommentDeleted {
        pr: PrNumber,
        comment_id: CommentId,
    },
    PullRequest {
        pr: PrNumber,
        action: String,
        head_sha: Sha,
    },
    PullRequestEdited {
        pr: PrNumber,
        updated_at: DateTime<Utc>,
    },
    CheckSuite {
        suite_id: u64,
        action: String,
        updated_at: DateTime<Utc>,
    },
    Status {
        sha: Sha,
        context: String,
        state: String,
    },
    PullRequestReview {
        pr: PrNumber,
        review_id: u64,
        action: String,
    },
}

/// Extracts a dedupe key from webhook event data.
pub fn extract_dedupe_key(event: &WebhookEventData) -> DedupeKey {
    match event {
        WebhookEventData::IssueCommentCreated { pr, comment_id } => {
            DedupeKey::issue_comment_created(*pr, *comment_id)
        }
        WebhookEventData::IssueCommentEdited {
            pr,
            comment_id,
            updated_at,
        } => DedupeKey::issue_comment_edited(*pr, *comment_id, updated_at),
        WebhookEventData::IssueCommentDeleted { pr, comment_id } => {
            DedupeKey::issue_comment_deleted(*pr, *comment_id)
        }
        WebhookEventData::PullRequest {
            pr,
            action,
            head_sha,
        } => DedupeKey::pull_request(*pr, action, head_sha),
        WebhookEventData::PullRequestEdited { pr, updated_at } => {
            DedupeKey::pull_request_edited(*pr, updated_at)
        }
        WebhookEventData::CheckSuite {
            suite_id,
            action,
            updated_at,
        } => DedupeKey::check_suite(*suite_id, action, updated_at),
        WebhookEventData::Status {
            sha,
            context,
            state,
        } => DedupeKey::status(sha, context, state),
        WebhookEventData::PullRequestReview {
            pr,
            review_id,
            action,
        } => DedupeKey::pull_request_review(*pr, *review_id, action),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_pr_number() -> impl Strategy<Value = PrNumber> {
        (1u64..100000).prop_map(PrNumber)
    }

    fn arb_comment_id() -> impl Strategy<Value = CommentId> {
        (1u64..u64::MAX).prop_map(CommentId)
    }

    fn arb_sha() -> impl Strategy<Value = Sha> {
        "[0-9a-f]{40}".prop_map(|s| Sha::parse(s).unwrap())
    }

    fn arb_datetime() -> impl Strategy<Value = DateTime<Utc>> {
        (946684800i64..4102444800i64).prop_map(|secs| DateTime::from_timestamp(secs, 0).unwrap())
    }

    fn arb_action() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("created".to_string()),
            Just("edited".to_string()),
            Just("deleted".to_string()),
            Just("opened".to_string()),
            Just("closed".to_string()),
            Just("synchronize".to_string()),
            Just("completed".to_string()),
            Just("requested".to_string()),
        ]
    }

    fn arb_context() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9/_-]{1,50}".prop_map(String::from)
    }

    fn arb_state() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("pending".to_string()),
            Just("success".to_string()),
            Just("failure".to_string()),
            Just("error".to_string()),
        ]
    }

    proptest! {
        /// Dedupe keys are deterministic.
        #[test]
        fn issue_comment_created_deterministic(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
        ) {
            let key1 = DedupeKey::issue_comment_created(pr, comment_id);
            let key2 = DedupeKey::issue_comment_created(pr, comment_id);
            prop_assert_eq!(key1, key2);
        }

        #[test]
        fn issue_comment_edited_deterministic(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
            updated_at in arb_datetime(),
        ) {
            let key1 = DedupeKey::issue_comment_edited(pr, comment_id, &updated_at);
            let key2 = DedupeKey::issue_comment_edited(pr, comment_id, &updated_at);
            prop_assert_eq!(key1, key2);
        }

        #[test]
        fn pull_request_deterministic(
            pr in arb_pr_number(),
            action in arb_action(),
            head_sha in arb_sha(),
        ) {
            let key1 = DedupeKey::pull_request(pr, &action, &head_sha);
            let key2 = DedupeKey::pull_request(pr, &action, &head_sha);
            prop_assert_eq!(key1, key2);
        }

        #[test]
        fn check_suite_deterministic(
            suite_id in 1u64..u64::MAX,
            action in arb_action(),
            updated_at in arb_datetime(),
        ) {
            let key1 = DedupeKey::check_suite(suite_id, &action, &updated_at);
            let key2 = DedupeKey::check_suite(suite_id, &action, &updated_at);
            prop_assert_eq!(key1, key2);
        }

        #[test]
        fn status_deterministic(
            sha in arb_sha(),
            context in arb_context(),
            state in arb_state(),
        ) {
            let key1 = DedupeKey::status(&sha, &context, &state);
            let key2 = DedupeKey::status(&sha, &context, &state);
            prop_assert_eq!(key1, key2);
        }

        /// Different inputs produce different keys.
        #[test]
        fn different_prs_different_keys(
            pr1 in arb_pr_number(),
            pr2 in arb_pr_number(),
            comment_id in arb_comment_id(),
        ) {
            prop_assume!(pr1 != pr2);
            let key1 = DedupeKey::issue_comment_created(pr1, comment_id);
            let key2 = DedupeKey::issue_comment_created(pr2, comment_id);
            prop_assert_ne!(key1, key2);
        }

        #[test]
        fn different_comments_different_keys(
            pr in arb_pr_number(),
            comment_id1 in arb_comment_id(),
            comment_id2 in arb_comment_id(),
        ) {
            prop_assume!(comment_id1 != comment_id2);
            let key1 = DedupeKey::issue_comment_created(pr, comment_id1);
            let key2 = DedupeKey::issue_comment_created(pr, comment_id2);
            prop_assert_ne!(key1, key2);
        }

        #[test]
        fn different_actions_different_keys(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
            updated_at in arb_datetime(),
        ) {
            let key1 = DedupeKey::issue_comment_created(pr, comment_id);
            let key2 = DedupeKey::issue_comment_edited(pr, comment_id, &updated_at);
            let key3 = DedupeKey::issue_comment_deleted(pr, comment_id);
            // Compare using as_str() to avoid move issues
            prop_assert_ne!(key1.as_str(), key2.as_str());
            prop_assert_ne!(key2.as_str(), key3.as_str());
            prop_assert_ne!(key1.as_str(), key3.as_str());
        }

        #[test]
        fn different_timestamps_different_keys(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
            updated_at1 in arb_datetime(),
            updated_at2 in arb_datetime(),
        ) {
            prop_assume!(updated_at1 != updated_at2);
            let key1 = DedupeKey::issue_comment_edited(pr, comment_id, &updated_at1);
            let key2 = DedupeKey::issue_comment_edited(pr, comment_id, &updated_at2);
            prop_assert_ne!(key1, key2);
        }

        /// Serde roundtrip preserves key.
        #[test]
        fn serde_roundtrip(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
        ) {
            let key = DedupeKey::issue_comment_created(pr, comment_id);
            let json = serde_json::to_string(&key).unwrap();
            let parsed: DedupeKey = serde_json::from_str(&json).unwrap();
            prop_assert_eq!(key, parsed);
        }

        /// extract_dedupe_key produces correct keys.
        #[test]
        fn extract_issue_comment_created(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
        ) {
            let event = WebhookEventData::IssueCommentCreated { pr, comment_id };
            let key = extract_dedupe_key(&event);
            let expected = DedupeKey::issue_comment_created(pr, comment_id);
            prop_assert_eq!(key, expected);
        }

        #[test]
        fn extract_pull_request(
            pr in arb_pr_number(),
            action in arb_action(),
            head_sha in arb_sha(),
        ) {
            let event = WebhookEventData::PullRequest {
                pr,
                action: action.clone(),
                head_sha: head_sha.clone(),
            };
            let key = extract_dedupe_key(&event);
            let expected = DedupeKey::pull_request(pr, &action, &head_sha);
            prop_assert_eq!(key, expected);
        }
    }

    // ─── Unit tests ───

    #[test]
    fn key_format_matches_expected() {
        let key = DedupeKey::issue_comment_created(PrNumber(123), CommentId(456789));
        assert_eq!(key.as_str(), "issue_comment:123:456789:created");

        let key =
            DedupeKey::pull_request(PrNumber(42), "opened", &Sha::parse("a".repeat(40)).unwrap());
        assert_eq!(
            key.as_str(),
            format!("pull_request:42:opened:{}", "a".repeat(40))
        );
    }

    #[test]
    fn display_matches_as_str() {
        let key = DedupeKey::issue_comment_created(PrNumber(123), CommentId(456));
        assert_eq!(format!("{}", key), key.as_str());
    }
}
