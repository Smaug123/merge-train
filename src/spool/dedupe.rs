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
    ///
    /// Note: The context is escaped to prevent collisions when it contains
    /// the separator character (`:`). For example, `ci:build` becomes `ci\:build`.
    pub fn status(sha: &Sha, context: &str, state: &str) -> Self {
        // Escape backslashes first, then colons, to allow unambiguous parsing
        let escaped_context = context.replace('\\', "\\\\").replace(':', "\\:");
        DedupeKey(format!(
            "status:{}:{}:{}",
            sha.as_str(),
            escaped_context,
            state
        ))
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

// ─── Dedupe tracking and TTL-based pruning ───

use std::collections::HashMap;

/// Default TTL for dedupe keys (24 hours).
pub const DEFAULT_DEDUPE_TTL_HOURS: i64 = 24;

/// Checks if a dedupe key has been seen.
///
/// Returns `true` if the key exists in the seen set, meaning this event
/// is a duplicate and should be skipped.
pub fn is_duplicate(seen_keys: &HashMap<String, DateTime<Utc>>, key: &DedupeKey) -> bool {
    seen_keys.contains_key(key.as_str())
}

/// Records a dedupe key as seen with the current timestamp.
///
/// This should be called after successfully processing an event.
pub fn mark_seen(seen_keys: &mut HashMap<String, DateTime<Utc>>, key: &DedupeKey) {
    seen_keys.insert(key.as_str().to_string(), Utc::now());
}

/// Prunes dedupe keys older than the specified TTL.
///
/// Returns the number of keys pruned.
pub fn prune_expired_keys(seen_keys: &mut HashMap<String, DateTime<Utc>>, ttl_hours: i64) -> usize {
    let cutoff = Utc::now() - chrono::Duration::hours(ttl_hours);
    let before_len = seen_keys.len();
    seen_keys.retain(|_, timestamp| *timestamp > cutoff);
    before_len - seen_keys.len()
}

/// Prunes dedupe keys using the default TTL (24 hours).
///
/// Returns the number of keys pruned.
pub fn prune_expired_keys_default(seen_keys: &mut HashMap<String, DateTime<Utc>>) -> usize {
    prune_expired_keys(seen_keys, DEFAULT_DEDUPE_TTL_HOURS)
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

        // ─── Comprehensive extract_dedupe_key tests for all event types ───

        #[test]
        fn extract_issue_comment_edited(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
            updated_at in arb_datetime(),
        ) {
            let event = WebhookEventData::IssueCommentEdited {
                pr,
                comment_id,
                updated_at,
            };
            let key = extract_dedupe_key(&event);
            let expected = DedupeKey::issue_comment_edited(pr, comment_id, &updated_at);
            prop_assert_eq!(key, expected);
        }

        #[test]
        fn extract_issue_comment_deleted(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
        ) {
            let event = WebhookEventData::IssueCommentDeleted { pr, comment_id };
            let key = extract_dedupe_key(&event);
            let expected = DedupeKey::issue_comment_deleted(pr, comment_id);
            prop_assert_eq!(key, expected);
        }

        #[test]
        fn extract_pull_request_edited(
            pr in arb_pr_number(),
            updated_at in arb_datetime(),
        ) {
            let event = WebhookEventData::PullRequestEdited { pr, updated_at };
            let key = extract_dedupe_key(&event);
            let expected = DedupeKey::pull_request_edited(pr, &updated_at);
            prop_assert_eq!(key, expected);
        }

        #[test]
        fn extract_check_suite(
            suite_id in 1u64..u64::MAX,
            action in arb_action(),
            updated_at in arb_datetime(),
        ) {
            let event = WebhookEventData::CheckSuite {
                suite_id,
                action: action.clone(),
                updated_at,
            };
            let key = extract_dedupe_key(&event);
            let expected = DedupeKey::check_suite(suite_id, &action, &updated_at);
            prop_assert_eq!(key, expected);
        }

        #[test]
        fn extract_status(
            sha in arb_sha(),
            context in arb_context(),
            state in arb_state(),
        ) {
            let event = WebhookEventData::Status {
                sha: sha.clone(),
                context: context.clone(),
                state: state.clone(),
            };
            let key = extract_dedupe_key(&event);
            let expected = DedupeKey::status(&sha, &context, &state);
            prop_assert_eq!(key, expected);
        }

        #[test]
        fn extract_pull_request_review(
            pr in arb_pr_number(),
            review_id in 1u64..u64::MAX,
            action in arb_action(),
        ) {
            let event = WebhookEventData::PullRequestReview {
                pr,
                review_id,
                action: action.clone(),
            };
            let key = extract_dedupe_key(&event);
            let expected = DedupeKey::pull_request_review(pr, review_id, &action);
            prop_assert_eq!(key, expected);
        }

        /// Status events with colon-containing contexts still produce correct keys.
        #[test]
        fn extract_status_with_colons_in_context(
            sha in arb_sha(),
            state in arb_state(),
        ) {
            // Use a context that contains colons (common in CI systems)
            let context = "ci:build:test:lint";
            let event = WebhookEventData::Status {
                sha: sha.clone(),
                context: context.to_string(),
                state: state.clone(),
            };
            let key = extract_dedupe_key(&event);

            // Verify the key contains escaped colons
            prop_assert!(key.as_str().contains("ci\\:build\\:test\\:lint"));

            let expected = DedupeKey::status(&sha, context, &state);
            prop_assert_eq!(key, expected);
        }

        // ─── Collision-free property tests ───

        /// Different status contexts always produce different keys (collision-free).
        ///
        /// This is the critical property that the escaping fix ensures.
        /// Without escaping, "a:b" + "c" could collide with "a" + "b:c".
        #[test]
        fn status_different_contexts_never_collide(
            sha in arb_sha(),
            ctx1 in "[a-zA-Z0-9:/_-]{1,30}",
            ctx2 in "[a-zA-Z0-9:/_-]{1,30}",
            state in arb_state(),
        ) {
            prop_assume!(ctx1 != ctx2);

            let key1 = DedupeKey::status(&sha, &ctx1, &state);
            let key2 = DedupeKey::status(&sha, &ctx2, &state);

            prop_assert_ne!(key1, key2, "Different contexts must produce different keys");
        }

        /// Different status states always produce different keys.
        #[test]
        fn status_different_states_never_collide(
            sha in arb_sha(),
            context in "[a-zA-Z0-9:/_-]{1,30}",
            state1 in arb_state(),
            state2 in arb_state(),
        ) {
            prop_assume!(state1 != state2);

            let key1 = DedupeKey::status(&sha, &context, &state1);
            let key2 = DedupeKey::status(&sha, &context, &state2);

            prop_assert_ne!(key1, key2, "Different states must produce different keys");
        }

        /// Contexts with colons in different positions produce different keys.
        ///
        /// Tests that "a:b" and "a" always produce different keys, even though
        /// they share a common prefix. The escaping ensures the key format is
        /// unambiguous.
        #[test]
        fn status_colon_position_matters(
            sha in arb_sha(),
            a in "[a-zA-Z0-9]{1,10}",
            b in "[a-zA-Z0-9]{1,10}",
            state in arb_state(),
        ) {
            let ctx1 = format!("{}:{}", a, b); // "a:b"
            let ctx2 = a.clone();              // "a"

            // These contexts are always different (ctx1 has colon, ctx2 doesn't)
            prop_assume!(ctx1 != ctx2);

            let key1 = DedupeKey::status(&sha, &ctx1, &state);
            let key2 = DedupeKey::status(&sha, &ctx2, &state);

            prop_assert_ne!(key1, key2);
        }

        /// Escaping is consistent: same input always produces same output.
        #[test]
        fn status_escaping_is_deterministic(
            sha in arb_sha(),
            context in "[a-zA-Z0-9:/_\\\\-]{1,30}",
            state in arb_state(),
        ) {
            let key1 = DedupeKey::status(&sha, &context, &state);
            let key2 = DedupeKey::status(&sha, &context, &state);

            prop_assert_eq!(key1, key2);
        }

        /// Contexts with backslashes are properly escaped and don't collide.
        #[test]
        fn status_backslash_escaping_prevents_collisions(
            sha in arb_sha(),
            state in arb_state(),
        ) {
            // "a\:b" (literal backslash-colon) vs "a:b" (just colon)
            // These must produce different keys
            let ctx_with_backslash = r"a\:b";
            let ctx_with_colon = "a:b";

            let key1 = DedupeKey::status(&sha, ctx_with_backslash, &state);
            let key2 = DedupeKey::status(&sha, ctx_with_colon, &state);

            prop_assert_ne!(key1, key2, "Backslash-colon and plain colon must differ");
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

    // ─── Status key escaping tests ───

    #[test]
    fn status_key_escapes_colons_in_context() {
        let sha = Sha::parse("a".repeat(40)).unwrap();

        // Context with colon should be escaped
        let key = DedupeKey::status(&sha, "ci:build", "success");
        assert!(key.as_str().contains("ci\\:build"));
        assert_eq!(
            key.as_str(),
            format!("status:{}:ci\\:build:success", "a".repeat(40))
        );
    }

    #[test]
    fn status_key_escapes_backslashes_in_context() {
        let sha = Sha::parse("a".repeat(40)).unwrap();

        // Context with backslash should be escaped
        let key = DedupeKey::status(&sha, "ci\\test", "success");
        assert!(key.as_str().contains("ci\\\\test"));
    }

    #[test]
    fn status_key_different_contexts_produce_different_keys() {
        let sha = Sha::parse("a".repeat(40)).unwrap();

        // Basic test: different contexts produce different keys
        let key1 = DedupeKey::status(&sha, "ci:build", "success");
        let key2 = DedupeKey::status(&sha, "ci", "success");
        assert_ne!(key1, key2);

        // Multiple levels of colons still produce distinct keys
        let key3 = DedupeKey::status(&sha, "a:b:c", "pending");
        let key4 = DedupeKey::status(&sha, "a:b", "pending");
        let key5 = DedupeKey::status(&sha, "a", "pending");
        assert_ne!(key3, key4);
        assert_ne!(key3, key5);
        assert_ne!(key4, key5);
    }

    #[test]
    fn status_key_escaping_distinguishes_colon_vs_escaped_colon() {
        let sha = Sha::parse("a".repeat(40)).unwrap();

        // This is the key collision case that escaping prevents:
        // Without escaping, "a\:b" (literal backslash-colon in context) could produce
        // the same key as "a:b" (just colon). With proper escaping:
        // - "a:b"  → "a\:b" in key (colon escaped)
        // - "a\:b" → "a\\\:b" in key (backslash escaped to \\, then colon escaped to \:)
        let key_colon = DedupeKey::status(&sha, "a:b", "success");
        let key_backslash_colon = DedupeKey::status(&sha, r"a\:b", "success");

        assert_ne!(
            key_colon, key_backslash_colon,
            "literal backslash-colon must differ from plain colon"
        );

        // Verify the actual escaped forms
        // "a:b" escapes to "a\:b" (colon becomes backslash-colon)
        assert!(
            key_colon.as_str().contains(r"a\:b"),
            "key_colon should contain escaped colon: {}",
            key_colon.as_str()
        );
        // "a\:b" escapes to "a\\\:b" (backslash becomes \\, colon becomes \:)
        assert!(
            key_backslash_colon.as_str().contains(r"a\\\:b"),
            "key_backslash_colon should contain double-escaped form: {}",
            key_backslash_colon.as_str()
        );
    }

    #[test]
    fn status_key_context_without_special_chars_unchanged() {
        let sha = Sha::parse("a".repeat(40)).unwrap();

        // Simple context without colons or backslashes
        let key = DedupeKey::status(&sha, "continuous-integration", "success");
        assert_eq!(
            key.as_str(),
            format!("status:{}:continuous-integration:success", "a".repeat(40))
        );
    }

    // ─── Dedupe tracking tests ───

    proptest! {
        /// Once a key is marked as seen, is_duplicate returns true.
        #[test]
        fn marked_key_is_duplicate(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
        ) {
            let mut seen = HashMap::new();
            let key = DedupeKey::issue_comment_created(pr, comment_id);

            // Initially not a duplicate
            prop_assert!(!is_duplicate(&seen, &key));

            // Mark as seen
            mark_seen(&mut seen, &key);

            // Now it's a duplicate
            prop_assert!(is_duplicate(&seen, &key));
        }

        /// Different keys are not duplicates of each other.
        #[test]
        fn different_keys_not_duplicate(
            pr1 in arb_pr_number(),
            pr2 in arb_pr_number(),
            comment_id in arb_comment_id(),
        ) {
            prop_assume!(pr1 != pr2);

            let mut seen = HashMap::new();
            let key1 = DedupeKey::issue_comment_created(pr1, comment_id);
            let key2 = DedupeKey::issue_comment_created(pr2, comment_id);

            // Mark key1 as seen
            mark_seen(&mut seen, &key1);

            // key1 is duplicate, key2 is not
            prop_assert!(is_duplicate(&seen, &key1));
            prop_assert!(!is_duplicate(&seen, &key2));
        }

        /// Pruning removes only keys older than TTL.
        #[test]
        fn pruning_respects_ttl(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
        ) {
            let mut seen = HashMap::new();
            let key = DedupeKey::issue_comment_created(pr, comment_id);

            // Add a key with current timestamp
            mark_seen(&mut seen, &key);
            prop_assert_eq!(seen.len(), 1);

            // Pruning with 24 hour TTL should not remove it
            let pruned = prune_expired_keys(&mut seen, 24);
            prop_assert_eq!(pruned, 0);
            prop_assert_eq!(seen.len(), 1);

            // Manually set timestamp to 25 hours ago
            let old_timestamp = Utc::now() - chrono::Duration::hours(25);
            seen.insert(key.as_str().to_string(), old_timestamp);

            // Now pruning should remove it
            let pruned = prune_expired_keys(&mut seen, 24);
            prop_assert_eq!(pruned, 1);
            prop_assert_eq!(seen.len(), 0);
        }
    }

    #[test]
    fn prune_mixed_ages() {
        let mut seen = HashMap::new();

        // Add some fresh keys
        let fresh_key = DedupeKey::issue_comment_created(PrNumber(1), CommentId(1));
        mark_seen(&mut seen, &fresh_key);

        // Add some old keys (manually set timestamp)
        let old_key = DedupeKey::issue_comment_created(PrNumber(2), CommentId(2));
        let old_timestamp = Utc::now() - chrono::Duration::hours(25);
        seen.insert(old_key.as_str().to_string(), old_timestamp);

        // Add another old key
        let old_key2 = DedupeKey::issue_comment_created(PrNumber(3), CommentId(3));
        seen.insert(old_key2.as_str().to_string(), old_timestamp);

        assert_eq!(seen.len(), 3);

        // Prune with 24 hour TTL
        let pruned = prune_expired_keys_default(&mut seen);

        // Should remove 2 old keys, keep 1 fresh
        assert_eq!(pruned, 2);
        assert_eq!(seen.len(), 1);
        assert!(is_duplicate(&seen, &fresh_key));
        assert!(!is_duplicate(&seen, &old_key));
        assert!(!is_duplicate(&seen, &old_key2));
    }

    #[test]
    fn empty_seen_set_not_duplicate() {
        let seen = HashMap::new();
        let key = DedupeKey::issue_comment_created(PrNumber(123), CommentId(456));
        assert!(!is_duplicate(&seen, &key));
    }
}
