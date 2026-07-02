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
//! - `pull_request.<action>`: `pull_request:<pr>:<action>:<head_sha>:<updated_at>`
//! - `pull_request.edited`: `pull_request:<pr>:edited:<base>:<updated_at>`
//! - `check_suite.<action>`: `check_suite:<suite_id>:<action>:<updated_at>`
//! - `status`: `status:<sha>:<context>:<state>:<updated_at>`
//!
//! Keys for events that can legitimately repeat (`status`, `pull_request`,
//! `check_suite`) include the event timestamp: without it, e.g. CI going
//! success → failure → success within the TTL would dedupe the second
//! success and deadlock a train waiting on it.
//!
//! # TTL-based Expiration
//!
//! Seen dedupe keys live in the Store's `dedupe_keys` table with the time
//! first seen; `Store::prune_dedupe` removes keys past the retention period.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{CommentId, PrNumber, Sha};
use crate::webhooks::events::{CommentAction, GitHubEvent, PrAction};

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
    /// The head SHA alone cannot distinguish legitimate repeats (e.g. close →
    /// reopen → close of the same head), so `updated_at` is part of the key.
    pub fn pull_request(
        pr: PrNumber,
        action: &str,
        head_sha: &Sha,
        updated_at: &DateTime<Utc>,
    ) -> Self {
        DedupeKey(format!(
            "pull_request:{}:{}:{}:{}",
            pr.0,
            action,
            head_sha.as_str(),
            updated_at.to_rfc3339()
        ))
    }

    /// Creates a dedupe key for a `pull_request.edited` event.
    ///
    /// Edits don't necessarily change the head SHA (e.g., base retarget,
    /// title change), so the key uses `updated_at` — plus the base branch:
    /// GitHub timestamps are second-resolution, and a title edit followed
    /// immediately by a base retarget can share `updated_at`; without the
    /// base in the key the retarget would be dropped as a duplicate and the
    /// cached topology would never learn it (Codex M5 round 11). The base
    /// is the only edit the bot acts on, so it is the disambiguator.
    pub fn pull_request_edited(
        pr: PrNumber,
        base_branch: &str,
        updated_at: &DateTime<Utc>,
    ) -> Self {
        DedupeKey(format!(
            "pull_request:{}:edited:{}:{}",
            pr.0,
            base_branch,
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
    /// `updated_at` distinguishes legitimate repeats of the same
    /// (sha, context, state) triple, e.g. CI success → failure → success.
    ///
    /// The context is escaped to prevent collisions when it contains the
    /// separator character (`:`): backslashes first, then colons, so the key
    /// parses unambiguously.
    pub fn status(sha: &Sha, context: &str, state: &str, updated_at: &DateTime<Utc>) -> Self {
        let escaped_context = context.replace('\\', "\\\\").replace(':', "\\:");
        DedupeKey(format!(
            "status:{}:{}:{}:{}",
            sha.as_str(),
            escaped_context,
            state,
            updated_at.to_rfc3339()
        ))
    }

    /// Creates a dedupe key for a `pull_request_review` event.
    pub fn pull_request_review(pr: PrNumber, review_id: u64, action: &str) -> Self {
        DedupeKey(format!(
            "pull_request_review:{}:{}:{}",
            pr.0, review_id, action
        ))
    }

    /// The dedupe key for a parsed webhook event, or `None` for events with
    /// no dedupe identity (a comment on a plain issue — the bot ignores it,
    /// so duplicate handling is moot).
    pub fn for_event(event: &GitHubEvent) -> Option<DedupeKey> {
        match event {
            GitHubEvent::IssueComment(e) => {
                let pr = e.pr_number?;
                Some(match e.action {
                    CommentAction::Created => DedupeKey::issue_comment_created(pr, e.comment_id),
                    CommentAction::Edited => {
                        DedupeKey::issue_comment_edited(pr, e.comment_id, &e.updated_at)
                    }
                    CommentAction::Deleted => DedupeKey::issue_comment_deleted(pr, e.comment_id),
                })
            }
            // Edits don't necessarily change the head SHA (base retarget,
            // title change), so the edited key deliberately omits it.
            GitHubEvent::PullRequest(e) => Some(match e.action {
                PrAction::Edited => {
                    DedupeKey::pull_request_edited(e.pr_number, &e.base_branch, &e.updated_at)
                }
                action => DedupeKey::pull_request(
                    e.pr_number,
                    action.as_str(),
                    &e.head_sha,
                    &e.updated_at,
                ),
            }),
            GitHubEvent::CheckSuite(e) => Some(DedupeKey::check_suite(
                e.suite_id,
                e.action.as_str(),
                &e.updated_at,
            )),
            GitHubEvent::Status(e) => Some(DedupeKey::status(
                &e.sha,
                &e.context,
                e.state.as_str(),
                &e.updated_at,
            )),
            GitHubEvent::PullRequestReview(e) => Some(DedupeKey::pull_request_review(
                e.pr_number,
                e.review_id,
                e.action.as_str(),
            )),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RepoId;
    use crate::webhooks::events::MergeStatus;
    use crate::webhooks::events::{
        CheckSuiteAction, CheckSuiteEvent, IssueCommentEvent, PullRequestEvent,
        PullRequestReviewEvent, ReviewAction, ReviewState, StatusEvent, StatusState,
    };
    use proptest::prelude::*;

    fn repo() -> RepoId {
        RepoId::new("owner", "repo")
    }

    fn comment_event(
        action: CommentAction,
        pr_number: Option<PrNumber>,
        comment_id: u64,
        updated_at: DateTime<Utc>,
    ) -> IssueCommentEvent {
        IssueCommentEvent {
            repo: repo(),
            action,
            pr_number,
            comment_id: CommentId(comment_id),
            body: String::new(),
            author_id: 1,
            author_login: "a".to_owned(),
            sender_id: 1,
            sender_login: "a".to_owned(),
            pr_author_id: 2,
            updated_at,
        }
    }

    fn pr_event(
        action: PrAction,
        pr_number: PrNumber,
        head_sha: Sha,
        updated_at: DateTime<Utc>,
    ) -> PullRequestEvent {
        PullRequestEvent {
            repo: repo(),
            action,
            pr_number,
            merge_status: MergeStatus::NotMerged,
            head_sha,
            base_branch: "main".to_owned(),
            head_branch: "feature".to_owned(),
            is_draft: false,
            author_id: 1,
            updated_at,
        }
    }

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

        /// Repeats of the same (sha, context, state) triple at different
        /// times are distinct events: CI success → failure → success within
        /// the dedupe TTL must not collapse, or a train waiting on the final
        /// success deadlocks.
        #[test]
        fn status_repeats_at_different_times_are_distinct(
            sha in arb_sha(),
            context in arb_context(),
            state in arb_state(),
            updated_at1 in arb_datetime(),
            updated_at2 in arb_datetime(),
        ) {
            prop_assume!(updated_at1 != updated_at2);
            let key1 = DedupeKey::status(&sha, &context, &state, &updated_at1);
            let key2 = DedupeKey::status(&sha, &context, &state, &updated_at2);
            prop_assert_ne!(key1, key2);
        }

        /// Repeats of the same (pr, action, head_sha) triple at different
        /// times are distinct events (e.g. close → reopen → close of the
        /// same head).
        #[test]
        fn pull_request_repeats_at_different_times_are_distinct(
            pr in arb_pr_number(),
            action in arb_action(),
            head_sha in arb_sha(),
            updated_at1 in arb_datetime(),
            updated_at2 in arb_datetime(),
        ) {
            prop_assume!(updated_at1 != updated_at2);
            let key1 = DedupeKey::pull_request(pr, &action, &head_sha, &updated_at1);
            let key2 = DedupeKey::pull_request(pr, &action, &head_sha, &updated_at2);
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

        // ─── DedupeKey::for_event: the parsed-event → key mapping ───

        /// Every keyed comment action maps through `for_event` to the same
        /// key the dedicated constructor builds.
        #[test]
        fn for_event_issue_comment_matches_constructors(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
            updated_at in arb_datetime(),
        ) {
            for (action, expected) in [
                (CommentAction::Created, DedupeKey::issue_comment_created(pr, CommentId(comment_id.0))),
                (CommentAction::Edited, DedupeKey::issue_comment_edited(pr, CommentId(comment_id.0), &updated_at)),
                (CommentAction::Deleted, DedupeKey::issue_comment_deleted(pr, CommentId(comment_id.0))),
            ] {
                let event = GitHubEvent::IssueComment(comment_event(action, Some(pr), comment_id.0, updated_at));
                prop_assert_eq!(DedupeKey::for_event(&event), Some(expected));
            }
        }

        /// A comment on a plain issue (no PR) has no dedupe identity.
        #[test]
        fn for_event_non_pr_comment_has_no_key(
            comment_id in arb_comment_id(),
            updated_at in arb_datetime(),
        ) {
            let event = GitHubEvent::IssueComment(comment_event(
                CommentAction::Created, None, comment_id.0, updated_at,
            ));
            prop_assert_eq!(DedupeKey::for_event(&event), None);
        }

        /// Non-edited PR actions key on (pr, action, head, updated_at).
        #[test]
        fn for_event_pull_request_matches_constructor(
            pr in arb_pr_number(),
            head_sha in arb_sha(),
            updated_at in arb_datetime(),
        ) {
            let event = pr_event(PrAction::Synchronize, pr, head_sha.clone(), updated_at);
            prop_assert_eq!(
                DedupeKey::for_event(&GitHubEvent::PullRequest(event)),
                Some(DedupeKey::pull_request(pr, "synchronize", &head_sha, &updated_at))
            );
        }

        /// GitHub timestamps are second-resolution, so a title edit followed
        /// immediately by a base retarget can share `updated_at` — the keys
        /// must still differ or the retarget is dropped as a duplicate and
        /// the cached topology never learns it (Codex M5 round 11).
        #[test]
        fn for_event_edited_pr_distinguishes_same_second_base_changes(
            pr in arb_pr_number(),
            head in arb_sha(),
            updated_at in arb_datetime(),
        ) {
            let mut a = pr_event(PrAction::Edited, pr, head.clone(), updated_at);
            a.base_branch = "main".to_owned();
            let mut b = pr_event(PrAction::Edited, pr, head, updated_at);
            b.base_branch = "pr-1".to_owned();
            prop_assert_ne!(
                DedupeKey::for_event(&GitHubEvent::PullRequest(a)),
                DedupeKey::for_event(&GitHubEvent::PullRequest(b))
            );
        }

        /// The `edited` key deliberately ignores the head SHA: edits (title,
        /// base retarget) don't change it, so including it would fail to
        /// dedupe redeliveries whose payloads differ only in a racing head.
        #[test]
        fn for_event_edited_pr_ignores_head_sha(
            pr in arb_pr_number(),
            head_a in arb_sha(),
            head_b in arb_sha(),
            updated_at in arb_datetime(),
        ) {
            let a = pr_event(PrAction::Edited, pr, head_a, updated_at);
            let b = pr_event(PrAction::Edited, pr, head_b, updated_at);
            prop_assert_eq!(
                DedupeKey::for_event(&GitHubEvent::PullRequest(a)),
                DedupeKey::for_event(&GitHubEvent::PullRequest(b))
            );
        }

        #[test]
        fn for_event_check_suite_matches_constructor(
            suite_id in 1u64..u64::MAX,
            head_sha in arb_sha(),
            updated_at in arb_datetime(),
        ) {
            let event = GitHubEvent::CheckSuite(CheckSuiteEvent {
                repo: repo(),
                action: CheckSuiteAction::Completed,
                head_sha,
                conclusion: None,
                pull_requests: vec![],
                suite_id,
                updated_at,
            });
            prop_assert_eq!(
                DedupeKey::for_event(&event),
                Some(DedupeKey::check_suite(suite_id, "completed", &updated_at))
            );
        }

        #[test]
        fn for_event_status_matches_constructor(
            sha in arb_sha(),
            context in arb_context(),
            updated_at in arb_datetime(),
        ) {
            let event = GitHubEvent::Status(StatusEvent {
                repo: repo(),
                sha: sha.clone(),
                state: StatusState::Success,
                context: context.clone(),
                description: None,
                target_url: None,
                updated_at,
            });
            prop_assert_eq!(
                DedupeKey::for_event(&event),
                Some(DedupeKey::status(&sha, &context, "success", &updated_at))
            );
        }

        #[test]
        fn for_event_review_matches_constructor(
            pr in arb_pr_number(),
            review_id in 1u64..u64::MAX,
        ) {
            let event = GitHubEvent::PullRequestReview(PullRequestReviewEvent {
                repo: repo(),
                action: ReviewAction::Dismissed,
                pr_number: pr,
                state: ReviewState::Dismissed,
                reviewer_id: 1,
                reviewer_login: "r".to_owned(),
                body: String::new(),
                review_id,
            });
            prop_assert_eq!(
                DedupeKey::for_event(&event),
                Some(DedupeKey::pull_request_review(pr, review_id, "dismissed"))
            );
        }

        /// Totality: every event shape except a non-PR comment gets a key.
        #[test]
        fn for_event_total_over_keyed_shapes(
            pr in arb_pr_number(),
            comment_id in arb_comment_id(),
            sha in arb_sha(),
            updated_at in arb_datetime(),
        ) {
            let keyed: Vec<GitHubEvent> = vec![
                GitHubEvent::IssueComment(comment_event(CommentAction::Created, Some(pr), comment_id.0, updated_at)),
                GitHubEvent::PullRequest(pr_event(PrAction::Opened, pr, sha.clone(), updated_at)),
                GitHubEvent::CheckSuite(CheckSuiteEvent {
                    repo: repo(),
                    action: CheckSuiteAction::Completed,
                    head_sha: sha.clone(),
                    conclusion: None,
                    pull_requests: vec![],
                    suite_id: 7,
                    updated_at,
                }),
                GitHubEvent::Status(StatusEvent {
                    repo: repo(),
                    sha: sha.clone(),
                    state: StatusState::Failure,
                    context: "ci".to_owned(),
                    description: None,
                    target_url: None,
                    updated_at,
                }),
                GitHubEvent::PullRequestReview(PullRequestReviewEvent {
                    repo: repo(),
                    action: ReviewAction::Submitted,
                    pr_number: pr,
                    state: ReviewState::Approved,
                    reviewer_id: 1,
                    reviewer_login: "r".to_owned(),
                    body: String::new(),
                    review_id: 9,
                }),
            ];
            for event in &keyed {
                prop_assert!(DedupeKey::for_event(event).is_some());
            }
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
            updated_at in arb_datetime(),
        ) {
            prop_assume!(ctx1 != ctx2);

            let key1 = DedupeKey::status(&sha, &ctx1, &state, &updated_at);
            let key2 = DedupeKey::status(&sha, &ctx2, &state, &updated_at);

            prop_assert_ne!(key1, key2, "Different contexts must produce different keys");
        }

        /// Different status states always produce different keys.
        #[test]
        fn status_different_states_never_collide(
            sha in arb_sha(),
            context in "[a-zA-Z0-9:/_-]{1,30}",
            state1 in arb_state(),
            state2 in arb_state(),
            updated_at in arb_datetime(),
        ) {
            prop_assume!(state1 != state2);

            let key1 = DedupeKey::status(&sha, &context, &state1, &updated_at);
            let key2 = DedupeKey::status(&sha, &context, &state2, &updated_at);

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
            updated_at in arb_datetime(),
        ) {
            let ctx1 = format!("{}:{}", a, b); // "a:b"
            let ctx2 = a.clone();              // "a"

            // These contexts are always different (ctx1 has colon, ctx2 doesn't)
            prop_assume!(ctx1 != ctx2);

            let key1 = DedupeKey::status(&sha, &ctx1, &state, &updated_at);
            let key2 = DedupeKey::status(&sha, &ctx2, &state, &updated_at);

            prop_assert_ne!(key1, key2);
        }

        /// Contexts with backslashes are properly escaped and don't collide.
        #[test]
        fn status_backslash_escaping_prevents_collisions(
            sha in arb_sha(),
            state in arb_state(),
            updated_at in arb_datetime(),
        ) {
            // "a\:b" (literal backslash-colon) vs "a:b" (just colon)
            // These must produce different keys
            let ctx_with_backslash = r"a\:b";
            let ctx_with_colon = "a:b";

            let key1 = DedupeKey::status(&sha, ctx_with_backslash, &state, &updated_at);
            let key2 = DedupeKey::status(&sha, ctx_with_colon, &state, &updated_at);

            prop_assert_ne!(key1, key2, "Backslash-colon and plain colon must differ");
        }
    }

    // ─── Unit tests ───

    /// A fixed timestamp for unit tests of key formats.
    fn t0() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn key_format_matches_expected() {
        let key = DedupeKey::issue_comment_created(PrNumber(123), CommentId(456789));
        assert_eq!(key.as_str(), "issue_comment:123:456789:created");

        let updated_at = t0();
        let key = DedupeKey::pull_request(
            PrNumber(42),
            "opened",
            &Sha::parse("a".repeat(40)).unwrap(),
            &updated_at,
        );
        assert_eq!(
            key.as_str(),
            format!(
                "pull_request:42:opened:{}:{}",
                "a".repeat(40),
                updated_at.to_rfc3339()
            )
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
        let updated_at = t0();

        // Context with colon should be escaped
        let key = DedupeKey::status(&sha, "ci:build", "success", &updated_at);
        assert!(key.as_str().contains("ci\\:build"));
        assert_eq!(
            key.as_str(),
            format!(
                "status:{}:ci\\:build:success:{}",
                "a".repeat(40),
                updated_at.to_rfc3339()
            )
        );
    }

    #[test]
    fn status_key_escapes_backslashes_in_context() {
        let sha = Sha::parse("a".repeat(40)).unwrap();

        // Context with backslash should be escaped
        let key = DedupeKey::status(&sha, "ci\\test", "success", &t0());
        assert!(key.as_str().contains("ci\\\\test"));
    }

    #[test]
    fn status_key_different_contexts_produce_different_keys() {
        let sha = Sha::parse("a".repeat(40)).unwrap();
        let updated_at = t0();

        // Basic test: different contexts produce different keys
        let key1 = DedupeKey::status(&sha, "ci:build", "success", &updated_at);
        let key2 = DedupeKey::status(&sha, "ci", "success", &updated_at);
        assert_ne!(key1, key2);

        // Multiple levels of colons still produce distinct keys
        let key3 = DedupeKey::status(&sha, "a:b:c", "pending", &updated_at);
        let key4 = DedupeKey::status(&sha, "a:b", "pending", &updated_at);
        let key5 = DedupeKey::status(&sha, "a", "pending", &updated_at);
        assert_ne!(key3, key4);
        assert_ne!(key3, key5);
        assert_ne!(key4, key5);
    }

    #[test]
    fn status_key_escaping_distinguishes_colon_vs_escaped_colon() {
        let sha = Sha::parse("a".repeat(40)).unwrap();
        let updated_at = t0();

        // This is the key collision case that escaping prevents:
        // Without escaping, "a\:b" (literal backslash-colon in context) could produce
        // the same key as "a:b" (just colon). With proper escaping:
        // - "a:b"  → "a\:b" in key (colon escaped)
        // - "a\:b" → "a\\\:b" in key (backslash escaped to \\, then colon escaped to \:)
        let key_colon = DedupeKey::status(&sha, "a:b", "success", &updated_at);
        let key_backslash_colon = DedupeKey::status(&sha, r"a\:b", "success", &updated_at);

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
        let updated_at = t0();

        // Simple context without colons or backslashes
        let key = DedupeKey::status(&sha, "continuous-integration", "success", &updated_at);
        assert_eq!(
            key.as_str(),
            format!(
                "status:{}:continuous-integration:success:{}",
                "a".repeat(40),
                updated_at.to_rfc3339()
            )
        );
    }
}
