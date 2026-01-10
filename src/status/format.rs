//! Status comment formatting for recovery and user visibility.
//!
//! Status comments embed machine-readable JSON in an HTML comment, alongside
//! human-readable status. The JSON contains the full `TrainRecord` for disaster recovery.

use crate::types::train::TrainRecord;

/// Maximum size for error.message in status comments (4KB).
const MAX_ERROR_MESSAGE_LEN: usize = 4096;

/// Maximum size for error.stderr in status comments (2KB).
const MAX_ERROR_STDERR_LEN: usize = 2048;

/// Aggressive truncation limit when first truncation still exceeds size (500 chars).
const AGGRESSIVE_TRUNCATE_LEN: usize = 500;

/// Maximum safe size for the JSON portion (60KB, leaving room for human text).
const MAX_JSON_SIZE: usize = 60 * 1024;

/// GitHub's comment size limit (65536 characters).
pub const GITHUB_COMMENT_SIZE_LIMIT: usize = 65536;

/// The marker that begins a status comment JSON block.
pub const STATUS_COMMENT_START: &str = "<!-- merge-train-state\n";

/// The marker that ends a status comment JSON block.
pub const STATUS_COMMENT_END: &str = "\n-->";

/// Formats a status comment containing machine-readable JSON and human-readable text.
///
/// The comment format is:
/// ```text
/// <!-- merge-train-state
/// {"version": 1, ...}
/// -->
/// **Merge Train Status**
///
/// <human_message>
/// ```
///
/// The `status_comment_id` field is excluded from the JSON (it would be redundant
/// and confusing since the comment ID is the comment itself).
///
/// The human_message will be truncated if necessary to ensure the total comment
/// size stays within GitHub's 65536 character limit.
///
/// # Panics
///
/// Panics if the JSON portion exceeds `MAX_JSON_SIZE` (60KB) even after aggressive
/// truncation of error fields. This indicates a bug in size estimation (the 50 PR
/// limit should always fit within this budget).
pub fn format_status_comment(train: &TrainRecord, human_message: &str) -> String {
    // First try with normal truncation
    let truncated = truncate_for_size_limit(train.clone());
    let json = format_train_json(&truncated);
    let json = escape_html_comment_terminator(&json);

    if json.len() <= MAX_JSON_SIZE {
        return format_comment_body_with_limit(&json, human_message);
    }

    // Try aggressive truncation
    let aggressive = truncate_aggressively(truncated);
    let json = format_train_json(&aggressive);
    let json = escape_html_comment_terminator(&json);

    if json.len() <= MAX_JSON_SIZE {
        return format_comment_body_with_limit(&json, human_message);
    }

    // This should never happen with the 50 PR limit
    panic!(
        "JSON portion of status comment exceeds size limit after aggressive truncation. \
         JSON size: {} bytes, limit: {} bytes. This is a bug — please report it.",
        json.len(),
        MAX_JSON_SIZE
    );
}

/// Formats the TrainRecord to JSON, excluding `status_comment_id`.
fn format_train_json(train: &TrainRecord) -> String {
    // Create a copy without status_comment_id for serialization
    let mut train_for_json = train.clone();
    train_for_json.status_comment_id = None;
    serde_json::to_string_pretty(&train_for_json)
        .expect("TrainRecord serialization should not fail")
}

/// Fixed overhead for the comment structure (markers + title).
/// `<!-- merge-train-state\n` + `\n-->` + `\n**Merge Train Status**\n\n`
const COMMENT_OVERHEAD: usize =
    STATUS_COMMENT_START.len() + STATUS_COMMENT_END.len() + "\n**Merge Train Status**\n\n".len();

/// Formats the complete comment body with JSON and human message.
///
/// Truncates `human_message` if necessary to stay within GitHub's comment size limit.
fn format_comment_body_with_limit(json: &str, human_message: &str) -> String {
    let used = COMMENT_OVERHEAD + json.len();
    let max_human_len = GITHUB_COMMENT_SIZE_LIMIT.saturating_sub(used);

    let human = if human_message.len() > max_human_len {
        truncate_with_suffix(human_message, max_human_len)
    } else {
        human_message.to_string()
    };

    format!(
        "{}{}{}\n**Merge Train Status**\n\n{}",
        STATUS_COMMENT_START, json, STATUS_COMMENT_END, human
    )
}

/// Escapes the HTML comment terminator `-->` in JSON to prevent breaking the comment.
///
/// Replaces `-->` with `--\u003e` which is a valid JSON escape for `>`.
/// JSON parsers automatically decode this during deserialization.
fn escape_html_comment_terminator(json: &str) -> String {
    json.replace("-->", r"--\u003e")
}

/// Truncates variable-length fields to ensure the status comment fits within size limits.
///
/// Truncates:
/// - `error.message` to 4KB
/// - `error.stderr` to 2KB
///
/// All other fields are preserved exactly. The truncation happens before JSON serialization
/// to ensure accurate size estimation.
pub fn truncate_for_size_limit(mut train: TrainRecord) -> TrainRecord {
    if let Some(ref mut error) = train.error {
        if error.message.len() > MAX_ERROR_MESSAGE_LEN {
            error.message = truncate_with_suffix(&error.message, MAX_ERROR_MESSAGE_LEN);
        }
        if let Some(ref mut stderr) = error.stderr
            && stderr.len() > MAX_ERROR_STDERR_LEN
        {
            *stderr = truncate_with_suffix(stderr, MAX_ERROR_STDERR_LEN);
        }
    }
    train
}

/// Aggressively truncates error fields to 500 characters each.
fn truncate_aggressively(mut train: TrainRecord) -> TrainRecord {
    if let Some(ref mut error) = train.error {
        if error.message.len() > AGGRESSIVE_TRUNCATE_LEN {
            error.message = truncate_with_suffix(&error.message, AGGRESSIVE_TRUNCATE_LEN);
        }
        if let Some(ref mut stderr) = error.stderr
            && stderr.len() > AGGRESSIVE_TRUNCATE_LEN
        {
            *stderr = truncate_with_suffix(stderr, AGGRESSIVE_TRUNCATE_LEN);
        }
    }
    train
}

/// Truncates a string to the given length with a "... [truncated]" suffix.
fn truncate_with_suffix(s: &str, max_len: usize) -> String {
    const SUFFIX: &str = "... [truncated]";

    if s.len() <= max_len {
        return s.to_string();
    }

    // Ensure we have room for the suffix
    let content_len = max_len.saturating_sub(SUFFIX.len());

    // Find a valid UTF-8 boundary
    let mut end = content_len;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }

    format!("{}{}", &s[..end], SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{arb_cascade_phase, arb_pr_number, arb_sha, arb_train_error};
    use crate::types::CommentId;
    use crate::types::train::{TrainRecord, TrainState};
    use chrono::{DateTime, Utc};
    use proptest::prelude::*;

    fn arb_train_state() -> impl Strategy<Value = TrainState> {
        prop_oneof![
            Just(TrainState::Running),
            Just(TrainState::Stopped),
            Just(TrainState::WaitingCi),
            Just(TrainState::Aborted),
            Just(TrainState::NeedsManualReview),
        ]
    }

    fn arb_datetime() -> impl Strategy<Value = DateTime<Utc>> {
        (946684800i64..4102444800i64).prop_map(|secs| DateTime::from_timestamp(secs, 0).unwrap())
    }

    fn arb_train_record() -> impl Strategy<Value = TrainRecord> {
        (
            arb_pr_number(),
            arb_pr_number(),
            arb_train_state(),
            arb_cascade_phase(),
            prop::option::of(arb_pr_number()),
            prop::option::of(arb_sha()),
            prop::option::of(arb_sha()),
            prop::option::of(arb_train_error()),
            any::<u64>(),
            arb_datetime(),
            prop::option::of(arb_datetime()),
            prop::option::of(any::<u64>().prop_map(CommentId)),
        )
            .prop_map(
                |(
                    original_root_pr,
                    current_pr,
                    state,
                    cascade_phase,
                    predecessor_pr,
                    predecessor_head_sha,
                    last_squash_sha,
                    error,
                    recovery_seq,
                    started_at,
                    ended_at,
                    status_comment_id,
                )| {
                    TrainRecord {
                        version: 1,
                        recovery_seq,
                        state,
                        original_root_pr,
                        current_pr,
                        cascade_phase,
                        predecessor_pr,
                        predecessor_head_sha,
                        last_squash_sha,
                        started_at,
                        ended_at,
                        error,
                        status_comment_id,
                    }
                },
            )
    }

    /// Generates a train with a realistic large descendant set (up to 50 PRs).
    fn arb_large_train() -> impl Strategy<Value = TrainRecord> {
        (
            arb_pr_number(),
            prop::collection::vec(arb_pr_number(), 10..50),
            arb_train_state(),
            prop::option::of(arb_sha()),
            any::<u64>(),
            arb_datetime(),
        )
            .prop_map(
                |(root_pr, descendants, state, squash_sha, recovery_seq, started_at)| {
                    use crate::types::train::{CascadePhase, DescendantProgress};
                    let mut progress = DescendantProgress::new(descendants);
                    // Mark some as completed/skipped
                    let frozen = progress.frozen_descendants.clone();
                    for (i, pr) in frozen.iter().enumerate() {
                        if i % 3 == 0 {
                            progress.mark_completed(*pr);
                        } else if i % 5 == 0 {
                            progress.mark_skipped(*pr);
                        }
                    }

                    let cascade_phase = if let Some(sha) = squash_sha {
                        CascadePhase::Reconciling {
                            progress,
                            squash_sha: sha,
                        }
                    } else {
                        CascadePhase::Preparing { progress }
                    };

                    TrainRecord {
                        version: 1,
                        recovery_seq,
                        state,
                        original_root_pr: root_pr,
                        current_pr: root_pr,
                        cascade_phase,
                        predecessor_pr: None,
                        predecessor_head_sha: None,
                        last_squash_sha: None,
                        started_at,
                        ended_at: None,
                        error: None,
                        status_comment_id: None,
                    }
                },
            )
    }

    mod truncation {
        use super::*;
        use crate::types::train::TrainError;

        #[test]
        fn truncate_with_suffix_preserves_short_strings() {
            let s = "short message";
            assert_eq!(truncate_with_suffix(s, 100), s);
        }

        #[test]
        fn truncate_with_suffix_adds_suffix() {
            let s = "a".repeat(1000);
            let truncated = truncate_with_suffix(&s, 100);
            assert!(truncated.ends_with("... [truncated]"));
            assert!(truncated.len() <= 100);
        }

        #[test]
        fn truncate_with_suffix_handles_utf8() {
            // Multi-byte UTF-8 characters
            let s = "🚂".repeat(100);
            let truncated = truncate_with_suffix(&s, 50);
            // Should not panic and should be valid UTF-8
            assert!(truncated.len() <= 50);
            assert!(truncated.is_char_boundary(truncated.len()));
        }

        #[test]
        fn truncate_for_size_limit_truncates_error_message() {
            let mut train = TrainRecord::new(1.into());
            train.error = Some(TrainError::new("test", "x".repeat(10000)));

            let truncated = truncate_for_size_limit(train);
            let msg = &truncated.error.unwrap().message;
            assert!(msg.len() <= MAX_ERROR_MESSAGE_LEN);
            assert!(msg.ends_with("... [truncated]"));
        }

        #[test]
        fn truncate_for_size_limit_truncates_stderr() {
            let mut train = TrainRecord::new(1.into());
            train.error = Some(TrainError::new("test", "msg").with_stderr("x".repeat(5000)));

            let truncated = truncate_for_size_limit(train);
            let stderr = truncated.error.unwrap().stderr.unwrap();
            assert!(stderr.len() <= MAX_ERROR_STDERR_LEN);
            assert!(stderr.ends_with("... [truncated]"));
        }

        #[test]
        fn truncate_for_size_limit_preserves_other_fields() {
            let mut train = TrainRecord::new(1.into());
            train.recovery_seq = 42;
            train.error = Some(TrainError::new("big_error", "x".repeat(10000)));

            let truncated = truncate_for_size_limit(train.clone());

            // All fields except error.message should be identical
            assert_eq!(truncated.version, train.version);
            assert_eq!(truncated.recovery_seq, train.recovery_seq);
            assert_eq!(truncated.state, train.state);
            assert_eq!(truncated.original_root_pr, train.original_root_pr);
            assert_eq!(truncated.current_pr, train.current_pr);
            assert_eq!(truncated.cascade_phase, train.cascade_phase);
            assert_eq!(
                truncated.error.as_ref().unwrap().error_type,
                train.error.as_ref().unwrap().error_type
            );
        }

        proptest! {
            #[test]
            fn truncation_preserves_all_fields_except_error_content(train in arb_train_record()) {
                let truncated = truncate_for_size_limit(train.clone());

                // All fields except error.message/stderr should be identical
                prop_assert_eq!(truncated.version, train.version);
                prop_assert_eq!(truncated.recovery_seq, train.recovery_seq);
                prop_assert_eq!(truncated.state, train.state);
                prop_assert_eq!(truncated.original_root_pr, train.original_root_pr);
                prop_assert_eq!(truncated.current_pr, train.current_pr);
                prop_assert_eq!(truncated.cascade_phase, train.cascade_phase);
                prop_assert_eq!(truncated.predecessor_pr, train.predecessor_pr);
                prop_assert_eq!(truncated.predecessor_head_sha, train.predecessor_head_sha);
                prop_assert_eq!(truncated.last_squash_sha, train.last_squash_sha);
                prop_assert_eq!(truncated.started_at, train.started_at);
                prop_assert_eq!(truncated.ended_at, train.ended_at);
                prop_assert_eq!(truncated.status_comment_id, train.status_comment_id);

                // Error type should be preserved even if message is truncated
                if let (Some(orig), Some(trunc)) = (&train.error, &truncated.error) {
                    prop_assert_eq!(&trunc.error_type, &orig.error_type);
                }
            }
        }
    }

    mod format {
        use super::*;

        #[test]
        fn format_includes_markers() {
            let train = TrainRecord::new(1.into());
            let comment = format_status_comment(&train, "Train is running");

            assert!(comment.starts_with(STATUS_COMMENT_START));
            assert!(comment.contains(STATUS_COMMENT_END));
            assert!(comment.contains("**Merge Train Status**"));
            assert!(comment.contains("Train is running"));
        }

        #[test]
        fn format_excludes_status_comment_id() {
            let mut train = TrainRecord::new(1.into());
            train.status_comment_id = Some(CommentId(12345));

            let comment = format_status_comment(&train, "msg");

            // The JSON should not contain status_comment_id
            assert!(!comment.contains("status_comment_id"));
            assert!(!comment.contains("12345"));
        }

        #[test]
        fn large_human_message_is_truncated() {
            let train = TrainRecord::new(1.into());
            // Create a human message larger than the remaining space
            let huge_message = "x".repeat(70000);

            let comment = format_status_comment(&train, &huge_message);

            assert!(
                comment.len() <= GITHUB_COMMENT_SIZE_LIMIT,
                "Comment length {} exceeds GitHub limit {}",
                comment.len(),
                GITHUB_COMMENT_SIZE_LIMIT
            );
            assert!(
                comment.contains("... [truncated]"),
                "Large human message should be truncated"
            );
        }

        #[test]
        fn html_comment_terminator_is_escaped() {
            use crate::types::train::TrainError;

            let mut train = TrainRecord::new(1.into());
            // Error message containing the HTML comment terminator
            train.error = Some(TrainError::new("test", "error --> happened"));

            let comment = format_status_comment(&train, "status");

            // The literal --> should not appear in the JSON portion
            // (it would prematurely close the HTML comment)
            let json_start = comment.find(STATUS_COMMENT_START).unwrap();
            let json_end = comment.find(STATUS_COMMENT_END).unwrap();
            let json_portion = &comment[json_start..json_end];

            assert!(
                !json_portion.contains("-->"),
                "JSON should not contain literal '-->' which would break HTML comment"
            );
            // The escaped version should be present
            assert!(
                json_portion.contains(r"--\u003e"),
                "JSON should contain escaped form"
            );
        }

        proptest! {
            #[test]
            fn format_under_github_limit(train in arb_train_record()) {
                let comment = format_status_comment(&train, "Status message");
                prop_assert!(comment.len() <= GITHUB_COMMENT_SIZE_LIMIT,
                    "Comment length {} exceeds GitHub limit {}", comment.len(), GITHUB_COMMENT_SIZE_LIMIT);
            }

            #[test]
            fn large_trains_under_github_limit(train in arb_large_train()) {
                let comment = format_status_comment(&train, "Status message for a large train with many descendants");
                prop_assert!(comment.len() <= GITHUB_COMMENT_SIZE_LIMIT,
                    "Large train comment length {} exceeds GitHub limit {}", comment.len(), GITHUB_COMMENT_SIZE_LIMIT);
            }

            #[test]
            fn large_human_message_stays_under_limit(
                train in arb_train_record(),
                human_len in 0usize..100000
            ) {
                let human_message = "x".repeat(human_len);
                let comment = format_status_comment(&train, &human_message);
                prop_assert!(comment.len() <= GITHUB_COMMENT_SIZE_LIMIT,
                    "Comment length {} exceeds GitHub limit {} with human_len {}",
                    comment.len(), GITHUB_COMMENT_SIZE_LIMIT, human_len);
            }
        }
    }
}
