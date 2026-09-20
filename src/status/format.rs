//! Status comment formatting for recovery and user visibility.
//!
//! Status comments embed machine-readable JSON in an HTML comment, alongside
//! human-readable status. The JSON contains the `TrainRecord` for disaster recovery
//! (excluding `status_comment_id`, with error text possibly truncated for size).

use crate::types::PrNumber;
use crate::types::train::{TrainRecord, TrainState};
use thiserror::Error;

/// The train's JSON representation exceeds the status-comment budget even
/// after aggressive truncation of error fields.
///
/// The cascade engine must treat this as an abort condition
/// (`AbortReason::StatusCommentTooLarge`): without a status comment there is
/// no backup state, and proceeding without backup state is uncharacterizable.
#[derive(Debug, Error)]
#[error(
    "status comment JSON is {json_len} bytes after aggressive truncation \
     (limit {limit} bytes)"
)]
pub struct StatusCommentTooLarge {
    pub json_len: usize,
    pub limit: usize,
}

/// Maximum size for error.message in status comments (4KB bytes).
const MAX_ERROR_MESSAGE_LEN: usize = 4096;

/// Maximum size for error.stderr in status comments (2KB bytes).
const MAX_ERROR_STDERR_LEN: usize = 2048;

/// Aggressive truncation limit when first truncation still exceeds size (500 bytes).
const AGGRESSIVE_TRUNCATE_LEN: usize = 500;

/// Maximum safe size for the JSON portion (60KB bytes, leaving room for human text).
const MAX_JSON_SIZE: usize = 60 * 1024;

/// GitHub's comment size limit (65536 bytes).
pub const GITHUB_COMMENT_SIZE_LIMIT: usize = 65536;

/// The largest train an operator may configure the bot to run
/// (`MERGE_TRAIN_MAX_STACK_SIZE`; see `cascade::TrainSizeCap`).
///
/// The status comment carries the whole `TrainRecord`, and a train's members
/// appear in up to three of its lists (`frozen_descendants`, `known_stack`,
/// and one of `completed`/`skipped`). Nothing truncates those lists — only
/// the error fields are truncatable — so the train size is the only thing
/// bounding the JSON, and a train above the bound would abort mid-flight
/// with [`StatusCommentTooLarge`] rather than fail to start.
///
/// The bound is measured, not assumed:
/// `the_worst_case_train_at_the_ceiling_still_fits` searches for the largest
/// train that survives the worst case this schema admits — 20-digit PR
/// numbers, a 255-byte default branch, every member in all three lists, and
/// error text of pure control characters, each byte of which JSON escaping
/// expands sixfold. That search says 605. This ceiling keeps a sixth of the
/// budget in reserve for schema growth, and
/// `the_measured_headroom_has_not_been_eaten` fails if a future field spends
/// it.
pub const MAX_SUPPORTED_TRAIN_SIZE: usize = 500;

/// The marker that begins a status comment JSON block.
pub const STATUS_COMMENT_START: &str = "<!-- merge-train-state\n";

/// The marker that ends a status comment JSON block.
pub const STATUS_COMMENT_END: &str = "\n-->";

/// The last thing a train's status comment says. Shared by the engine,
/// which posts it when the train retires, and by the retry that owes that
/// update until it lands — so a rewrite cannot quietly say something
/// weaker than the original. `fanned_into` names the independent trains a
/// fan-out spawned; it is empty for every other ending.
pub fn terminal_message(record: &TrainRecord, fanned_into: &[PrNumber]) -> String {
    if !fanned_into.is_empty() {
        return format!(
            "🎉 Train completed. Descendants {} are now independent trains.",
            // `PrNumber` displays its own `#`.
            fanned_into
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    match &record.state {
        TrainState::Stopped { .. } => "🛑 Merge train stopped by request.".to_owned(),
        TrainState::Aborted { error, .. } => format!(
            "🛑 Merge train aborted: {}\n\nFix the issue and re-issue `@merge-train start`.",
            error.message
        ),
        TrainState::Completed { .. } => "🎉 Merge train completed.".to_owned(),
        TrainState::NeedsManualReview => "⚠️ Merge train needs manual review.".to_owned(),
        TrainState::Running | TrainState::WaitingCi => "Merge train status.".to_owned(),
    }
}

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
/// # Errors
///
/// Returns [`StatusCommentTooLarge`] if the JSON portion exceeds 60KB even
/// after aggressive truncation of error fields. Within
/// [`MAX_SUPPORTED_TRAIN_SIZE`] the only way to get there is a pathological
/// non-truncatable field; the caller must abort the train rather than run
/// without backup state.
pub fn format_status_comment(
    train: &TrainRecord,
    human_message: &str,
) -> Result<String, StatusCommentTooLarge> {
    let json = escaped_json(truncate_for_size_limit(train.clone()));
    if json.len() <= MAX_JSON_SIZE {
        return Ok(format_comment_body_with_limit(&json, human_message));
    }

    let json = escaped_json(truncate_aggressively(truncate_for_size_limit(
        train.clone(),
    )));
    if json.len() <= MAX_JSON_SIZE {
        return Ok(format_comment_body_with_limit(&json, human_message));
    }

    Err(StatusCommentTooLarge {
        json_len: json.len(),
        limit: MAX_JSON_SIZE,
    })
}

/// Formats the TrainRecord to comment-safe JSON, excluding `status_comment_id`.
fn escaped_json(mut train: TrainRecord) -> String {
    train.status_comment_id = None;
    let json =
        serde_json::to_string_pretty(&train).expect("TrainRecord serialization should not fail");
    escape_html_comment_terminators(&json)
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

/// Escapes the HTML comment terminators `-->` and `--!>` in JSON to prevent
/// breaking the comment (HTML5 closes a comment at either sequence).
///
/// Replaces the final `>` with `\u003e`, a valid JSON escape that parsers
/// decode back during deserialization, so the roundtrip is lossless.
/// The two replacements cannot interfere: neither replacement output
/// contains (or completes) the other pattern.
fn escape_html_comment_terminators(json: &str) -> String {
    json.replace("-->", r"--\u003e")
        .replace("--!>", r"--!\u003e")
}

/// Truncates variable-length fields to ensure the status comment fits within size limits.
///
/// Truncates:
/// - `error.message` to 4KB bytes
/// - `error.stderr` to 2KB bytes
///
/// All other fields are preserved exactly. The truncation happens before JSON serialization
/// to ensure accurate size estimation.
pub fn truncate_for_size_limit(train: TrainRecord) -> TrainRecord {
    truncate_error_fields(train, MAX_ERROR_MESSAGE_LEN, MAX_ERROR_STDERR_LEN)
}

/// Aggressively truncates error fields to 500 bytes each.
fn truncate_aggressively(train: TrainRecord) -> TrainRecord {
    truncate_error_fields(train, AGGRESSIVE_TRUNCATE_LEN, AGGRESSIVE_TRUNCATE_LEN)
}

fn truncate_error_fields(
    mut train: TrainRecord,
    max_message_len: usize,
    max_stderr_len: usize,
) -> TrainRecord {
    if let TrainState::Aborted { ref mut error, .. } = train.state {
        if error.message.len() > max_message_len {
            error.message = truncate_with_suffix(&error.message, max_message_len);
        }
        if let Some(ref mut stderr) = error.stderr
            && stderr.len() > max_stderr_len
        {
            *stderr = truncate_with_suffix(stderr, max_stderr_len);
        }
    }
    train
}

/// Truncates a string to at most `max_len` bytes with a "... [truncated]" suffix.
///
/// If `max_len` is too small to fit the suffix, truncates to `max_len` bytes
/// at a valid UTF-8 boundary without the suffix.
fn truncate_with_suffix(s: &str, max_len: usize) -> String {
    const SUFFIX: &str = "... [truncated]";

    if s.len() <= max_len {
        return s.to_string();
    }

    // If max_len is too small to fit the suffix, just truncate without it
    if max_len < SUFFIX.len() {
        let mut end = max_len;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        return s[..end].to_string();
    }

    // Ensure we have room for the suffix
    let content_len = max_len - SUFFIX.len();

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
    use crate::test_utils::test_timestamp;
    use crate::test_utils::{
        arb_datetime, arb_hostile_text, arb_pr_number, arb_sha, arb_train_record, arb_train_state,
    };
    use crate::types::train::TrainRecord;
    use crate::types::{CommentId, Sha};
    use proptest::prelude::*;

    /// Generates a train with a realistic large descendant set (up to 50 PRs).
    fn arb_large_train() -> impl Strategy<Value = TrainRecord> {
        (
            arb_pr_number(),
            prop::collection::vec(arb_pr_number(), 10..=50),
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
                    let frozen = progress.frozen_descendants().to_vec();
                    for (i, pr) in frozen.iter().enumerate() {
                        if i % 3 == 0 {
                            progress.mark_completed(*pr).unwrap();
                        } else if i % 5 == 0 {
                            progress.mark_skipped(*pr).unwrap();
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
                        last_squash_parent_sha: None,
                        started_at,
                        parent: None,
                        default_branch: "main".to_owned(),
                        watermark: None,
                        status_comment_id: None,
                    }
                },
            )
    }

    /// The most expensive train of `members` members this schema admits:
    /// every PR number 20 digits, a 255-byte default branch, every member in
    /// `frozen_descendants`, `known_stack` AND `completed`, and `error_text`
    /// in both truncatable fields.
    ///
    /// Nothing here is realistic; that is the point. The ceiling has to hold
    /// for the worst input, not the typical one.
    fn worst_case_train(members: usize, error_text: &str) -> TrainRecord {
        use crate::types::train::{
            CascadePhase, DescendantProgress, TrainError, TrainErrorKind, TrainState,
        };

        let prs: Vec<PrNumber> = (0..members)
            .map(|i| PrNumber(u64::MAX - i as u64))
            .collect();
        let mut progress = DescendantProgress::with_known_stack(prs.clone(), prs.clone());
        for pr in &prs {
            progress.mark_completed(*pr).unwrap();
        }

        TrainRecord {
            version: 1,
            recovery_seq: u64::MAX,
            state: TrainState::Aborted {
                error: TrainError {
                    kind: TrainErrorKind::MergeConflict,
                    message: error_text.to_owned(),
                    stderr: Some(error_text.to_owned()),
                },
                ended_at: test_timestamp(),
            },
            original_root_pr: PrNumber(u64::MAX),
            current_pr: PrNumber(u64::MAX),
            cascade_phase: CascadePhase::Reconciling {
                progress,
                squash_sha: Sha::parse("a".repeat(40)).unwrap(),
            },
            predecessor_pr: Some(PrNumber(u64::MAX)),
            predecessor_head_sha: Some(Sha::parse("b".repeat(40)).unwrap()),
            last_squash_parent_sha: Some(Sha::parse("c".repeat(40)).unwrap()),
            started_at: test_timestamp(),
            parent: None,
            // GitHub's own branch-name limit.
            default_branch: "m".repeat(255),
            watermark: None,
            status_comment_id: Some(CommentId(u64::MAX)),
        }
    }

    /// Error text whose every byte costs six in the JSON: a control
    /// character escapes to `\u00XX`. This is the most expansion the
    /// truncatable fields can buy.
    fn most_expensive_error_text() -> String {
        "\u{1}".repeat(64 * 1024)
    }

    /// The largest train `format_status_comment` accepts, found by search.
    ///
    /// "Fits" is monotone in the member count: the verdict depends only on
    /// whether the aggressively-truncated JSON is within budget, and that
    /// grows with every member added, so a binary search is sound.
    fn largest_train_that_fits() -> usize {
        let fits = |n: usize| {
            format_status_comment(&worst_case_train(n, &most_expensive_error_text()), "x").is_ok()
        };
        let (mut lo, mut hi) = (0usize, 8192usize);
        assert!(fits(lo), "an empty train must always fit");
        assert!(!fits(hi), "the search's upper bound must not fit");
        while lo < hi - 1 {
            let mid = lo + (hi - lo) / 2;
            if fits(mid) { lo = mid } else { hi = mid }
        }
        lo
    }

    /// The guarantee [`MAX_SUPPORTED_TRAIN_SIZE`] exists to make: a train the
    /// operator is allowed to configure always has a status comment to live
    /// in, so it can never abort mid-cascade for being unrepresentable.
    #[test]
    fn the_worst_case_train_at_the_ceiling_still_fits() {
        let train = worst_case_train(MAX_SUPPORTED_TRAIN_SIZE, &most_expensive_error_text());
        assert!(
            format_status_comment(&train, "x").is_ok(),
            "the worst case at the ceiling of {MAX_SUPPORTED_TRAIN_SIZE} must fit"
        );
    }

    /// A tripwire for schema growth. The ceiling was chosen with the measured
    /// cliff a sixth above it; if a new `TrainRecord` field spends that
    /// reserve, this fails while there is still margin, rather than the
    /// ceiling silently becoming unsafe.
    #[test]
    fn the_measured_headroom_has_not_been_eaten() {
        let cliff = largest_train_that_fits();
        assert!(
            cliff >= 600,
            "the worst-case train now tops out at {cliff}, leaving too little \
             margin above the {MAX_SUPPORTED_TRAIN_SIZE}-PR ceiling. Either the \
             record grew a field or the budget shrank: re-measure, and lower \
             MAX_SUPPORTED_TRAIN_SIZE (a breaking change for anyone who \
             configured a larger train) or win the space back."
        );
    }

    proptest! {
        /// Property: whatever the error text and however many members up to
        /// the ceiling, a status comment can be formatted. The error fields
        /// are the only truncatable part, so this is the claim that
        /// truncation always recovers enough room.
        #[test]
        fn any_train_within_the_ceiling_can_be_formatted(
            members in 0usize..=MAX_SUPPORTED_TRAIN_SIZE,
            error_text in arb_hostile_text(64),
            human in arb_hostile_text(16),
        ) {
            let train = worst_case_train(members, &error_text);
            prop_assert!(format_status_comment(&train, &human).is_ok());
        }

        /// Property: and the comment it produces is within GitHub's limit,
        /// not merely within the JSON budget.
        #[test]
        fn a_formatted_comment_within_the_ceiling_fits_github(
            members in 0usize..=MAX_SUPPORTED_TRAIN_SIZE,
            error_text in arb_hostile_text(64),
        ) {
            let train = worst_case_train(members, &error_text);
            let body = format_status_comment(&train, "status").unwrap();
            prop_assert!(
                body.len() <= GITHUB_COMMENT_SIZE_LIMIT,
                "comment of {} bytes exceeds GitHub's {GITHUB_COMMENT_SIZE_LIMIT}",
                body.len()
            );
        }
    }

    mod truncation {
        use super::*;
        use crate::types::train::TrainError;
        use crate::types::train::TrainErrorKind;

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
            // Multi-byte UTF-8 characters: 🚂 is 4 bytes
            let s = "🚂".repeat(100); // 400 bytes total
            let truncated = truncate_with_suffix(&s, 50);
            // Should not panic and should be valid UTF-8
            assert!(truncated.len() <= 50);

            // The suffix is 15 bytes, so content gets 35 bytes max.
            // 35 is not a valid char boundary (🚂 is 4 bytes), so it backs up to 32.
            // 32 bytes = 8 emoji characters, plus 15 byte suffix = 47 bytes total.
            assert_eq!(truncated.len(), 47);
            assert_eq!(&truncated[..32], "🚂🚂🚂🚂🚂🚂🚂🚂");
            assert!(truncated.ends_with("... [truncated]"));
        }

        #[test]
        fn truncate_with_suffix_handles_small_max_len() {
            // When max_len is smaller than the suffix, truncate without suffix
            let s = "hello world";
            let truncated = truncate_with_suffix(s, 5);
            assert_eq!(truncated, "hello");
            assert_eq!(truncated.len(), 5);

            // Edge case: max_len of 0
            let truncated = truncate_with_suffix(s, 0);
            assert_eq!(truncated, "");

            // Edge case: max_len cuts into multi-byte char
            let s = "🚂hello";
            let truncated = truncate_with_suffix(s, 2);
            // 🚂 is 4 bytes, so we can't fit any of it in 2 bytes
            assert_eq!(truncated, "");
        }

        #[test]
        fn truncate_for_size_limit_truncates_error_message() {
            let mut train = TrainRecord::new(1.into(), test_timestamp());
            train.abort(
                TrainError::new(TrainErrorKind::ApiError, "x".repeat(10000)),
                test_timestamp(),
            );

            let truncated = truncate_for_size_limit(train);
            let msg = &truncated.state.error().unwrap().message;
            assert!(msg.len() <= MAX_ERROR_MESSAGE_LEN);
            assert!(msg.ends_with("... [truncated]"));
        }

        #[test]
        fn truncate_for_size_limit_truncates_stderr() {
            let mut train = TrainRecord::new(1.into(), test_timestamp());
            train.abort(
                TrainError::new(TrainErrorKind::ApiError, "msg").with_stderr("x".repeat(5000)),
                test_timestamp(),
            );

            let truncated = truncate_for_size_limit(train);
            let stderr = truncated.state.error().unwrap().stderr.as_ref().unwrap();
            assert!(stderr.len() <= MAX_ERROR_STDERR_LEN);
            assert!(stderr.ends_with("... [truncated]"));
        }

        proptest! {
            #[test]
            fn truncation_preserves_all_fields_except_error_content(train in arb_train_record()) {
                let truncated = truncate_for_size_limit(train.clone());

                // All fields except error message/stderr should be identical
                prop_assert_eq!(truncated.version, train.version);
                prop_assert_eq!(truncated.recovery_seq, train.recovery_seq);
                prop_assert_eq!(truncated.original_root_pr, train.original_root_pr);
                prop_assert_eq!(truncated.current_pr, train.current_pr);
                prop_assert_eq!(truncated.cascade_phase, train.cascade_phase);
                prop_assert_eq!(truncated.predecessor_pr, train.predecessor_pr);
                prop_assert_eq!(truncated.predecessor_head_sha, train.predecessor_head_sha);
                prop_assert_eq!(truncated.started_at, train.started_at);
                prop_assert_eq!(truncated.status_comment_id, train.status_comment_id);
                prop_assert_eq!(truncated.state.ended_at(), train.state.ended_at());

                // Error kind preserved even if message is truncated
                if let (Some(orig), Some(trunc)) = (train.state.error(), truncated.state.error()) {
                    prop_assert_eq!(trunc.kind, orig.kind);
                }
            }
        }
    }

    mod format {
        use super::*;

        #[test]
        fn format_includes_markers() {
            let train = TrainRecord::new(1.into(), test_timestamp());
            let comment = format_status_comment(&train, "Train is running").unwrap();

            assert!(comment.starts_with(STATUS_COMMENT_START));
            assert!(comment.contains(STATUS_COMMENT_END));
            assert!(comment.contains("**Merge Train Status**"));
            assert!(comment.contains("Train is running"));
        }

        #[test]
        fn format_excludes_status_comment_id() {
            let mut train = TrainRecord::new(1.into(), test_timestamp());
            train.status_comment_id = Some(CommentId(12345));

            let comment = format_status_comment(&train, "msg").unwrap();

            // The JSON should not contain the status_comment_id field.
            // We only check for the field name, not the numeric value, since
            // the value could legitimately appear elsewhere (timestamps, etc.).
            assert!(!comment.contains("status_comment_id"));
        }

        #[test]
        fn large_human_message_is_truncated() {
            let train = TrainRecord::new(1.into(), test_timestamp());
            // Create a human message larger than the remaining space
            let huge_message = "x".repeat(70000);

            let comment = format_status_comment(&train, &huge_message).unwrap();

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
        fn bang_comment_terminator_is_escaped() {
            use crate::types::train::{TrainError, TrainErrorKind};

            // HTML5 parsers also close comments at `--!>`; if it survives into
            // the JSON portion, the rendered comment visually breaks and
            // exposes the JSON blob.
            let mut train = TrainRecord::new(1.into(), test_timestamp());
            train.abort(
                TrainError::new(TrainErrorKind::ApiError, "error --!> happened"),
                test_timestamp(),
            );

            let comment = format_status_comment(&train, "status").unwrap();

            let json_start = comment.find(STATUS_COMMENT_START).unwrap();
            let json_end = comment.find(STATUS_COMMENT_END).unwrap();
            let json_portion = &comment[json_start..json_end];

            assert!(
                !json_portion.contains("--!>"),
                "JSON should not contain literal '--!>' which closes HTML5 comments"
            );
        }

        #[test]
        fn oversize_train_is_an_error_not_a_panic() {
            use crate::types::train::{CascadePhase, DescendantProgress};

            // The 50-PR cap is an engine policy, not a type invariant, so a
            // pathological frozen set can exceed the JSON budget; that must
            // surface as an error the engine can turn into an abort, not a
            // panic.
            let mut train = TrainRecord::new(1.into(), test_timestamp());
            let huge: Vec<_> = (1..20_000u64).map(crate::types::PrNumber).collect();
            train.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(huge),
            };

            let result = format_status_comment(&train, "status");
            assert!(result.is_err());
        }

        #[test]
        fn html_comment_terminator_is_escaped() {
            use crate::types::train::{TrainError, TrainErrorKind};

            let mut train = TrainRecord::new(1.into(), test_timestamp());
            // Error message containing the HTML comment terminator
            train.abort(
                TrainError::new(TrainErrorKind::ApiError, "error --> happened"),
                test_timestamp(),
            );

            let comment = format_status_comment(&train, "status").unwrap();

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
                let comment = format_status_comment(&train, "Status message").unwrap();
                prop_assert!(comment.len() <= GITHUB_COMMENT_SIZE_LIMIT,
                    "Comment length {} exceeds GitHub limit {}", comment.len(), GITHUB_COMMENT_SIZE_LIMIT);
            }

            #[test]
            fn large_trains_under_github_limit(train in arb_large_train()) {
                let comment = format_status_comment(&train, "Status message for a large train with many descendants").unwrap();
                prop_assert!(comment.len() <= GITHUB_COMMENT_SIZE_LIMIT,
                    "Large train comment length {} exceeds GitHub limit {}", comment.len(), GITHUB_COMMENT_SIZE_LIMIT);
            }

            #[test]
            fn large_human_message_stays_under_limit(
                train in arb_train_record(),
                human_len in 0usize..100000
            ) {
                let human_message = "x".repeat(human_len);
                let comment = format_status_comment(&train, &human_message).unwrap();
                prop_assert!(comment.len() <= GITHUB_COMMENT_SIZE_LIMIT,
                    "Comment length {} exceeds GitHub limit {} with human_len {}",
                    comment.len(), GITHUB_COMMENT_SIZE_LIMIT, human_len);
            }
        }
    }
}
