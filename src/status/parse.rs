//! Status comment parsing for recovery.
//!
//! Extracts machine-readable JSON from status comments embedded in HTML comments.

use crate::status::format::{STATUS_COMMENT_END, STATUS_COMMENT_START};
use crate::types::train::TrainRecord;
use thiserror::Error;

/// Errors that can occur when parsing a status comment.
#[derive(Debug, Error)]
pub enum ParseError {
    /// The comment does not contain the start marker: it is not a status
    /// comment at all (or the bot never wrote one).
    #[error("status comment start marker not found")]
    StartMarkerNotFound,

    /// The start marker is present but the end marker is missing. The bot
    /// always writes both, so this means the comment was truncated or
    /// corrupted after writing — a recovery-relevant diagnosis, distinct
    /// from "not a status comment".
    #[error("status comment end marker not found; comment is truncated or corrupted")]
    EndMarkerNotFound,

    /// The JSON portion of the comment is malformed.
    #[error("invalid JSON in status comment: {0}")]
    InvalidJson(#[from] serde_json::Error),
}

/// Parses a status comment body to extract the embedded `TrainRecord`.
///
/// The expected format is:
/// ```text
/// <!-- merge-train-state
/// {"version": 1, ...}
/// -->
/// **Merge Train Status**
/// ...
/// ```
///
/// # Errors
///
/// Returns `ParseError::StartMarkerNotFound` if the comment is not a status
/// comment, `ParseError::EndMarkerNotFound` if it is one but was truncated
/// or corrupted, and `ParseError::InvalidJson` if the JSON is malformed or
/// doesn't match the `TrainRecord` schema.
pub fn parse_status_comment(body: &str) -> Result<TrainRecord, ParseError> {
    let json = extract_json(body)?;
    let record: TrainRecord = serde_json::from_str(json)?;
    Ok(record)
}

/// Extracts the JSON portion from a status comment body.
fn extract_json(body: &str) -> Result<&str, ParseError> {
    let start = body
        .find(STATUS_COMMENT_START)
        .ok_or(ParseError::StartMarkerNotFound)?;
    let json_start = start + STATUS_COMMENT_START.len();

    let end = body[json_start..]
        .find(STATUS_COMMENT_END)
        .ok_or(ParseError::EndMarkerNotFound)?;

    Ok(&body[json_start..json_start + end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::format::format_status_comment;
    use crate::test_utils::arb_train_record;
    use crate::test_utils::test_timestamp;
    use crate::types::train::{CascadePhase, DescendantProgress, TrainState};
    use crate::types::{PrNumber, Sha};
    use proptest::prelude::*;

    mod parse {
        use super::*;

        #[test]
        fn parse_missing_marker_returns_error() {
            let body = "Just a regular comment without any markers";
            let result = parse_status_comment(body);
            assert!(matches!(result, Err(ParseError::StartMarkerNotFound)));
        }

        #[test]
        fn parse_malformed_json_returns_error() {
            let body = "<!-- merge-train-state\n{not valid json}\n-->";
            let result = parse_status_comment(body);
            assert!(matches!(result, Err(ParseError::InvalidJson(_))));
        }

        #[test]
        fn truncated_comment_is_distinguished_from_non_status_comment() {
            // A missing end marker means a bot-authored comment was truncated
            // or corrupted after writing; recovery must be able to tell this
            // apart from "this comment is not a status comment".
            let body = "<!-- merge-train-state\n{\"version\": 1}";
            let result = parse_status_comment(body);
            assert!(matches!(result, Err(ParseError::EndMarkerNotFound)));
        }

        #[test]
        fn parse_design_md_example() {
            // The example from DESIGN.md (with full 40-char SHA)
            let body = r#"<!-- merge-train-state
{
  "version": 1,
  "recovery_seq": 42,
  "state": "running",
  "original_root_pr": 123,
  "current_pr": 124,
  "cascade_phase": {
    "Reconciling": {
      "completed": [125],
      "skipped": [],
      "frozen_descendants": [125, 126],
      "squash_sha": "abc123def456789012345678901234567890abcd"
    }
  },
  "predecessor_pr": 123,
  "started_at": "2024-01-15T09:00:00Z"
}
-->
**Merge Train Status**

Train is currently reconciling descendants after squash-merge.
"#;

            let result = parse_status_comment(body);
            assert!(result.is_ok(), "Failed to parse: {:?}", result);

            let train = result.unwrap();
            assert_eq!(train.version, 1);
            assert_eq!(train.recovery_seq, 42);
            assert_eq!(train.state, TrainState::Running);
            assert_eq!(train.original_root_pr, PrNumber(123));
            assert_eq!(train.current_pr, PrNumber(124));
            assert_eq!(train.predecessor_pr, Some(PrNumber(123)));

            // Verify cascade phase
            match &train.cascade_phase {
                CascadePhase::Reconciling {
                    progress,
                    squash_sha,
                } => {
                    assert_eq!(
                        progress.frozen_descendants(),
                        vec![PrNumber(125), PrNumber(126)]
                    );
                    assert!(progress.completed().contains(&PrNumber(125)));
                    assert!(progress.skipped().is_empty());
                    assert_eq!(
                        squash_sha,
                        &Sha::parse("abc123def456789012345678901234567890abcd").unwrap()
                    );
                }
                other => panic!("Expected Reconciling phase, got {:?}", other),
            }
        }

        #[test]
        fn parse_idle_phase() {
            let body = r#"<!-- merge-train-state
{
  "version": 1,
  "recovery_seq": 0,
  "state": "running",
  "original_root_pr": 1,
  "current_pr": 1,
  "cascade_phase": "Idle",
  "started_at": "2024-01-01T00:00:00Z"
}
-->
**Merge Train Status**
"#;

            let result = parse_status_comment(body);
            assert!(result.is_ok(), "Failed to parse: {:?}", result);

            let train = result.unwrap();
            assert_eq!(train.cascade_phase, CascadePhase::Idle);
        }

        #[test]
        fn parse_with_error() {
            let body = r#"<!-- merge-train-state
{
  "version": 1,
  "recovery_seq": 10,
  "state": {
    "aborted": {
      "ended_at": "2024-01-01T01:00:00Z",
      "error": {
        "error_type": "merge_conflict",
        "message": "Merge conflict in src/main.rs",
        "stderr": null
      }
    }
  },
  "original_root_pr": 1,
  "current_pr": 1,
  "cascade_phase": "Idle",
  "started_at": "2024-01-01T00:00:00Z"
}
-->
**Merge Train Status**

Train aborted due to merge conflict.
"#;

            let result = parse_status_comment(body);
            assert!(result.is_ok(), "Failed to parse: {:?}", result);

            let train = result.unwrap();
            let error = train.state.error().expect("aborted train carries error");
            assert_eq!(error.kind, crate::types::TrainErrorKind::MergeConflict);
            assert_eq!(error.message, "Merge conflict in src/main.rs");
            assert_eq!(
                train.state.ended_at().map(|t| t.to_rfc3339()),
                Some("2024-01-01T01:00:00+00:00".to_string())
            );
        }
    }

    mod roundtrip {
        use super::*;

        const TRUNCATION_SUFFIX: &str = "... [truncated]";

        /// Verifies that a parsed string is either equal to the original or is a valid truncation.
        /// A valid truncation means the parsed value (without suffix) is a prefix of the original.
        fn verify_truncation_roundtrip(
            parsed: &str,
            original: &str,
            field_name: &str,
        ) -> Result<(), proptest::test_runner::TestCaseError> {
            if parsed == original {
                return Ok(());
            }
            // If truncated, parsed should end with suffix and content should be prefix of original
            prop_assert!(
                parsed.ends_with(TRUNCATION_SUFFIX),
                "{} was modified but doesn't have truncation suffix. parsed={:?}, original={:?}",
                field_name,
                parsed,
                original
            );
            let content = &parsed[..parsed.len() - TRUNCATION_SUFFIX.len()];
            prop_assert!(
                original.starts_with(content),
                "{} truncation is invalid: content {:?} is not a prefix of original {:?}",
                field_name,
                content,
                original
            );
            Ok(())
        }

        proptest! {
            /// The core correctness property: parse(format(train)) == train
            #[test]
            fn roundtrip_preserves_train_record(train in arb_train_record()) {
                let comment = format_status_comment(&train, "Test message").unwrap();
                let parsed = parse_status_comment(&comment);

                prop_assert!(parsed.is_ok(), "Failed to parse formatted comment: {:?}", parsed.err());
                let parsed_train = parsed.unwrap();

                // Compare all fields
                prop_assert_eq!(parsed_train.version, train.version);
                prop_assert_eq!(parsed_train.recovery_seq, train.recovery_seq);
                prop_assert_eq!(parsed_train.original_root_pr, train.original_root_pr);
                prop_assert_eq!(parsed_train.current_pr, train.current_pr);
                prop_assert_eq!(parsed_train.cascade_phase, train.cascade_phase);
                prop_assert_eq!(parsed_train.predecessor_pr, train.predecessor_pr);
                prop_assert_eq!(parsed_train.predecessor_head_sha, train.predecessor_head_sha);
                prop_assert_eq!(parsed_train.started_at, train.started_at);

                // State comparison: exact, except that an aborted train's error
                // message/stderr may have been truncated for the size limit.
                match (&parsed_train.state, &train.state) {
                    (
                        TrainState::Aborted { ended_at: parsed_ended, error: p },
                        TrainState::Aborted { ended_at: orig_ended, error: t },
                    ) => {
                        prop_assert_eq!(parsed_ended, orig_ended);
                        prop_assert_eq!(p.kind, t.kind);
                        verify_truncation_roundtrip(&p.message, &t.message, "message")?;
                        match (&p.stderr, &t.stderr) {
                            (None, None) => {}
                            (Some(ps), Some(ts)) => {
                                verify_truncation_roundtrip(ps, ts, "stderr")?;
                            }
                            _ => prop_assert!(false, "stderr presence mismatch: parsed={:?}, original={:?}", p.stderr, t.stderr),
                        }
                    }
                    (p, t) => prop_assert_eq!(p, t),
                }

                // status_comment_id is intentionally excluded
                prop_assert!(parsed_train.status_comment_id.is_none());
            }
        }

        #[test]
        fn roundtrip_with_all_phases() {
            let phases = vec![
                CascadePhase::Idle,
                CascadePhase::Preparing {
                    progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
                },
                CascadePhase::SquashPending {
                    progress: DescendantProgress::new(vec![PrNumber(2)]),
                },
                CascadePhase::Reconciling {
                    progress: DescendantProgress::new(vec![PrNumber(2)]),
                    squash_sha: Sha::parse("1234567890abcdef1234567890abcdef12345678").unwrap(),
                },
                CascadePhase::CatchingUp {
                    progress: DescendantProgress::new(vec![]),
                    squash_sha: Sha::parse("abcdef1234567890abcdef1234567890abcdef12").unwrap(),
                },
                CascadePhase::Retargeting {
                    progress: DescendantProgress::new(vec![PrNumber(4)]),
                    squash_sha: Sha::parse("deadbeef1234567890abcdef1234567890abcdef").unwrap(),
                },
            ];

            for phase in phases {
                let mut train = TrainRecord::new(PrNumber(1), test_timestamp());
                train.cascade_phase = phase.clone();

                let comment = format_status_comment(&train, "Test").unwrap();
                let parsed = parse_status_comment(&comment).expect("Failed to parse");

                assert_eq!(
                    parsed.cascade_phase, phase,
                    "Phase roundtrip failed for {:?}",
                    phase
                );
            }
        }

        #[test]
        fn roundtrip_with_completed_and_skipped() {
            let mut progress = DescendantProgress::new(vec![
                PrNumber(10),
                PrNumber(20),
                PrNumber(30),
                PrNumber(40),
            ]);
            progress.mark_completed(PrNumber(10)).unwrap();
            progress.mark_completed(PrNumber(20)).unwrap();
            progress.mark_skipped(PrNumber(30)).unwrap();

            let mut train = TrainRecord::new(PrNumber(1), test_timestamp());
            train.cascade_phase = CascadePhase::Preparing { progress };

            let comment = format_status_comment(&train, "Test").unwrap();
            let parsed = parse_status_comment(&comment).expect("Failed to parse");

            match &parsed.cascade_phase {
                CascadePhase::Preparing { progress } => {
                    assert_eq!(
                        progress.frozen_descendants(),
                        vec![PrNumber(10), PrNumber(20), PrNumber(30), PrNumber(40)]
                    );
                    assert!(progress.completed().contains(&PrNumber(10)));
                    assert!(progress.completed().contains(&PrNumber(20)));
                    assert!(progress.skipped().contains(&PrNumber(30)));
                    assert!(!progress.completed().contains(&PrNumber(40)));
                    assert!(!progress.skipped().contains(&PrNumber(40)));
                }
                _ => panic!("Wrong phase"),
            }
        }

        #[test]
        fn roundtrip_with_html_comment_terminator_in_error() {
            use crate::types::train::{TrainError, TrainErrorKind};

            let mut train = TrainRecord::new(PrNumber(1), test_timestamp());
            // Error message containing the HTML comment terminator sequence
            let dangerous_message = "error --> happened\nwith --> multiple --> arrows";
            train.abort(
                TrainError::new(TrainErrorKind::ApiError, dangerous_message),
                test_timestamp(),
            );

            let comment = format_status_comment(&train, "Test").unwrap();
            let parsed = parse_status_comment(&comment).expect("Failed to parse comment with -->");

            let parsed_error = parsed
                .state
                .error()
                .expect("Error should be preserved")
                .clone();
            assert_eq!(parsed_error.kind, TrainErrorKind::ApiError);
            // The message should be unescaped back to original
            assert_eq!(parsed_error.message, dangerous_message);
        }
    }

    mod extract_json {
        use super::*;

        #[test]
        fn extract_handles_extra_content_before_and_after() {
            let body =
                "Some prefix text\n<!-- merge-train-state\n{\"version\": 1}\n-->\nSome suffix text";
            let json = extract_json(body).unwrap();
            assert_eq!(json, "{\"version\": 1}");
        }

        #[test]
        fn extract_handles_multiple_markers() {
            // Should find the first one
            let body = "<!-- merge-train-state\n{\"first\": true}\n-->\n<!-- merge-train-state\n{\"second\": true}\n-->";
            let json = extract_json(body).unwrap();
            assert_eq!(json, "{\"first\": true}");
        }
    }
}
