//! Status comment parsing for recovery.
//!
//! Extracts machine-readable JSON from status comments embedded in HTML comments.

use crate::status::format::{STATUS_COMMENT_END, STATUS_COMMENT_START};
use crate::types::train::TrainRecord;
use thiserror::Error;

/// Errors that can occur when parsing a status comment.
#[derive(Debug, Error)]
pub enum ParseError {
    /// The comment does not contain a status comment marker.
    #[error("status comment marker not found")]
    MarkerNotFound,

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
/// Returns `ParseError::MarkerNotFound` if the comment doesn't contain the
/// expected markers.
///
/// Returns `ParseError::InvalidJson` if the JSON is malformed or doesn't
/// match the `TrainRecord` schema.
pub fn parse_status_comment(body: &str) -> Result<TrainRecord, ParseError> {
    let json = extract_json(body)?;
    let record: TrainRecord = serde_json::from_str(json)?;
    Ok(record)
}

/// Extracts the JSON portion from a status comment body.
fn extract_json(body: &str) -> Result<&str, ParseError> {
    let start = body
        .find(STATUS_COMMENT_START)
        .ok_or(ParseError::MarkerNotFound)?;
    let json_start = start + STATUS_COMMENT_START.len();

    let end = body[json_start..]
        .find(STATUS_COMMENT_END)
        .ok_or(ParseError::MarkerNotFound)?;

    Ok(&body[json_start..json_start + end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::format::format_status_comment;
    use crate::test_utils::{arb_cascade_phase, arb_pr_number, arb_sha, arb_train_error};
    use crate::types::train::{CascadePhase, DescendantProgress, TrainRecord, TrainState};
    use crate::types::{PrNumber, Sha};
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
                        // Exclude status_comment_id since it's not serialized
                        status_comment_id: None,
                    }
                },
            )
    }

    mod parse {
        use super::*;

        #[test]
        fn parse_missing_marker_returns_error() {
            let body = "Just a regular comment without any markers";
            let result = parse_status_comment(body);
            assert!(matches!(result, Err(ParseError::MarkerNotFound)));
        }

        #[test]
        fn parse_malformed_json_returns_error() {
            let body = "<!-- merge-train-state\n{not valid json}\n-->";
            let result = parse_status_comment(body);
            assert!(matches!(result, Err(ParseError::InvalidJson(_))));
        }

        #[test]
        fn parse_incomplete_marker_returns_error() {
            // Missing end marker
            let body = "<!-- merge-train-state\n{\"version\": 1}";
            let result = parse_status_comment(body);
            assert!(matches!(result, Err(ParseError::MarkerNotFound)));
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
  "last_squash_sha": "abc123def456789012345678901234567890abcd",
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
                        progress.frozen_descendants,
                        vec![PrNumber(125), PrNumber(126)]
                    );
                    assert!(progress.completed.contains(&PrNumber(125)));
                    assert!(progress.skipped.is_empty());
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
  "state": "aborted",
  "original_root_pr": 1,
  "current_pr": 1,
  "cascade_phase": "Idle",
  "started_at": "2024-01-01T00:00:00Z",
  "ended_at": "2024-01-01T01:00:00Z",
  "error": {
    "error_type": "merge_conflict",
    "message": "Merge conflict in src/main.rs"
  }
}
-->
**Merge Train Status**

Train aborted due to merge conflict.
"#;

            let result = parse_status_comment(body);
            assert!(result.is_ok(), "Failed to parse: {:?}", result);

            let train = result.unwrap();
            assert_eq!(train.state, TrainState::Aborted);
            assert!(train.error.is_some());
            let error = train.error.unwrap();
            assert_eq!(error.error_type, "merge_conflict");
            assert_eq!(error.message, "Merge conflict in src/main.rs");
        }
    }

    mod roundtrip {
        use super::*;

        proptest! {
            /// The core correctness property: parse(format(train)) == train
            #[test]
            fn roundtrip_preserves_train_record(train in arb_train_record()) {
                let comment = format_status_comment(&train, "Test message");
                let parsed = parse_status_comment(&comment);

                prop_assert!(parsed.is_ok(), "Failed to parse formatted comment: {:?}", parsed.err());
                let parsed_train = parsed.unwrap();

                // Compare all fields
                prop_assert_eq!(parsed_train.version, train.version);
                prop_assert_eq!(parsed_train.recovery_seq, train.recovery_seq);
                prop_assert_eq!(parsed_train.state, train.state);
                prop_assert_eq!(parsed_train.original_root_pr, train.original_root_pr);
                prop_assert_eq!(parsed_train.current_pr, train.current_pr);
                prop_assert_eq!(parsed_train.cascade_phase, train.cascade_phase);
                prop_assert_eq!(parsed_train.predecessor_pr, train.predecessor_pr);
                prop_assert_eq!(parsed_train.predecessor_head_sha, train.predecessor_head_sha);
                prop_assert_eq!(parsed_train.last_squash_sha, train.last_squash_sha);
                prop_assert_eq!(parsed_train.started_at, train.started_at);
                prop_assert_eq!(parsed_train.ended_at, train.ended_at);

                // Error comparison (message may be truncated but error_type preserved)
                match (&parsed_train.error, &train.error) {
                    (None, None) => {}
                    (Some(p), Some(t)) => {
                        prop_assert_eq!(&p.error_type, &t.error_type);
                        // Message may be truncated, so just check it starts with the original
                        // or the original starts with it (for truncation case)
                        let starts_same = t.message.starts_with(&p.message)
                            || p.message.starts_with(&t.message)
                            || p.message.contains("[truncated]");
                        prop_assert!(starts_same, "Error message mismatch");
                    }
                    _ => prop_assert!(false, "Error presence mismatch"),
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
                let mut train = TrainRecord::new(PrNumber(1));
                train.cascade_phase = phase.clone();

                let comment = format_status_comment(&train, "Test");
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
            progress.mark_completed(PrNumber(10));
            progress.mark_completed(PrNumber(20));
            progress.mark_skipped(PrNumber(30));

            let mut train = TrainRecord::new(PrNumber(1));
            train.cascade_phase = CascadePhase::Preparing { progress };

            let comment = format_status_comment(&train, "Test");
            let parsed = parse_status_comment(&comment).expect("Failed to parse");

            match &parsed.cascade_phase {
                CascadePhase::Preparing { progress } => {
                    assert_eq!(
                        progress.frozen_descendants,
                        vec![PrNumber(10), PrNumber(20), PrNumber(30), PrNumber(40)]
                    );
                    assert!(progress.completed.contains(&PrNumber(10)));
                    assert!(progress.completed.contains(&PrNumber(20)));
                    assert!(progress.skipped.contains(&PrNumber(30)));
                    assert!(!progress.completed.contains(&PrNumber(40)));
                    assert!(!progress.skipped.contains(&PrNumber(40)));
                }
                _ => panic!("Wrong phase"),
            }
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
