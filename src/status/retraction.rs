//! Retraction receipts: the bot's durable tombstone for a retracted
//! predecessor declaration.
//!
//! A live retraction (the author deletes the declaring comment, or edits it
//! to no longer declare) removes the edge from the STATE — but the deletion
//! leaves no trace in GitHub's *present*: an older declaration comment on
//! the same PR still reads `@bot predecessor #N`, and a lost-DB crawl,
//! which can only replay surviving comments, would resurrect the edge the
//! user retracted — and a recovered train re-freezing a later cascade level
//! would then DRIVE the re-attached descendant (found by the lost_db
//! differential harness; owner ruling 2026-07-18: stop-shaped residuals are
//! acceptable, silently driving retracted topology is not).
//!
//! So the worker posts a RECEIPT comment when it applies a retraction:
//! bot-authored, machine-parseable, sitting on the retracting PR, naming
//! the RETRACTED comment's id. During a crawl, a receipt tombstones every
//! declaration on that PR whose comment id is at or below the retracted
//! one — GitHub comment ids are globally monotonic, so that range is
//! exactly "the retracted declaration and everything it had superseded".
//!
//! The tombstone anchors to the RETRACTED comment's id, never the
//! receipt's own: the receipt is posted when the bot PROCESSES the
//! deletion webhook, and with backlog lag the user may already have
//! re-declared by then — a receipt-id-ordered tombstone would kill that
//! legitimate re-declaration (the lost_db differential property caught
//! exactly this race on the first candidate design).
//!
//! Receipts are posted best-effort, like every status update: a receipt
//! lost to an outage re-opens the resurrection window for that one
//! retraction (documented residual — the same envelope as a failed status
//! comment update).
//!
//! # Format
//!
//! ```text
//! <!-- merge-train-retraction
//! {"version": 1, "pr": 123, "retracted": 4567}
//! -->
//! **Predecessor declaration retracted** ...
//! ```

use serde::{Deserialize, Serialize};

use crate::types::{CommentId, PrNumber};

/// The marker that begins a retraction receipt JSON block.
pub const RETRACTION_RECEIPT_START: &str = "<!-- merge-train-retraction\n";

/// The marker that ends a retraction receipt JSON block.
pub const RETRACTION_RECEIPT_END: &str = "\n-->";

/// The machine-readable half of a receipt.
///
/// `pr` names the PR whose declaration was retracted; a receipt only counts
/// when it sits ON that PR (one posted anywhere else is forged or misplaced
/// — the same trust gate status comments apply to `original_root_pr`).
/// `retracted` is the id of the deleted (or edited-away) owning comment —
/// the tombstone's anchor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RetractionReceipt {
    version: u32,
    pr: PrNumber,
    retracted: CommentId,
}

/// Formats the receipt the worker posts on `pr` when the declaration owned
/// by comment `retracted` is retracted. `predecessor` (the edge being
/// removed) is cosmetic.
pub fn format_retraction_receipt(
    pr: PrNumber,
    retracted: CommentId,
    predecessor: Option<PrNumber>,
) -> String {
    let receipt = RetractionReceipt {
        version: 1,
        pr,
        retracted,
    };
    let json = serde_json::to_string(&receipt).expect("receipt serialization cannot fail");
    let human = match predecessor {
        Some(p) => format!("PR #{pr} no longer declares #{p} as its predecessor."),
        None => format!("PR #{pr} no longer declares a predecessor."),
    };
    format!(
        "{RETRACTION_RECEIPT_START}{json}{RETRACTION_RECEIPT_END}\n\
         **Predecessor declaration retracted**: {human} Declaration comments \
         up to the retracted one no longer count; a new `predecessor` \
         comment re-declares."
    )
}

/// Parses a comment body as a retraction receipt, yielding the PR it
/// attests and the retracted comment id anchoring the tombstone. `None`
/// for anything that is not a well-formed receipt.
pub fn parse_retraction_receipt(body: &str) -> Option<(PrNumber, CommentId)> {
    let start = body.find(RETRACTION_RECEIPT_START)?;
    let json_start = start + RETRACTION_RECEIPT_START.len();
    let end = body[json_start..].find(RETRACTION_RECEIPT_END)?;
    let receipt: RetractionReceipt =
        serde_json::from_str(&body[json_start..json_start + end]).ok()?;
    (receipt.version == 1).then_some((receipt.pr, receipt.retracted))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        /// Every formatted receipt parses back to its PR and anchor — for
        /// all PR numbers, anchors, and predecessor variants.
        #[test]
        fn format_parse_roundtrip(
            pr in 1u64..u64::MAX,
            retracted in 1u64..u64::MAX,
            predecessor in proptest::option::of(1u64..u64::MAX),
        ) {
            let body = format_retraction_receipt(
                PrNumber(pr),
                CommentId(retracted),
                predecessor.map(PrNumber),
            );
            prop_assert_eq!(
                parse_retraction_receipt(&body),
                Some((PrNumber(pr), CommentId(retracted)))
            );
        }
    }

    #[test]
    fn non_receipts_do_not_parse() {
        assert_eq!(parse_retraction_receipt("just a comment"), None);
        assert_eq!(
            parse_retraction_receipt("<!-- merge-train-state\n{\"version\":1}\n-->"),
            None
        );
        assert_eq!(
            parse_retraction_receipt("<!-- merge-train-retraction\n{not json}\n-->"),
            None
        );
        assert_eq!(
            parse_retraction_receipt(
                "<!-- merge-train-retraction\n{\"version\":2,\"pr\":1,\"retracted\":5}\n-->"
            ),
            None,
            "unknown versions are not trusted"
        );
    }
}
