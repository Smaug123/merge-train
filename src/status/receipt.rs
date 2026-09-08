//! Receipts: the bot's durable, machine-readable record of what it decided
//! about a predecessor declaration — evidence that outlives the state DB.
//!
//! A lost-DB crawl can only replay the comments that survive in GitHub's
//! present, and a comment's mere survival does not say what the live
//! handler made of it:
//!
//! - A **retraction** (the author deletes the declaring comment, edits it to
//!   no longer declare, or edits it to declare a DIFFERENT predecessor)
//!   removes the edge from the state but leaves an older declaration
//!   comment on the same PR still reading `@bot predecessor #N`. Replaying
//!   it would resurrect the edge the user retracted, and a recovered train
//!   re-freezing a later cascade level would then DRIVE the re-attached
//!   descendant (found by the lost_db differential harness; owner ruling
//!   2026-07-18: stop-shaped residuals are acceptable, silently driving
//!   retracted topology is not).
//! - A **rejection** (live validation refused the declaration: not in a
//!   stack, base mismatch, cycle, already declared, or a late addition onto
//!   a merged predecessor) records nothing, but the comment survives, and
//!   re-validating it against the PRESENT can accept what live refused —
//!   the declaration's context has changed (its target merged, the other
//!   declaration was retracted, or the live delivery order differed from
//!   comment-id order).
//!
//! So the worker posts a RECEIPT comment on the PR whenever it applies a
//! retraction or refuses a declaration: bot-authored, machine-parseable,
//! naming the comment it is about. A crawl trusts a receipt only on the PR
//! it names (a receipt posted anywhere else is forged or misplaced — the
//! same trust gate status comments apply to `original_root_pr`).
//!
//! - A retraction receipt tombstones every declaration on that PR whose
//!   comment id is at or below the retracted one: GitHub comment ids are
//!   globally monotonic, so that range is exactly "the retracted
//!   declaration and everything it had superseded". It anchors to the
//!   RETRACTED comment's id, never the receipt's own: the receipt is posted
//!   when the bot PROCESSES the retraction, and with backlog lag the user
//!   may already have re-declared by then — a receipt-id-ordered tombstone
//!   would kill that legitimate re-declaration.
//! - A rejection receipt tombstones exactly the rejected comment.
//!
//! Receipts are posted best-effort, like every status update: a receipt
//! lost to an outage re-opens that one decision's window (documented
//! residual — the same envelope as a failed status comment update).
//!
//! # Format
//!
//! ```text
//! <!-- merge-train-receipt
//! {"version":1,"kind":"retraction","pr":123,"comment":4567}
//! -->
//! **Predecessor declaration retracted** ...
//! ```

use serde::{Deserialize, Serialize};

use crate::types::{CommentId, PrNumber};

/// The marker that begins a receipt's JSON block.
pub const RECEIPT_START: &str = "<!-- merge-train-receipt\n";

/// The marker that ends a receipt's JSON block.
pub const RECEIPT_END: &str = "\n-->";

/// What a receipt attests about one declaration comment on one PR.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Receipt {
    /// The declaration owned by `retracted` on `pr` was retracted. Every
    /// declaration on `pr` at or below that id no longer counts.
    Retraction { pr: PrNumber, retracted: CommentId },
    /// Live validation refused the declaration in comment `rejected` on
    /// `pr`. That comment never counts.
    Rejection { pr: PrNumber, rejected: CommentId },
}

impl Receipt {
    /// The PR the receipt is about; a receipt only counts when it sits ON
    /// this PR.
    pub fn pr(self) -> PrNumber {
        match self {
            Receipt::Retraction { pr, .. } | Receipt::Rejection { pr, .. } => pr,
        }
    }
}

/// The machine-readable half of a receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Wire {
    version: u32,
    kind: Kind,
    pr: PrNumber,
    comment: CommentId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Retraction,
    Rejection,
}

fn block(wire: &Wire) -> String {
    let json = serde_json::to_string(wire).expect("receipt serialization cannot fail");
    format!("{RECEIPT_START}{json}{RECEIPT_END}")
}

/// Formats the receipt the worker posts on `pr` when the declaration owned
/// by comment `retracted` is retracted. `predecessor` (the edge being
/// removed) is cosmetic.
pub fn format_retraction_receipt(
    pr: PrNumber,
    retracted: CommentId,
    predecessor: Option<PrNumber>,
) -> String {
    let human = match predecessor {
        Some(p) => format!("PR #{pr} no longer declares #{p} as its predecessor."),
        None => format!("PR #{pr} no longer declares a predecessor."),
    };
    format!(
        "{}\n**Predecessor declaration retracted**: {human} Declaration comments \
         up to the retracted one no longer count; a new `predecessor` \
         comment re-declares.",
        block(&Wire {
            version: 1,
            kind: Kind::Retraction,
            pr,
            comment: retracted,
        })
    )
}

/// Formats the rejection the worker posts on `pr` when the declaration in
/// comment `rejected` is refused: the human `message` first, the receipt
/// block after it.
pub fn format_rejection_receipt(pr: PrNumber, rejected: CommentId, message: &str) -> String {
    format!(
        "{message}\n{}",
        block(&Wire {
            version: 1,
            kind: Kind::Rejection,
            pr,
            comment: rejected,
        })
    )
}

/// Parses a comment body as a receipt. `None` for anything that is not a
/// well-formed, version-1 receipt.
pub fn parse_receipt(body: &str) -> Option<Receipt> {
    let start = body.find(RECEIPT_START)?;
    let json_start = start + RECEIPT_START.len();
    let end = body[json_start..].find(RECEIPT_END)?;
    let wire: Wire = serde_json::from_str(&body[json_start..json_start + end]).ok()?;
    if wire.version != 1 {
        return None;
    }
    Some(match wire.kind {
        Kind::Retraction => Receipt::Retraction {
            pr: wire.pr,
            retracted: wire.comment,
        },
        Kind::Rejection => Receipt::Rejection {
            pr: wire.pr,
            rejected: wire.comment,
        },
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        /// Every formatted receipt parses back to itself — both kinds, all
        /// ids, any cosmetic text.
        #[test]
        fn format_parse_roundtrip(
            pr in 1u64..u64::MAX,
            comment in 1u64..u64::MAX,
            predecessor in proptest::option::of(1u64..u64::MAX),
            message in "[^\\-]{0,64}",
        ) {
            let retraction = format_retraction_receipt(
                PrNumber(pr),
                CommentId(comment),
                predecessor.map(PrNumber),
            );
            prop_assert_eq!(
                parse_receipt(&retraction),
                Some(Receipt::Retraction { pr: PrNumber(pr), retracted: CommentId(comment) })
            );
            let rejection = format_rejection_receipt(PrNumber(pr), CommentId(comment), &message);
            prop_assert_eq!(
                parse_receipt(&rejection),
                Some(Receipt::Rejection { pr: PrNumber(pr), rejected: CommentId(comment) })
            );
            prop_assert!(rejection.starts_with(&message), "the human message leads");
        }
    }

    #[test]
    fn non_receipts_do_not_parse() {
        assert_eq!(parse_receipt("just a comment"), None);
        assert_eq!(
            parse_receipt("<!-- merge-train-state\n{\"version\":1}\n-->"),
            None
        );
        assert_eq!(
            parse_receipt("<!-- merge-train-receipt\n{not json}\n-->"),
            None
        );
        assert_eq!(
            parse_receipt(
                "<!-- merge-train-receipt\n{\"version\":2,\"kind\":\"rejection\",\"pr\":1,\"comment\":5}\n-->"
            ),
            None,
            "unknown versions are not trusted"
        );
        assert_eq!(
            parse_receipt(
                "<!-- merge-train-receipt\n{\"version\":1,\"kind\":\"eviction\",\"pr\":1,\"comment\":5}\n-->"
            ),
            None,
            "unknown kinds are not trusted"
        );
    }
}
