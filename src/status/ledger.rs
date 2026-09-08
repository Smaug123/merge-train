//! The stack ledger: the bot's durable record of what it decided about ONE
//! PR's predecessor — a comment it writes and rewrites in place, so that a
//! store rebuilt from GitHub alone READS the topology rather than deriving
//! it again from the users' comments.
//!
//! A lost-DB crawl can only see GitHub's present. Reconstructing the
//! predecessor graph from the declaration comments means re-running the
//! live path's rules over inputs that no longer exist: the order the
//! deliveries actually arrived in, who edited a comment (the API reports
//! only its original author), and the state of every PR at the moment the
//! decision was taken. Each of those gaps is a way for the reconstruction
//! to disagree with the decision that was actually made — to resurrect an
//! edge the user retracted, or to accept a declaration the live path
//! refused.
//!
//! So the decision itself is written down. When the bot records a
//! predecessor — or removes one — it writes a ledger comment on that PR
//! saying exactly what the state now holds. Recovery reads it back.
//!
//! # Trust
//!
//! A ledger counts only when it is authored by the bot AND sits on the PR
//! its `pr` field names: a ledger found anywhere else is forged or
//! misplaced, the same gate status comments apply to `original_root_pr`.
//! Where a PR carries two (a crash between `PostComment` and the event
//! that records its id leaves an orphan), the highest `seq` wins — it is
//! the store's own event sequence number, so it orders writes exactly as
//! the store made them.
//!
//! # The two rules a crawl obeys
//!
//! - **The ledger grants.** An edge exists because a ledger says so, not
//!   because a comment can be read as declaring one.
//! - **The owning comment may revoke.** If the comment the ledger names is
//!   gone, or no longer declares that predecessor, the edge is dropped.
//!   This is not the old derivation returning: it is two durable records
//!   corroborating, and every disagreement resolves towards FEWER edges,
//!   which is the stop-shaped direction. It covers the one dangerous
//!   residual — a retraction whose ledger write was lost to an outage —
//!   without asking who edited anything.
//!
//! # Format
//!
//! ```text
//! <!-- merge-train-stack
//! {"version":1,"pr":5,"predecessor":4,"owner":900,"seq":37}
//! -->
//! **Stacked on #4.** ...
//! ```

use serde::{Deserialize, Serialize};

use crate::types::{CommentId, PrNumber};

/// The marker that begins a ledger's JSON block.
pub const LEDGER_START: &str = "<!-- merge-train-stack\n";

/// The marker that ends a ledger's JSON block.
pub const LEDGER_END: &str = "\n-->";

/// The declaration a ledger records: which PR is the predecessor, and the
/// comment that asked for it. Both or neither — a predecessor with no
/// owning comment is not a state the bot can reach, so it is not one this
/// type can express.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Declaration {
    pub predecessor: PrNumber,
    pub owner: CommentId,
}

/// What the bot's state holds for one PR's predecessor, as written on that
/// PR for a future crawl to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StackLedger {
    /// The PR this ledger is about; it counts only on that PR.
    pub pr: PrNumber,
    /// The declaration in force, or `None` — the PR declares no
    /// predecessor, which is as much a decision as declaring one.
    pub declared: Option<Declaration>,
    /// The store event sequence number this ledger was written for: the
    /// order the store made its writes in, and so the order a crawl
    /// resolves duplicates by.
    pub seq: u64,
    /// The highest declaration comment on this PR whose fate the bot has
    /// SETTLED — recorded or retracted. A crawl reads it to tell an old
    /// superseded declaration, which this ledger already accounts for,
    /// from one that appeared while the bot was away and may mean the
    /// topology moved under a train.
    pub settled_through: Option<CommentId>,
}

/// The machine-readable half of a ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Wire {
    version: u32,
    pr: PrNumber,
    predecessor: Option<PrNumber>,
    owner: Option<CommentId>,
    seq: u64,
    #[serde(default)]
    settled_through: Option<CommentId>,
}

/// Formats the ledger comment for `ledger`: the machine block first, then
/// the human sentence.
pub fn format_stack_ledger(ledger: &StackLedger) -> String {
    let wire = Wire {
        version: 1,
        pr: ledger.pr,
        predecessor: ledger.declared.map(|d| d.predecessor),
        owner: ledger.declared.map(|d| d.owner),
        seq: ledger.seq,
        settled_through: ledger.settled_through,
    };
    let json = serde_json::to_string(&wire).expect("ledger serialization cannot fail");
    let human = match ledger.declared {
        // `PrNumber` displays its own `#`.
        Some(Declaration { predecessor, owner }) => format!(
            "**Stacked on {predecessor}.** PR {} declares {predecessor} as its \
             predecessor, in comment {owner}.",
            ledger.pr
        ),
        None => format!("**Not stacked.** PR {} declares no predecessor.", ledger.pr),
    };
    format!(
        "{LEDGER_START}{json}{LEDGER_END}\n{human}\n\nThis comment is the bot's \
         record of the declaration, kept so the stack survives the loss of its \
         database. It is rewritten in place; edits to it are overwritten.",
    )
}

/// Parses a comment body as a stack ledger. `None` for anything that is not
/// a well-formed, version-1 ledger — including one whose predecessor and
/// owning comment disagree about whether a declaration exists.
pub fn parse_stack_ledger(body: &str) -> Option<StackLedger> {
    let start = body.find(LEDGER_START)?;
    let json_start = start + LEDGER_START.len();
    let end = body[json_start..].find(LEDGER_END)?;
    let wire: Wire = serde_json::from_str(&body[json_start..json_start + end]).ok()?;
    if wire.version != 1 {
        return None;
    }
    let declared = match (wire.predecessor, wire.owner) {
        (Some(predecessor), Some(owner)) => Some(Declaration { predecessor, owner }),
        (None, None) => None,
        // Half a declaration is not one: a ledger that cannot say which
        // comment asked for the edge cannot be corroborated against it,
        // and an owner with no predecessor names nothing.
        _ => return None,
    };
    Some(StackLedger {
        pr: wire.pr,
        declared,
        seq: wire.seq,
        settled_through: wire.settled_through,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        /// Every ledger the bot writes parses back to itself.
        #[test]
        fn format_parse_roundtrip(
            pr in 1u64..u64::MAX,
            declared in proptest::option::of((1u64..u64::MAX, 1u64..u64::MAX)),
            seq in 0u64..u64::MAX,
            settled_through in proptest::option::of(1u64..u64::MAX),
        ) {
            let ledger = StackLedger {
                pr: PrNumber(pr),
                declared: declared.map(|(predecessor, owner)| Declaration {
                    predecessor: PrNumber(predecessor),
                    owner: CommentId(owner),
                }),
                seq,
                settled_through: settled_through.map(CommentId),
            };
            prop_assert_eq!(parse_stack_ledger(&format_stack_ledger(&ledger)), Some(ledger));
        }
    }

    #[test]
    fn non_ledgers_do_not_parse() {
        assert_eq!(parse_stack_ledger("just a comment"), None);
        assert_eq!(
            parse_stack_ledger("<!-- merge-train-state\n{\"version\":1}\n-->"),
            None,
            "a status comment is not a ledger"
        );
        assert_eq!(
            parse_stack_ledger("<!-- merge-train-stack\n{not json}\n-->"),
            None
        );
        assert_eq!(
            parse_stack_ledger(
                "<!-- merge-train-stack\n{\"version\":2,\"pr\":1,\"predecessor\":2,\
                 \"owner\":9,\"seq\":0}\n-->"
            ),
            None,
            "unknown versions are not trusted"
        );
        assert_eq!(
            parse_stack_ledger(
                "<!-- merge-train-stack\n{\"version\":1,\"pr\":1,\"predecessor\":2,\
                 \"owner\":null,\"seq\":0}\n-->"
            ),
            None,
            "a predecessor with no owning comment cannot be corroborated"
        );
        assert_eq!(
            parse_stack_ledger(
                "<!-- merge-train-stack\n{\"version\":1,\"pr\":1,\"predecessor\":null,\
                 \"owner\":9,\"seq\":0}\n-->"
            ),
            None,
            "an owner with no predecessor names nothing"
        );
    }

    /// The human half names the PRs, so a reader of the thread can see what
    /// the bot recorded without reading JSON.
    #[test]
    fn the_human_half_names_the_prs() {
        let stacked = format_stack_ledger(&StackLedger {
            pr: PrNumber(5),
            declared: Some(Declaration {
                predecessor: PrNumber(4),
                owner: CommentId(900),
            }),
            seq: 7,
            settled_through: Some(CommentId(900)),
        });
        assert!(stacked.contains("**Stacked on #4.**"), "{stacked}");
        assert!(stacked.contains("in comment 900"), "{stacked}");
        assert!(stacked.contains("PR #5"), "{stacked}");
        let bare = format_stack_ledger(&StackLedger {
            pr: PrNumber(5),
            declared: None,
            seq: 8,
            settled_through: Some(CommentId(901)),
        });
        assert!(bare.contains("**Not stacked.**"), "{bare}");
    }
}
