//! First-contact bootstrap: the pure decision half of the GitHub crawl
//! (DESIGN §Bootstrap algorithm, Phase 2).
//!
//! A fresh store — a brand-new repo, or a repo whose state DB was LOST —
//! knows nothing: no default branch, no PR cache, no predecessor topology,
//! no trains. Webhooks only describe the future, so the first delivery
//! triggers a crawl of the present: repository settings, open PRs, recently
//! merged PRs, and every crawled PR's comments. This module turns those
//! fetched facts into state events; the pipeline does the fetching and
//! appends the result as one atomic batch.
//!
//! What the crawl rebuilds, and from where:
//!
//! - **PR cache** — open and recently merged PRs, plus any *seed* PR the
//!   wake-up webhook named that the list endpoints miss (a closed-unmerged
//!   root, fetched individually by the caller).
//! - **Predecessor topology** — the bot's own STACK LEDGERS, one per PR
//!   (`status::ledger`), read back rather than re-derived. An edge exists
//!   because a ledger says the bot recorded one; the comment the ledger
//!   names may revoke it by no longer declaring that predecessor. The
//!   crawl does not re-run the live path's validation, does not guess a
//!   delivery order from comment ids, and does not ask who edited what:
//!   those inputs are gone, and every earlier attempt to reconstruct the
//!   decision from them was another way to disagree with it. A
//!   declaration NO ledger accounts for cannot make an edge — but it can
//!   still abort a train whose stack it touches, because it is evidence
//!   the topology moved in a way the crawl cannot reconstruct. Comments
//!   may take topology away; they may never add it.
//!
//!   The exception is ONBOARDING: a repository with no trace of the bot —
//!   not one ledger, not one status comment — has never had a decision
//!   made about it, so its declarations are read as the live path would
//!   read them and recorded (owner's ruling, 2026-09-08). No train can
//!   exist there to endanger.
//! - **Trains** — the bot's own status comments (`merge-train-state`
//!   blocks), the designed off-disk backup: bot-authored, parseable, and
//!   sitting on their own `original_root_pr` (a record posted anywhere
//!   else is forged or misplaced — ignored). Per root, the latest
//!   incarnation (`started_at`) at its highest `recovery_seq` wins, and is
//!   adopted via `TrainRecordAdopted` — the same event, with the same
//!   ledger-boundary semantics, as restore-from-backup adoption. ACTIVE
//!   adopted trains are handed back for M6 recovery marking (worktree
//!   cleanup + resume through the evaluate path).
//!
//! **Envelope**: a train whose status comment was deleted AND whose DB was
//! lost is not resurrected — there is nothing sound to resurrect it from.
//! The stack behaves as if no train was running; already-merged PRs are in
//! the cache, so a fresh `start` gets the engine's loud validations (and
//! the late-addition answer) rather than silence.

use std::collections::{HashMap, HashSet};

use tracing::{error, warn};

use crate::commands::{Command, parse_command};
use crate::effects::PrData;
use crate::effects::github::CommentData;
use crate::persistence::event::{StateEvent, StateEventPayload};
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::state::RepoState;
use crate::state::descendants::collect_all_descendants;
use crate::state::validation::validate_predecessor_declaration;
use crate::status::parse::parse_status_comment;
use crate::types::{
    CommentId, MergeStateStatus, PrNumber, TrainError, TrainErrorKind, TrainLineage, TrainRecord,
};

use super::pipeline::cache_fill_events;

/// The crawl's decision: events to append, the roots of adopted ACTIVE
/// trains (the caller marks them for M6 recovery), and every PR the crawl
/// *referenced* but did not fetch — declaration targets and adopted-train
/// members absent from the crawl. The caller fetches those, lists their
/// comments, and RE-RUNS the crawl to a fixpoint: a closed-unmerged root
/// is absent from both list endpoints but named by its descendants'
/// declarations, and only by pulling it in can its status comment be found
/// and its train adopted/aborted (Codex crawl review rounds 6–7).
pub(crate) struct CrawlOutcome {
    pub events: Vec<StateEventPayload>,
    pub recovered_roots: Vec<PrNumber>,
    pub referenced_uncrawled: Vec<PrNumber>,
    /// PRs whose stack ledger the crawl did NOT believe — the comment it
    /// names no longer declares that predecessor, or it closed a cycle.
    /// The recovered state holds no edge for them, so their ledger is now
    /// wrong and is owed a rewrite.
    pub stale_ledgers: Vec<PrNumber>,
}

/// The stack ledger a PR carries, if any: bot-authored, parsing as a
/// ledger, and naming the PR it sits on — a ledger found anywhere else is
/// forged or misplaced, the same gate status comments get.
///
/// Where a crash between the post and the event recording its id left
/// TWO, the higher COMMENT ID wins. Not the stated `seq`: that is the
/// store's event sequence number, which restarts from nothing when the
/// database is rebuilt, so an orphan from a previous life can carry a
/// larger one than the ledger the store actually kept (Codex crawl review
/// round 14, P1). Comment ids are globally monotonic, and the later of two
/// posts is always the one the store went on to use.
fn ledger_on(
    pr: PrNumber,
    pr_comments: &[CommentData],
    bot_user_id: u64,
) -> Option<(CommentId, crate::status::StackLedger)> {
    pr_comments
        .iter()
        // `0` is the deny-safe sentinel for an omitted account on either
        // side, and two sentinels do not match.
        .filter(|c| c.author_id == bot_user_id && c.author_id != 0)
        .filter_map(|c| {
            crate::status::parse_stack_ledger(&c.body)
                .filter(|l| l.pr == pr)
                .map(|l| (c.id, l))
        })
        .max_by_key(|(id, _)| *id)
}

/// Whether adding `pr -> predecessor` would close a loop in the topology
/// built so far.
fn closes_a_cycle(topology: &RepoState, pr: PrNumber, predecessor: PrNumber) -> bool {
    let mut seen = HashSet::new();
    let mut at = predecessor;
    loop {
        if at == pr {
            return true;
        }
        if !seen.insert(at) {
            return false; // a loop that does not pass through `pr`
        }
        match topology.prs.get(&at).and_then(|p| p.predecessor) {
            Some(next) => at = next,
            None => return false,
        }
    }
}

/// The PRs a train record involves: its root, current PR, and the frozen
/// descendant set its phase carries.
/// Every PR the record's cascade involves: its primaries, the frozen set,
/// and the whole stack it knew at the freeze. The staleness check ("has
/// everything merged?") must see the deeper levels too, or a three-deep
/// train whose first two levels merged would be completed with its tail
/// still open (Codex crawl review round 6, P1).
fn all_involved(record: &TrainRecord) -> Vec<PrNumber> {
    let mut prs = members(record);
    if let Some(progress) = record.cascade_phase.progress() {
        for pr in progress.known_stack() {
            if !prs.contains(pr) {
                prs.push(*pr);
            }
        }
    }
    prs
}

fn members(record: &TrainRecord) -> Vec<PrNumber> {
    let mut members = vec![record.original_root_pr, record.current_pr];
    if let Some(progress) = record.cascade_phase.progress() {
        members.extend_from_slice(progress.frozen_descendants());
    }
    members
}

/// Builds a scratch [`RepoState`] from the topology events (default branch,
/// PR cache fills, predecessor declarations) so the stack-extension check
/// can walk the descendants index `apply_event` maintains.
fn replay_topology(
    default_branch: &str,
    events: &[StateEventPayload],
    now: chrono::DateTime<chrono::Utc>,
) -> RepoState {
    let mut state = RepoState::from_snapshot(PersistedRepoSnapshot::new(default_branch.to_owned()));
    for (seq, payload) in events.iter().enumerate() {
        state.apply_event(&StateEvent {
            seq: seq as u64,
            ts: now,
            payload: payload.clone(),
        });
    }
    state
}

/// The stack a mid-phase record knows: its CORE (the frozen set plus the
/// root and current PR) and the recorded descendant closure walked from
/// the root and current PR.
fn known_stack(
    topology: &RepoState,
    record: &TrainRecord,
) -> Option<(HashSet<PrNumber>, HashSet<PrNumber>)> {
    let progress = record.cascade_phase.progress()?;
    let mut core: HashSet<PrNumber> = progress.frozen_descendants().iter().copied().collect();
    core.insert(record.original_root_pr);
    core.insert(record.current_pr);
    // The stack the RECORD says the train knew, when it says so: recorded
    // with the freeze, it answers "was this PR part of the stack?" for
    // every depth without guessing from comment ids.
    core.extend(progress.known_stack().iter().copied());
    let mut stack = core.clone();
    for anchor in [record.original_root_pr, record.current_pr] {
        stack.extend(collect_all_descendants(
            anchor,
            &topology.descendants,
            &topology.prs,
        ));
    }
    Some((core, stack))
}

/// Whether the crawled topology EXTENDS the adopted train's stack: a PR
/// outside the core declaring a stack member as its predecessor, declared
/// AFTER the train's watermark. Live aborts an active train whose stack
/// grows under it (`topology_change_abort`); after a DB loss the frozen set
/// cannot be fully trusted, so recovery aborts on any extension (owner
/// ruling, round 11).
///
/// The watermark separates baseline from extension: GitHub comment ids are
/// globally monotonic, so a non-edited declaration owned by a LOWER id than
/// the train's first status comment provably predates the train — a
/// pre-declared grandchild sits in the closure outside the frozen set
/// without being an extension (the lost_db differential harness's first
/// finding). An unattributable edge stays conservative.
///
/// Every declaration INTO the stack is checked, not only the closure walked
/// from the root: a gap that also severs a frozen member's own link (its
/// declaration deleted — live would abort on that removal, the crawl
/// deliberately does not) leaves that member outside the walk, and an
/// extension onto it would otherwise be missed — the next cascade level
/// would freeze and drive the new PR (lost_db envelope finding).
///
/// Only *extensions* are detected, not pure reorders or removals within the
/// frozen set: the cascade prepares each frozen descendant against
/// `current_pr` (the frozen frontier), not against its live-declared
/// predecessor, so a recovered train's git operations stay self-consistent
/// regardless of intra-set churn — and detecting removal/reorder cannot be
/// done without false-positives on the legitimate mid-cascade state where a
/// merged member blocks traversal to its still-pending children. An
/// `Idle`-phase train has no frozen set yet (it will freeze against the
/// current topology at its next `Preparing`), so it is never flagged.
fn stack_extended(topology: &RepoState, record: &TrainRecord) -> bool {
    let Some((core, stack)) = known_stack(topology, record) else {
        return false;
    };
    // A record that RECORDED the stack it froze against needs no
    // heuristic: anything declaring into the stack from outside what the
    // train knew is an extension. Comment ids only prove creation order,
    // not when a declaration entered the bot's state — a comment created
    // before the status comment but delivered after the freeze is a real
    // late extension (Codex crawl review round 4, P1) — so the watermark
    // is the FALLBACK, for records written before `known_stack` existed.
    let recorded_stack = record
        .cascade_phase
        .progress()
        .is_some_and(|p| !p.known_stack().is_empty());
    // The record's own watermark — the FIRST status comment's id — never
    // its current comment id, which a recovery repost moves above
    // declarations made after the freeze. A record without one (posted
    // before its `StatusCommentPosted` was applied) was adopted from the
    // first comment itself, whose id is the same watermark.
    let watermark = record.watermark.or(record.status_comment_id);
    topology.prs.values().any(|p| {
        !core.contains(&p.number)
            && p.predecessor.is_some_and(|t| stack.contains(&t))
            && (recorded_stack
                || match (p.predecessor_comment_id, watermark) {
                    // Declared before the train's own status comment
                    // existed: visible at (or before) the freeze.
                    (Some(declared), Some(mark)) => declared > mark,
                    // Unattributable — stay conservative.
                    _ => true,
                })
    })
}

/// Whether an unledgered predecessor-shaped comment touches `record`'s
/// stack: naming a member as its predecessor (an extension the bot never
/// recorded), or sitting ON a member and naming something else (a member
/// re-pointed out of the stack). Either way the topology the train froze
/// against may have moved, and the crawl cannot tell what the live path
/// would have made of the comment — so it does the one safe thing and
/// aborts, which is the owner's ruling for the recovery path (stop-shaped
/// residuals are acceptable; silently driving changed topology is not).
///
/// RESIDUAL: a comment the live path would have REFUSED — junk, a
/// stranger's, a duplicate — aborts a recovered train too. That is a
/// visible stop the user answers with `start`, and it costs a DB loss to
/// reach.
fn unledgered_touches(
    topology: &RepoState,
    record: &TrainRecord,
    unledgered: &[(PrNumber, PrNumber)],
) -> bool {
    let Some((_, stack)) = known_stack(topology, record) else {
        return false;
    };
    unledgered
        .iter()
        .any(|(source, target)| stack.contains(target) || stack.contains(source))
}

/// Everything `crawl_events` reads: repository facts, the fetched data, and
/// the caller's exclusions.
pub(crate) struct CrawlInput<'a> {
    pub default_branch: &'a str,
    /// Open, recently merged, and individually fetched PRs, as one slice:
    /// every check reads `state`, so the split never matters.
    pub crawled_prs: &'a [PrData],
    /// Each crawled PR's comments, id-ordered; PRs missing from it simply
    /// contribute no declarations or records.
    pub comments: &'a [(PrNumber, Vec<CommentData>)],
    pub bot_name: &'a str,
    pub bot_user_id: u64,
    /// Referenced PRs a permanent `GetPr` failure could not fetch.
    pub unfetchable: &'a HashSet<PrNumber>,
    /// The comment-listing cap was reached: some crawled PRs' comments
    /// were never read, so a ledger — or a whole train member — may be
    /// missing from everything below.
    pub comments_truncated: bool,
    /// Stamps the completion of stale records.
    pub now: chrono::DateTime<chrono::Utc>,
}

/// Turns crawled facts into state events. Pure: fetching is the caller's.
pub(crate) fn crawl_events(input: &CrawlInput<'_>) -> CrawlOutcome {
    let CrawlInput {
        default_branch,
        crawled_prs,
        comments,
        bot_name,
        bot_user_id,
        unfetchable,
        comments_truncated,
        now,
    } = *input;
    let mut events = vec![StateEventPayload::DefaultBranchSet {
        branch: default_branch.to_owned(),
    }];

    // PR cache fills first: declarations and adoptions below refer to them,
    // and `apply_event` skips events about unknown PRs. `crawled_prs` is the
    // union of open, recently-merged, and any seed PRs the caller fetched
    // individually (a closed-unmerged root the wake-up webhook named); the
    // open/merged split never matters here — every check reads `p.state` —
    // so they arrive as one slice.
    let all_prs: Vec<&PrData> = crawled_prs.iter().collect();
    for pr in &all_prs {
        events.extend(cache_fill_events(pr.number, pr, MergeStateStatus::Unknown));
    }

    // The topology, READ from the bot's own stack ledgers rather than
    // derived a second time from the users' declaration comments. Two
    // rules, and no others:
    //
    // - **the ledger grants**: an edge exists because the bot wrote down
    //   that it had recorded one, not because a comment can be read as
    //   declaring one;
    // - **the owning comment may revoke**: if the comment the ledger names
    //   is gone, or no longer declares that predecessor, the edge is
    //   dropped.
    //
    // The second rule is not the old derivation returning. It is two
    // durable records corroborating, and every disagreement between them
    // resolves towards FEWER edges — a recovered train missing a member
    // aborts, where a fabricated edge would drive a PR the user never
    // stacked. It covers the one dangerous residual the ledger alone
    // leaves: a retraction whose ledger write was lost to an outage.
    //
    // RESIDUAL: a maintainer who edits somebody else's declaration comment
    // to name a different predecessor revokes that edge for a crawl, even
    // though the live path refused their edit. Stop-shaped, and it takes a
    // DB loss and a maintainer to reach.
    let mut edges: Vec<(PrNumber, PrNumber, CommentId)> = Vec::new();
    let mut stale_ledgers: Vec<PrNumber> = Vec::new();
    // Predecessor-shaped comments no ledger accounts for. They cannot make
    // an edge — the bot may have refused them, or never seen them at all —
    // but they are evidence that a train's stack may have moved in a way
    // the crawl cannot reconstruct, and that only ever ABORTS a train
    // (`unledgered_touches`). Comments may take topology away; they may
    // never add it.
    let mut unledgered: Vec<(PrNumber, PrNumber)> = Vec::new();
    // Per PR, the highest declaration comment its ledger says the bot has
    // SETTLED — recorded or retracted. Everything at or below it is a
    // comment whose fate the bot decided; only what lies above it can be a
    // change that happened while the bot was away.
    let mut settled: HashMap<PrNumber, CommentId> = HashMap::new();
    for (pr, pr_comments) in comments {
        let Some((ledger_comment, ledger)) = ledger_on(*pr, pr_comments, bot_user_id) else {
            continue;
        };
        // Where the ledger lives, so the recovered store rewrites that
        // comment rather than posting a second record of the same PR.
        events.push(StateEventPayload::StackLedgerPosted {
            pr: *pr,
            comment_id: ledger_comment,
        });
        if let Some(through) = ledger.settled_through {
            settled.insert(*pr, through);
        }
        let Some(crate::status::Declaration { predecessor, owner }) = ledger.declared else {
            continue;
        };
        let corroborated = pr_comments.iter().find(|c| c.id == owner).is_some_and(|c| {
            matches!(
                parse_command(&c.body, bot_name),
                Some(Command::Predecessor(target)) if target == predecessor
            )
        });
        if !corroborated {
            warn!(
                %pr, %predecessor, comment = %owner,
                "the comment a stack ledger names no longer declares that predecessor; \
                 dropping the edge and re-writing the ledger"
            );
            stale_ledgers.push(*pr);
            continue;
        }
        edges.push((*pr, predecessor, owner));
    }
    edges.sort_unstable();

    // ONBOARDING. A repository with no trace of the bot — not one ledger,
    // not one status comment — has never had a decision made about it, so
    // there is nothing to disagree with: the declarations sitting on its
    // PRs are read the way the live path would read them, recorded, and
    // written to ledgers (the `PredecessorDeclared` events below mark them
    // owed). This is the ONE place the crawl derives anything, and it is
    // reachable only where no train can exist to endanger — a repository
    // with a status comment is a recovery, not an onboarding.
    //
    // RESIDUAL (owner's ruling, 2026-09-08): a repository whose every bot
    // comment was deleted looks new, so its declarations are adopted
    // afresh — including any the bot had refused.
    let onboarding = comments.iter().all(|(_, pr_comments)| {
        pr_comments.iter().all(|c| {
            c.author_id != bot_user_id
                || (crate::status::parse_stack_ledger(&c.body).is_none()
                    && parse_status_comment(&c.body).is_err())
        })
    });
    if onboarding {
        // Author-only and non-edited, exactly as the live handler
        // authorizes them; replayed in comment-id order, so the first
        // VALID declaration on a PR wins as it would have over time.
        let authors: HashMap<PrNumber, u64> =
            all_prs.iter().map(|p| (p.number, p.author_id)).collect();
        let mut candidates: Vec<(CommentId, PrNumber, PrNumber)> = Vec::new();
        for (pr, pr_comments) in comments {
            for comment in pr_comments {
                let Some(Command::Predecessor(target)) = parse_command(&comment.body, bot_name)
                else {
                    continue;
                };
                // An edited body cannot be attributed — the API reports
                // only the original author, never the editor — so it is
                // not a declaration anyone can be held to.
                if comment.edited || comment.author_id == 0 {
                    continue;
                }
                if authors.get(pr) == Some(&comment.author_id) {
                    candidates.push((comment.id, *pr, target));
                }
            }
        }
        candidates.sort_unstable();
        let mut scratch = replay_topology(default_branch, &events, now);
        for &(comment_id, pr, predecessor) in &candidates {
            let Some(cached) = scratch.prs.get(&pr) else {
                continue;
            };
            if cached.predecessor.is_some() {
                continue; // first declaration wins
            }
            if validate_predecessor_declaration(cached, predecessor, &scratch.prs, default_branch)
                .is_err()
            {
                continue;
            }
            // Ownership moves FORWARD, exactly as the live path moves it:
            // the first valid declaration wins the edge, and a later
            // comment restating the same predecessor takes ownership of
            // it. Without this an onboarded repository would record the
            // oldest comment as owner, and a retraction aimed at the
            // newest would not remove the edge.
            let owner = candidates
                .iter()
                .filter(|(id, p, target)| *p == pr && *target == predecessor && *id > comment_id)
                .map(|(id, _, _)| *id)
                .max()
                .unwrap_or(comment_id);
            let decl = StateEventPayload::PredecessorDeclared {
                pr,
                predecessor,
                comment_id: owner,
            };
            scratch.apply_event(&StateEvent {
                seq: events.len() as u64,
                ts: now,
                payload: decl.clone(),
            });
            edges.push((pr, predecessor, owner));
        }
        edges.sort_unstable();
    }

    // Everything predecessor-shaped that the ledgers do not account for. A
    // comment restating the very edge its PR's ledger holds is accounted
    // for; anything else on that PR is a change the bot never recorded.
    let authors: HashMap<PrNumber, u64> = all_prs.iter().map(|p| (p.number, p.author_id)).collect();
    for (pr, pr_comments) in comments {
        let believed = edges
            .iter()
            .find(|(p, _, _)| p == pr)
            .map(|(_, t, o)| (*t, *o));
        for comment in pr_comments {
            let Some(Command::Predecessor(target)) = parse_command(&comment.body, bot_name) else {
                continue;
            };
            // Accounted for: the ledger's own owning comment, a
            // restatement of the very edge it holds, or anything the
            // ledger says was already settled — a superseded declaration
            // the user replaced long ago is not news.
            let accounted_for = believed
                .is_some_and(|(t, owner)| comment.id == owner || target == t)
                || settled
                    .get(pr)
                    .is_some_and(|through| comment.id <= *through);
            // And only a comment the live path COULD have accepted is
            // evidence at all. Authorship is the one gate that needs no
            // history — the comment carries its author and the PR carries
            // its own — so a stranger's declaration, which live refuses,
            // does not abort a recovered train. An EDITED body stays
            // evidence: the API reports only the original author, never
            // the editor, so it cannot be cleared this way and the
            // conservative reading stands.
            let could_have_been_accepted = comment.edited
                || (comment.author_id != 0 && authors.get(pr) == Some(&comment.author_id));
            if !accounted_for && could_have_been_accepted {
                unledgered.push((*pr, target));
            }
        }
    }

    // The topology scratch: built from the cache fills, then grown one
    // ledger edge at a time, so the train checks below can walk the
    // descendants index `apply_event` maintains.
    let mut topology = replay_topology(default_branch, &events, now);
    for (pr, predecessor, comment_id) in edges.iter().copied() {
        // A cycle cannot arise from decisions the live path made — it
        // refuses them — so one here means the ledgers disagree with each
        // other (hand-edited, or forged). Dropping the edge that closes
        // the loop keeps the recovered state walkable; the alternative is
        // a descendants index that never terminates.
        if closes_a_cycle(&topology, pr, predecessor) {
            error!(
                %pr, %predecessor,
                "stack ledgers describe a cycle; dropping the edge that closes it"
            );
            stale_ledgers.push(pr);
            continue;
        }
        let decl = StateEventPayload::PredecessorDeclared {
            pr,
            predecessor,
            comment_id,
        };
        topology.apply_event(&StateEvent {
            seq: events.len() as u64,
            ts: now,
            payload: decl.clone(),
        });
        events.push(decl);
    }

    // A declaration target the crawl has not fetched is referenced-but-
    // uncrawled: the caller fetches it and re-runs, because a target that
    // is a closed-unmerged train root (invisible to both list endpoints)
    // carries a status comment the crawl must see to adopt and abort its
    // train (Codex crawl review round 7).
    //
    // UNLEDGERED declarations are followed too — for DISCOVERY only, never
    // for their edge. A declaration whose ledger write was lost may be the
    // only route to a closed-unmerged root's status comment, and without
    // the fetch that root's train is neither adopted nor aborted (Codex
    // crawl review round 14, P2). The fetch cap still bounds it.
    let crawled_numbers: HashSet<PrNumber> = all_prs.iter().map(|p| p.number).collect();
    let mut referenced_uncrawled: Vec<PrNumber> = Vec::new();
    for predecessor in edges
        .iter()
        .map(|(_, predecessor, _)| predecessor)
        .chain(unledgered.iter().map(|(_, target)| target))
    {
        if !crawled_numbers.contains(predecessor) && !referenced_uncrawled.contains(predecessor) {
            referenced_uncrawled.push(*predecessor);
        }
    }

    // Train recovery from the bot's status comments. Trust gates: authored
    // by the bot, parseable, and posted on its own root PR. Per root, the
    // latest incarnation at its highest recovery_seq (ties to the later
    // comment) wins.
    let mut best: HashMap<PrNumber, (TrainRecord, crate::types::CommentId)> = HashMap::new();
    // Every trusted record's lineage, by the PR it sits on — fan-out
    // evidence must survive the child starting a fresh train of its own
    // (whose newest record has no lineage): the OLDER record still proves
    // the parent fanned out (Codex crawl review round 2, P1).
    let mut lineages: HashMap<PrNumber, Vec<TrainLineage>> = HashMap::new();
    for (pr, pr_comments) in comments {
        for comment in pr_comments {
            if comment.author_id != bot_user_id {
                continue;
            }
            let Ok(record) = parse_status_comment(&comment.body) else {
                continue;
            };
            if record.original_root_pr != *pr {
                continue;
            }
            if let Some(parent) = &record.parent {
                lineages.entry(*pr).or_default().push(parent.clone());
            }
            // Ordered by COMMENT ID, which GitHub makes globally
            // monotonic: `started_at` is a wall clock that can step
            // backwards or collide across a stop-and-restart, and picking
            // the older record there would resurrect a dead train or lose
            // the live one (Codex crawl review round 6, P2). A train's own
            // comment is UPDATED in place as it advances, so the highest
            // id on a root is its newest incarnation; `recovery_seq` only
            // breaks ties within one.
            let candidate_key = (comment.id, record.recovery_seq);
            let supersedes = best
                .get(&record.original_root_pr)
                .is_none_or(|(b, id)| candidate_key > (*id, b.recovery_seq));
            if supersedes {
                best.insert(record.original_root_pr, (record, comment.id));
            }
        }
    }
    let merged_numbers: HashSet<PrNumber> = all_prs
        .iter()
        .filter(|p| p.state.is_merged())
        .map(|p| p.number)
        .collect();

    let mut recovered_roots = Vec::new();

    // Adopted-train members absent from the crawl (a frozen descendant, or
    // a closed root, that neither list endpoint returned) join the
    // referenced-uncrawled set so the caller fetches them and re-crawls.
    let mut roots: Vec<PrNumber> = best.keys().copied().collect();
    roots.sort_unstable();
    for root in roots {
        let (mut record, comment_id) = best.remove(&root).expect("keyed by best");
        record.status_comment_id = Some(comment_id);
        // Staleness (Codex crawl review, P2): status updates are
        // best-effort, so a train that FINISHED can leave an ACTIVE
        // comment behind (the final update failed) — and an unfinished
        // train necessarily has unmerged members. A mid-phase record whose
        // current PR and every frozen descendant are all merged has
        // nothing left to do: adopt it as completed (removal semantics)
        // rather than resurrect a zombie that would redo pushes against
        // branches users have since moved.
        // Only where the record SAYS what its stack was: a progress record
        // with no `known_stack` cannot tell a two-deep train from a
        // three-deep one whose tail is still open, and completing it would
        // orphan that tail (Codex crawl review round 14, P2).
        let all_members_merged = record
            .cascade_phase
            .progress()
            .is_some_and(|p| !p.known_stack().is_empty())
            && all_involved(&record)
                .iter()
                .skip(1) // the root legitimately merges early in the cascade
                .all(|m| merged_numbers.contains(m));
        // A stale FAN-OUT parent (Codex crawl review round 8): fan-out's
        // best-effort completion update to the old root's comment can fail,
        // leaving it ACTIVE — with still-open children, so the
        // all-members-merged check above does not fire. But each fan-out
        // child is a fresh root whose record names its parent's identity
        // (root + started_at, compared by equality — never clock order), so
        // a frozen descendant carrying its own adopted root record with
        // THIS lineage means this train already fanned out. Complete it,
        // or it resurrects in parallel with its children (double squash).
        let own_identity = TrainLineage {
            root,
            started_at: record.started_at,
        };
        let fanned_out = record.cascade_phase.progress().is_some()
            && members(&record).iter().skip(1).any(|m| {
                lineages
                    .get(m)
                    .is_some_and(|parents| parents.contains(&own_identity))
            });
        // Synthesized completion is a real `TrainCompleted` event after the
        // adoption, not a doctored record: the store then owes the status
        // comment its final word like any completion, so the comment does
        // not stay visibly — and machine-readably — ACTIVE (Codex crawl
        // review round 2, P2).
        let complete = record.state.is_active() && (all_members_merged || fanned_out);
        // The default branch the train was created against must be the one
        // the crawl just fetched: its cascade steps (retargets, catch-up
        // merges) name that branch, and a train already mid-`CatchingUp`
        // or `Retargeting` would otherwise carry on against the NEW branch
        // (Codex crawl review, P1). A record without one is unknown —
        // conservative: abort.
        let default_branch_changed =
            record.state.is_active() && !complete && record.default_branch != default_branch;
        let created_against = record.default_branch.clone();
        // A stack extended during the gap aborts, matching the live
        // topology-change abort (Codex crawl review round 4).
        let extended = record.state.is_active()
            && !complete
            && (stack_extended(&topology, &record)
                || unledgered_touches(&topology, &record, &unledgered));
        // A frozen member with NO recorded predecessor was unstacked
        // during the gap: no ledger claims an edge for it, or the comment
        // its ledger named no longer declares one. Either way the frozen
        // set is stale, and driving it would merge a PR the user
        // unstacked (lost_db envelope finding). Only FETCHED members can
        // be judged: one the crawl has not seen yet is reported below for
        // the next fixpoint pass, and one that cannot be fetched aborts
        // below anyway.
        let severed = record.state.is_active()
            && !complete
            && record.cascade_phase.progress().is_some_and(|p| {
                p.frozen_descendants()
                    .iter()
                    .any(|m| topology.prs.get(m).is_some_and(|c| c.predecessor.is_none()))
            });
        // A member the caller could not fetch (a permanent `GetPr` 404 —
        // deleted, or the token lost access) means the train references a
        // PR the crawl cannot see. Recovering it would let its first
        // evaluation hit `UnknownPr` and stick; abort cleanly instead
        // (Codex crawl review round 12).
        // The crawl could not read every PR's comments, so a member's
        // ledger may simply be missing: an `Idle` train has no frozen set
        // to check, and would squash its root without preparing the
        // descendant whose edge went unread (Codex crawl review round 14,
        // P1). Nothing is recoverable from a partial read.
        let truncated = comments_truncated && record.state.is_active() && !complete;
        let unreachable: Option<PrNumber> = (record.state.is_active() && !complete)
            .then(|| {
                members(&record)
                    .into_iter()
                    .find(|m| unfetchable.contains(m))
            })
            .flatten();
        if record.state.is_active()
            && !complete
            && !extended
            && !severed
            && !default_branch_changed
            && !truncated
            && unreachable.is_none()
        {
            recovered_roots.push(root);
            for member in members(&record) {
                if !crawled_numbers.contains(&member) && !referenced_uncrawled.contains(&member) {
                    referenced_uncrawled.push(member);
                }
            }
        }
        events.push(StateEventPayload::TrainRecordAdopted {
            root_pr: root,
            record,
        });
        if complete {
            events.push(StateEventPayload::TrainCompleted { root_pr: root });
        }
        if extended {
            events.push(StateEventPayload::TrainAborted {
                root_pr: root,
                error: TrainError::new(
                    TrainErrorKind::PredecessorChanged,
                    format!(
                        "PR #{root}'s stack was extended while its train was interrupted \
                         (a new predecessor declaration appeared); a merge train cannot \
                         safely resume over changed topology — re-issue `@merge-train start`."
                    ),
                ),
            });
        } else if severed {
            events.push(StateEventPayload::TrainAborted {
                root_pr: root,
                error: TrainError::new(
                    TrainErrorKind::PredecessorChanged,
                    format!(
                        "PR #{root}'s stack changed while its train was interrupted (a member \
                         no longer declares its predecessor); a merge train cannot safely \
                         resume over changed topology — re-issue `@merge-train start`."
                    ),
                ),
            });
        } else if default_branch_changed {
            events.push(StateEventPayload::TrainAborted {
                root_pr: root,
                error: TrainError::new(
                    TrainErrorKind::BaseBranchMismatch,
                    format!(
                        "PR #{root}'s train was created against default branch \
                         `{created_against}`, but the repository's default branch is now \
                         `{default_branch}`; a merge train cannot safely resume against a \
                         different default branch — re-issue `@merge-train start`."
                    ),
                ),
            });
        } else if truncated {
            events.push(StateEventPayload::TrainAborted {
                root_pr: root,
                error: TrainError::new(
                    TrainErrorKind::ApiError,
                    format!(
                        "PR #{root}'s train cannot be recovered: the bot hit its \
                         comment-listing cap while crawling this repository, so some \
                         PRs' records were never read. Re-issue `@merge-train start` \
                         once the repository is within the supported size."
                    ),
                ),
            });
        } else if let Some(member) = unreachable {
            events.push(StateEventPayload::TrainAborted {
                root_pr: root,
                error: TrainError::new(
                    TrainErrorKind::ApiError,
                    format!(
                        "PR #{root}'s train references PR #{member}, which the bot cannot \
                         fetch (deleted, or the token lost access); the train cannot resume \
                         — re-issue `@merge-train start` once the PR is reachable."
                    ),
                ),
            });
        }
    }
    referenced_uncrawled.sort_unstable();
    referenced_uncrawled.dedup();

    CrawlOutcome {
        events,
        recovered_roots,
        referenced_uncrawled,
        stale_ledgers,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::status::format::format_status_comment;
    use crate::types::{CommentId, PrState, Sha, TrainState};

    const BOT: u64 = 424_242;
    const AUTHOR: u64 = 100;
    const STRANGER: u64 = 200;

    fn test_now() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 3, 0, 0, 0).unwrap()
    }

    /// A record as the live path creates it: against default branch `main`.
    fn train(root: PrNumber, started_at: chrono::DateTime<Utc>) -> TrainRecord {
        let mut record = TrainRecord::new(root, started_at);
        record.default_branch = "main".to_owned();
        record
    }

    /// A root PR: targets `main`, branch `pr-<n>`.
    fn pr(number: u64, author_id: u64, state: PrState) -> PrData {
        PrData {
            number: PrNumber(number),
            head_sha: Sha::parse("a".repeat(40)).unwrap(),
            head_ref: format!("pr-{number}"),
            base_ref: "main".to_owned(),
            state,
            is_draft: false,
            author_id,
        }
    }

    /// A stacked PR whose base branch is its predecessor's head branch, so
    /// `validate_predecessor_declaration`'s base-match check passes.
    fn child(number: u64, author_id: u64, predecessor: u64, state: PrState) -> PrData {
        PrData {
            base_ref: format!("pr-{predecessor}"),
            ..pr(number, author_id, state)
        }
    }

    fn comment(id: u64, author_id: u64, body: &str) -> CommentData {
        CommentData {
            id: CommentId(id),
            author_id,
            body: body.to_owned(),
            edited: false,
        }
    }

    /// The bot's stack-ledger comment for `pr`, stating `declared` (the
    /// predecessor and the comment that asked for it). Its settled
    /// watermark is that comment, which is what the live path writes for a
    /// declaration; `settled_ledger` states a different one.
    fn ledger(id: u64, pr: u64, declared: Option<(u64, u64)>, seq: u64) -> CommentData {
        let through = declared.map(|(_, owner)| owner);
        settled_ledger(id, pr, declared, seq, through)
    }

    /// A ledger stating an explicit settled watermark: the highest
    /// declaration comment on the PR whose fate the bot had decided.
    fn settled_ledger(
        id: u64,
        pr: u64,
        declared: Option<(u64, u64)>,
        seq: u64,
        settled_through: Option<u64>,
    ) -> CommentData {
        comment(
            id,
            BOT,
            &crate::status::format_stack_ledger(&crate::status::StackLedger {
                pr: PrNumber(pr),
                declared: declared.map(|(predecessor, owner)| crate::status::Declaration {
                    predecessor: PrNumber(predecessor),
                    owner: CommentId(owner),
                }),
                seq,
                settled_through: settled_through.map(CommentId),
            }),
        )
    }

    fn declared(events: &[StateEventPayload]) -> Vec<(PrNumber, PrNumber)> {
        events
            .iter()
            .filter_map(|e| match e {
                StateEventPayload::PredecessorDeclared {
                    pr, predecessor, ..
                } => Some((*pr, *predecessor)),
                _ => None,
            })
            .collect()
    }

    /// The ledger GRANTS: an edge is recorded because the bot wrote down
    /// that it recorded one — not because the crawl can re-derive it. This
    /// edge would fail live validation today (the declarer's base is not
    /// its predecessor's branch), and it is still recorded: the decision
    /// was made when the state was different, and re-judging it against
    /// the present is precisely the mistake this design removes.
    #[test]
    fn a_ledger_edge_is_recorded_without_re_validating_it() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            // Based on `main`, not on `pr-1`: a fresh declaration would be
            // refused as a base mismatch.
            pr(2, AUTHOR, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                ledger(11, 2, Some((1, 10)), 5),
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert_eq!(declared(&outcome.events), vec![(PrNumber(2), PrNumber(1))]);
        assert!(outcome.stale_ledgers.is_empty());
    }

    /// The owning comment REVOKES: gone, or no longer declaring that
    /// predecessor, and the edge goes with it — whatever else survives on
    /// the PR. The ledger is then wrong, and is reported for rewriting.
    #[test]
    fn the_owning_comment_revokes_the_ledgers_edge() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let run = |pr_comments: Vec<CommentData>| {
            let comments = vec![(PrNumber(2), pr_comments)];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &crawled,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                unfetchable: &HashSet::new(),
                comments_truncated: false,
                now: test_now(),
            });
            (declared(&outcome.events), outcome.stale_ledgers)
        };
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                ledger(11, 2, Some((1, 10)), 5),
            ]),
            (vec![(PrNumber(2), PrNumber(1))], vec![]),
            "corroborated: the edge stands"
        );
        assert_eq!(
            run(vec![ledger(11, 2, Some((1, 10)), 5)]),
            (vec![], vec![PrNumber(2)]),
            "the owning comment is gone: revoked"
        );
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #9"),
                ledger(11, 2, Some((1, 10)), 5),
            ]),
            (vec![], vec![PrNumber(2)]),
            "it declares something else now: revoked"
        );
        assert_eq!(
            run(vec![
                // Another comment declaring the same thing is not the one
                // the ledger names, and cannot stand in for it.
                comment(9, AUTHOR, "@merge-train predecessor #1"),
                ledger(11, 2, Some((1, 10)), 5),
            ]),
            (vec![], vec![PrNumber(2)]),
            "a lookalike does not corroborate"
        );
    }

    /// A crash between posting a ledger and recording its id leaves two on
    /// the PR. The store's own sequence number orders them: the higher
    /// `seq` is the later write, and it wins.
    #[test]
    fn the_highest_seq_ledger_wins() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                // The older write still claims the edge; the newer one
                // records the retraction that followed it.
                ledger(11, 2, Some((1, 10)), 5),
                ledger(12, 2, None, 6),
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert!(declared(&outcome.events).is_empty(), "the newer write wins");
        assert!(
            outcome.events.iter().any(|e| matches!(
                e,
                StateEventPayload::StackLedgerPosted {
                    pr: PrNumber(2),
                    comment_id: CommentId(12)
                }
            )),
            "and the store is told where THAT one lives"
        );
    }

    /// The same trust gates status comments get: a ledger counts only when
    /// the bot wrote it and it sits on the PR it names.
    #[test]
    fn forged_or_misplaced_ledgers_are_ignored() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let mut forged = ledger(11, 2, Some((1, 10)), 5);
        forged.author_id = AUTHOR;
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                forged,
                // A real ledger, but about another PR: misplaced.
                ledger(12, 3, Some((1, 10)), 6),
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert!(
            declared(&outcome.events).is_empty(),
            "neither a user's ledger nor a misplaced one grants an edge"
        );
    }

    /// Ledgers that describe a cycle cannot come from decisions the live
    /// path made — it refuses them — so one here means the ledgers have
    /// been tampered with. The edge that closes the loop is dropped, or
    /// the recovered state would have a descendants index that never
    /// terminates.
    #[test]
    fn a_cycle_in_the_ledgers_is_broken() {
        let crawled = vec![
            child(1, AUTHOR, 2, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![
            (
                PrNumber(1),
                vec![
                    comment(10, AUTHOR, "@merge-train predecessor #2"),
                    ledger(11, 1, Some((2, 10)), 5),
                ],
            ),
            (
                PrNumber(2),
                vec![
                    comment(20, AUTHOR, "@merge-train predecessor #1"),
                    ledger(21, 2, Some((1, 20)), 6),
                ],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(1), PrNumber(2))],
            "the first edge stands; the one closing the loop is dropped"
        );
        assert_eq!(outcome.stale_ledgers, vec![PrNumber(2)]);
    }

    /// Onboarding runs only where the bot has left NO record: a repository
    /// carrying a status comment is a recovery, and there a declaration no
    /// ledger claims is not an edge — it is evidence the topology moved,
    /// which aborts the train it touches rather than rebuilding it.
    #[test]
    fn a_repository_with_a_status_comment_is_recovered_not_onboarded() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
            child(3, AUTHOR, 2, PrState::Open),
        ];
        let comments = vec![
            (
                PrNumber(1),
                vec![comment(
                    50,
                    BOT,
                    &format_status_comment(&record, "s").unwrap(),
                )],
            ),
            (
                PrNumber(2),
                vec![
                    comment(10, AUTHOR, "@merge-train predecessor #1"),
                    ledger(11, 2, Some((1, 10)), 5),
                ],
            ),
            // #3 declares onto a frozen member, and no ledger says the bot
            // ever recorded it.
            (
                PrNumber(3),
                vec![comment(60, AUTHOR, "@merge-train predecessor #2")],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: ts,
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(1))],
            "only the ledgered edge is rebuilt"
        );
        assert!(
            outcome.events.iter().any(|e| matches!(
                e,
                StateEventPayload::TrainAborted {
                    root_pr: PrNumber(1),
                    ..
                }
            )),
            "and the unledgered declaration into its stack aborts the train"
        );
        assert!(outcome.recovered_roots.is_empty());
    }

    /// Onboarding: a repository with no trace of the bot has never had a
    /// decision made about it, so the declarations on its PRs are read the
    /// way the live path would read them — author-only, unedited, first
    /// valid one per PR — and recorded (owner's ruling, 2026-09-08).
    #[test]
    fn a_repository_with_no_bot_records_is_onboarded() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
            child(3, AUTHOR, 2, PrState::Open),
        ];
        let mut edited = comment(31, AUTHOR, "@merge-train predecessor #2");
        edited.edited = true;
        let comments = vec![
            (
                PrNumber(2),
                vec![
                    comment(20, STRANGER, "@merge-train predecessor #1"),
                    comment(21, AUTHOR, "@merge-train predecessor #1"),
                ],
            ),
            (PrNumber(3), vec![edited]),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(1))],
            "the author's declaration is adopted; a stranger's is not, and an \
             edited comment cannot be attributed to anyone"
        );
    }

    /// A declaration the ledger says the bot already SETTLED is not
    /// evidence of anything — the user superseded it, or retracted it, and
    /// the bot ruled on it. Only one ABOVE that watermark can be a change
    /// that happened while the bot was away, and only that aborts.
    ///
    /// Without the watermark the ordinary sequence "declare, re-declare,
    /// retract" would leave the first comment sitting on the PR looking
    /// exactly like a gap extension, and every recovered train touching
    /// that PR would abort.
    #[test]
    fn only_declarations_above_the_settled_watermark_are_evidence() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
            child(3, AUTHOR, 2, PrState::Open),
        ];
        let run = |stray: u64| {
            let comments = vec![
                (
                    PrNumber(1),
                    vec![comment(
                        50,
                        BOT,
                        &format_status_comment(&record, "s").unwrap(),
                    )],
                ),
                (
                    PrNumber(2),
                    vec![
                        comment(10, AUTHOR, "@merge-train predecessor #1"),
                        ledger(11, 2, Some((1, 10)), 5),
                    ],
                ),
                (
                    PrNumber(3),
                    vec![
                        // A declaration into the train's stack, and a
                        // ledger saying #3 declares nothing — the user
                        // retracted it — settled through comment 20.
                        comment(stray, AUTHOR, "@merge-train predecessor #2"),
                        settled_ledger(21, 3, None, 6, Some(20)),
                    ],
                ),
            ];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &crawled,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                unfetchable: &HashSet::new(),
                comments_truncated: false,
                now: ts,
            });
            outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }))
        };
        assert!(
            !run(15),
            "a declaration the ledger settled is not evidence: the train resumes"
        );
        assert!(
            run(30),
            "one above the watermark appeared while the bot was away: abort"
        );
    }

    /// Only a comment the live path COULD have accepted is evidence: a
    /// stranger's declaration is refused live, so it must not abort a
    /// recovered train. An EDITED one still counts — its editor cannot be
    /// identified, so the conservative reading stands.
    #[test]
    fn only_a_declaration_live_could_have_accepted_is_evidence() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
            child(3, AUTHOR, 2, PrState::Open),
        ];
        let run = |stray: CommentData| {
            let comments = vec![
                (
                    PrNumber(1),
                    vec![comment(
                        50,
                        BOT,
                        &format_status_comment(&record, "s").unwrap(),
                    )],
                ),
                (
                    PrNumber(2),
                    vec![
                        comment(10, AUTHOR, "@merge-train predecessor #1"),
                        ledger(11, 2, Some((1, 10)), 5),
                    ],
                ),
                (PrNumber(3), vec![stray]),
            ];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &crawled,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                unfetchable: &HashSet::new(),
                comments_truncated: false,
                now: ts,
            });
            outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }))
        };
        assert!(
            !run(comment(30, STRANGER, "@merge-train predecessor #2")),
            "a stranger's declaration is refused live; it must not abort recovery"
        );
        assert!(
            run(comment(30, AUTHOR, "@merge-train predecessor #2")),
            "the author's does"
        );
        let mut edited = comment(30, STRANGER, "@merge-train predecessor #2");
        edited.edited = true;
        assert!(
            run(edited),
            "and an edited one does too: its editor cannot be identified"
        );
    }

    /// Duplicate ledgers are ordered by COMMENT ID, never by the sequence
    /// number they state: sequence numbers restart when the database is
    /// rebuilt, so an orphan from a previous life can carry a larger one
    /// than the ledger the store actually kept (Codex crawl review round
    /// 14, P1).
    #[test]
    fn duplicate_ledgers_are_ordered_by_comment_id() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                // The orphan: posted FIRST, and stating a sequence number
                // from a database that has since been rebuilt.
                settled_ledger(11, 2, None, 9_000, Some(10)),
                // What the store went on to write, at a fresh sequence.
                ledger(12, 2, Some((1, 10)), 3),
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(1))],
            "the later comment wins, whatever sequence numbers they claim"
        );
    }

    /// A crawl that could not read every PR's comments recovers no train:
    /// a member's ledger may simply be missing, and an `Idle` train — which
    /// has no frozen set to check — would squash its root without
    /// preparing the descendant whose edge went unread (Codex crawl review
    /// round 14, P1).
    #[test]
    fn a_truncated_crawl_recovers_no_train() {
        let ts = test_now();
        let record = train(PrNumber(1), ts);
        let crawled = vec![pr(1, AUTHOR, PrState::Open)];
        let comments = vec![(
            PrNumber(1),
            vec![comment(
                50,
                BOT,
                &format_status_comment(&record, "s").unwrap(),
            )],
        )];
        let run = |comments_truncated: bool| {
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &crawled,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                unfetchable: &HashSet::new(),
                comments_truncated,
                now: ts,
            });
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        assert_eq!(run(false), (vec![PrNumber(1)], false), "a whole crawl");
        assert_eq!(run(true), (vec![], true), "a truncated one aborts instead");
    }

    /// A record that cannot say what its stack was is never completed from
    /// its frontier alone: a three-deep train whose root and direct child
    /// have merged looks finished, and completing it would orphan the tail
    /// (Codex crawl review round 14, P2).
    #[test]
    fn a_record_without_a_known_stack_is_not_completed() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.current_pr = PrNumber(2);
        record.cascade_phase = CascadePhase::Reconciling {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
            squash_sha: Sha::parse("b".repeat(40)).unwrap(),
        };
        let merged = |n: u64| {
            pr(
                n,
                AUTHOR,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("c".repeat(40)).unwrap(),
                },
            )
        };
        let crawled = vec![merged(1), merged(2)];
        let comments = vec![(
            PrNumber(1),
            vec![comment(
                1,
                BOT,
                &format_status_comment(&record, "stale").unwrap(),
            )],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: ts,
        });
        assert!(
            !outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainCompleted { .. })),
            "a record with no recorded stack is not completed from its frontier"
        );
    }

    /// A declaration whose ledger write was lost still guides DISCOVERY:
    /// its target may be a closed-unmerged train root, and the crawl must
    /// fetch it to find that root's status comment — without granting the
    /// edge (Codex crawl review round 14, P2).
    #[test]
    fn an_unledgered_declarations_target_is_still_fetched() {
        let crawled = vec![child(2, AUTHOR, 77, PrState::Open)];
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(10, AUTHOR, "@merge-train predecessor #77"),
                // A ledger exists for the PR, so this is a recovery, not an
                // onboarding — and it does NOT claim the edge.
                settled_ledger(11, 2, None, 4, Some(5)),
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert!(
            declared(&outcome.events).is_empty(),
            "no ledger claims the edge, so no edge"
        );
        assert_eq!(
            outcome.referenced_uncrawled,
            vec![PrNumber(77)],
            "but its target is fetched, in case it is a closed root with a train"
        );
    }

    /// A train member the caller could not fetch (permanent 404) aborts the
    /// train instead of recovering it into an `UnknownPr` stall (Codex crawl
    /// review round 12).
    #[test]
    fn a_train_with_an_unfetchable_member_aborts() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let body = format_status_comment(&record, "mid").unwrap();
        // Only #1 is crawled; #2 (a frozen member) is unfetchable.
        let crawled = vec![pr(1, AUTHOR, PrState::Open)];
        let comments = vec![(PrNumber(1), vec![comment(1, BOT, &body)])];
        let unfetchable = HashSet::from([PrNumber(2)]);
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &unfetchable,
            comments_truncated: false,
            now: ts,
        });
        assert!(
            outcome.recovered_roots.is_empty(),
            "a train with an unreachable member must not be recovered"
        );
        assert!(
            outcome.events.iter().any(|e| matches!(
                e,
                StateEventPayload::TrainAborted {
                    root_pr: PrNumber(1),
                    ..
                }
            )),
            "it aborts cleanly instead"
        );
    }

    /// A bot status comment on its own root is adopted (id repaired), and
    /// an ACTIVE record is handed back for recovery marking.
    #[test]
    fn trains_recover_from_bot_status_comments() {
        let ts = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let mut record = train(PrNumber(1), ts);
        record.recovery_seq = 4;
        let body = format_status_comment(&record, "mid-flight").unwrap();
        let open = vec![pr(1, AUTHOR, PrState::Open)];
        let comments = vec![(PrNumber(1), vec![comment(8, BOT, &body)])];

        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert_eq!(outcome.recovered_roots, vec![PrNumber(1)]);
        let adopted = outcome
            .events
            .iter()
            .find_map(|e| match e {
                StateEventPayload::TrainRecordAdopted { record, .. } => Some(record),
                _ => None,
            })
            .expect("adopted");
        assert_eq!(adopted.recovery_seq, 4);
        assert_eq!(adopted.status_comment_id, Some(CommentId(8)));
    }

    /// Forged records — a user-authored status comment, or a bot record
    /// posted on a PR that is not its root — are ignored.
    #[test]
    fn forged_or_misplaced_records_are_ignored() {
        let ts = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let record = train(PrNumber(1), ts);
        let body = format_status_comment(&record, "s").unwrap();
        let open = vec![pr(1, AUTHOR, PrState::Open), pr(2, AUTHOR, PrState::Open)];
        let comments = vec![
            (PrNumber(1), vec![comment(1, STRANGER, &body)]),
            (PrNumber(2), vec![comment(2, BOT, &body)]),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert!(outcome.recovered_roots.is_empty());
        assert!(
            !outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainRecordAdopted { .. }))
        );
    }

    /// Several records on one root: the latest incarnation (started_at) at
    /// its highest recovery_seq wins. Completed records adopt (their
    /// removal semantics close out the map) but are not marked for
    /// recovery.
    #[test]
    fn the_latest_incarnation_wins_and_completed_is_not_recovered() {
        let t0 = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();
        let mut old = train(PrNumber(1), t0);
        old.recovery_seq = 50;
        let mut newer = train(PrNumber(1), t1);
        newer.recovery_seq = 2;
        newer.state = TrainState::Completed { ended_at: t1 };
        let open = vec![pr(1, AUTHOR, PrState::Open)];
        let comments = vec![(
            PrNumber(1),
            vec![
                comment(1, BOT, &format_status_comment(&old, "old").unwrap()),
                comment(2, BOT, &format_status_comment(&newer, "new").unwrap()),
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert!(outcome.recovered_roots.is_empty(), "completed: no recovery");
        let adopted = outcome
            .events
            .iter()
            .find_map(|e| match e {
                StateEventPayload::TrainRecordAdopted { record, .. } => Some(record),
                _ => None,
            })
            .expect("adopted");
        assert_eq!(adopted.started_at, t1, "the newer incarnation wins");
    }

    /// A ledger edge whose target the crawl has not fetched is RECORDED —
    /// the bot decided it, and the crawl no longer re-judges the decision —
    /// and its target is reported, so the caller fetches it and re-runs:
    /// that target may be a closed-unmerged train root, invisible to both
    /// list endpoints, whose status comment the crawl must see.
    #[test]
    fn a_ledger_edge_to_an_uncrawled_predecessor_is_recorded_and_reported() {
        let open = vec![child(2, AUTHOR, 77, PrState::Open)];
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(1, AUTHOR, "@merge-train predecessor #77"),
                ledger(2, 2, Some((77, 1)), 2),
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(77))],
            "the ledger's edge stands whether or not its target was crawled"
        );
        assert_eq!(
            outcome.referenced_uncrawled,
            vec![PrNumber(77)],
            "but #77 is still fetched — it may be a closed root carrying a train"
        );
    }

    /// Staleness: an ACTIVE record whose current PR and every frozen
    /// descendant are merged is a train that FINISHED but whose final
    /// (best-effort) comment update failed. It adopts as completed —
    /// no zombie resurrection redoing pushes (Codex crawl review, P2).
    #[test]
    fn a_finished_trains_stale_active_comment_is_not_resurrected() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.current_pr = PrNumber(2);
        record.cascade_phase = CascadePhase::Retargeting {
            // A record that SAYS what its stack was — completion from the
            // frontier alone is refused for one that does not, since it
            // cannot tell a two-deep train from a three-deep one whose
            // tail is still open.
            progress: DescendantProgress::with_known_stack(
                vec![PrNumber(2)],
                vec![PrNumber(1), PrNumber(2)],
            ),
            squash_sha: Sha::parse("b".repeat(40)).unwrap(),
        };
        let body = format_status_comment(&record, "stale").unwrap();
        // Everything merged: the train has nothing left to do.
        let merged = vec![
            pr(
                1,
                AUTHOR,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("c".repeat(40)).unwrap(),
                },
            ),
            pr(
                2,
                AUTHOR,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("d".repeat(40)).unwrap(),
                },
            ),
        ];
        let comments = vec![(PrNumber(1), vec![comment(1, BOT, &body)])];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &merged,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: ts,
        });
        assert!(outcome.recovered_roots.is_empty(), "no zombie");
        // Adopted as it is, then COMPLETED by a real event: application
        // removes it, and the store owes the stale comment its final word.
        assert!(
            outcome.events.iter().any(|e| matches!(
                e,
                StateEventPayload::TrainCompleted { root_pr } if *root_pr == PrNumber(1)
            )),
            "completed by event: {:?}",
            outcome.events
        );
    }

    /// A fan-out parent whose best-effort completion update failed leaves a
    /// stale ACTIVE comment. If the crawl adopted it as active it would
    /// resurrect the old train in parallel with the child roots it fanned
    /// into — two trains over the same PRs, risking a double squash. The
    /// parent is completed instead, detected by a child root's own record
    /// being born after it (Codex crawl review round 8, P2).
    #[test]
    fn a_stale_fan_out_parent_is_not_resurrected_alongside_its_children() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let t0 = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();

        // The parent #1, still ACTIVE in its stale comment, frozen [#2, #3].
        let mut parent = train(PrNumber(1), t0);
        parent.cascade_phase = CascadePhase::Reconciling {
            progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
            squash_sha: Sha::parse("a".repeat(40)).unwrap(),
        };
        // The children, each a fresh root whose record names the parent's
        // identity (root + started_at). Their own clocks are irrelevant:
        // child 3's is even EARLIER than the parent's (a backwards step).
        let lineage = Some(TrainLineage {
            root: PrNumber(1),
            started_at: t0,
        });
        let mut child2 = train(PrNumber(2), t1);
        child2.parent = lineage.clone();
        let mut child3 = train(PrNumber(3), t0 - chrono::Duration::hours(1));
        child3.parent = lineage;

        let open = vec![
            pr(1, AUTHOR, PrState::Open),
            pr(2, AUTHOR, PrState::Open),
            pr(3, AUTHOR, PrState::Open),
        ];
        let comments = vec![
            (
                PrNumber(1),
                vec![comment(
                    1,
                    BOT,
                    &format_status_comment(&parent, "stale").unwrap(),
                )],
            ),
            (
                PrNumber(2),
                vec![comment(
                    2,
                    BOT,
                    &format_status_comment(&child2, "c2").unwrap(),
                )],
            ),
            (
                PrNumber(3),
                vec![comment(
                    3,
                    BOT,
                    &format_status_comment(&child3, "c3").unwrap(),
                )],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: t1,
        });
        assert_eq!(
            outcome.recovered_roots,
            vec![PrNumber(2), PrNumber(3)],
            "only the child roots recover; the fanned-out parent does not"
        );
        assert!(
            outcome.events.iter().any(|e| matches!(
                e,
                StateEventPayload::TrainCompleted { root_pr } if *root_pr == PrNumber(1)
            )),
            "the parent is completed by event (application removes it)"
        );
    }

    /// A genuinely unfinished train — an unmerged member — IS recovered,
    /// and members the crawl did not see (closed-unmerged PRs) are handed
    /// back for individual fetching.
    #[test]
    fn unfinished_trains_recover_and_report_uncrawled_members() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
        };
        let body = format_status_comment(&record, "mid").unwrap();
        // PR 2 is open and still declares #1; PR 3 was closed unmerged
        // during the outage — the crawl's lists never see it.
        let open = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![
            (PrNumber(1), vec![comment(1, BOT, &body)]),
            (
                PrNumber(2),
                vec![
                    comment(0, AUTHOR, "@merge-train predecessor #1"),
                    ledger(1, 2, Some((1, 0)), 2),
                ],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: ts,
        });
        assert_eq!(outcome.recovered_roots, vec![PrNumber(1)]);
        assert_eq!(outcome.referenced_uncrawled, vec![PrNumber(3)]);
    }

    /// A stack EXTENDED during the DB-loss gap — a new PR declaring a stack
    /// member as its predecessor — must abort the adopted active train, not
    /// silently resume over changed topology. In live operation that
    /// declaration fires `topology_change_abort`; the crawl records it as
    /// baseline, so bootstrap must synthesize the same abort (Codex crawl
    /// review round 4, P1).
    #[test]
    fn a_stack_extended_during_the_gap_aborts_the_adopted_train() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let body = format_status_comment(&record, "mid").unwrap();
        // #2 is the original stack member; #3 was ADDED during the gap,
        // declaring #2 — extending the active train's stack.
        let open = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
            child(3, AUTHOR, 2, PrState::Open),
        ];
        let comments = vec![
            (PrNumber(1), vec![comment(1, BOT, &body)]),
            (
                PrNumber(2),
                vec![
                    comment(2, AUTHOR, "@merge-train predecessor #1"),
                    ledger(3, 2, Some((1, 2)), 2),
                ],
            ),
            (
                PrNumber(3),
                vec![
                    comment(3, AUTHOR, "@merge-train predecessor #2"),
                    ledger(4, 3, Some((2, 3)), 3),
                ],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: ts,
        });
        assert!(
            outcome.recovered_roots.is_empty(),
            "an extended stack must not silently resume"
        );
        assert!(
            outcome.events.iter().any(|e| matches!(
                e,
                StateEventPayload::TrainAborted {
                    root_pr: PrNumber(1),
                    ..
                }
            )),
            "the extended-stack train must abort"
        );
    }

    /// A NORMAL mid-cascade active train (its declared stack unchanged since
    /// freeze) recovers — the extension check must not false-abort it.
    #[test]
    fn an_unchanged_stack_still_recovers() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let body = format_status_comment(&record, "mid").unwrap();
        let open = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![
            (PrNumber(1), vec![comment(1, BOT, &body)]),
            (
                PrNumber(2),
                vec![
                    comment(2, AUTHOR, "@merge-train predecessor #1"),
                    ledger(3, 2, Some((1, 2)), 2),
                ],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: ts,
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(1))],
            "the unchanged declared edge is recorded"
        );
        assert_eq!(outcome.recovered_roots, vec![PrNumber(1)]);
        assert!(
            !outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }))
        );
    }

    /// Cache fills precede declarations and adoptions, so `apply_event`
    /// (which skips unknown PRs) accepts them.
    #[test]
    fn fills_precede_declarations_and_adoptions() {
        let ts = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let record = train(PrNumber(1), ts);
        let body = format_status_comment(&record, "s").unwrap();
        let open = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![
            (PrNumber(1), vec![comment(1, BOT, &body)]),
            (
                PrNumber(2),
                vec![
                    comment(2, AUTHOR, "@merge-train predecessor #1"),
                    ledger(3, 2, Some((1, 2)), 2),
                ],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        let first_fill = outcome
            .events
            .iter()
            .position(|e| matches!(e, StateEventPayload::PrOpened { .. }))
            .unwrap();
        let decl = outcome
            .events
            .iter()
            .position(|e| matches!(e, StateEventPayload::PredecessorDeclared { .. }))
            .unwrap();
        let adopt = outcome
            .events
            .iter()
            .position(|e| matches!(e, StateEventPayload::TrainRecordAdopted { .. }))
            .unwrap();
        assert!(first_fill < decl && first_fill < adopt);
    }

    /// A frozen member carrying a root record of ANOTHER lineage — a
    /// previous incarnation's child, or a record with no lineage at all —
    /// is not proof that this parent fanned out: kinship is lineage
    /// equality, not birth order. The parent stays active and recovers.
    #[test]
    fn a_member_record_of_another_lineage_does_not_complete_the_parent() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let t0 = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();
        let mut parent = train(PrNumber(1), t0);
        parent.cascade_phase = CascadePhase::Reconciling {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
            squash_sha: Sha::parse("a".repeat(40)).unwrap(),
        };
        // A stopped record on #2 born after the parent, from an older
        // incarnation of #1.
        let mut stranger = train(PrNumber(2), t1);
        stranger.parent = Some(TrainLineage {
            root: PrNumber(1),
            started_at: t0 - chrono::Duration::days(3),
        });
        stranger.state = TrainState::Stopped { ended_at: t1 };
        let open = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![
            (
                PrNumber(1),
                vec![comment(
                    1,
                    BOT,
                    &format_status_comment(&parent, "p").unwrap(),
                )],
            ),
            (
                PrNumber(2),
                vec![
                    comment(0, AUTHOR, "@merge-train predecessor #1"),
                    ledger(1, 2, Some((1, 0)), 1),
                    comment(2, BOT, &format_status_comment(&stranger, "s").unwrap()),
                ],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: t1,
        });
        assert_eq!(outcome.recovered_roots, vec![PrNumber(1)]);
    }

    /// An active train whose recorded default branch is not the one the
    /// crawl fetched aborts instead of resuming: its cascade steps name
    /// that branch. An unknown (empty) default branch is treated the same
    /// way; a matching one resumes.
    #[test]
    fn a_default_branch_change_during_the_gap_aborts_the_train() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let run = |recorded: &str| {
            let mut record = train(PrNumber(1), ts);
            record.default_branch = recorded.to_owned();
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(vec![PrNumber(2)]),
            };
            let open = vec![
                pr(1, AUTHOR, PrState::Open),
                child(2, AUTHOR, 1, PrState::Open),
            ];
            let comments = vec![
                (
                    PrNumber(1),
                    vec![comment(
                        1,
                        BOT,
                        &format_status_comment(&record, "s").unwrap(),
                    )],
                ),
                (
                    PrNumber(2),
                    vec![
                        comment(2, AUTHOR, "@merge-train predecessor #1"),
                        ledger(3, 2, Some((1, 2)), 2),
                    ],
                ),
            ];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &open,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                unfetchable: &HashSet::new(),
                comments_truncated: false,
                now: ts,
            });
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        assert_eq!(run("main"), (vec![PrNumber(1)], false));
        assert_eq!(run("trunk"), (vec![], true));
        assert_eq!(run(""), (vec![], true), "unknown is not trusted");
    }

    /// The root is merged BY HAND during the gap (frozen [#2], mid-phase),
    /// and a new PR is stacked onto the frozen member. The descendant walk
    /// from the root stops at merged PRs, so #4 is not in the closure —
    /// but #2 is in the frozen set, and the next cascade level would
    /// freeze and drive #4. An extension INTO the stack is an extension
    /// wherever the walk reaches (lost_db envelope finding).
    #[test]
    fn an_extension_onto_a_frozen_member_of_a_merged_root_still_aborts() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let merged = PrState::Merged {
            merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
        };
        let open = vec![
            pr(1, AUTHOR, merged),
            child(2, AUTHOR, 1, PrState::Open),
            child(4, AUTHOR, 2, PrState::Open),
        ];
        let comments = vec![
            (
                PrNumber(1),
                vec![comment(
                    50,
                    BOT,
                    &format_status_comment(&record, "s").unwrap(),
                )],
            ),
            (
                PrNumber(2),
                vec![
                    comment(10, AUTHOR, "@merge-train predecessor #1"),
                    ledger(11, 2, Some((1, 10)), 2),
                ],
            ),
            (
                PrNumber(4),
                vec![
                    comment(60, AUTHOR, "@merge-train predecessor #2"),
                    ledger(61, 4, Some((2, 60)), 4),
                ],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: ts,
        });
        assert_eq!(outcome.recovered_roots, vec![], "extended: not resumed");
        assert!(
            outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. })),
            "extended: aborted"
        );
    }

    /// A frozen member whose declaration is GONE (deleted during the gap —
    /// live would have aborted on the removal) or EDITED (unattributable,
    /// never recorded) has been unstacked: the frozen set is stale and the
    /// train aborts rather than merge a PR the user unstacked (lost_db
    /// envelope finding). A member whose declaration survives resumes.
    #[test]
    fn a_frozen_member_without_a_recorded_predecessor_aborts_the_train() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let run = |member_comments: Vec<CommentData>| {
            let mut record = train(PrNumber(1), ts);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(vec![PrNumber(2)]),
            };
            let open = vec![
                pr(1, AUTHOR, PrState::Open),
                child(2, AUTHOR, 1, PrState::Open),
            ];
            let comments = vec![
                (
                    PrNumber(1),
                    vec![comment(
                        50,
                        BOT,
                        &format_status_comment(&record, "s").unwrap(),
                    )],
                ),
                (PrNumber(2), member_comments),
            ];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &open,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                unfetchable: &HashSet::new(),
                comments_truncated: false,
                now: ts,
            });
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        let declaration = comment(10, AUTHOR, "@merge-train predecessor #1");
        let member_ledger = ledger(11, 2, Some((1, 10)), 3);
        assert_eq!(
            run(vec![declaration.clone(), member_ledger.clone()]),
            (vec![PrNumber(1)], false),
            "intact: resumes"
        );
        assert_eq!(run(vec![]), (vec![], true), "both gone: aborts");
        assert_eq!(
            run(vec![member_ledger.clone()]),
            (vec![], true),
            "the declaration the ledger names is gone: the ledger is revoked, and \
             the unstacked member aborts"
        );
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #9"),
                member_ledger,
            ]),
            (vec![], true),
            "it declares something else now: revoked too"
        );
        assert_eq!(
            run(vec![declaration]),
            (vec![], true),
            "no ledger claims the edge: the crawl does not invent one"
        );
    }

    /// Fan-out evidence is any trusted status record on a member naming
    /// this parent incarnation — not only the member's LATEST record: a
    /// child that was stopped and then started afresh has a newest record
    /// with no lineage, while its older record still proves the parent
    /// fanned out (Codex crawl review round 2, P1).
    #[test]
    fn an_older_child_record_still_proves_the_fan_out() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let t0 = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2026, 7, 3, 0, 0, 0).unwrap();
        let mut parent = train(PrNumber(1), t0);
        parent.cascade_phase = CascadePhase::Reconciling {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
            squash_sha: Sha::parse("a".repeat(40)).unwrap(),
        };
        // The fan-out child (older record, with lineage), stopped; then a
        // fresh train on #2 (newest record, no lineage).
        let mut stopped_child = train(PrNumber(2), t1);
        stopped_child.parent = Some(TrainLineage {
            root: PrNumber(1),
            started_at: t0,
        });
        stopped_child.state = TrainState::Stopped { ended_at: t1 };
        let fresh = train(PrNumber(2), t2);
        let open = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![
            (
                PrNumber(1),
                vec![comment(
                    1,
                    BOT,
                    &format_status_comment(&parent, "p").unwrap(),
                )],
            ),
            (
                PrNumber(2),
                vec![
                    comment(0, AUTHOR, "@merge-train predecessor #1"),
                    comment(2, BOT, &format_status_comment(&stopped_child, "c").unwrap()),
                    comment(3, BOT, &format_status_comment(&fresh, "f").unwrap()),
                ],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: t2,
        });
        assert_eq!(
            outcome.recovered_roots,
            vec![PrNumber(2)],
            "the fresh child train recovers; the fanned-out parent does not"
        );
        assert!(outcome.events.iter().any(|e| matches!(
            e,
            StateEventPayload::TrainCompleted { root_pr } if *root_pr == PrNumber(1)
        )));
    }

    /// `0` is the sentinel for an omitted account on either side; two
    /// sentinels are not a match (Codex crawl review round 2, P2).
    #[test]
    fn omitted_authors_never_match() {
        let crawled = vec![pr(1, 0, PrState::Open), child(2, 0, 1, PrState::Open)];
        let comments = vec![(
            PrNumber(2),
            vec![comment(5, 0, "@merge-train predecessor #1")],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: test_now(),
        });
        assert_eq!(declared(&outcome.events), vec![]);
    }

    /// The recorded stack answers "was this PR part of the train?" at any
    /// depth: a declaration created BEFORE the status comment but applied
    /// after the freeze (a delayed webhook) is a real late extension, and
    /// comment ids cannot see that — they prove creation order only
    /// (Codex crawl review round 4, P1). With the stack recorded, the id
    /// ordering is not consulted at all.
    #[test]
    fn a_low_id_declaration_outside_the_recorded_stack_is_an_extension() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let run = |known: Vec<PrNumber>| {
            let mut record = train(PrNumber(1), ts);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::with_known_stack(vec![PrNumber(2)], known),
            };
            record.watermark = Some(CommentId(50));
            let open = vec![
                pr(1, AUTHOR, PrState::Open),
                child(2, AUTHOR, 1, PrState::Open),
                child(3, AUTHOR, 2, PrState::Open),
            ];
            // #3's declaration (id 20) PREDATES the status comment (50),
            // so the watermark alone would call it baseline.
            let comments = vec![
                (
                    PrNumber(1),
                    vec![comment(
                        50,
                        BOT,
                        &format_status_comment(&record, "s").unwrap(),
                    )],
                ),
                (
                    PrNumber(2),
                    vec![
                        comment(10, AUTHOR, "@merge-train predecessor #1"),
                        ledger(11, 2, Some((1, 10)), 2),
                    ],
                ),
                (
                    PrNumber(3),
                    vec![
                        comment(20, AUTHOR, "@merge-train predecessor #2"),
                        ledger(21, 3, Some((2, 20)), 3),
                    ],
                ),
            ];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &open,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                unfetchable: &HashSet::new(),
                comments_truncated: false,
                now: ts,
            });
            outcome.recovered_roots
        };
        assert_eq!(
            run(vec![PrNumber(2), PrNumber(3)]),
            vec![PrNumber(1)],
            "#3 was in the stack the train recorded: baseline"
        );
        assert_eq!(
            run(vec![PrNumber(2)]),
            vec![],
            "#3 was NOT in the recorded stack: an extension, whatever its id"
        );
    }

    /// A stale comment from an EARLIER phase of a deep train must not be
    /// completed just because its own frozen level merged: the tail it
    /// recorded in `known_stack` is still open, and completing would
    /// abandon it (Codex crawl review round 6, P1).
    #[test]
    fn a_deep_trains_open_tail_prevents_synthesized_completion() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let merged = |n: u64| PrData {
            state: PrState::Merged {
                merge_commit_sha: Sha::parse("c".repeat(40)).unwrap(),
            },
            ..pr(n, AUTHOR, PrState::Open)
        };
        let run = |known: Vec<PrNumber>| {
            let mut record = train(PrNumber(1), ts);
            // The comment is stale at phase one: frozen [#2], but the
            // train knew #3 too.
            record.cascade_phase = CascadePhase::Reconciling {
                progress: DescendantProgress::with_known_stack(vec![PrNumber(2)], known),
                squash_sha: Sha::parse("a".repeat(40)).unwrap(),
            };
            let crawled = vec![
                merged(1),
                merged(2),
                child(3, AUTHOR, 2, PrState::Open), // the tail, still open
            ];
            let comments = vec![
                (
                    PrNumber(1),
                    vec![comment(
                        50,
                        BOT,
                        &format_status_comment(&record, "s").unwrap(),
                    )],
                ),
                (
                    PrNumber(3),
                    vec![
                        comment(10, AUTHOR, "@merge-train predecessor #2"),
                        ledger(11, 3, Some((2, 10)), 3),
                    ],
                ),
            ];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &crawled,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                unfetchable: &HashSet::new(),
                comments_truncated: false,
                now: ts,
            });
            outcome.events.iter().any(|e| {
                matches!(e, StateEventPayload::TrainCompleted { root_pr } if *root_pr == PrNumber(1))
            })
        };
        assert!(
            !run(vec![PrNumber(2), PrNumber(3)]),
            "the recorded tail #3 is open: not complete"
        );
        assert!(
            run(vec![PrNumber(2)]),
            "with only #2 ever known, everything it involved has merged"
        );
    }

    /// Incarnations are ordered by the globally monotonic COMMENT ID: a
    /// wall clock that steps backwards between two starts would otherwise
    /// select the older record and resurrect a dead train (Codex crawl
    /// review round 6, P2).
    #[test]
    fn the_newest_comment_wins_even_when_the_clock_stepped_back() {
        let ts = test_now();
        let earlier = ts - chrono::Duration::hours(1);
        // The NEWER incarnation (comment 20) started EARLIER by the clock.
        let mut old = train(PrNumber(1), ts);
        old.state = TrainState::Stopped { ended_at: ts };
        let new = train(PrNumber(1), earlier);
        let crawled = vec![pr(1, AUTHOR, PrState::Open)];
        let comments = vec![(
            PrNumber(1),
            vec![
                comment(10, BOT, &format_status_comment(&old, "old").unwrap()),
                comment(20, BOT, &format_status_comment(&new, "new").unwrap()),
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: ts,
        });
        assert_eq!(
            outcome.recovered_roots,
            vec![PrNumber(1)],
            "the newest comment's ACTIVE record is adopted, not the older stop"
        );
    }
}
