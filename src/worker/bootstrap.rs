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
//! - **Predecessor topology** — `@bot predecessor #N` comments, replayed in
//!   comment-id order through the SAME `validate_predecessor_declaration`
//!   the live command path runs, so the crawl reconstructs the edges the
//!   live STATE holds: only the PR author's non-edited declarations count,
//!   the first VALID one on a PR wins, a new comment restating the current
//!   predecessor transfers ownership, an invalid edge
//!   (closed/missing/mismatched/cycle) is dropped, and a MERGED-predecessor
//!   edge is KEPT (it was recorded while the predecessor was open and still
//!   gates `is_root`'s reconciliation proof).
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

use crate::commands::{Command, parse_command};
use crate::effects::PrData;
use crate::effects::github::CommentData;
use crate::persistence::event::{StateEvent, StateEventPayload};
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::state::RepoState;
use crate::state::descendants::collect_all_descendants;
use crate::state::validation::validate_predecessor_declaration;
use crate::status::Receipt;
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
    /// The triggering comment is TOMBSTONED — a receipt says the live path
    /// retracted or refused it. The crawl records nothing for it, and the
    /// delivery must not be handled either: handled fresh against today's
    /// topology it could be accepted after all, bypassing the crawl's
    /// extension check (Codex crawl review round 4, P1).
    pub trigger_tombstoned: bool,
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

/// Whether any EDITED declaration extends `record`'s stack: a non-member PR
/// declaring a stack member as its predecessor. Edited edges cannot be
/// recorded (their editor is unattributable — round 2) and a single-value
/// `RepoState` predecessor cannot hold more than one edited edge per PR, so
/// they are checked DIRECTLY here rather than folded into `topology`: EVERY
/// edited edge is tested, not just the first per PR (Codex crawl review
/// round 18). The train's known stack is its frozen set + primaries + the
/// RECORDED descendant closure; an edited edge whose target is inside it and
/// whose source is outside it is a possible extension, and recovery aborts
/// on any extension (owner ruling).
fn edited_extends(
    topology: &RepoState,
    record: &TrainRecord,
    edited_edges: &[(PrNumber, PrNumber)],
) -> bool {
    let Some((_, stack)) = known_stack(topology, record) else {
        return false;
    };
    edited_edges
        .iter()
        .any(|(source, target)| stack.contains(target) && !stack.contains(source))
}

/// The delivery that woke the crawl, when it is a comment event. Its comment
/// is live input the command handler processes right after the crawl, so
/// the crawl never PERSISTS it as a historical declaration (round 10); and
/// when it is an authorized retraction — the PR author deleted the comment,
/// or edited it to no longer declare — the crawl applies it as a
/// tombstone: the retracted comment may have owned the PR's declaration,
/// and with it gone the crawl would otherwise promote an older surviving
/// declaration the handler can no longer see as retracted (Codex crawl
/// review, P1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TriggerComment {
    pub id: CommentId,
    /// `Some(pr)` when the trigger retracts on `pr` (authorized: sender is
    /// the PR author; deleted, or edited to a non-declaring body).
    pub retraction: Option<PrNumber>,
    /// Whether the trigger's SENDER is the PR author. Historical edited
    /// comments are unattributable and stay conservative (possible
    /// extensions), but the trigger's editor is known: an unauthorized
    /// triggering edit must not abort a recovered train that the pipeline
    /// is about to reject anyway (Codex crawl review round 2, P1).
    pub authorized: bool,
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
    pub trigger: Option<TriggerComment>,
    /// Referenced PRs a permanent `GetPr` failure could not fetch.
    pub unfetchable: &'a HashSet<PrNumber>,
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
        trigger,
        unfetchable,
        now,
    } = *input;
    let skip_comment = trigger.map(|t| t.id);
    let mut trigger_tombstoned = false;
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
    let authors: HashMap<PrNumber, u64> = all_prs.iter().map(|p| (p.number, p.author_id)).collect();

    // The topology scratch: built from the cache fills, then grown one
    // validated declaration at a time so the crawl records EXACTLY the
    // predecessor edges the live handler would have persisted.
    let mut topology = replay_topology(default_branch, &events, now);

    // Candidate declarations: author-only (the live pipeline's rule), the
    // bot never declares. EDITED comments are refused outright — the API
    // reports only the original author, never the editor, so an edited body
    // cannot be attributed, and honoring it would reopen the
    // edit-impersonation hole the live path closes by authorizing the
    // SENDER (Codex crawl review round 2). Replayed in comment-id order so
    // each is validated against the edges accepted before it — giving
    // first-declaration-wins exactly as the live handler does over time.
    let mut candidates: Vec<(crate::types::CommentId, PrNumber, PrNumber)> = Vec::new();
    // Edited predecessor declarations are NOT recorded (their editor cannot
    // be attributed — round 2), but they are still a POSSIBLE topology
    // change: an edited comment that declares a frozen member as its
    // predecessor extends a recovered train, and the crawl aborts on any
    // extension (owner ruling, round 11). So they are kept here only to
    // enrich the stack-extension scratch below, never to persist an edge.
    let mut edited_edges: Vec<(PrNumber, PrNumber)> = Vec::new();
    // Receipts — the bot's durable record of its own decisions, which a
    // comment's mere survival cannot convey (`status::receipt`). A
    // RETRACTION receipt tombstones every declaration on its PR at or
    // below the RETRACTED comment's id: ids are globally monotonic, so that
    // range is the retracted declaration and everything it had superseded,
    // while a re-declaration posted later — possibly BEFORE the lagging
    // receipt itself — stands. A REJECTION receipt tombstones exactly the
    // comment live refused: re-validating it against the PRESENT could
    // accept it (its target merged since; the conflicting declaration was
    // retracted since; live saw the deliveries in a different order than
    // comment-id order). Both count only on the PR they name (the
    // status-comment misplacement gate).
    let mut retractions: HashMap<PrNumber, Vec<CommentId>> = HashMap::new();
    let mut rejected: HashSet<(PrNumber, CommentId)> = HashSet::new();
    // The triggering delivery's own authorized retraction has no receipt
    // yet (the worker posts one after this crawl); apply it directly — but
    // only where the retracted comment could have OWNED the declaration.
    // Live retracts only the owning comment, and ownership moves forward
    // in comment id, so a surviving declaration ABOVE the retracted id
    // proves the retracted one did not own it and nothing is tombstoned
    // (Codex crawl review round 3, P1). RESIDUAL, stop-shaped: a deletion
    // is bodyless, so an author deleting an UNRELATED comment posted after
    // their own declaration still tombstones it — the edge is dropped and
    // re-declaring restores it.
    let trigger_retraction = match trigger {
        Some(TriggerComment {
            id,
            retraction: Some(pr),
            ..
        }) => {
            let outranked = comments.iter().any(|(p, cs)| {
                *p == pr
                    && cs.iter().any(|c| {
                        c.id > id
                            && !c.edited
                            && authors.get(p) == Some(&c.author_id)
                            && c.author_id != 0
                            && matches!(
                                parse_command(&c.body, bot_name),
                                Some(Command::Predecessor(_))
                            )
                    })
            });
            (!outranked).then_some((pr, id))
        }
        _ => None,
    };
    if let Some((pr, id)) = trigger_retraction {
        retractions.entry(pr).or_default().push(id);
    }
    for (pr, pr_comments) in comments {
        for comment in pr_comments {
            // The bot never declares — but its receipts tombstone.
            if comment.author_id == bot_user_id {
                match crate::status::parse_receipt(&comment.body) {
                    Some(receipt) if receipt.pr() != *pr => {}
                    Some(Receipt::Retraction { pr, retracted }) => {
                        retractions.entry(pr).or_default().push(retracted);
                    }
                    Some(Receipt::Rejection { pr, rejected: id }) => {
                        rejected.insert((pr, id));
                    }
                    None => {}
                }
                continue;
            }
            let Some(Command::Predecessor(target)) = parse_command(&comment.body, bot_name) else {
                continue;
            };
            if comment.edited {
                // The triggering edit's editor IS known: unauthorized, it
                // is about to be rejected by the pipeline and must not
                // shape the topology at all.
                if trigger.is_some_and(|t| t.id == comment.id && !t.authorized) {
                    continue;
                }
                // An edited body could have been added by ANY editor with
                // rights (live authorizes the editor via `sender_id`, which
                // the crawl lacks), so the ORIGINAL author is irrelevant:
                // count it as a possible extension regardless (Codex crawl
                // review round 15). Never recorded (round 2).
                tracing::warn!(
                    %pr, comment = %comment.id,
                    "not recording an EDITED predecessor declaration (the editor \
                     cannot be verified); it still counts as a possible extension"
                );
                edited_edges.push((*pr, target));
                continue;
            }
            // A recorded (non-edited) declaration must be the PR author's —
            // a REAL author: `0` is the deny-safe sentinel for an omitted
            // account on either side, and two sentinels do not match
            // (Codex crawl review round 2, P2).
            if comment.author_id == 0 || authors.get(pr) != Some(&comment.author_id) {
                continue;
            }
            candidates.push((comment.id, *pr, target));
        }
    }
    candidates.sort_by_key(|(id, _, _)| *id);

    // Every declaration target not in the crawl is referenced-but-uncrawled:
    // the caller fetches it and re-runs, because a target that is a
    // closed-unmerged train root (invisible to both list endpoints) carries
    // a status comment the crawl must see to adopt+abort its train (Codex
    // crawl review round 7). The declaration edge itself may still be
    // dropped once fetched (a closed predecessor fails validation) — the
    // fetch is for train discovery, not the edge. EDITED declarations count
    // too: their EDGE stays untrusted (never recorded), but their target
    // may be the closed root whose train must be discovered, so it is still
    // reported for fetching (Codex crawl review — edited fixpoint discovery).
    // A declaration the live path already killed — retracted, or refused
    // with a rejection receipt — is not evidence of anything, so its target
    // is not worth a fetch (Codex crawl review round 3, P1: every
    // predecessor-shaped comment used to cost one `GetPr`).
    let tombstoned = |pr: &PrNumber, id: crate::types::CommentId| {
        retractions
            .get(pr)
            .is_some_and(|anchors| anchors.iter().any(|anchor| *anchor >= id))
            || rejected.contains(&(*pr, id))
    };
    let crawled_numbers: HashSet<PrNumber> = all_prs.iter().map(|p| p.number).collect();
    let mut referenced_uncrawled: Vec<PrNumber> = Vec::new();
    for target in candidates
        .iter()
        .filter(|(id, pr, _)| !tombstoned(pr, *id))
        .map(|(_, _, target)| target)
        .chain(edited_edges.iter().map(|(_, target)| target))
    {
        if !crawled_numbers.contains(target) && !referenced_uncrawled.contains(target) {
            referenced_uncrawled.push(*target);
        }
    }

    // Reconstruct predecessor edges as the LIVE STATE holds them (Codex
    // crawl review rounds 5, 9), running each candidate through the same
    // `validate_predecessor_declaration` the live command path does:
    //
    // - an invalid edge (closed/missing/mismatched predecessor, cycle) is
    //   dropped — recording it would wedge `is_root` or fabricate a bogus
    //   stack extension (round 5);
    // - a re-statement of a PR's CURRENT predecessor from a new comment
    //   transfers ownership (the live handler does this unconditionally):
    //   record it so `predecessor_comment_id` tracks the latest comment,
    //   or a later retraction targets the wrong one (round 9);
    // - a MERGED predecessor is KEPT, not treated as a late addition: the
    //   edge was recorded while the predecessor was open and still lives in
    //   the state, and `is_root`'s reconciliation-proof gate depends on it
    //   — dropping it would let a mid-cascade descendant merge as a plain
    //   root, bypassing that proof (round 9, P1).
    // Replayed to a FIXPOINT, not in one pass: a declaration whose target
    // was not yet in a stack when its own id came round can become valid
    // once a LATER comment re-declares that target (retract #2, then
    // re-declare it — #3's older edge is one live keeps and a single
    // id-ordered pass drops). Anything live actually REJECTED carries a
    // rejection receipt and is tombstoned above, so no pass can resurrect
    // it (Codex harness review round 7, P1). Each pass records at least
    // one edge or the loop ends, and edges are bounded by the PR count.
    let mut pending_candidates = candidates;
    while !pending_candidates.is_empty() {
        let mut retry = Vec::new();
        let mut recorded_any = false;
        for (comment_id, pr, predecessor) in std::mem::take(&mut pending_candidates) {
            // A retraction anchored at or above this declaration tombstones
            // it: the live path retracted the anchor's declaration, and
            // everything the anchor had superseded died with it. (A
            // declaration with a HIGHER id than every anchor is a
            // re-declaration and stands.) A rejection names it exactly.
            if retractions
                .get(&pr)
                .is_some_and(|anchors| anchors.iter().any(|anchor| *anchor >= comment_id))
                || rejected.contains(&(pr, comment_id))
            {
                if skip_comment == Some(comment_id) {
                    trigger_tombstoned = true;
                }
                continue;
            }
            let recordable = match topology.prs.get(&pr) {
                None => false,
                // Same predecessor already declared: a new comment restating it
                // is an ownership transfer; the topology is unchanged.
                Some(cached) if cached.predecessor == Some(predecessor) => true,
                Some(cached) => {
                    match validate_predecessor_declaration(
                        cached,
                        predecessor,
                        &topology.prs,
                        default_branch,
                    ) {
                        // Validation must skip the base-match check for a
                        // MERGED predecessor (a legitimate mid-cascade
                        // descendant was retargeted to the default branch) —
                        // but live only ever HOLDS a merged-predecessor edge
                        // that was recorded while the predecessor was OPEN,
                        // when the declarer's base matched its branch, and the
                        // cascade's own retarget afterwards produces only the
                        // default branch. Any other base is an edge live never
                        // held: a declaration live REJECTED as mismatched
                        // whose target has since merged. Recording it would
                        // fabricate topology and SHADOW the PR's real later
                        // declaration as already-declared, stranding the stack
                        // tail (lost_db differential finding).
                        Ok(()) => {
                            let implausible_merged_base =
                                topology.prs.get(&predecessor).is_some_and(|p| {
                                    p.state.is_merged()
                                        && cached.base_ref != p.head_ref
                                        && cached.base_ref != default_branch
                                });
                            if implausible_merged_base {
                                tracing::warn!(
                                    %pr, %predecessor,
                                    "dropping a crawled declaration onto a merged \
                                     predecessor whose branch never matched the \
                                     declarer's base (live rejected it)"
                                );
                            }
                            !implausible_merged_base
                        }
                        Err(e) => {
                            tracing::warn!(
                                %pr, %predecessor, error = %e,
                                "dropping an invalid crawled predecessor declaration"
                            );
                            false
                        }
                    }
                }
            };
            if !recordable {
                // Only "the target is not (yet) in a stack" can be fixed by a
                // later declaration; every other refusal is final.
                if topology
                    .prs
                    .get(&pr)
                    .is_some_and(|c| c.predecessor.is_none())
                    && topology
                        .prs
                        .get(&predecessor)
                        .is_some_and(|p| p.predecessor.is_none() && p.base_ref != default_branch)
                {
                    retry.push((comment_id, pr, predecessor));
                }
                continue;
            }
            let is_trigger = skip_comment == Some(comment_id);
            // The TRIGGERING delivery's own comment is live input the handler
            // processes next, treated as a FRESH declaration — so a MERGED
            // predecessor is a late addition the handler records nothing for
            // (LateAddition). It must not create a scratch edge either: a
            // phantom late-addition edge would shadow a genuinely-later valid
            // declaration on the same PR as `AlreadyHasPredecessor` and drop
            // it, and after a DB loss no webhook replays to recover it (Codex
            // crawl review round 12). An OPEN-predecessor triggering edge IS
            // applied to the scratch — the handler will record it, and
            // `stack_extended` needs it to catch an extension (round 11).
            if is_trigger
                && topology
                    .prs
                    .get(&predecessor)
                    .is_some_and(|p| p.state.is_merged())
            {
                continue;
            }
            let decl = StateEventPayload::PredecessorDeclared {
                pr,
                predecessor,
                comment_id,
            };
            // Apply to the topology scratch — the stack-extension check and
            // validation of later candidates must see this edge.
            topology.apply_event(&StateEvent {
                seq: events.len() as u64,
                ts: now,
                payload: decl.clone(),
            });
            // But do NOT persist the triggering comment: it is live input the
            // command handler processes next in the same delivery, and
            // pre-recording it would suppress the handler's late-addition
            // answer (round 10). It still shaped the topology above, so a
            // triggering comment that extends an active train is caught by
            // `stack_extended` and aborted (round 11) — the abort, not the
            // edge, is what recovery owes.
            if !is_trigger {
                events.push(decl);
            }
            recorded_any = true;
        }
        if !recorded_any {
            break;
        }
        pending_candidates = retry;
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
    let _ = &trigger_tombstoned;

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
        let all_members_merged = record.cascade_phase.progress().is_some()
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
                || edited_extends(&topology, &record, &edited_edges));
        // A frozen member with NO recorded predecessor was unstacked during
        // the gap: its declaration was deleted (live aborts on that removal
        // — `topology_change_abort` — and the crawl cannot see the
        // deletion) or edited (unattributable, never recorded). Either way
        // the frozen set is stale, and driving it would merge a PR the user
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
        trigger_tombstoned,
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

    /// Declarations obey the live pipeline's authorization: only the PR
    /// author's comments declare, and the FIRST valid declaration wins
    /// (a later distinct declaration rejects as already-declared, exactly
    /// like the live handler).
    #[test]
    fn declarations_are_author_only_and_first_valid_wins() {
        let open = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(1, STRANGER, "@merge-train predecessor #1"),
                comment(2, AUTHOR, "@merge-train predecessor #1"),
                comment(3, AUTHOR, "@merge-train predecessor #9"),
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(1))],
            "the stranger's comment is ignored; the author's first valid one wins"
        );
    }

    /// A retraction RECEIPT (the bot's tombstone, posted when a live
    /// retraction is applied) kills every declaration on that PR whose id
    /// is at or below its ANCHOR — the retracted comment's id — replaying
    /// the live declare/retract history in utterance order. A
    /// re-declaration above the anchor stands even when the lagging
    /// receipt's own id is higher (the backlog race), and a receipt
    /// sitting on a DIFFERENT PR than it names is forged/misplaced and
    /// ignored (the status-comment trust gate).
    #[test]
    fn a_retraction_receipt_tombstones_earlier_declarations() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let bot_receipt = |id: u64, named: u64, anchor: u64| CommentData {
            id: CommentId(id),
            author_id: BOT,
            body: crate::status::format_retraction_receipt(
                PrNumber(named),
                CommentId(anchor),
                Some(PrNumber(1)),
            ),
            edited: false,
        };
        let run = |comments: Vec<(PrNumber, Vec<CommentData>)>| {
            declared(
                &crawl_events(&CrawlInput {
                    default_branch: "main",
                    crawled_prs: &crawled,
                    comments: &comments,
                    bot_name: "merge-train",
                    bot_user_id: BOT,
                    trigger: None,
                    unfetchable: &HashSet::new(),
                    now: test_now(),
                })
                .events,
            )
        };

        // Declaration then receipt: the edge is retracted, not resurrected.
        let edges = run(vec![(
            PrNumber(2),
            vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                bot_receipt(11, 2, 10),
            ],
        )]);
        assert_eq!(
            edges,
            vec![],
            "a receipt tombstones the anchored declaration"
        );

        // A re-declaration ABOVE the anchor stands — even though the
        // lagging receipt's own comment id is the highest of all (the
        // user re-declared while the deletion sat in the backlog).
        let edges = run(vec![(
            PrNumber(2),
            vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                comment(12, AUTHOR, "@merge-train predecessor #1"),
                bot_receipt(13, 2, 10),
            ],
        )]);
        assert_eq!(
            edges,
            vec![(PrNumber(2), PrNumber(1))],
            "a re-declaration above the anchor survives a lagging receipt"
        );

        // A receipt on the WRONG PR is ignored.
        let edges = run(vec![
            (
                PrNumber(2),
                vec![comment(10, AUTHOR, "@merge-train predecessor #1")],
            ),
            (PrNumber(1), vec![bot_receipt(11, 2, 10)]),
        ]);
        assert_eq!(
            edges,
            vec![(PrNumber(2), PrNumber(1))],
            "a misplaced receipt must not tombstone"
        );
    }

    /// A declaration REJECTED live (base mismatch against an OPEN
    /// predecessor) must stay rejected after that predecessor MERGES. Live
    /// only ever holds a merged-predecessor edge recorded while the
    /// predecessor was open — when the declarer's base matched its branch —
    /// and the cascade's own retarget afterwards moves the base to the
    /// default branch; any other base is an edge live never held.
    /// Fabricating it would also SHADOW the PR's real declaration as
    /// already-declared and misroute the stack (found by the lost_db
    /// differential property: junk 3→#1 fabricated after #1 merged,
    /// shadowing the honest 3→#2 and stranding #3 unmerged).
    #[test]
    fn a_rejected_declaration_is_not_fabricated_once_its_target_merges() {
        let merged_one = PrData {
            state: PrState::Merged {
                merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
            },
            ..pr(1, AUTHOR, PrState::Open)
        };
        let crawled = vec![
            merged_one,
            child(2, AUTHOR, 1, PrState::Open),
            child(3, AUTHOR, 2, PrState::Open),
        ];
        let comments = vec![
            (
                PrNumber(3),
                vec![
                    // Lowest id, so it replays FIRST: 3 → #1, whose base
                    // "pr-2" matched neither "pr-1" nor "main" — live
                    // rejected it while #1 was open.
                    comment(1, AUTHOR, "@merge-train predecessor #1"),
                    comment(3, AUTHOR, "@merge-train predecessor #2"),
                ],
            ),
            (
                PrNumber(2),
                vec![comment(2, AUTHOR, "@merge-train predecessor #1")],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        let edges = declared(&outcome.events);
        assert!(
            !edges.contains(&(PrNumber(3), PrNumber(1))),
            "the live-rejected declaration must not be fabricated: {edges:?}"
        );
        assert!(
            edges.contains(&(PrNumber(3), PrNumber(2))),
            "the honest declaration must not be shadowed: {edges:?}"
        );
        assert!(
            edges.contains(&(PrNumber(2), PrNumber(1))),
            "the legitimate merged-predecessor edge is kept (round 9): {edges:?}"
        );
    }

    /// The crawl runs the SAME validation the live command path does and
    /// drops the edges the handler would reject: a closed predecessor, a
    /// base mismatch (recording either would wedge `is_root` or fabricate a
    /// bogus stack extension — Codex crawl review round 5).
    #[test]
    fn invalid_declarations_are_dropped() {
        // #1 closed; #2 -> #1 rejected. #3 -> #4 with base "main" != #4 head
        // (mismatch) rejected.
        let mut closed_one = pr(1, AUTHOR, PrState::Closed);
        closed_one.head_ref = "pr-1".to_owned();
        let crawled = vec![
            closed_one,
            child(2, AUTHOR, 1, PrState::Open),
            pr(3, AUTHOR, PrState::Open), // base "main", not #4's head → mismatch
            pr(4, AUTHOR, PrState::Open),
        ];
        let comments = vec![
            (
                PrNumber(2),
                vec![comment(1, AUTHOR, "@merge-train predecessor #1")],
            ),
            (
                PrNumber(3),
                vec![comment(2, AUTHOR, "@merge-train predecessor #4")],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert!(
            declared(&outcome.events).is_empty(),
            "every rejected declaration is dropped, got {:?}",
            declared(&outcome.events)
        );
    }

    /// The triggering delivery's own comment is not RECORDED by the crawl,
    /// so the live command handler (which runs next in the same delivery)
    /// can answer it — e.g. emit `LateAddition` for a merged predecessor —
    /// instead of finding it already recorded (Codex crawl review round 10).
    #[test]
    fn the_triggering_comment_is_not_recorded_by_the_crawl() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![comment(7, AUTHOR, "@merge-train predecessor #1")],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: Some(TriggerComment {
                id: CommentId(7),
                retraction: None,
                authorized: true,
            }),
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert!(
            declared(&outcome.events).is_empty(),
            "the delivery's own comment is left for the live handler to record"
        );
    }

    /// An EDITED extension declaration must also abort a recovered train.
    /// The edge is not recorded (its editor cannot be attributed — round
    /// 2), but it is a possible topology change, and recovery aborts on any
    /// extension (owner ruling). Without this, neither the crawl (edited
    /// comments skipped) nor the later `topology_change_abort` (reads
    /// pre-declaration state) catches it (Codex crawl review, P1).
    #[test]
    fn an_edited_extension_declaration_aborts_the_recovered_train() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let body = format_status_comment(&record, "mid").unwrap();
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
            child(3, AUTHOR, 2, PrState::Open),
        ];
        // #2 -> #1 recorded (the existing frozen stack); #3 -> #2 via an
        // EDITED comment (the extension) — not recorded, but must abort.
        let mut edited = comment(9, AUTHOR, "@merge-train predecessor #2");
        edited.edited = true;
        let comments = vec![
            (PrNumber(1), vec![comment(1, BOT, &body)]),
            (
                PrNumber(2),
                vec![comment(2, AUTHOR, "@merge-train predecessor #1")],
            ),
            (PrNumber(3), vec![edited]),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: ts,
        });
        assert!(
            outcome.recovered_roots.is_empty(),
            "the edited extension must not silently resume"
        );
        assert!(
            outcome.events.iter().any(|e| matches!(
                e,
                StateEventPayload::TrainAborted {
                    root_pr: PrNumber(1),
                    ..
                }
            )),
            "an edited extension must abort the recovered train"
        );
        // The edited edge itself is never recorded.
        assert!(
            !declared(&outcome.events).contains(&(PrNumber(3), PrNumber(2))),
            "the edited declaration is not persisted"
        );
    }

    /// Every edited declaration is checked for an extension, not just the
    /// first per PR: if #3 has an earlier edited comment pointing elsewhere
    /// and a LATER one declaring frozen #2, the extension must still abort
    /// the recovered train (Codex crawl review round 18, P2).
    #[test]
    fn a_later_edited_extension_after_another_edit_still_aborts() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let body = format_status_comment(&record, "mid").unwrap();
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
            pr(3, AUTHOR, PrState::Open),
        ];
        // #3's FIRST edited comment points at #99 (not a member); the LATER
        // one declares frozen #2 (the extension).
        let mut first = comment(8, AUTHOR, "@merge-train predecessor #99");
        first.edited = true;
        let mut second = comment(9, AUTHOR, "@merge-train predecessor #2");
        second.edited = true;
        let comments = vec![
            (PrNumber(1), vec![comment(1, BOT, &body)]),
            (
                PrNumber(2),
                vec![comment(2, AUTHOR, "@merge-train predecessor #1")],
            ),
            (PrNumber(3), vec![first, second]),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: ts,
        });
        assert!(
            outcome.recovered_roots.is_empty()
                && outcome.events.iter().any(|e| matches!(
                    e,
                    StateEventPayload::TrainAborted {
                        root_pr: PrNumber(1),
                        ..
                    }
                )),
            "a later edited extension must abort even after an earlier edit"
        );
    }

    /// An edited declaration's target is reported for fixpoint fetching
    /// even though its edge is never recorded: the target may be a
    /// closed-unmerged train root reachable only through the edited
    /// declaration, and it must be fetched so its train is discovered and
    /// adopted/aborted rather than orphaned (Codex crawl review round 16,
    /// P2 — edited fixpoint discovery).
    #[test]
    fn an_edited_declaration_target_is_reported_for_fetching() {
        let crawled = vec![child(2, AUTHOR, 77, PrState::Open)];
        let mut edited = comment(1, AUTHOR, "@merge-train predecessor #77");
        edited.edited = true;
        let comments = vec![(PrNumber(2), vec![edited])];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert!(
            outcome.referenced_uncrawled.contains(&PrNumber(77)),
            "the edited declaration's uncrawled target must be fetched, got {:?}",
            outcome.referenced_uncrawled
        );
        assert!(
            declared(&outcome.events).is_empty(),
            "the edited edge itself is still not recorded"
        );
    }

    /// An edited extension counts even when the comment was ORIGINALLY
    /// written by someone other than the PR author: edited bodies are
    /// authorized live by the editor (`sender_id`), which the crawl cannot
    /// see, so the original author is irrelevant to the conservative
    /// extension check (Codex crawl review round 15, P2).
    #[test]
    fn an_edited_extension_by_a_non_author_editor_aborts() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let body = format_status_comment(&record, "mid").unwrap();
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
            child(3, AUTHOR, 2, PrState::Open),
        ];
        // #3's extension comment was originally authored by STRANGER but
        // edited (by whoever has rights) to declare #2.
        let mut edited = comment(9, STRANGER, "@merge-train predecessor #2");
        edited.edited = true;
        let comments = vec![
            (PrNumber(1), vec![comment(1, BOT, &body)]),
            (
                PrNumber(2),
                vec![comment(2, AUTHOR, "@merge-train predecessor #1")],
            ),
            (PrNumber(3), vec![edited]),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: ts,
        });
        assert!(
            outcome.recovered_roots.is_empty()
                && outcome.events.iter().any(|e| matches!(
                    e,
                    StateEventPayload::TrainAborted {
                        root_pr: PrNumber(1),
                        ..
                    }
                )),
            "an edited extension aborts regardless of the comment's original author"
        );
    }

    /// But the triggering comment must still be SEEN for topology analysis:
    /// if it extends an active recovered train (new PR #3 declares frozen
    /// #2), the crawl's stack-extension check must fire and abort the train,
    /// even though the edge itself is left for the handler to record. The
    /// round-10 skip must not defeat the round-4 abort (Codex crawl review
    /// round 11, P1).
    #[test]
    fn a_triggering_extension_comment_still_aborts_the_train() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
        };
        let body = format_status_comment(&record, "mid").unwrap();
        // #3 is a new PR declaring frozen #2 — the triggering comment (id 7).
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
            child(3, AUTHOR, 2, PrState::Open),
        ];
        let comments = vec![
            (PrNumber(1), vec![comment(1, BOT, &body)]),
            (
                PrNumber(2),
                vec![comment(2, AUTHOR, "@merge-train predecessor #1")],
            ),
            (
                PrNumber(3),
                vec![comment(7, AUTHOR, "@merge-train predecessor #2")],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: Some(TriggerComment {
                id: CommentId(7),
                retraction: None,
                authorized: true,
            }),
            unfetchable: &HashSet::new(),
            now: ts,
        });
        assert!(
            outcome.recovered_roots.is_empty(),
            "the extended train must not silently resume"
        );
        assert!(
            outcome.events.iter().any(|e| matches!(
                e,
                StateEventPayload::TrainAborted {
                    root_pr: PrNumber(1),
                    ..
                }
            )),
            "the extension via the triggering comment must still abort the train"
        );
        // The edge itself is NOT recorded by the crawl — the handler owns it.
        assert!(
            !declared(&outcome.events).contains(&(PrNumber(3), PrNumber(2))),
            "the triggering edge is left for the handler to record"
        );
    }

    /// A skipped (triggering) merged-predecessor comment must not leave a
    /// phantom scratch edge that shadows a genuinely-later valid
    /// declaration on the same PR (Codex crawl review round 12). #2's old
    /// triggering comment declares merged #1 (a late addition the handler
    /// records nothing for); a later comment declares open #3. The later,
    /// real edge must survive.
    #[test]
    fn a_skipped_late_addition_does_not_shadow_a_later_declaration() {
        let crawled = vec![
            pr(
                1,
                AUTHOR,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
                },
            ),
            pr(3, AUTHOR, PrState::Open),
            child(2, AUTHOR, 3, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(5, AUTHOR, "@merge-train predecessor #1"), // triggering, merged
                comment(9, AUTHOR, "@merge-train predecessor #3"), // later, real
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: Some(TriggerComment {
                id: CommentId(5),
                retraction: None,
                authorized: true,
            }),
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(3))],
            "the later real declaration must not be shadowed by the skipped phantom"
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
            trigger: None,
            unfetchable: &unfetchable,
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

    /// A declaration to a MERGED predecessor is KEPT, not treated as a late
    /// addition: the edge was recorded while the predecessor was open and
    /// still lives in the state, and `is_root`'s reconciliation-proof gate
    /// depends on it — dropping it lets a mid-cascade descendant merge as a
    /// plain root, bypassing that proof (Codex crawl review round 9, P1).
    #[test]
    fn a_declaration_to_a_merged_predecessor_is_kept() {
        let crawled = vec![
            pr(
                1,
                AUTHOR,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
                },
            ),
            // #2 retargeted to main after #1 merged, still declaring #1.
            pr(2, AUTHOR, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![comment(1, AUTHOR, "@merge-train predecessor #1")],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(1))],
            "the historical edge to the merged predecessor is preserved"
        );
    }

    /// A new comment restating a PR's CURRENT predecessor transfers
    /// ownership (the live handler does this): the recorded declaration
    /// carries the LATER comment id, so a later retract/edit targets the
    /// right comment (Codex crawl review round 9, P2).
    #[test]
    fn a_same_predecessor_restatement_transfers_ownership() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(5, AUTHOR, "@merge-train predecessor #1"),
                comment(9, AUTHOR, "@merge-train predecessor #1"),
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        let owning = outcome.events.iter().rev().find_map(|e| match e {
            StateEventPayload::PredecessorDeclared {
                pr: PrNumber(2),
                comment_id,
                ..
            } => Some(*comment_id),
            _ => None,
        });
        assert_eq!(
            owning,
            Some(CommentId(9)),
            "the later comment owns the declaration"
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
            trigger: None,
            unfetchable: &HashSet::new(),
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
            trigger: None,
            unfetchable: &HashSet::new(),
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
            trigger: None,
            unfetchable: &HashSet::new(),
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

    /// An EDITED comment cannot be attributed to its author (GitHub reports
    /// only the original author, not the editor), so the crawl must not
    /// honor its declaration — the edit-impersonation hole the live path
    /// closes by checking `sender_id` (Codex crawl review round 2, P2).
    #[test]
    fn edited_declaration_comments_are_not_trusted() {
        // #2 is a valid child of #1, so only the edit flag can drop it.
        let open = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let mut edited = comment(1, AUTHOR, "@merge-train predecessor #1");
        edited.edited = true;
        let comments = vec![(PrNumber(2), vec![edited])];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert!(
            declared(&outcome.events).is_empty(),
            "an edited body has an unknowable author; fail closed"
        );
    }

    /// A declaration naming a predecessor outside the crawl is dropped:
    /// `ListOpenPrs` returns every open PR, so an unrecorded predecessor is
    /// necessarily closed or merged (or a typo) — not a valid edge — and
    /// validation rejects it, exactly as the live path would (Codex crawl
    /// review round 5; supersedes the earlier "fetch the target" answer).
    #[test]
    fn a_declaration_to_an_uncrawled_predecessor_is_dropped_but_reported() {
        let open = vec![child(2, AUTHOR, 77, PrState::Open)];
        let comments = vec![(
            PrNumber(2),
            vec![comment(1, AUTHOR, "@merge-train predecessor #77")],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert!(
            declared(&outcome.events).is_empty(),
            "a declaration to an uncrawled predecessor is dropped (unvalidated)"
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
            progress: DescendantProgress::new(vec![PrNumber(2)]),
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
            trigger: None,
            unfetchable: &HashSet::new(),
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
            trigger: None,
            unfetchable: &HashSet::new(),
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
                vec![comment(0, AUTHOR, "@merge-train predecessor #1")],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
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
                vec![comment(2, AUTHOR, "@merge-train predecessor #1")],
            ),
            (
                PrNumber(3),
                vec![comment(3, AUTHOR, "@merge-train predecessor #2")],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
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
                vec![comment(2, AUTHOR, "@merge-train predecessor #1")],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
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
                vec![comment(2, AUTHOR, "@merge-train predecessor #1")],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
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
            trigger: None,
            unfetchable: &HashSet::new(),
            now: t1,
        });
        assert_eq!(outcome.recovered_roots, vec![PrNumber(1)]);
    }

    /// A REJECTION receipt tombstones exactly the comment it names: live
    /// refused that declaration, and re-validating it against the present
    /// (where it may now pass — here the target is open and matching)
    /// must not accept it. Its neighbours are untouched.
    #[test]
    fn a_rejection_receipt_tombstones_exactly_the_named_comment() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
            child(3, AUTHOR, 2, PrState::Open),
        ];
        let rejection = |id: u64, named: u64, rejected: u64| CommentData {
            id: CommentId(id),
            author_id: BOT,
            body: crate::status::format_rejection_receipt(
                PrNumber(named),
                CommentId(rejected),
                "Cannot declare predecessor: not in a stack.",
            ),
            edited: false,
        };
        // Live saw #3's declaration (id 11) BEFORE #2's (id 10) and rejected
        // it — #2 was not yet stacked — then accepted #2's. Replayed in id
        // order both would pass; the receipt says live refused 11.
        let comments = vec![
            (
                PrNumber(2),
                vec![comment(10, AUTHOR, "@merge-train predecessor #1")],
            ),
            (
                PrNumber(3),
                vec![
                    comment(11, AUTHOR, "@merge-train predecessor #2"),
                    rejection(12, 3, 11),
                    // A receipt naming ANOTHER PR's comment is misplaced.
                    rejection(13, 2, 10),
                ],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(1))],
            "the rejected comment is dead; the misplaced receipt is ignored"
        );
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
                    vec![comment(2, AUTHOR, "@merge-train predecessor #1")],
                ),
            ];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &open,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                trigger: None,
                unfetchable: &HashSet::new(),
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

    /// The extension watermark is the record's own `watermark` (the first
    /// status comment's id), not its CURRENT comment id: recovery reposts
    /// a deleted status comment under a newer id, and a declaration made
    /// after the freeze but before the repost would otherwise compare
    /// below the current id and pass as baseline (monolith review, P2).
    #[test]
    fn the_extension_watermark_is_the_first_comment_not_the_current_one() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let run = |watermark: Option<u64>| {
            let mut record = train(PrNumber(1), ts);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(vec![PrNumber(2)]),
            };
            record.watermark = watermark.map(CommentId);
            let open = vec![
                pr(1, AUTHOR, PrState::Open),
                child(2, AUTHOR, 1, PrState::Open),
                child(3, AUTHOR, 2, PrState::Open),
            ];
            // The status comment was reposted as id 50; #3's declaration
            // (id 20) came after the original comment (id 5).
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
                    vec![comment(3, AUTHOR, "@merge-train predecessor #1")],
                ),
                (
                    PrNumber(3),
                    vec![comment(20, AUTHOR, "@merge-train predecessor #2")],
                ),
            ];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &open,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                trigger: None,
                unfetchable: &HashSet::new(),
                now: ts,
            });
            outcome.recovered_roots
        };
        assert_eq!(
            run(Some(5)),
            vec![],
            "declared after the freeze: an extension, aborted"
        );
        // A record with no watermark was embedded by the FIRST comment
        // itself (posted before its `StatusCommentPosted` landed), so that
        // comment's own id is the watermark: here 50, above the declaration.
        assert_eq!(
            run(None),
            vec![PrNumber(1)],
            "no recorded watermark: the adopting comment's id stands in"
        );
    }

    /// The triggering delivery's own authorized retraction is applied as a
    /// tombstone: the deleted comment (id 9) owned #2's declaration live,
    /// and an older comment (id 5) on the same PR still declares the same
    /// predecessor. Without the tombstone the crawl would promote id 5 and
    /// the handler — seeing no owner to retract — would leave the edge the
    /// author just removed (Codex crawl review, P1).
    #[test]
    fn the_triggering_retraction_tombstones_older_declarations() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![comment(5, AUTHOR, "@merge-train predecessor #1")],
        )];
        let run = |retraction: Option<PrNumber>| {
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &crawled,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                trigger: Some(TriggerComment {
                    id: CommentId(9),
                    retraction,
                    authorized: true,
                }),
                unfetchable: &HashSet::new(),
                now: test_now(),
            });
            declared(&outcome.events)
        };
        assert_eq!(
            run(Some(PrNumber(2))),
            vec![],
            "the older declaration died with the owner"
        );
        assert_eq!(
            run(None),
            vec![(PrNumber(2), PrNumber(1))],
            "a non-retracting trigger leaves it standing"
        );
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
                vec![comment(10, AUTHOR, "@merge-train predecessor #1")],
            ),
            (
                PrNumber(4),
                vec![comment(60, AUTHOR, "@merge-train predecessor #2")],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &open,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
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
                trigger: None,
                unfetchable: &HashSet::new(),
                now: ts,
            });
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        let intact = comment(10, AUTHOR, "@merge-train predecessor #1");
        let mut edited = intact.clone();
        edited.edited = true;
        assert_eq!(
            run(vec![intact]),
            (vec![PrNumber(1)], false),
            "intact: resumes"
        );
        assert_eq!(run(vec![]), (vec![], true), "deleted: aborts");
        assert_eq!(run(vec![edited]), (vec![], true), "edited: aborts");
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
            trigger: None,
            unfetchable: &HashSet::new(),
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

    /// A triggering EDIT by a stranger is about to be rejected by the
    /// pipeline; it must not count as a possible extension and abort the
    /// recovered train. The same edit by the author (authorized) does
    /// (Codex crawl review round 2, P1).
    #[test]
    fn an_unauthorized_triggering_edit_is_not_an_extension() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let run = |authorized: bool| {
            let mut record = train(PrNumber(1), ts);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(vec![PrNumber(2)]),
            };
            let open = vec![
                pr(1, AUTHOR, PrState::Open),
                child(2, AUTHOR, 1, PrState::Open),
                pr(3, STRANGER, PrState::Open),
            ];
            let mut edited = comment(60, STRANGER, "@merge-train predecessor #2");
            edited.edited = true;
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
                    vec![comment(10, AUTHOR, "@merge-train predecessor #1")],
                ),
                (PrNumber(3), vec![edited]),
            ];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &open,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                trigger: Some(TriggerComment {
                    id: CommentId(60),
                    retraction: None,
                    authorized,
                }),
                unfetchable: &HashSet::new(),
                now: ts,
            });
            outcome.recovered_roots
        };
        assert_eq!(
            run(false),
            vec![PrNumber(1)],
            "a stranger's edit changes nothing"
        );
        assert_eq!(
            run(true),
            vec![],
            "the author's edit is a possible extension: abort"
        );
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
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert_eq!(declared(&outcome.events), vec![]);
    }

    /// The triggering retraction tombstones only where the retracted
    /// comment could have OWNED the declaration: a surviving declaration
    /// with a HIGHER id proves it did not (ownership moves forward), so
    /// that edge stands (Codex crawl review round 3, P1).
    #[test]
    fn a_triggering_retraction_below_a_later_declaration_tombstones_nothing() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        // The trigger deletes comment 9; #2's surviving declaration is
        // comment 20 — above it, so 9 never owned the edge.
        let comments = vec![(
            PrNumber(2),
            vec![comment(20, AUTHOR, "@merge-train predecessor #1")],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: Some(TriggerComment {
                id: CommentId(9),
                retraction: Some(PrNumber(2)),
                authorized: true,
            }),
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(1))],
            "a declaration above the retracted id is untouched"
        );
    }

    /// A tombstoned declaration's target is not worth fetching: the live
    /// path already killed the declaration, so it is evidence of nothing
    /// (Codex crawl review round 3, P1 — the fixpoint used to pay one
    /// `GetPr` per predecessor-shaped comment).
    #[test]
    fn tombstoned_declarations_do_not_drive_the_fixpoint() {
        let crawled = vec![pr(1, AUTHOR, PrState::Open)];
        let receipt = |id: u64, anchor: u64| CommentData {
            id: CommentId(id),
            author_id: BOT,
            body: crate::status::format_retraction_receipt(PrNumber(1), CommentId(anchor), None),
            edited: false,
        };
        let comments = vec![(
            PrNumber(1),
            vec![
                comment(10, AUTHOR, "@merge-train predecessor #777"),
                receipt(11, 10),
                comment(12, AUTHOR, "@merge-train predecessor #888"),
            ],
        )];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        assert_eq!(
            outcome.referenced_uncrawled,
            vec![PrNumber(888)],
            "only the live declaration's target is fetched"
        );
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
                    vec![comment(10, AUTHOR, "@merge-train predecessor #1")],
                ),
                (
                    PrNumber(3),
                    vec![comment(20, AUTHOR, "@merge-train predecessor #2")],
                ),
            ];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &open,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                trigger: None,
                unfetchable: &HashSet::new(),
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

    /// A triggering comment a receipt already killed is reported, so the
    /// pipeline can close its delivery instead of letting the handler
    /// accept it against today's topology (Codex crawl review round 4, P1).
    #[test]
    fn a_receipted_trigger_is_reported_as_tombstoned() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let rejection = CommentData {
            id: CommentId(11),
            author_id: BOT,
            body: crate::status::format_rejection_receipt(
                PrNumber(2),
                CommentId(10),
                "Cannot declare predecessor: not in a stack.",
            ),
            edited: false,
        };
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                rejection,
            ],
        )];
        let run = |trigger: Option<u64>| {
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &crawled,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                trigger: trigger.map(|id| TriggerComment {
                    id: CommentId(id),
                    retraction: None,
                    authorized: true,
                }),
                unfetchable: &HashSet::new(),
                now: test_now(),
            });
            (outcome.trigger_tombstoned, declared(&outcome.events))
        };
        assert_eq!(run(Some(10)), (true, vec![]), "the trigger itself is dead");
        assert_eq!(
            run(Some(99)),
            (false, vec![]),
            "an unrelated trigger is not"
        );
        assert_eq!(run(None), (false, vec![]));
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
                    vec![comment(10, AUTHOR, "@merge-train predecessor #2")],
                ),
            ];
            let outcome = crawl_events(&CrawlInput {
                default_branch: "main",
                crawled_prs: &crawled,
                comments: &comments,
                bot_name: "merge-train",
                bot_user_id: BOT,
                trigger: None,
                unfetchable: &HashSet::new(),
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
            trigger: None,
            unfetchable: &HashSet::new(),
            now: ts,
        });
        assert_eq!(
            outcome.recovered_roots,
            vec![PrNumber(1)],
            "the newest comment's ACTIVE record is adopted, not the older stop"
        );
    }

    /// Declarations replay to a FIXPOINT: #3 declared #2 while #2 was
    /// stacked, then #2's declaration was retracted and RE-declared from a
    /// newer comment. In id order #3's edge looks invalid when its turn
    /// comes (its target is unstacked at that moment), but live kept it —
    /// a second pass, after #2's re-declaration lands, records it (Codex
    /// harness review round 7, P1).
    #[test]
    fn a_re_declared_target_revalidates_its_descendants() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
            child(3, AUTHOR, 2, PrState::Open),
        ];
        let receipt = |id: u64, pr_num: u64, anchor: u64| CommentData {
            id: CommentId(id),
            author_id: BOT,
            body: crate::status::format_retraction_receipt(
                PrNumber(pr_num),
                CommentId(anchor),
                None,
            ),
            edited: false,
        };
        let comments = vec![
            (
                PrNumber(2),
                vec![
                    comment(10, AUTHOR, "@merge-train predecessor #1"),
                    receipt(30, 2, 10),
                    comment(40, AUTHOR, "@merge-train predecessor #1"),
                ],
            ),
            (
                PrNumber(3),
                vec![comment(20, AUTHOR, "@merge-train predecessor #2")],
            ),
        ];
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            trigger: None,
            unfetchable: &HashSet::new(),
            now: test_now(),
        });
        let mut edges = declared(&outcome.events);
        edges.sort_unstable();
        assert_eq!(
            edges,
            vec![(PrNumber(2), PrNumber(1)), (PrNumber(3), PrNumber(2))],
            "the descendant's surviving edge is kept, as live keeps it"
        );
    }
}
