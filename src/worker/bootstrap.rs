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
//!   not one comment of the bot's, ledger, status or reply — has never had
//!   a decision made about it, so its declarations are read as the live
//!   path would read them and recorded (owner's ruling, 2026-09-08). No
//!   train can exist there to endanger.
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
use crate::state::validation::validate_predecessor_declaration;
use crate::status::parse::parse_status_comment;
use crate::types::{CommentId, MergeStateStatus, PrNumber, TrainLineage, TrainRecord};

use super::adoption;
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
    /// The crawl could not read every PR's comments: the topology may be
    /// missing a descendant's ledger, and no train may be started over it
    /// until an operator resolves that.
    pub topology_incomplete: bool,
}

/// Every ledger-shaped bot comment on `pr` that names it — bot-authored,
/// parsing as a ledger, naming the PR it sits on; a ledger found anywhere
/// else is forged or misplaced, the same gate status comments get — newest
/// comment first, each with whether it is TRUSTED: in bytes the bot
/// wrote last (`Edited`). Only a trusted ledger is the bot's record; one
/// somebody else edited last is theirs whatever it says, and grants
/// nothing — it is recorded as the PR's ledger only so that the live path
/// rewrites it.
/// The newest is what the recovered store records: comment ids are
/// globally monotonic, and never the stated `seq`, which restarts with a
/// rebuilt database (Codex crawl review round 14, P1).
fn ledgers_on(
    pr: PrNumber,
    pr_comments: &[CommentData],
    bot_user_id: u64,
) -> Vec<(CommentId, Option<crate::status::StackLedger>, bool)> {
    let mut found: Vec<(CommentId, Option<crate::status::StackLedger>, bool)> = pr_comments
        .iter()
        // `0` is the deny-safe sentinel for an omitted account on either
        // side, and two sentinels do not match.
        .filter(|c| c.author_id == bot_user_id && c.author_id != 0)
        .filter_map(|c| {
            // The bot's own record only in bytes the bot wrote LAST: GitHub
            // names the bot as the author however a maintainer edits the
            // comment, and an old ledger somebody pasted back over the
            // bot's newer one is theirs too (`Edited`).
            let trusted = c.body_written_by(bot_user_id).is_some();
            match crate::status::parse_stack_ledger(&c.body) {
                Some(ledger) if ledger.pr == pr => Some((c.id, Some(ledger), trusted)),
                Some(_) => None,
                // Not a ledger, and not the bot's own bytes: somebody else
                // edited this comment of the bot's — into malformed JSON,
                // into prose, with the marker erased altogether. It grants
                // nothing and is not the PR's ledger, but it MAY have been
                // the bot's newest record (a retraction), and comment ids
                // cannot say (a crash orphan can sit above the real record);
                // an older ledger beneath it may be the superseded one. Fail
                // closed: it taints the PR's ledgers, whatever it was (Codex
                // topology review, P1, three times over).
                None if !trusted => Some((c.id, None, false)),
                None => None,
            }
        })
        .collect();
    found.sort_by_key(|(id, _, _)| std::cmp::Reverse(*id));
    found
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
    let pr_authors: HashMap<PrNumber, u64> =
        all_prs.iter().map(|p| (p.number, p.author_id)).collect();
    for (pr, pr_comments) in comments {
        let found = ledgers_on(*pr, pr_comments, bot_user_id);
        // The PR's ledger is the newest comment that IS a ledger; a damaged
        // comment above it is a bar, not a ledger.
        let Some((ledger_comment, _, _)) = found.iter().find(|(_, l, _)| l.is_some()).copied()
        else {
            continue;
        };
        let newest_comment = found
            .first()
            .map(|(id, _, _)| *id)
            .unwrap_or(ledger_comment);
        // Where the ledger lives, so the recovered store rewrites that
        // comment rather than posting a second record of the same PR.
        events.push(StateEventPayload::StackLedgerPosted {
            pr: *pr,
            comment_id: ledger_comment,
        });
        // Only bytes the bot wrote last are the bot's record.
        let trusted: Vec<(CommentId, crate::status::StackLedger)> = found
            .iter()
            .filter_map(|(id, l, trusted)| trusted.then_some(()).and(l.map(|l| (*id, l))))
            .collect();
        // The recorded comment must end up stating what the store holds,
        // and the PR must end up with ONE ledger: unless the newest
        // comment is the bot's own and the only one, the ledger is owed a
        // rewrite — no event the crawl records dirties it by itself, and
        // the live sync's write is what restates the comment and
        // neutralizes the rest (Codex topology review, P2).
        // ...and no comment of the bot's on the PR that is not its own
        // bytes at all — whatever it parses as, whichever PR it claims,
        // whatever its id: an edit can hide the bot's last word, ids cannot
        // order it against the rest (a crash orphan can sit above the real
        // record), and an edited-but-valid lower record beneath a real
        // retraction is exactly the superseded one (Codex topology review,
        // P1, again).
        let tainted = pr_comments
            .iter()
            .any(|c| c.author_id == bot_user_id && c.body_written_by(bot_user_id).is_none());
        let newest_is_the_record =
            !tainted && trusted.first().is_some_and(|(id, _)| *id == newest_comment);
        if !newest_is_the_record || found.len() > 1 {
            stale_ledgers.push(*pr);
        }
        // The watermark, restored BEFORE any verdict on the edge: whatever
        // becomes of the edge, the recovered PR's next ledger rewrite would
        // otherwise erase it, and a surviving older declaration redelivered
        // after the loss would be handled as fresh and reinstall an edge
        // the author withdrew (Codex topology review, P1). With several
        // trusted records, the HIGHEST they state is what the store had
        // settled — that is the one that supersedes redeliveries; the
        // LOWEST floors the evidence, so that more of the declarations
        // above it count as a change the bot never saw (the abort-shaped
        // direction).
        let watermarks = trusted.iter().filter_map(|(_, l)| l.settled_through);
        if let Some(through) = watermarks.clone().max() {
            events.push(StateEventPayload::DeclarationsSettled { pr: *pr, through });
        }
        if let Some(floor) = watermarks.min() {
            settled.insert(*pr, floor);
        }
        // ...and nothing is granted from an OLDER trusted record beneath an
        // untrusted newest one: the older may be the superseded one — a
        // crash orphan declaring a comment that survives, beneath the real
        // record that retracted it — and the newest is the bot's unknown
        // last word. Fail closed (Codex topology review, P1).
        if !newest_is_the_record {
            warn!(
                %pr, comment = %ledger_comment,
                "the PR's newest stack ledger is not in the bot's own bytes; granting no edge \
                 and re-writing it"
            );
            continue;
        }
        let Some((_, ledger)) = trusted.first().copied() else {
            unreachable!("the newest comment is a trusted record");
        };
        // Several ledgers that DISAGREE are a conflict the crawl cannot
        // adjudicate — a crash orphan the live path has not yet
        // neutralized, or a forgery beside the real record — and a
        // surviving declaration proves nothing about which is genuine (a
        // forged newer ledger can name an old declaration whose
        // restatement the real record retracted). Fail closed: no edge,
        // and the chosen comment is owed a rewrite to what the store
        // holds; the live path neutralizes the rest.
        if trusted
            .iter()
            .any(|(_, other)| other.declared != ledger.declared)
        {
            warn!(
                %pr, records = trusted.len(),
                "the PR's stack ledgers disagree; granting no edge and re-writing the newest"
            );
            continue;
        }
        let Some(crate::status::Declaration { predecessor, owner }) = ledger.declared else {
            continue;
        };
        // Corroboration is what the LIVE path would have accepted: the
        // comment the ledger names still declares that predecessor, in the
        // PR author's OWN bytes — written by them, unedited, or last
        // edited by them, whoever created it: the author declaring by
        // editing a stranger's comment is live's restatement by edit (the
        // recovery model found the "unedited" rule dropping that edge). A
        // stranger's bytes corroborate nothing, however equal: a stranger
        // restoring the author's withdrawn text must not pass, and a bot
        // reply a maintainer forged into a ledger could otherwise name a
        // stranger's, or a doctored, declaration the live path refused.
        let corroborated = pr_comments.iter().find(|c| c.id == owner).is_some_and(|c| {
            pr_authors.get(pr).is_some_and(|author| {
                *author != 0
                    && c.body_written_by(*author).is_some_and(|body| {
                        matches!(
                            parse_command(body, bot_name),
                            Some(Command::Predecessor(target)) if target == predecessor
                        )
                    })
            })
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

    // ONBOARDING. A repository with no trace of the bot — not one comment
    // of the bot's: no ledger, no status comment, no reply — has never had
    // a decision made about it, so there is nothing to disagree with: the
    // declarations sitting on its PRs are read the way the live path would
    // read them, recorded, and written to ledgers (the `PredecessorDeclared`
    // events below mark them owed). This is the ONE place the crawl derives
    // anything, and it is reachable only where no train can exist to
    // endanger — a repository with a status comment is a recovery, not an
    // onboarding.
    //
    // RESIDUAL (owner's ruling, 2026-09-08): a repository whose every bot
    // comment was deleted looks new, so its declarations are adopted
    // afresh — including any the bot had refused.
    // ...and only when every PR's comments were READ: a crawl the listing
    // cap truncated cannot tell a new repository from one whose records
    // went unread, and would write derived edges to ledgers a later,
    // complete crawl trusts (Codex topology review, P2).
    // ANY comment of the bot's is prior contact — a repository where the
    // bot has only ever refused declarations has no ledger and no status
    // comment, and its refusals survive; onboarding it would accept a
    // declaration the bot refused, against whatever changed meanwhile
    // (Codex topology review, P1). `0` is the deny-safe sentinel for an
    // omitted account, and never the bot.
    let onboarding = !comments_truncated
        && bot_user_id != 0
        && comments
            .iter()
            .all(|(_, pr_comments)| pr_comments.iter().all(|c| c.author_id != bot_user_id));
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
                if comment.edited.is_edited() || comment.author_id == 0 {
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
            // A predecessor that has already merged is a LATE ADDITION on
            // the live path — the validator accepts it as resolved, and
            // the handler records no edge but hands it to a distinct flow.
            // Onboarding records no edge either.
            if scratch
                .prs
                .get(&predecessor)
                .is_some_and(|p| matches!(p.state, crate::types::PrState::Merged { .. }))
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
                // ...unless EDITED: the watermark orders creation ids, and
                // an old comment edited into a declaration during the gap
                // sits below it while being news.
                || settled
                    .get(pr)
                    .is_some_and(|through| comment.id <= *through && !comment.edited.is_edited());
            // And only a comment the live path COULD have accepted is
            // evidence at all. Authorship is the one gate that needs no
            // history — the comment carries its author and the PR carries
            // its own — so a stranger's declaration, which live refuses,
            // does not abort a recovered train. An EDITED body stays
            // evidence: the API reports only the original author, never
            // the editor, so it cannot be cleared this way and the
            // conservative reading stands.
            let could_have_been_accepted = comment.edited.is_edited()
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

    // Train recovery from the bot's status comments (`adoption`). Trust
    // gates: authored by the bot, in bytes the bot wrote LAST, parseable,
    // and posted on its own root PR — a bot reply a maintainer edited into
    // a running record parses like the bot's own, and so does an old
    // running record pasted back over the bot's stopped one; either would
    // otherwise run a train nobody started (`Edited`).
    let mut records: HashMap<PrNumber, Vec<(CommentId, TrainRecord)>> = HashMap::new();
    // Every trusted record's lineage, by the PR it sits on — fan-out
    // evidence must survive the child starting a fresh train of its own
    // (whose newest record has no lineage): the OLDER record still proves
    // the parent fanned out (Codex crawl review round 2, P1).
    let mut lineages: HashMap<PrNumber, Vec<TrainLineage>> = HashMap::new();
    // Per root, the newest status-shaped bot comment that is NOT a trusted
    // record. Somebody else's bytes bar whatever they say — an edit into
    // prose, into malformed JSON, or with the markers erased altogether: a
    // newer bot comment on the root that is not the bot's own may have
    // been its last word, and the crawl cannot know (a failed terminal
    // update leaves an older active record beneath a newer stopped one).
    // The bot's own bytes bar only when status-SHAPED and unparseable —
    // the bot never writes that — and never when they are the root's own
    // ledger, which shares the markers.
    let mut barred: HashMap<PrNumber, CommentId> = HashMap::new();
    for (pr, pr_comments) in comments {
        for comment in pr_comments {
            if comment.author_id != bot_user_id {
                continue;
            }
            let trusted = comment.body_written_by(bot_user_id);
            let Some(record) = trusted.and_then(|body| parse_status_comment(body).ok()) else {
                let shaped = comment
                    .body
                    .contains(crate::status::format::STATUS_COMMENT_START);
                let own_ledger =
                    trusted.is_some_and(|body| crate::status::parse_stack_ledger(body).is_some());
                if trusted.is_none() || (shaped && !own_ledger) {
                    let bar = barred.entry(*pr).or_insert(comment.id);
                    *bar = (*bar).max(comment.id);
                }
                continue;
            };
            if record.original_root_pr != *pr {
                continue;
            }
            if let Some(parent) = &record.parent {
                lineages.entry(*pr).or_default().push(parent.clone());
            }
            records.entry(*pr).or_default().push((comment.id, record));
        }
    }
    let merged_numbers: HashSet<PrNumber> = all_prs
        .iter()
        .filter(|p| p.state.is_merged())
        .map(|p| p.number)
        .collect();
    let present = adoption::Present {
        topology: &topology,
        default_branch,
        merged: &merged_numbers,
        unfetchable,
        comments_truncated,
        lineages: &lineages,
        unledgered: &unledgered,
    };

    // Per root: the chosen record, its footprint, and whether a newer
    // untrusted comment bars it (newer than the root's newest trusted
    // record: the bar is about the bot's LAST word on the root).
    let mut roots: Vec<PrNumber> = records.keys().copied().collect();
    roots.sort_unstable();
    let chosen: Vec<(
        PrNumber,
        TrainRecord,
        adoption::Footprint,
        Option<CommentId>,
    )> = roots
        .into_iter()
        .filter_map(|root| {
            let on_root = &records[&root];
            let (comment_id, mut record) = adoption::choose(on_root)?;
            record.status_comment_id = Some(comment_id);
            let bar = barred.get(&root).copied();
            let footprint = adoption::Footprint::of(&topology, &record);
            Some((root, record, footprint, bar))
        })
        .collect();
    let live: Vec<(PrNumber, &adoption::Footprint)> = chosen
        .iter()
        .filter(|(root, record, footprint, bar)| {
            record.state.is_active()
                && bar.is_none()
                && !adoption::completes(*root, record, footprint, &present)
        })
        .map(|(root, _, footprint, _)| (*root, footprint))
        .collect();
    let overlapping = adoption::overlapping(&live);

    let mut recovered_roots = Vec::new();
    for (root, record, footprint, bar) in chosen {
        let verdict = adoption::judge(
            root,
            &record,
            &footprint,
            &present,
            bar,
            overlapping.get(&root).copied(),
        );
        // Everything the record names is fetched whatever this pass ruled:
        // the verdict is provisional until the fixpoint, and the proof that
        // turns an abort into a completion — a fan-out sibling's lineage —
        // may sit on a member not yet crawled (Codex trains review, P2).
        // Adopted-train members absent from the crawl (a frozen
        // descendant, or a closed root, that neither list endpoint
        // returned) join the referenced-uncrawled set so the caller
        // fetches them and re-crawls.
        // ...and whatever the verdict, a completion included: a child that
        // closed unmerged during the gap is in neither listing, and its
        // own active record must still be read and retired (Codex trains
        // review, P2).
        if record.state.is_active() {
            for member in &footprint.named {
                if !crawled_numbers.contains(member) && !referenced_uncrawled.contains(member) {
                    referenced_uncrawled.push(*member);
                }
            }
        }
        if let adoption::Verdict::Abort(why) = &verdict {
            warn!(%root, ?why, "the recovered train is aborted");
        }
        events.push(StateEventPayload::TrainRecordAdopted {
            root_pr: root,
            record,
        });
        match verdict {
            adoption::Verdict::Retired => {}
            adoption::Verdict::Recover => recovered_roots.push(root),
            // Synthesized completion is a real `TrainCompleted` event after
            // the adoption, not a doctored record: the store then owes the
            // status comment its final word like any completion (Codex
            // crawl review round 2, P2).
            adoption::Verdict::Complete => {
                events.push(StateEventPayload::TrainCompleted { root_pr: root });
            }
            adoption::Verdict::Abort(why) => {
                events.push(StateEventPayload::TrainAborted {
                    root_pr: root,
                    error: why.error(root, default_branch),
                });
            }
        }
    }
    referenced_uncrawled.sort_unstable();
    referenced_uncrawled.dedup();
    stale_ledgers.sort_unstable();
    stale_ledgers.dedup();

    CrawlOutcome {
        events,
        recovered_roots,
        referenced_uncrawled,
        stale_ledgers,
        topology_incomplete: comments_truncated,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::effects::github::Edited;
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
            edited: Edited::Never,
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
        edited.edited = Edited::By {
            editor: Some(AUTHOR),
        };
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
        edited.edited = Edited::By {
            editor: Some(AUTHOR),
        };
        assert!(
            run(edited),
            "and an edited one does too: its editor cannot be identified"
        );
    }

    /// Duplicate ledgers are ordered by COMMENT ID, never by the sequence
    /// number they state: sequence numbers restart when the database is
    /// rebuilt, so an orphan from a previous life can carry a larger one
    /// than the ledger the store actually kept (Codex crawl review round
    /// 14, P1). The newest is what the recovered store records — and where
    /// the two DISAGREE, as a crash orphan and the record that superseded
    /// it do, no edge is granted and the newest is owed a rewrite: the
    /// crawl cannot tell that orphan from a forgery beside a real record,
    /// and the double fault (an orphan the live path had not yet cleaned
    /// up, then a database loss) costs one re-declaration where the other
    /// choice could resurrect a retracted edge.
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
        let recovered = replay_topology("main", &outcome.events, test_now());
        assert_eq!(
            recovered.prs[&PrNumber(2)].ledger_comment_id,
            Some(CommentId(12)),
            "the later comment is the record, whatever sequence numbers they claim"
        );
        assert!(
            declared(&outcome.events).is_empty(),
            "but a disagreement grants no edge"
        );
        assert_eq!(
            outcome.stale_ledgers,
            vec![PrNumber(2)],
            "and owes the record a rewrite"
        );
    }

    /// The comment the crawl records as the PR's ledger is the NEWEST
    /// ledger-shaped bot comment, whether or not it is the bot's own
    /// bytes; the record it believes is the newest TRUSTED one. Whenever
    /// those differ — a forgery above the real record — nothing is
    /// granted: the older trusted record may be the superseded one, and
    /// the newest is the bot's unknown last word (Codex topology review,
    /// P1). Whenever they differ, or the bot has more than one
    /// ledger-shaped comment on the PR, the ledger is owed a rewrite, so
    /// that the live sync restates it and neutralizes the rest. No event
    /// the crawl records dirties a ledger by itself (Codex topology
    /// review, P2).
    #[test]
    fn a_recorded_comment_that_is_not_the_trusted_record_is_owed_a_rewrite() {
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
            let recovered = replay_topology("main", &outcome.events, test_now());
            (
                declared(&outcome.events),
                outcome.stale_ledgers,
                recovered.prs[&PrNumber(2)].ledger_comment_id,
            )
        };
        let mut forged = ledger(12, 2, Some((1, 10)), 3);
        forged.edited = Edited::By {
            editor: Some(STRANGER),
        };
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                ledger(11, 2, None, 3),
                forged.clone(),
            ]),
            (vec![], vec![PrNumber(2)], Some(CommentId(12))),
            "the trusted record grants nothing; the forgery above it is rewritten"
        );
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                ledger(11, 2, Some((1, 10)), 3),
                forged,
            ]),
            (vec![], vec![PrNumber(2)], Some(CommentId(12))),
            "a forgery above the real record: nothing is granted from the older one either"
        );
        // A newer record DAMAGED rather than edited — its JSON corrupted
        // by a maintainer — is not a ledger any more, and bars older
        // grants all the same: it may have been the retraction (Codex
        // topology review, P1). The PR's ledger is the newest comment
        // that IS one.
        let mut damaged = settled_ledger(12, 2, None, 5, Some(20));
        damaged.body = damaged.body.replacen("\"version\"", "\"version", 1);
        assert!(
            crate::status::parse_stack_ledger(&damaged.body).is_none(),
            "precondition"
        );
        damaged.edited = Edited::By {
            editor: Some(STRANGER),
        };
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                ledger(11, 2, Some((1, 10)), 3),
                damaged,
            ]),
            (vec![], vec![PrNumber(2)], Some(CommentId(11))),
            "a damaged newer record bars the older one"
        );
        // ...or edited into prose, the marker gone with it; and even an
        // OLDER comment of the bot's edited by somebody else taints the
        // PR's ledgers: comment ids cannot say which of the bot's comments
        // carried its last word (Codex topology review, P1).
        for (id, text) in [
            (12, "never mind, said the bot"),
            (5, "an old reply, edited"),
        ] {
            let mut erased = comment(id, BOT, text);
            erased.edited = Edited::By {
                editor: Some(STRANGER),
            };
            assert_eq!(
                run(vec![
                    comment(10, AUTHOR, "@merge-train predecessor #1"),
                    ledger(11, 2, Some((1, 10)), 3),
                    erased,
                ]),
                (vec![], vec![PrNumber(2)], Some(CommentId(11))),
                "somebody else's bytes on the PR (comment {id}) taint its ledgers"
            );
        }
        // ...and whatever it parses as: an edited-but-valid LOWER ledger
        // beneath the bot's real retraction is exactly the superseded
        // record, and one edited to claim another PR is no less an edit
        // (Codex topology review, P1). Ids cannot order these either.
        let mut edited_orphan = ledger(11, 2, Some((1, 10)), 3);
        edited_orphan.edited = Edited::By {
            editor: Some(STRANGER),
        };
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                edited_orphan,
                settled_ledger(12, 2, None, 5, Some(20)),
            ]),
            (vec![], vec![PrNumber(2)], Some(CommentId(12))),
            "an edited lower ledger taints the PR: nothing is granted"
        );
        let mut misnamed = ledger(5, 3, Some((1, 10)), 3);
        misnamed.edited = Edited::By {
            editor: Some(STRANGER),
        };
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                misnamed,
                ledger(11, 2, Some((1, 10)), 3),
            ]),
            (vec![], vec![PrNumber(2)], Some(CommentId(11))),
            "an edited ledger claiming another PR taints this one"
        );
        // The older trusted record may be the SUPERSEDED one: a crash
        // orphan declaring comment 10, beneath the real record that
        // retracted restatement 20 — edited by a maintainer. Comment 10
        // survives and would corroborate the orphan, resurrecting the
        // withdrawn edge (Codex topology review, P1). Fail closed.
        let mut retraction = settled_ledger(12, 2, None, 5, Some(20));
        retraction.edited = Edited::By {
            editor: Some(STRANGER),
        };
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                ledger(11, 2, Some((1, 10)), 3),
                retraction,
            ]),
            (vec![], vec![PrNumber(2)], Some(CommentId(12))),
            "an untrusted newest record bars the older ones"
        );
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                ledger(11, 2, Some((1, 10)), 3),
                ledger(12, 2, Some((1, 10)), 4),
            ]),
            (
                vec![(PrNumber(2), PrNumber(1))],
                vec![PrNumber(2)],
                Some(CommentId(12))
            ),
            "two of the bot's own, agreeing: a crash orphan to neutralize"
        );
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                ledger(11, 2, Some((1, 10)), 3),
            ]),
            (
                vec![(PrNumber(2), PrNumber(1))],
                vec![],
                Some(CommentId(11))
            ),
            "one record, the bot's own: nothing owed"
        );
    }

    /// A crawl that could not read every PR's comments cannot tell a new
    /// repository from one whose records went unread: onboarding there
    /// would derive edges from declarations the bot may have refused,
    /// and write them to ledgers a later, complete crawl trusts. No
    /// trace of the bot is proof only when everything was read (Codex
    /// topology review, P2).
    #[test]
    fn a_truncated_crawl_onboards_nothing() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![comment(10, AUTHOR, "@merge-train predecessor #1")],
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
                now: test_now(),
            });
            declared(&outcome.events)
        };
        assert_eq!(
            run(false),
            vec![(PrNumber(2), PrNumber(1))],
            "complete: onboarded"
        );
        assert_eq!(run(true), vec![], "truncated: nothing is derived");
    }

    /// Any comment of the bot's is prior contact: a repository where the
    /// bot has only ever REFUSED declarations has no ledger and no status
    /// comment, and its refusals survive. Onboarding it would accept a
    /// declaration the bot refused, against whatever changed meanwhile
    /// (Codex topology review, P1). Only a repository with not one
    /// comment of the bot's is new.
    #[test]
    fn a_bots_reply_alone_is_prior_contact_and_prevents_onboarding() {
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
            declared(&outcome.events)
        };
        let declaration = comment(10, AUTHOR, "@merge-train predecessor #1");
        assert_eq!(
            run(vec![declaration.clone()]),
            vec![(PrNumber(2), PrNumber(1))],
            "no trace of the bot: onboarded"
        );
        assert_eq!(
            run(vec![
                declaration,
                comment(
                    11,
                    BOT,
                    "❌ #1 cannot be a predecessor: base branch mismatch."
                ),
            ]),
            vec![],
            "the bot's refusal survives: a recovery, and the refused declaration is no edge"
        );
    }

    /// Once the cascade has advanced past the root, the CURRENT PR is a
    /// descendant the record relies on, frozen set or not: its declaration
    /// deleted during the gap severs the stack, and the train aborts rather
    /// than squash a PR the user unstacked.
    #[test]
    fn an_unstacked_current_pr_severs_the_train() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let run = |member_comments: Vec<CommentData>| {
            let mut record = train(PrNumber(1), ts);
            record.current_pr = PrNumber(2);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::new(vec![]),
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
        let ledger_only = vec![ledger(11, 2, Some((1, 10)), 3)]; // its declaration is gone
        assert_eq!(run(ledger_only), (vec![], true), "severed: abort");
        let intact = vec![
            comment(10, AUTHOR, "@merge-train predecessor #1"),
            ledger(11, 2, Some((1, 10)), 3),
        ];
        assert_eq!(run(intact), (vec![PrNumber(1)], false), "intact: recover");
    }

    /// A signature proves the bot wrote those bytes at SOME time, not
    /// that the bot is who put them there now: an old running record a
    /// maintainer pasted back over the bot's stopped one verifies just
    /// the same. Only bytes the bot wrote LAST are its record — a bot
    /// comment somebody else edited last recovers nothing, whatever it
    /// says, and the bot's own later edit is as good as its post.
    #[test]
    fn a_replayed_status_record_recovers_no_train() {
        let ts = test_now();
        let record = train(PrNumber(1), ts);
        let crawled = vec![pr(1, AUTHOR, PrState::Open)];
        let run = |edited: Edited| {
            let mut status = comment(50, BOT, &format_status_comment(&record, "s").unwrap());
            status.edited = edited;
            let comments = vec![(PrNumber(1), vec![status])];
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
            outcome.recovered_roots
        };
        assert_eq!(run(Edited::Never), vec![PrNumber(1)], "as posted");
        assert_eq!(
            run(Edited::By { editor: Some(BOT) }),
            vec![PrNumber(1)],
            "the bot's own edit"
        );
        assert_eq!(
            run(Edited::By {
                editor: Some(STRANGER)
            }),
            vec![],
            "replayed by somebody else"
        );
        assert_eq!(
            run(Edited::By { editor: None }),
            vec![],
            "edited by an account GitHub cannot name"
        );
    }

    /// The newest status-shaped bot comment on a root is the bot's LAST
    /// word there. If it is not in the bot's own bytes the crawl cannot
    /// know what that word was, and an older, trusted ACTIVE record
    /// beneath it is not the train's present: a failed terminal update
    /// leaves exactly that shape — an old active record, a newer stopped
    /// one — and a maintainer's edit to the newer one (a word of prose)
    /// must not resurrect the train the newer one stopped. The bot's own
    /// newer word stands, whatever it says.
    #[test]
    fn a_newer_untrusted_record_bars_the_older_active_one() {
        let ts = test_now();
        let active = train(PrNumber(1), ts);
        let mut stopped = active.clone();
        stopped.state = TrainState::Stopped { ended_at: ts };
        stopped.recovery_seq = 1;
        let crawled = vec![pr(1, AUTHOR, PrState::Open)];
        let run = |newer: &crate::types::TrainRecord, edited: Edited| {
            let mut newest = comment(60, BOT, &format_status_comment(newer, "s").unwrap());
            newest.edited = edited;
            let comments = vec![(
                PrNumber(1),
                vec![
                    comment(50, BOT, &format_status_comment(&active, "s").unwrap()),
                    newest,
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
            outcome.recovered_roots
        };
        assert_eq!(
            run(
                &stopped,
                Edited::By {
                    editor: Some(STRANGER)
                }
            ),
            vec![],
            "somebody else's newer word bars the older record"
        );
        assert_eq!(
            run(&stopped, Edited::By { editor: Some(BOT) }),
            vec![],
            "the bot's own newer word: stopped"
        );
        assert_eq!(
            run(&active, Edited::By { editor: Some(BOT) }),
            vec![PrNumber(1)],
            "the bot's own newer word: still active"
        );
    }

    /// An IDLE train has no frozen set yet and freezes against the
    /// topology at its next step — so a ledgered extension is no threat
    /// to it. An UNLEDGERED declaration onto its root is: no edge is
    /// installed for it, the next freeze cannot pick it up, and the
    /// train would squash the root without preparing the PR the user
    /// stacked (Codex trains review, P1). The closure walked from the
    /// root and current PR is the idle train's stack.
    #[test]
    fn an_unledgered_declaration_onto_an_idle_trains_root_aborts_it() {
        let ts = test_now();
        let record = train(PrNumber(1), ts);
        assert!(matches!(
            record.cascade_phase,
            crate::types::CascadePhase::Idle
        ));
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let run = |member_comments: Vec<CommentData>| {
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
                crawled_prs: &crawled,
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
        assert_eq!(
            run(vec![comment(10, AUTHOR, "@merge-train predecessor #1")]),
            (vec![], true),
            "unledgered: the train aborts"
        );
        assert_eq!(
            run(vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                ledger(11, 2, Some((1, 10)), 3),
            ]),
            (vec![PrNumber(1)], false),
            "ledgered: the next freeze picks it up"
        );
    }

    /// A newer status-SHAPED bot comment on the root bars older records
    /// whether or not it still parses: a terminal record edited into
    /// malformed JSON, or shorn of its closing marker, is as much the
    /// bot's unknown last word as one edited into prose (Codex trains
    /// review, P1). A ledger of the bot's own on the root is not a bar.
    #[test]
    fn a_newer_unparseable_status_block_bars_the_older_active_one() {
        let ts = test_now();
        let active = train(PrNumber(1), ts);
        let mut stopped = active.clone();
        stopped.state = TrainState::Stopped { ended_at: ts };
        let crawled = vec![pr(1, AUTHOR, PrState::Open)];
        let run = |newest: CommentData| {
            let comments = vec![(
                PrNumber(1),
                vec![
                    comment(50, BOT, &format_status_comment(&active, "s").unwrap()),
                    newest,
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
            outcome.recovered_roots
        };
        let well_formed = format_status_comment(&stopped, "s").unwrap();
        assert!(
            well_formed.contains(crate::status::format::STATUS_COMMENT_START),
            "precondition"
        );
        let malformed = well_formed.replacen("\"version\"", "\"version", 1);
        assert!(parse_status_comment(&malformed).is_err(), "precondition");
        let shorn = well_formed.replacen(crate::status::format::STATUS_COMMENT_END, "", 1);
        assert!(parse_status_comment(&shorn).is_err(), "precondition");
        // ...or of its OPENING marker: nothing then says the comment was a
        // record, and only somebody else's edit can have made it so.
        let unmarked = well_formed.replacen(crate::status::format::STATUS_COMMENT_START, "", 1);
        assert!(
            !unmarked.contains(crate::status::format::STATUS_COMMENT_START),
            "precondition"
        );
        let mut erased = comment(60, BOT, &unmarked);
        erased.edited = Edited::By {
            editor: Some(STRANGER),
        };
        assert_eq!(
            run(erased),
            vec![],
            "somebody else erased the bot's newer word: a bar all the same"
        );
        let mut reply = comment(60, BOT, &unmarked);
        reply.edited = Edited::By { editor: Some(BOT) };
        assert_eq!(
            run(reply),
            vec![PrNumber(1)],
            "the bot's own unmarked bytes are a plain reply, no bar"
        );
        for body in [&malformed, &shorn] {
            let mut newest = comment(60, BOT, body);
            newest.edited = Edited::By {
                editor: Some(STRANGER),
            };
            assert_eq!(
                run(newest),
                vec![],
                "somebody else's unparseable newer word bars"
            );
            let mut newest = comment(60, BOT, body);
            newest.edited = Edited::By { editor: Some(BOT) };
            assert_eq!(
                run(newest),
                vec![],
                "even in the bot's own bytes, it is unknown"
            );
        }
        assert_eq!(
            run(ledger(60, 1, None, 3)),
            vec![PrNumber(1)],
            "the root's own ledger, the bot's bytes, is no bar"
        );
    }

    /// Status updates can lag several cascade steps behind: the record
    /// still says current #1 with #2 frozen, while the train has since
    /// fanned out AT #2 into #3 and #4, whose records name this parent
    /// incarnation. Those children are beyond the record's frozen
    /// frontier, and prove the fan-out all the same: the parent is
    /// completed, not resumed alongside them (Codex trains review, P2).
    #[test]
    fn a_fan_out_beyond_the_frozen_frontier_still_completes_the_parent() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let t0 = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();
        let mut parent = train(PrNumber(1), t0);
        parent.cascade_phase = CascadePhase::Reconciling {
            progress: DescendantProgress::new(vec![PrNumber(2)]),
            squash_sha: Sha::parse("a".repeat(40)).unwrap(),
        };
        let lineage = TrainLineage {
            root: PrNumber(1),
            started_at: t0,
        };
        let mut child3 = train(PrNumber(3), t1);
        child3.parent = Some(lineage.clone());
        let mut child4 = train(PrNumber(4), t1);
        child4.parent = Some(lineage);
        let merged = |n: u64, pred: u64| {
            child(
                n,
                AUTHOR,
                pred,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
                },
            )
        };
        let crawled = vec![
            pr(
                1,
                AUTHOR,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("c".repeat(40)).unwrap(),
                },
            ),
            merged(2, 1),
            child(3, AUTHOR, 2, PrState::Open),
            child(4, AUTHOR, 2, PrState::Open),
        ];
        let declare = |pr: u64, target: u64| {
            (
                PrNumber(pr),
                vec![
                    comment(
                        pr * 10,
                        AUTHOR,
                        &format!("@merge-train predecessor #{target}"),
                    ),
                    ledger(pr * 10 + 1, pr, Some((target, pr * 10)), 5),
                ],
            )
        };
        let mut comments = vec![
            (
                PrNumber(1),
                vec![comment(
                    1,
                    BOT,
                    &format_status_comment(&parent, "p").unwrap(),
                )],
            ),
            declare(2, 1),
            declare(3, 2),
            declare(4, 2),
        ];
        comments[2].1.push(comment(
            39,
            BOT,
            &format_status_comment(&child3, "c").unwrap(),
        ));
        comments[3].1.push(comment(
            49,
            BOT,
            &format_status_comment(&child4, "c").unwrap(),
        ));
        let outcome = crawl_events(&CrawlInput {
            default_branch: "main",
            crawled_prs: &crawled,
            comments: &comments,
            bot_name: "merge-train",
            bot_user_id: BOT,
            unfetchable: &HashSet::new(),
            comments_truncated: false,
            now: t1,
        });
        assert_eq!(
            outcome.recovered_roots,
            vec![PrNumber(3), PrNumber(4)],
            "the children recover; the fanned-out parent does not"
        );
        assert!(outcome.events.iter().any(|e| matches!(
            e,
            StateEventPayload::TrainCompleted { root_pr } if *root_pr == PrNumber(1)
        )));
    }

    /// One PR belongs to at most one active train — live `start` refuses
    /// a PR another active train holds. Two ACTIVE records that share a
    /// PR (a stop whose terminal update failed left the old train's
    /// record active, and a fresh `start` on its member began another)
    /// are a contradiction the crawl cannot resolve: comment ids do not
    /// order them (a recovery repost moves a comment above later ones),
    /// and adopting both lets a `stop` on one leave the other to merge
    /// the PR. Fail closed: both abort (Codex trains review, P1).
    #[test]
    fn overlapping_active_trains_both_abort() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut old = train(PrNumber(1), ts);
        old.current_pr = PrNumber(2);
        old.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::with_known_stack(vec![], vec![PrNumber(1), PrNumber(2)]),
        };
        let fresh = train(PrNumber(2), ts);
        let mut stopped = fresh.clone();
        stopped.state = TrainState::Stopped { ended_at: ts };
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let run = |on_two: &crate::types::TrainRecord| {
            let comments = vec![
                (
                    PrNumber(1),
                    vec![comment(50, BOT, &format_status_comment(&old, "s").unwrap())],
                ),
                (
                    PrNumber(2),
                    vec![
                        comment(20, AUTHOR, "@merge-train predecessor #1"),
                        ledger(21, 2, Some((1, 20)), 3),
                        comment(60, BOT, &format_status_comment(on_two, "s").unwrap()),
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
            let mut aborted: Vec<PrNumber> = outcome
                .events
                .iter()
                .filter_map(|e| match e {
                    StateEventPayload::TrainAborted { root_pr, .. } => Some(*root_pr),
                    _ => None,
                })
                .collect();
            aborted.sort_unstable();
            (outcome.recovered_roots, aborted)
        };
        assert_eq!(
            run(&fresh),
            (vec![], vec![PrNumber(1), PrNumber(2)]),
            "two active trains on #2: neither is believed"
        );
        assert_eq!(
            run(&stopped),
            (vec![PrNumber(1)], vec![]),
            "the newer one stopped: the old record is the only active one"
        );
    }

    /// What a train OWNS is what the live path's `train_involving` says:
    /// its members and everything declared under them. An IDLE record
    /// names only its root, while a stack may hang off that root — so a
    /// fresh active train on a descendant overlaps it exactly as one on a
    /// frozen member would, and both abort (Codex trains review, P1).
    #[test]
    fn overlapping_active_trains_are_found_through_the_topology() {
        let ts = test_now();
        let old = train(PrNumber(1), ts);
        assert!(matches!(
            old.cascade_phase,
            crate::types::CascadePhase::Idle
        ));
        let fresh = train(PrNumber(2), ts);
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![
            (
                PrNumber(1),
                vec![comment(50, BOT, &format_status_comment(&old, "s").unwrap())],
            ),
            (
                PrNumber(2),
                vec![
                    comment(20, AUTHOR, "@merge-train predecessor #1"),
                    ledger(21, 2, Some((1, 20)), 3),
                    comment(60, BOT, &format_status_comment(&fresh, "s").unwrap()),
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
        let mut aborted: Vec<PrNumber> = outcome
            .events
            .iter()
            .filter_map(|e| match e {
                StateEventPayload::TrainAborted { root_pr, .. } => Some(*root_pr),
                _ => None,
            })
            .collect();
        aborted.sort_unstable();
        assert_eq!(
            (outcome.recovered_roots, aborted),
            (vec![], vec![PrNumber(1), PrNumber(2)]),
            "#2 hangs off the idle train's root: two active trains on it"
        );
    }

    /// Everything a record names is fetched before it is believed: a
    /// member of the recorded known stack beyond the frozen frontier can
    /// fall outside the recently-merged listing, and its merge state is
    /// what decides whether the train has finished. Left unfetched, a
    /// completed train would resume and prepare an already-merged member
    /// (Codex trains review, P2).
    #[test]
    fn a_recorded_member_beyond_the_frozen_frontier_is_fetched() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::with_known_stack(
                vec![PrNumber(2)],
                vec![PrNumber(1), PrNumber(2), PrNumber(3)],
            ),
        };
        let merged = |n: u64| PrState::Merged {
            merge_commit_sha: Sha::parse(n.to_string().repeat(40)).unwrap(),
        };
        let crawled = vec![pr(1, AUTHOR, merged(1)), child(2, AUTHOR, 1, merged(2))];
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
                    comment(20, AUTHOR, "@merge-train predecessor #1"),
                    ledger(21, 2, Some((1, 20)), 3),
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
        assert_eq!(
            outcome.referenced_uncrawled,
            vec![PrNumber(3)],
            "the known stack's #3 is asked for before the train is believed"
        );
    }

    /// A merged predecessor is the train's own history only if it chains
    /// back into the train: a member re-declared onto a stranger that
    /// has since merged left the train all the same, and the recovered
    /// cascade would retarget it and squash it under the train it left
    /// (Codex trains review, P1).
    #[test]
    fn a_member_reparented_onto_a_merged_stranger_severs_the_train() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let run = |predecessor: u64| {
            let mut record = train(PrNumber(1), ts);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::with_known_stack(
                    vec![PrNumber(2)],
                    vec![PrNumber(1), PrNumber(2)],
                ),
            };
            let crawled = vec![
                pr(1, AUTHOR, PrState::Open),
                child(2, AUTHOR, predecessor, PrState::Open),
                pr(
                    3,
                    AUTHOR,
                    PrState::Merged {
                        merge_commit_sha: Sha::parse("d".repeat(40)).unwrap(),
                    },
                ),
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
                        comment(
                            20,
                            AUTHOR,
                            &format!("@merge-train predecessor #{predecessor}"),
                        ),
                        ledger(21, 2, Some((predecessor, 20)), 3),
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
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        assert_eq!(run(1), (vec![PrNumber(1)], false), "still stacked: recover");
        assert_eq!(
            run(3),
            (vec![], true),
            "re-pointed at a merged stranger: severed all the same"
        );
    }

    /// A stale parent's record can say IDLE for its whole life — its
    /// first post landed and every update since failed — and still have
    /// fanned out: its children's records name that incarnation, which
    /// is proof whatever phase the parent claims. Without it the parent
    /// and its children overlap, and the valid children abort (Codex
    /// trains review, P2).
    #[test]
    fn an_idle_parents_fan_out_is_still_proven_by_its_children() {
        let t0 = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();
        let parent = train(PrNumber(1), t0);
        assert!(matches!(
            parent.cascade_phase,
            crate::types::CascadePhase::Idle
        ));
        let mut child2 = train(PrNumber(2), t1);
        child2.parent = Some(TrainLineage {
            root: PrNumber(1),
            started_at: t0,
        });
        let crawled = vec![
            pr(
                1,
                AUTHOR,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("c".repeat(40)).unwrap(),
                },
            ),
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
                    comment(20, AUTHOR, "@merge-train predecessor #1"),
                    ledger(21, 2, Some((1, 20)), 3),
                    comment(29, BOT, &format_status_comment(&child2, "c").unwrap()),
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
            now: t1,
        });
        assert_eq!(
            outcome.recovered_roots,
            vec![PrNumber(2)],
            "the child recovers; the idle parent is completed by its lineage"
        );
        assert!(outcome.events.iter().any(|e| matches!(
            e,
            StateEventPayload::TrainCompleted { root_pr } if *root_pr == PrNumber(1)
        )));
        assert!(
            !outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. })),
            "nothing aborts"
        );
    }

    /// The ROOT can be re-declared too: a running root declared onto an
    /// open PR is a member whose edge changed, which live aborts — and
    /// whose abort's status update can fail. A recovered root with an
    /// open predecessor is not a root any more; its train aborts. A
    /// merged predecessor is what a fan-out child's root always has, and
    /// no threat (Codex trains review, P1).
    #[test]
    fn a_root_re_declared_under_an_open_pr_severs_the_train() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let run = |predecessor_state: Option<PrState>| {
            let mut record = train(PrNumber(1), ts);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::with_known_stack(
                    vec![PrNumber(2)],
                    vec![PrNumber(1), PrNumber(2)],
                ),
            };
            let mut crawled = vec![child(2, AUTHOR, 1, PrState::Open)];
            let mut root_comments = vec![comment(
                50,
                BOT,
                &format_status_comment(&record, "s").unwrap(),
            )];
            match predecessor_state {
                Some(state) => {
                    crawled.push(child(1, AUTHOR, 9, PrState::Open));
                    crawled.push(pr(9, AUTHOR, state));
                    root_comments.push(comment(10, AUTHOR, "@merge-train predecessor #9"));
                    root_comments.push(ledger(11, 1, Some((9, 10)), 3));
                }
                None => crawled.push(pr(1, AUTHOR, PrState::Open)),
            }
            let comments = vec![
                (PrNumber(1), root_comments),
                (
                    PrNumber(2),
                    vec![
                        comment(20, AUTHOR, "@merge-train predecessor #1"),
                        ledger(21, 2, Some((1, 20)), 3),
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
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        assert_eq!(run(None), (vec![PrNumber(1)], false), "a root: recover");
        assert_eq!(
            run(Some(PrState::Merged {
                merge_commit_sha: Sha::parse("e".repeat(40)).unwrap(),
            })),
            (vec![PrNumber(1)], false),
            "under a merged PR, as a fan-out child's root is: recover"
        );
        assert_eq!(
            run(Some(PrState::Open)),
            (vec![], true),
            "under an open PR: not a root any more, severed"
        );
    }

    /// A root, or current PR, CLOSED unmerged during the gap: the train
    /// cannot go on, and live would have aborted it on the close webhook.
    /// The crawl aborts it before it resumes — resumed, it would push to
    /// its frozen descendants before its next refetch of the root found
    /// the close (Codex trains review, P2).
    #[test]
    fn a_closed_primary_pr_aborts_the_train_before_it_resumes() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let run = |closed: u64| {
            let mut record = train(PrNumber(1), ts);
            record.current_pr = PrNumber(2);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::with_known_stack(
                    vec![PrNumber(3)],
                    vec![PrNumber(1), PrNumber(2), PrNumber(3)],
                ),
            };
            let state = |n: u64| {
                if n == closed {
                    PrState::Closed
                } else {
                    PrState::Open
                }
            };
            let crawled = vec![
                pr(1, AUTHOR, state(1)),
                child(2, AUTHOR, 1, state(2)),
                child(3, AUTHOR, 2, state(3)),
            ];
            let declare = |pr: u64, target: u64| {
                (
                    PrNumber(pr),
                    vec![
                        comment(
                            pr * 10,
                            AUTHOR,
                            &format!("@merge-train predecessor #{target}"),
                        ),
                        ledger(pr * 10 + 1, pr, Some((target, pr * 10)), 5),
                    ],
                )
            };
            let comments = vec![
                (
                    PrNumber(1),
                    vec![comment(
                        50,
                        BOT,
                        &format_status_comment(&record, "s").unwrap(),
                    )],
                ),
                declare(2, 1),
                declare(3, 2),
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
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        assert_eq!(run(0), (vec![PrNumber(1)], false), "all open: recover");
        assert_eq!(run(1), (vec![], true), "the root closed: abort");
        assert_eq!(run(2), (vec![], true), "the current PR closed: abort");
    }

    /// A KNOWN member can move too: `1 <- 2 <- 3` froze with #2 as the
    /// frontier and #3 known behind it; #3 re-declared onto #1 during the
    /// gap is a direct descendant of the current PR that the frozen set
    /// does not hold. Driven as recorded, the train would merge #1 and #2
    /// and report completion with #3 never prepared (Codex trains
    /// review, P1). Live aborts on a member's edge changing; so does the
    /// crawl.
    #[test]
    fn a_known_member_moved_onto_the_frontier_aborts_the_train() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let run = |predecessor_of_3: u64| {
            let mut record = train(PrNumber(1), ts);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::with_known_stack(
                    vec![PrNumber(2)],
                    vec![PrNumber(1), PrNumber(2), PrNumber(3)],
                ),
            };
            let crawled = vec![
                pr(1, AUTHOR, PrState::Open),
                child(2, AUTHOR, 1, PrState::Open),
                child(3, AUTHOR, predecessor_of_3, PrState::Open),
            ];
            let declare = |pr: u64, target: u64| {
                (
                    PrNumber(pr),
                    vec![
                        comment(
                            pr * 10,
                            AUTHOR,
                            &format!("@merge-train predecessor #{target}"),
                        ),
                        ledger(pr * 10 + 1, pr, Some((target, pr * 10)), 5),
                    ],
                )
            };
            let comments = vec![
                (
                    PrNumber(1),
                    vec![comment(
                        50,
                        BOT,
                        &format_status_comment(&record, "s").unwrap(),
                    )],
                ),
                declare(2, 1),
                declare(3, predecessor_of_3),
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
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        assert_eq!(run(2), (vec![PrNumber(1)], false), "as frozen: recover");
        assert_eq!(
            run(1),
            (vec![], true),
            "#3 moved onto the frontier the frozen set does not hold: abort"
        );
    }

    /// Everything a record names is fetched BEFORE any verdict on it: a
    /// stale fan-out parent overlaps its restarted child, and the only
    /// proof the parent fanned out may sit on an uncrawled, closed
    /// sibling. Ruled on first, both trains abort; fetched first, the
    /// sibling's lineage completes the parent and the child recovers
    /// (Codex trains review, P2).
    #[test]
    fn a_trains_members_are_discovered_before_it_is_ruled_on() {
        use crate::types::{CascadePhase, DescendantProgress};
        let t0 = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();
        let mut parent = train(PrNumber(1), t0);
        parent.cascade_phase = CascadePhase::Reconciling {
            progress: DescendantProgress::with_known_stack(
                vec![PrNumber(2), PrNumber(3)],
                vec![PrNumber(1), PrNumber(2), PrNumber(3)],
            ),
            squash_sha: Sha::parse("a".repeat(40)).unwrap(),
        };
        let fresh_child = train(PrNumber(2), t1);
        // #3, closed, is not in the crawl: neither its state nor its
        // comments — and its record is where the parent's lineage lives.
        let crawled = vec![
            pr(
                1,
                AUTHOR,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
                },
            ),
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
                    comment(20, AUTHOR, "@merge-train predecessor #1"),
                    ledger(21, 2, Some((1, 20)), 3),
                    comment(29, BOT, &format_status_comment(&fresh_child, "c").unwrap()),
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
            now: t1,
        });
        assert_eq!(
            outcome.referenced_uncrawled,
            vec![PrNumber(3)],
            "the parent's member #3 is asked for, whatever this pass ruled"
        );
    }

    /// Within the chosen incarnation, the record JUDGED is the one
    /// supplementary recovery will adopt — the highest `recovery_seq` —
    /// not the one with the highest comment id: a delayed duplicate post
    /// of the same incarnation can outrank the live comment by id while
    /// carrying an older, idle-looking record, and judging that one would
    /// let the live record's severed member be merged (Codex trains
    /// review, P1).
    #[test]
    fn the_record_judged_is_the_one_recovery_would_adopt() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut live = train(PrNumber(1), ts);
        live.recovery_seq = 2;
        live.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::with_known_stack(
                vec![PrNumber(2)],
                vec![PrNumber(1), PrNumber(2)],
            ),
        };
        let mut delayed = train(PrNumber(1), ts);
        delayed.recovery_seq = 1;
        // #2 was unstacked during the gap: its declaration is gone.
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![
            (
                PrNumber(1),
                vec![
                    comment(50, BOT, &format_status_comment(&live, "s").unwrap()),
                    comment(60, BOT, &format_status_comment(&delayed, "s").unwrap()),
                ],
            ),
            (PrNumber(2), vec![ledger(21, 2, Some((1, 20)), 3)]),
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
        let adopted = outcome.events.iter().find_map(|e| match e {
            StateEventPayload::TrainRecordAdopted { record, .. } => Some(record.recovery_seq),
            _ => None,
        });
        assert_eq!(adopted, Some(2), "the highest sequence of the incarnation");
        assert!(
            outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. })),
            "and judged as such: its frozen member is severed"
        );
        assert_eq!(outcome.recovered_roots, vec![]);
    }

    /// A barred record is adopted and ABORTED, not merely left alone: the
    /// bar is a newer bot comment somebody else edited, and the live sync
    /// may rewrite that comment in the bot's own bytes (a ledger-shaped
    /// edit is owed a rewrite) — after a second loss nothing would bar
    /// the older active record any more. The abort's final word on the
    /// older comment is what survives (Codex trains review, P2).
    #[test]
    fn a_barred_record_is_aborted_durably() {
        let ts = test_now();
        let active = train(PrNumber(1), ts);
        let crawled = vec![pr(1, AUTHOR, PrState::Open)];
        let mut bar = comment(
            60,
            BOT,
            "stopped, said the bot, before somebody edited this",
        );
        bar.edited = Edited::By {
            editor: Some(STRANGER),
        };
        let comments = vec![(
            PrNumber(1),
            vec![
                comment(50, BOT, &format_status_comment(&active, "s").unwrap()),
                bar,
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
        assert_eq!(outcome.recovered_roots, vec![]);
        assert!(outcome.events.iter().any(|e| matches!(
            e,
            StateEventPayload::TrainRecordAdopted { root_pr, .. } if *root_pr == PrNumber(1)
        )));
        assert!(
            outcome.events.iter().any(|e| matches!(
                e,
                StateEventPayload::TrainAborted { root_pr, .. } if *root_pr == PrNumber(1)
            )),
            "aborted, so the older comment gets a final word"
        );
    }

    /// A known member BEHIND the frontier can be unstacked too: `1 <- 2
    /// <- 3` froze #2 with #3 known behind it, and #3's declaration
    /// deleted during the gap leaves it with no edge and no unledgered
    /// evidence either. Driven as recorded, the train would merge #1 and
    /// #2 and finish with #3 abandoned; live would have aborted on the
    /// removal (Codex trains review, P1). Every open member the record
    /// names is checked, not only the frontier.
    #[test]
    fn an_unstacked_known_member_behind_the_frontier_severs_the_train() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let run = |three_declares: bool| {
            let mut record = train(PrNumber(1), ts);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::with_known_stack(
                    vec![PrNumber(2)],
                    vec![PrNumber(1), PrNumber(2), PrNumber(3)],
                ),
            };
            let crawled = vec![
                pr(1, AUTHOR, PrState::Open),
                child(2, AUTHOR, 1, PrState::Open),
                if three_declares {
                    child(3, AUTHOR, 2, PrState::Open)
                } else {
                    pr(3, AUTHOR, PrState::Open)
                },
            ];
            let mut comments = vec![
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
                        comment(20, AUTHOR, "@merge-train predecessor #1"),
                        ledger(21, 2, Some((1, 20)), 5),
                    ],
                ),
            ];
            if three_declares {
                comments.push((
                    PrNumber(3),
                    vec![
                        comment(30, AUTHOR, "@merge-train predecessor #2"),
                        ledger(31, 3, Some((2, 30)), 5),
                    ],
                ));
            } else {
                // The declaration is gone; the ledger the bot rewrote says so.
                comments.push((PrNumber(3), vec![ledger(31, 3, None, 6)]));
            }
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
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        assert_eq!(
            run(true),
            (vec![PrNumber(1)], false),
            "as recorded: recover"
        );
        assert_eq!(
            run(false),
            (vec![], true),
            "#3 unstacked behind the frontier: severed"
        );
    }

    /// Comment ids do not order the bot's comments within an incarnation:
    /// the canonical comment (updated in place to stopped, then edited by
    /// somebody else) can sit BELOW a delayed active duplicate of the same
    /// incarnation. Any comment of the bot's on the root that is not the
    /// bot's own bytes bars the root, whatever its id (Codex trains
    /// review, P1).
    #[test]
    fn a_delayed_duplicate_does_not_unbar_an_untrusted_canonical_record() {
        let ts = test_now();
        let active = train(PrNumber(1), ts);
        let crawled = vec![pr(1, AUTHOR, PrState::Open)];
        let mut canonical = comment(50, BOT, "stopped; then edited by somebody else");
        canonical.edited = Edited::By {
            editor: Some(STRANGER),
        };
        let comments = vec![(
            PrNumber(1),
            vec![
                canonical,
                comment(60, BOT, &format_status_comment(&active, "s").unwrap()),
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
            vec![],
            "barred, whatever the ids say"
        );
        assert!(outcome.events.iter().any(|e| matches!(
            e,
            StateEventPayload::TrainAborted { root_pr, .. } if *root_pr == PrNumber(1)
        )));
    }

    /// Everything an ACTIVE record names is fetched whatever its verdict —
    /// a parent completed by its child's lineage included: a child that
    /// closed unmerged during the gap is in neither listing, and its own
    /// active record must still be read, adopted and retired (Codex
    /// trains review, P2).
    #[test]
    fn a_completing_parents_named_members_are_still_fetched() {
        use crate::types::{CascadePhase, DescendantProgress};
        let t0 = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();
        let mut parent = train(PrNumber(1), t0);
        parent.cascade_phase = CascadePhase::Reconciling {
            progress: DescendantProgress::with_known_stack(
                vec![PrNumber(2), PrNumber(3)],
                vec![PrNumber(1), PrNumber(2), PrNumber(3)],
            ),
            squash_sha: Sha::parse("a".repeat(40)).unwrap(),
        };
        let mut child2 = train(PrNumber(2), t1);
        child2.parent = Some(TrainLineage {
            root: PrNumber(1),
            started_at: t0,
        });
        let crawled = vec![
            pr(
                1,
                AUTHOR,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
                },
            ),
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
                    comment(20, AUTHOR, "@merge-train predecessor #1"),
                    ledger(21, 2, Some((1, 20)), 3),
                    comment(29, BOT, &format_status_comment(&child2, "c").unwrap()),
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
            now: t1,
        });
        assert!(outcome.events.iter().any(|e| matches!(
            e,
            StateEventPayload::TrainCompleted { root_pr } if *root_pr == PrNumber(1)
        )));
        assert_eq!(
            outcome.referenced_uncrawled,
            vec![PrNumber(3)],
            "the completed parent's #3 is still asked for"
        );
    }

    /// A frozen member RETARGETED during the gap — its base moved off its
    /// still-open predecessor's branch onto `main` — passes every
    /// topology check (its ledger and predecessor are unchanged), and the
    /// resume path enters `Preparing` past the base validation `begin_step`
    /// would have run. The cascade would squash it. The live rule applies
    /// here too: an open member's base must be its open predecessor's head
    /// (Codex trains review, P1).
    #[test]
    fn a_frozen_member_retargeted_off_its_predecessor_aborts_the_train() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let run = |base: &str| {
            let mut record = train(PrNumber(1), ts);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::with_known_stack(
                    vec![PrNumber(2)],
                    vec![PrNumber(1), PrNumber(2)],
                ),
            };
            let mut two = child(2, AUTHOR, 1, PrState::Open);
            two.base_ref = base.to_owned();
            let crawled = vec![pr(1, AUTHOR, PrState::Open), two];
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
                        comment(20, AUTHOR, "@merge-train predecessor #1"),
                        ledger(21, 2, Some((1, 20)), 3),
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
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        assert_eq!(
            run("pr-1"),
            (vec![PrNumber(1)], false),
            "based on its predecessor: recover"
        );
        assert_eq!(run("main"), (vec![], true), "retargeted onto main: abort");
    }

    /// A stale IDLE record can hide a whole cascade: `1 <- 2 <- 3` with #1
    /// and #2 merged and #3 still open. Walked through open PRs only, the
    /// stack ends at #1 and the record recovers; the engine's idle path
    /// then sees no open child and completes the train, abandoning #3.
    /// The train advanced past what its record says: an open tail behind
    /// merged history aborts it explicitly (Codex trains review, P2).
    #[test]
    fn an_open_tail_behind_merged_history_aborts_a_stale_idle_record() {
        let ts = test_now();
        let record = train(PrNumber(1), ts);
        assert!(matches!(
            record.cascade_phase,
            crate::types::CascadePhase::Idle
        ));
        let merged = |n: u64| PrState::Merged {
            merge_commit_sha: Sha::parse(n.to_string().repeat(40)).unwrap(),
        };
        let run = |with_tail: bool| {
            let mut crawled = vec![pr(1, AUTHOR, merged(1)), child(2, AUTHOR, 1, merged(2))];
            let declare = |pr: u64, target: u64| {
                (
                    PrNumber(pr),
                    vec![
                        comment(
                            pr * 10,
                            AUTHOR,
                            &format!("@merge-train predecessor #{target}"),
                        ),
                        ledger(pr * 10 + 1, pr, Some((target, pr * 10)), 5),
                    ],
                )
            };
            let mut comments = vec![
                (
                    PrNumber(1),
                    vec![comment(
                        50,
                        BOT,
                        &format_status_comment(&record, "s").unwrap(),
                    )],
                ),
                declare(2, 1),
            ];
            if with_tail {
                crawled.push(child(3, AUTHOR, 2, PrState::Open));
                comments.push(declare(3, 2));
            }
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
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        assert_eq!(run(false), (vec![PrNumber(1)], false), "no tail: recover");
        assert_eq!(
            run(true),
            (vec![], true),
            "#3 open behind merged #2: the record is stale; abort"
        );
    }

    /// The same for a ledger: one somebody else edited last grants no
    /// edge however well it verifies, and is recorded only so that the
    /// live path rewrites it.
    #[test]
    fn a_replayed_ledger_grants_nothing_and_is_rewritten() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let run = |edited: Edited| {
            let mut replayed = ledger(11, 2, Some((1, 10)), 3);
            replayed.edited = edited;
            let comments = vec![(
                PrNumber(2),
                vec![comment(10, AUTHOR, "@merge-train predecessor #1"), replayed],
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
            let recovered = replay_topology("main", &outcome.events, test_now());
            (
                declared(&outcome.events),
                outcome.stale_ledgers,
                recovered.prs[&PrNumber(2)].ledger_comment_id,
            )
        };
        assert_eq!(
            run(Edited::By { editor: Some(BOT) }),
            (
                vec![(PrNumber(2), PrNumber(1))],
                vec![],
                Some(CommentId(11))
            ),
            "the bot's own rewrite grants the edge"
        );
        assert_eq!(
            run(Edited::By {
                editor: Some(STRANGER)
            }),
            (vec![], vec![PrNumber(2)], Some(CommentId(11))),
            "somebody else's bytes grant nothing and are owed a rewrite"
        );
    }

    /// A frozen member re-declared onto ANOTHER stack during the gap
    /// still has a predecessor — just not one of the train's. Driving it
    /// would squash it under the old train it left; the train is severed
    /// as surely as by an undeclaration.
    #[test]
    fn a_frozen_member_reparented_out_of_the_stack_severs_the_train() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let run = |predecessor: u64| {
            let mut record = train(PrNumber(1), ts);
            record.cascade_phase = CascadePhase::Preparing {
                progress: DescendantProgress::with_known_stack(
                    vec![PrNumber(2)],
                    vec![PrNumber(1), PrNumber(2)],
                ),
            };
            let crawled = vec![
                pr(1, AUTHOR, PrState::Open),
                child(2, AUTHOR, predecessor, PrState::Open),
                pr(3, AUTHOR, PrState::Open),
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
                        comment(
                            20,
                            AUTHOR,
                            &format!("@merge-train predecessor #{predecessor}"),
                        ),
                        ledger(21, 2, Some((predecessor, 20)), 3),
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
            let aborted = outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. }));
            (outcome.recovered_roots, aborted)
        };
        assert_eq!(run(1), (vec![PrNumber(1)], false), "still stacked: recover");
        assert_eq!(run(3), (vec![], true), "re-pointed at #3: severed");
    }

    /// A sibling closed before the train started is outside the stack
    /// the freeze walked (closed PRs block the traversal) and outside the
    /// stack the train drives: its restored edge is no extension.
    #[test]
    fn a_closed_sibling_is_not_an_extension() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::with_known_stack(
                vec![PrNumber(3)],
                vec![PrNumber(1), PrNumber(3)],
            ),
        };
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Closed),
            child(3, AUTHOR, 1, PrState::Open),
        ];
        let declare = |pr: u64, target: u64| {
            (
                PrNumber(pr),
                vec![
                    comment(
                        pr * 10,
                        AUTHOR,
                        &format!("@merge-train predecessor #{target}"),
                    ),
                    ledger(pr * 10 + 1, pr, Some((target, pr * 10)), 5),
                ],
            )
        };
        let comments = vec![
            (
                PrNumber(1),
                vec![comment(
                    50,
                    BOT,
                    &format_status_comment(&record, "s").unwrap(),
                )],
            ),
            declare(2, 1),
            declare(3, 1),
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
        assert!(
            !outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. })),
            "the unchanged train is not aborted on its closed sibling"
        );
        assert_eq!(outcome.recovered_roots, vec![PrNumber(1)]);
    }

    /// An old comment EDITED into a declaration during the gap sits below
    /// the settled watermark by creation id and is news all the same: it
    /// is evidence, and a train it touches aborts.
    #[test]
    fn an_edited_comment_below_the_watermark_is_still_evidence() {
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
        let mut stray = comment(15, AUTHOR, "@merge-train predecessor #2");
        stray.edited = Edited::By {
            editor: Some(AUTHOR),
        };
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
                vec![stray, settled_ledger(21, 3, None, 6, Some(20))],
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
        assert!(
            outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. })),
            "an edited old comment declaring into the stack aborts the train"
        );
    }

    /// A member the train already merged is its past, not an extension: a
    /// quiet database loss restores its ledger edge into the stack, and the
    /// unchanged train must recover rather than abort on its own history.
    #[test]
    fn a_completed_member_is_not_an_extension() {
        use crate::types::{CascadePhase, DescendantProgress};
        let ts = test_now();
        let mut record = train(PrNumber(1), ts);
        record.current_pr = PrNumber(3);
        // At the step boundary, #2 had merged: the step's frozen set and
        // known stack were computed by a traversal that stops at merged
        // members, so neither lists it.
        let progress = DescendantProgress::with_known_stack(
            vec![PrNumber(3), PrNumber(4)],
            vec![PrNumber(1), PrNumber(3), PrNumber(4)],
        );
        record.cascade_phase = CascadePhase::Preparing { progress };
        let merged = pr(
            2,
            AUTHOR,
            PrState::Merged {
                merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
            },
        );
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            merged,
            child(3, AUTHOR, 2, PrState::Open),
            child(4, AUTHOR, 3, PrState::Open),
        ];
        let declare = |pr: u64, target: u64| {
            (
                PrNumber(pr),
                vec![
                    comment(
                        pr * 10,
                        AUTHOR,
                        &format!("@merge-train predecessor #{target}"),
                    ),
                    ledger(pr * 10 + 1, pr, Some((target, pr * 10)), 5),
                ],
            )
        };
        let comments = vec![
            (
                PrNumber(1),
                vec![comment(
                    50,
                    BOT,
                    &format_status_comment(&record, "s").unwrap(),
                )],
            ),
            declare(2, 1),
            declare(3, 2),
            declare(4, 3),
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
        assert!(
            !outcome
                .events
                .iter()
                .any(|e| matches!(e, StateEventPayload::TrainAborted { .. })),
            "the unchanged train is not aborted on its merged member"
        );
        assert_eq!(outcome.recovered_roots, vec![PrNumber(1)]);
    }

    /// Two ledgers that disagree grant nothing: a forged newer record naming
    /// an old declaration whose restatement the real record retracted must
    /// not resurrect the edge. The newest is owed a rewrite; the lowest
    /// watermark stands.
    #[test]
    fn disagreeing_ledgers_grant_no_edge() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(10, AUTHOR, "@merge-train predecessor #1"),
                // Restatement 20 was deleted; the real record retracted.
                settled_ledger(21, 2, None, 5, Some(20)),
                // A newer bot reply forged into a ledger naming comment 10.
                settled_ledger(22, 2, Some((1, 10)), u64::MAX, Some(10)),
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
            "no edge from a conflict"
        );
        assert_eq!(outcome.stale_ledgers, vec![PrNumber(2)]);
        let recovered = replay_topology("main", &outcome.events, test_now());
        assert_eq!(
            recovered.prs[&PrNumber(2)].ledger_comment_id,
            Some(CommentId(22))
        );
        assert_eq!(
            recovered.prs[&PrNumber(2)].declarations_settled_through,
            Some(CommentId(20)),
            "the highest watermark is persisted (it supersedes redeliveries); the lowest \
             only floors the evidence"
        );
    }

    /// Onboarding records no edge onto an already-merged predecessor: the
    /// live path treats that as a late addition and records none either.
    #[test]
    fn onboarding_records_no_edge_onto_a_merged_predecessor() {
        let merged = pr(
            1,
            AUTHOR,
            PrState::Merged {
                merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
            },
        );
        let crawled = vec![merged, child(2, AUTHOR, 1, PrState::Open)];
        let comments = vec![(
            PrNumber(2),
            vec![comment(10, AUTHOR, "@merge-train predecessor #1")],
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
        assert!(declared(&outcome.events).is_empty());
    }

    /// A ledger's edge is corroborated only by a declaration the live path
    /// could have accepted: the PR author's own, unedited. A forged ledger
    /// naming a stranger's declaration, or an edited one, grants nothing.
    #[test]
    fn a_ledger_naming_a_declaration_live_would_refuse_grants_nothing() {
        let run = |owner: CommentData| {
            let crawled = vec![
                pr(1, AUTHOR, PrState::Open),
                child(2, AUTHOR, 1, PrState::Open),
            ];
            let comments = vec![(PrNumber(2), vec![owner, ledger(12, 2, Some((1, 10)), 3)])];
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
            run(comment(10, STRANGER, "@merge-train predecessor #1")),
            (vec![], vec![PrNumber(2)]),
            "a stranger's declaration corroborates nothing"
        );
        let mut edited = comment(10, AUTHOR, "@merge-train predecessor #1");
        edited.edited = Edited::By {
            editor: Some(AUTHOR),
        };
        assert_eq!(
            run(edited.clone()),
            (vec![(PrNumber(2), PrNumber(1))], vec![]),
            "the author's own edit is the author's bytes"
        );
        edited.edited = Edited::By {
            editor: Some(STRANGER),
        };
        assert_eq!(
            run(edited),
            (vec![], vec![PrNumber(2)]),
            "a stranger's edit is not, however equal"
        );
        let mut adopted = comment(10, STRANGER, "@merge-train predecessor #1");
        adopted.edited = Edited::By {
            editor: Some(AUTHOR),
        };
        assert_eq!(
            run(adopted),
            (vec![(PrNumber(2), PrNumber(1))], vec![]),
            "a stranger's comment the author edited into the declaration is the author's"
        );
        assert_eq!(
            run(comment(10, AUTHOR, "@merge-train predecessor #1")),
            (vec![(PrNumber(2), PrNumber(1))], vec![]),
            "the author's own, unedited, does"
        );
    }

    /// The settled watermark a ledger carries is restored into the
    /// recovered state, even when the ledger records no edge: a retracted
    /// ledger's history must survive its next rewrite.
    #[test]
    fn a_ledgers_settled_watermark_is_restored() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![(PrNumber(2), vec![settled_ledger(12, 2, None, 3, Some(20))])];
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
        let recovered = replay_topology("main", &outcome.events, test_now());
        assert_eq!(
            recovered.prs[&PrNumber(2)].declarations_settled_through,
            Some(CommentId(20))
        );
    }

    /// With several trusted records, the watermark RESTORED is the highest
    /// they state: it now supersedes a redelivered older declaration, and
    /// the lowest would let one between the two reinstall a withdrawn
    /// edge. The lowest still floors the EVIDENCE: a declaration between
    /// the two counts as a change the bot may never have seen, and its
    /// target is fetched (Codex topology review, P1).
    #[test]
    fn the_restored_watermark_is_the_highest_trusted_one() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let comments = vec![(
            PrNumber(2),
            vec![
                settled_ledger(13, 2, None, 4, Some(702)),
                settled_ledger(12, 2, None, 3, Some(700)),
                comment(701, AUTHOR, "@merge-train predecessor #77"),
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
        let recovered = replay_topology("main", &outcome.events, test_now());
        assert_eq!(
            recovered.prs[&PrNumber(2)].declarations_settled_through,
            Some(CommentId(702)),
            "the highest watermark is restored"
        );
        assert!(
            outcome.referenced_uncrawled.contains(&PrNumber(77)),
            "a declaration above the lowest watermark is still evidence: {:?}",
            outcome.referenced_uncrawled
        );
    }

    /// A PR whose bot comments are TAINTED (one edited by somebody else)
    /// is granted no edge and owed a rewrite — but the trusted record's
    /// watermark is restored all the same, or the rewrite would erase it
    /// and a redelivered older declaration would reinstall the withdrawn
    /// edge (Codex topology review, P1).
    #[test]
    fn a_tainted_prs_trusted_watermark_is_still_restored() {
        let crawled = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open),
        ];
        let mut reply = comment(13, BOT, "Heads up: something");
        reply.edited = Edited::By {
            editor: Some(AUTHOR),
        };
        let comments = vec![(
            PrNumber(2),
            vec![reply, settled_ledger(12, 2, None, 3, Some(701))],
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
        let recovered = replay_topology("main", &outcome.events, test_now());
        assert_eq!(recovered.prs[&PrNumber(2)].predecessor, None);
        assert_eq!(
            recovered.prs[&PrNumber(2)].declarations_settled_through,
            Some(CommentId(701)),
            "the trusted record's watermark survives the taint"
        );
        assert!(outcome.stale_ledgers.contains(&PrNumber(2)));
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
        // The parent's root has squashed (its record is reconciling), as a
        // fan-out child's root's predecessor always has.
        let open = vec![
            pr(
                1,
                AUTHOR,
                PrState::Merged {
                    merge_commit_sha: Sha::parse("a".repeat(40)).unwrap(),
                },
            ),
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
                    // The bot recorded #2's own declaration; a ledger of
                    // the bot's on the root is no bar to its records.
                    ledger(1, 2, Some((1, 0)), 3),
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
