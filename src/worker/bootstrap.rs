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
//!   declaration NO ledger accounts for cannot make an edge. Comments may
//!   take topology away; they may never add it. (Such a declaration is
//!   followed for discovery — the PR it names is fetched — and nothing
//!   more.)
//!
//!   The exception is ONBOARDING: a repository with no trace of the bot —
//!   not one comment of the bot's, ledger, status or reply — has never had
//!   a decision made about it, so its declarations are read as the live
//!   path would read them and recorded (owner's ruling, 2026-09-08).
//! - **Trains** — not yet. The bot's status comments are the designed
//!   off-disk backup of its trains, and reading them back is the next
//!   change; until then a lost database loses every train, and a
//!   repository with a status comment on a PR is still a RECOVERY here
//!   (its declarations are not onboarded), never an onboarding.
//!
//! **Envelope**: after a DB loss the stack behaves as if no train was
//! running; already-merged PRs are in the cache, so a fresh `start` gets
//! the engine's loud validations (and the late-addition answer) rather
//! than silence.

use std::collections::{HashMap, HashSet};

use tracing::{error, warn};

use crate::commands::{Command, parse_command};
use crate::effects::PrData;
use crate::effects::github::CommentData;
use crate::persistence::event::{StateEvent, StateEventPayload};
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::state::RepoState;
use crate::state::validation::validate_predecessor_declaration;
use crate::types::{CommentId, MergeStateStatus, PrNumber};

use super::pipeline::cache_fill_events;

/// The crawl's decision: events to append, and every PR the crawl
/// *referenced* but did not fetch — declaration targets absent from the
/// crawl. The caller fetches those, lists their comments, and RE-RUNS the
/// crawl to a fixpoint: a closed-unmerged PR is absent from both list
/// endpoints but named by its descendants' declarations, and only by
/// pulling it in are its own records read (Codex crawl review rounds
/// 6–7).
pub(crate) struct CrawlOutcome {
    pub events: Vec<StateEventPayload>,
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
    // resolves towards FEWER edges — a missing edge costs a re-declaration,
    // where a fabricated edge would drive a PR the user never stacked. It
    // covers the one dangerous residual the ledger alone leaves: a
    // retraction whose ledger write was lost to an outage.
    //
    // RESIDUAL: a maintainer who edits somebody else's declaration comment
    // to name a different predecessor revokes that edge for a crawl, even
    // though the live path refused their edit. Stop-shaped, and it takes a
    // DB loss and a maintainer to reach.
    let mut edges: Vec<(PrNumber, PrNumber, CommentId)> = Vec::new();
    let mut stale_ledgers: Vec<PrNumber> = Vec::new();
    // Predecessor-shaped comments no ledger accounts for. They cannot make
    // an edge — the bot may have refused them, or never seen them at all —
    // and are followed only for DISCOVERY: the PR they name is fetched.
    // Comments may take topology away; they may never add it.
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
            // And only a comment the live path COULD have accepted counts
            // at all. Authorship is the one gate that needs no history —
            // the comment carries its author and the PR carries its own —
            // so a stranger's declaration, which live refuses, is not
            // followed. An EDITED body stays: the API reports only the
            // original author, never the editor, so it cannot be cleared
            // this way and the conservative reading stands.
            let could_have_been_accepted = comment.edited.is_edited()
                || (comment.author_id != 0 && authors.get(pr) == Some(&comment.author_id));
            if !accounted_for && could_have_been_accepted {
                unledgered.push((*pr, target));
            }
        }
    }

    // The topology scratch: built from the cache fills, then grown one
    // ledger edge at a time, so that each edge is checked against the
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
    // is closed unmerged is invisible to both list endpoints and carries
    // records of its own the crawl must read (Codex crawl review round 7).
    //
    // UNLEDGERED declarations are followed too — for DISCOVERY only, never
    // for their edge. A declaration whose ledger write was lost may be the
    // only route to such a PR (Codex crawl review round 14, P2). The fetch
    // cap still bounds it.
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

    referenced_uncrawled.sort_unstable();
    referenced_uncrawled.dedup();
    stale_ledgers.sort_unstable();
    stale_ledgers.dedup();

    CrawlOutcome {
        events,
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
    use crate::types::{CommentId, PrState, Sha};

    const BOT: u64 = 424_242;
    const AUTHOR: u64 = 100;
    const STRANGER: u64 = 200;

    fn test_now() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 3, 0, 0, 0).unwrap()
    }

    /// A record as the live path creates it: against default branch `main`.
    fn train(root: PrNumber, started_at: chrono::DateTime<Utc>) -> crate::types::TrainRecord {
        let mut record = crate::types::TrainRecord::new(root, started_at);
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

    /// A repository with a status comment is a RECOVERY, not an
    /// onboarding: a declaration no ledger claims is not read as an edge
    /// there, however plainly it declares one. (What it is instead —
    /// evidence that a train's stack moved — is the train adoption's
    /// concern.)
    #[test]
    fn a_repository_with_a_status_comment_is_not_onboarded() {
        let ts = test_now();
        let record = train(PrNumber(1), ts);
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
            // #3 declares, and no ledger says the bot ever recorded it.
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
            comments_truncated: false,
            now: ts,
        });
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(1))],
            "only the ledgered edge is rebuilt"
        );
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
            comments_truncated: false,
            now: test_now(),
        });
        assert_eq!(declared(&outcome.events), vec![]);
    }
}
