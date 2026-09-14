//! Train adoption from the bot's status comments: the pure judgement half
//! of the crawl. Given a root's records and the present the crawl
//! fetched, [`choose`] says which record is the root's, [`Footprint::of`]
//! says what that train is made of, and [`judge`] says what becomes of it
//! — one [`Verdict`], computed in one place, that the crawl only
//! dispatches on.
//!
//! The rules here are the owner's ruling for the recovery path: louder
//! than live, never quieter. Live aborts an active train whose stack
//! changes under it; after a database loss the frozen set cannot be fully
//! trusted, so the crawl aborts on anything the present disagrees with,
//! and a stop-shaped false positive costs a `start`.

use std::collections::{HashMap, HashSet};

use crate::state::RepoState;
use crate::state::descendants::collect_all_descendants;
use crate::types::{
    CommentId, PrNumber, PrState, TrainError, TrainErrorKind, TrainLineage, TrainRecord,
};

/// Which of the bot's trusted records on a root is the root's record: the
/// newest INCARNATION — by comment id, which GitHub makes globally
/// monotonic; `started_at` is a wall clock that can step backwards or
/// collide across a stop-and-restart — and within that incarnation
/// (records sharing its `started_at`) the highest `recovery_seq`, ties to
/// the later comment. A train's own comment is UPDATED in place as it
/// advances, so the highest id on a root is its newest incarnation; a
/// delayed duplicate post of the same incarnation can outrank the live
/// comment by id while carrying an older sequence, and the record judged
/// must be the one supplementary recovery would adopt — the highest
/// sequence — or the checks here run on the wrong record (Codex trains
/// review, P1).
pub(crate) fn choose(records: &[(CommentId, TrainRecord)]) -> Option<(CommentId, TrainRecord)> {
    let (_, newest) = records.iter().max_by_key(|(id, _)| *id)?;
    records
        .iter()
        .filter(|(_, r)| r.started_at == newest.started_at)
        .max_by_key(|(id, r)| (r.recovery_seq, *id))
        .cloned()
}

/// What a train is made of, computed once per record against the crawled
/// topology.
pub(crate) struct Footprint {
    /// Every PR the RECORD names: root, current, frozen descendants, and
    /// the whole stack it knew at the freeze.
    pub named: Vec<PrNumber>,
    /// The train's OWN set: what it names, plus the members it already
    /// merged or skipped — its past, which a later step's record no longer
    /// lists while a quiet crawl restores their ledger edges all the same.
    pub core: HashSet<PrNumber>,
    /// The core plus the crawled descendant closure of the root and the
    /// current PR: the stack as it stands now.
    pub stack: HashSet<PrNumber>,
    /// What the train OWNS, as the live path's `train_involving` sees it:
    /// what it names plus that closure. An IDLE record names only its root
    /// while a whole stack may hang off it.
    pub owned: HashSet<PrNumber>,
    /// The current PR's direct descendants AS FROZEN, for a mid-phase
    /// record; an idle record has no frontier yet.
    pub frontier: Option<Vec<PrNumber>>,
    /// Whether the record says what its stack was (`known_stack`), or is
    /// from before that field existed and falls back to the comment-id
    /// watermark.
    pub recorded_stack: bool,
}

impl Footprint {
    pub(crate) fn of(topology: &RepoState, record: &TrainRecord) -> Footprint {
        let progress = record.cascade_phase.progress();
        let mut named = vec![record.original_root_pr, record.current_pr];
        if let Some(p) = progress {
            named.extend_from_slice(p.frozen_descendants());
            for pr in p.known_stack() {
                if !named.contains(pr) {
                    named.push(*pr);
                }
            }
        }
        let mut core: HashSet<PrNumber> = named.iter().copied().collect();
        if let Some(p) = progress {
            core.extend(p.completed().iter().copied());
            core.extend(p.skipped().iter().copied());
        }
        let mut closure = HashSet::new();
        for anchor in [record.original_root_pr, record.current_pr] {
            closure.extend(collect_all_descendants(
                anchor,
                &topology.descendants,
                &topology.prs,
            ));
        }
        let stack: HashSet<PrNumber> = core.union(&closure).copied().collect();
        let owned: HashSet<PrNumber> = named.iter().copied().chain(closure).collect();
        Footprint {
            named,
            core,
            stack,
            owned,
            frontier: progress.map(|p| p.frozen_descendants().to_vec()),
            recorded_stack: progress.is_some_and(|p| !p.known_stack().is_empty()),
        }
    }
}

/// The present the crawl fetched, as far as a train's judgement reads it.
pub(crate) struct Present<'a> {
    pub topology: &'a RepoState,
    pub default_branch: &'a str,
    /// PRs the crawl found merged.
    pub merged: &'a HashSet<PrNumber>,
    /// PRs a permanent `GetPr` failure could not fetch.
    pub unfetchable: &'a HashSet<PrNumber>,
    /// The crawl could not read every PR's comments.
    pub comments_truncated: bool,
    /// Every trusted record's lineage, by the PR it sits on.
    pub lineages: &'a HashMap<PrNumber, Vec<TrainLineage>>,
    /// Predecessor-shaped comments no ledger accounts for, as (source,
    /// target).
    pub unledgered: &'a [(PrNumber, PrNumber)],
}

/// Why an adopted train is aborted rather than resumed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Abort {
    /// A newer status-shaped bot comment on the root is not in the bot's
    /// own bytes, or does not parse: the bot's unknown last word there.
    /// Aborted durably, so a later loss does not read the older record as
    /// active once the live sync has rewritten the evidence (Codex trains
    /// review, P2).
    Barred(CommentId),
    /// Another record adopted active shares a PR with this one.
    Overlaps(PrNumber),
    /// The stack grew, or moved onto the frontier, or an unledgered
    /// declaration touches it.
    Extended,
    /// The current PR or a frozen member no longer has a predecessor of the
    /// train's own.
    Severed,
    /// The root was declared onto an open PR: no longer a root.
    RootReparented,
    /// The root or the current PR closed unmerged.
    PrimaryClosed,
    /// The repository's default branch is not the one the train was
    /// created against.
    DefaultBranchChanged { created_against: String },
    /// The crawl could not read every PR's comments.
    Truncated,
    /// A PR the record names could not be fetched.
    Unreachable(PrNumber),
    /// An open member's base is no longer its still-open predecessor's
    /// branch: retargeted during the gap, and the resume path enters the
    /// phase past the base validation a step would have run.
    MemberRetargeted(PrNumber),
    /// An open PR sits behind merged history the record does not know:
    /// the train advanced past what its record says, and driven as
    /// recorded it would complete with that tail abandoned.
    AdvancedPastRecord(PrNumber),
}

impl Abort {
    pub(crate) fn error(&self, root: PrNumber, default_branch: &str) -> TrainError {
        match self {
            Abort::Barred(comment) => TrainError::new(
                TrainErrorKind::InternalInvariantViolation,
                format!(
                    "PR {root}'s train cannot be recovered: the bot's newest status comment on \
                     it (comment {comment}) was edited by somebody else, so its last word is \
                     unknown — re-issue `@merge-train start` if the train should run."
                ),
            ),
            Abort::Overlaps(other) => TrainError::new(
                TrainErrorKind::InternalInvariantViolation,
                format!(
                    "PR {root}'s train and PR {other}'s train were both recorded as active \
                     and share a PR; after a database loss the bot cannot tell which is live, \
                     so neither resumes — re-issue `@merge-train start` on the one that should."
                ),
            ),
            Abort::Extended => TrainError::new(
                TrainErrorKind::PredecessorChanged,
                format!(
                    "PR {root}'s stack was extended while its train was interrupted (a new \
                     predecessor declaration appeared); a merge train cannot safely resume \
                     over changed topology — re-issue `@merge-train start`."
                ),
            ),
            Abort::Severed => TrainError::new(
                TrainErrorKind::PredecessorChanged,
                format!(
                    "PR {root}'s stack changed while its train was interrupted (a member no \
                     longer declares its predecessor); a merge train cannot safely resume over \
                     changed topology — re-issue `@merge-train start`."
                ),
            ),
            Abort::RootReparented => TrainError::new(
                TrainErrorKind::PredecessorChanged,
                format!(
                    "PR {root} was declared onto another open PR while its train was \
                     interrupted, so it is no longer a root; a merge train cannot safely resume \
                     over changed topology — re-issue `@merge-train start`."
                ),
            ),
            Abort::PrimaryClosed => TrainError::new(
                TrainErrorKind::PrClosed,
                format!(
                    "PR {root}'s train cannot resume: its root or current PR was closed without \
                     merging while the train was interrupted."
                ),
            ),
            Abort::DefaultBranchChanged { created_against } => TrainError::new(
                TrainErrorKind::BaseBranchMismatch,
                format!(
                    "PR {root}'s train was created against default branch `{created_against}`, \
                     but the repository's default branch is now `{default_branch}`; a merge \
                     train cannot safely resume against a different default branch — re-issue \
                     `@merge-train start`."
                ),
            ),
            Abort::Truncated => TrainError::new(
                TrainErrorKind::ApiError,
                format!(
                    "PR {root}'s train cannot be recovered: the bot hit its comment-listing cap \
                     while crawling this repository, so some PRs' records were never read. \
                     Re-issue `@merge-train start` once the repository is within the supported \
                     size."
                ),
            ),
            Abort::MemberRetargeted(member) => TrainError::new(
                TrainErrorKind::BaseBranchMismatch,
                format!(
                    "PR {root}'s train cannot resume: PR {member} no longer targets its \
                     predecessor's branch (retargeted while the train was interrupted) — \
                     re-issue `@merge-train start` once the stack is consistent."
                ),
            ),
            Abort::AdvancedPastRecord(tail) => TrainError::new(
                TrainErrorKind::InternalInvariantViolation,
                format!(
                    "PR {root}'s train cannot be recovered: its record predates merges the \
                     train has since made, and PR {tail} is still open behind them — \
                     re-issue `@merge-train start` on what remains."
                ),
            ),
            Abort::Unreachable(member) => TrainError::new(
                TrainErrorKind::ApiError,
                format!(
                    "PR {root}'s train references PR {member}, which the bot cannot fetch \
                     (deleted, or the token lost access); the train cannot resume — re-issue \
                     `@merge-train start` once the PR is reachable."
                ),
            ),
        }
    }
}

/// What becomes of an adopted record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Not active: adopted as it is, nothing resumes.
    Retired,
    /// Active on paper, finished in fact: adopted and completed by a real
    /// event, so the store owes the comment its final word.
    Complete,
    /// Active and resumable: handed back for M6 recovery.
    Recover,
    /// Active on paper, contradicted by the present: adopted and aborted.
    Abort(Abort),
}

/// Whether an active record has in fact FINISHED. Status updates are
/// best-effort, so a train that finished can leave an ACTIVE comment behind
/// — and an unfinished train necessarily has unmerged members. A record
/// whose every named PR but the root has merged has nothing left to do
/// (only where it SAYS what its stack was: without `known_stack` a
/// two-deep train cannot be told from a three-deep one whose tail is still
/// open). A stale FAN-OUT parent is completed by its children's records
/// naming its identity (root + `started_at`, compared by equality — never
/// clock order), on any PR and whatever phase the parent's record claims:
/// a lineage is written only by the bot, in a child's own record.
pub(crate) fn completes(
    root: PrNumber,
    record: &TrainRecord,
    footprint: &Footprint,
    present: &Present<'_>,
) -> bool {
    let all_named_merged = footprint.recorded_stack
        && footprint
            .named
            .iter()
            .skip(1) // the root legitimately merges early in the cascade
            .all(|m| present.merged.contains(m));
    let own_identity = TrainLineage {
        root,
        started_at: record.started_at,
    };
    let fanned_out = present
        .lineages
        .iter()
        .any(|(m, parents)| *m != root && parents.contains(&own_identity));
    record.state.is_active() && (all_named_merged || fanned_out)
}

/// Which active, unfinished trains OWN a PR in common. One PR belongs to at
/// most one active train (live `start` refuses a PR another active train
/// holds); two records that would be adopted active and share a PR are a
/// contradiction the crawl cannot resolve — comment ids do not order two
/// roots' records, and adopting both lets a `stop` on one leave the other
/// to merge the PR. Both abort.
pub(crate) fn overlapping(live: &[(PrNumber, &Footprint)]) -> HashMap<PrNumber, PrNumber> {
    live.iter()
        .filter_map(|(root, footprint)| {
            live.iter()
                .find(|(other, theirs)| {
                    other != root && !footprint.owned.is_disjoint(&theirs.owned)
                })
                .map(|(other, _)| (*root, *other))
        })
        .collect()
}

/// Whether the crawled topology EXTENDS the train's stack. Live aborts an
/// active train whose stack grows under it; after a DB loss the frozen set
/// cannot be fully trusted, so recovery aborts on any extension (owner
/// ruling, round 11).
///
/// - An OPEN PR outside the core declaring a stack member as its
///   predecessor. Only an open PR, as only an open PR is walked into a
///   frozen set: a merged member the train drove earlier is its own
///   history, one merged by hand is not one the train would drive, and a
///   sibling closed before the train started was outside the freeze's walk
///   exactly as it is outside this one. For a record that recorded its
///   stack, anything from outside it; for an older record, the fallback is
///   the comment-id watermark — a declaration owned by a lower id than the
///   train's first status comment provably predates the train.
/// - An OPEN PR the frozen set does not hold declaring the CURRENT PR as
///   its predecessor: it moved onto the frontier during the gap, a known
///   member as much as a new one, and driving the frozen frontier would
///   leave it never prepared (Codex trains review, P1).
///
/// Every declaration INTO the stack is checked, not only the closure walked
/// from the root: a gap that also severs a frozen member's own link leaves
/// that member outside the walk. Pure reorders within the frozen set are
/// not extensions: the cascade prepares each frozen descendant against the
/// current PR, not its live-declared predecessor.
fn extended(record: &TrainRecord, footprint: &Footprint, present: &Present<'_>) -> bool {
    let watermark = record.watermark.or(record.status_comment_id);
    present.topology.prs.values().any(|p| {
        let moved_onto_frontier = footprint.frontier.as_ref().is_some_and(|frozen| {
            p.number != record.original_root_pr
                && p.state.is_open()
                && p.predecessor == Some(record.current_pr)
                && !frozen.contains(&p.number)
        });
        moved_onto_frontier
            || !footprint.core.contains(&p.number)
                && p.state.is_open()
                && p.predecessor.is_some_and(|t| footprint.stack.contains(&t))
                && (footprint.recorded_stack
                    || match (p.predecessor_comment_id, watermark) {
                        (Some(declared), Some(mark)) => declared > mark,
                        _ => true,
                    })
    })
}

/// Whether an unledgered predecessor-shaped comment touches the stack:
/// naming a member as its predecessor, or sitting ON a member and naming
/// something else. Either way the topology the train froze against may
/// have moved in a way the crawl cannot reconstruct. An IDLE train has no
/// frozen set and would freeze a LEDGERED extension in at its next step;
/// an unledgered one has no edge installed, so no freeze can pick it up —
/// its stack is what it owns (Codex trains review, P1).
fn unledgered_touches(footprint: &Footprint, present: &Present<'_>) -> bool {
    let stack = if footprint.frontier.is_some() {
        &footprint.stack
    } else {
        &footprint.owned
    };
    present
        .unledgered
        .iter()
        .any(|(source, target)| stack.contains(target) || stack.contains(source))
}

/// Whether `pr` is the train's own HISTORY: in its core, or a merged PR
/// that chains through merged PRs back into it — a member the train merged
/// earlier, which no step's record lists. A merged stranger chains nowhere.
fn history(footprint: &Footprint, present: &Present<'_>, mut at: PrNumber) -> bool {
    let mut seen = HashSet::new();
    loop {
        if footprint.core.contains(&at) {
            return true;
        }
        let Some(p) = present.topology.prs.get(&at) else {
            return false;
        };
        if !matches!(p.state, PrState::Merged { .. }) || !seen.insert(at) {
            return false;
        }
        let Some(next) = p.predecessor else {
            return false;
        };
        at = next;
    }
}

/// Whether an OPEN member the record names — the current PR, a frozen
/// member, or a known member behind the frontier — no longer has a
/// predecessor of the train's own: none at all (unstacked during the gap;
/// a deleted declaration leaves no unledgered evidence either), or one
/// outside the train (re-declared onto another stack; a merged predecessor
/// is tolerated only as history). Live aborts when a member's edge
/// changes; the crawl cannot tell that abort was written, and a member
/// behind the frontier abandoned would leave the train finishing without
/// it (Codex trains review, P1). Only FETCHED, open members can be judged:
/// one the crawl has not seen is fetched for the next pass; a merged one
/// is history and a closed one was outside the freeze's walk.
fn severed(record: &TrainRecord, footprint: &Footprint, present: &Present<'_>) -> bool {
    footprint
        .named
        .iter()
        .filter(|m| **m != record.original_root_pr)
        .any(|m| {
            present.topology.prs.get(m).is_some_and(|c| {
                c.state.is_open()
                    && c.predecessor
                        .is_none_or(|t| !history(footprint, present, t))
            })
        })
}

/// An open member the record names whose base is no longer its still-open
/// predecessor's branch — the live rule `begin_step` enforces before a
/// phase, which the resume path enters past (Codex trains review, P1).
fn retargeted(
    record: &TrainRecord,
    footprint: &Footprint,
    present: &Present<'_>,
) -> Option<PrNumber> {
    footprint
        .named
        .iter()
        .filter(|m| **m != record.original_root_pr)
        .find(|m| {
            present.topology.prs.get(m).is_some_and(|c| {
                c.state.is_open()
                    && crate::state::validation::validate_base_branch_matches_predecessor(
                        c,
                        &present.topology.prs,
                    )
                    .is_err()
            })
        })
        .copied()
}

/// For a record with no frontier (idle), an OPEN PR reachable from the root
/// only through MERGED members: the train has advanced past what its record
/// says (`1 <- 2 <- 3`, #1 and #2 merged, #3 open), and walked through
/// open PRs alone the stack ends at the root — the engine's idle path would
/// then complete the train with #3 abandoned (Codex trains review, P2).
fn open_tail_behind_history(
    record: &TrainRecord,
    footprint: &Footprint,
    present: &Present<'_>,
) -> Option<PrNumber> {
    if footprint.frontier.is_some() {
        return None;
    }
    let prs = &present.topology.prs;
    let mut seen = HashSet::new();
    let mut queue = vec![record.original_root_pr];
    while let Some(at) = queue.pop() {
        if !seen.insert(at) {
            continue;
        }
        for next in present.topology.descendants.get(&at).into_iter().flatten() {
            let Some(p) = prs.get(next) else { continue };
            if p.state.is_open() && !footprint.owned.contains(next) {
                return Some(*next);
            }
            if p.state.is_merged() || p.state.is_open() {
                queue.push(*next);
            }
        }
    }
    None
}

/// Whether the root was declared onto an OPEN PR: a member whose edge
/// changed, which live aborts, and whose abort's status update can fail.
/// Recovered, it would be driven as a root it no longer is. A merged
/// predecessor is what every fan-out child's root has. One the crawl has
/// not fetched is not known merged; the fixpoint pass fetches it first.
fn root_reparented(record: &TrainRecord, present: &Present<'_>) -> bool {
    present
        .topology
        .prs
        .get(&record.original_root_pr)
        .and_then(|p| p.predecessor)
        .is_some_and(|t| {
            !present
                .topology
                .prs
                .get(&t)
                .is_some_and(|p| p.state.is_merged())
        })
}

/// Whether the root or the current PR CLOSED unmerged during the gap: the
/// train cannot go on, and live would have aborted it on the close.
/// Aborted before it resumes — resumed, it would push to its frozen
/// descendants before its next refetch of the root found the close.
fn primary_closed(record: &TrainRecord, present: &Present<'_>) -> bool {
    [record.original_root_pr, record.current_pr]
        .iter()
        .any(|m| {
            present
                .topology
                .prs
                .get(m)
                .is_some_and(|p| matches!(p.state, PrState::Closed))
        })
}

/// The verdict on one root's chosen record. `barred` is the newest
/// status-shaped bot comment on the root that is not a trusted record,
/// when it is newer than the record; `overlaps` the other active train
/// that owns a PR in common, when there is one.
pub(crate) fn judge(
    root: PrNumber,
    record: &TrainRecord,
    footprint: &Footprint,
    present: &Present<'_>,
    barred: Option<CommentId>,
    overlaps: Option<PrNumber>,
) -> Verdict {
    if !record.state.is_active() {
        return Verdict::Retired;
    }
    if let Some(comment) = barred {
        return Verdict::Abort(Abort::Barred(comment));
    }
    if completes(root, record, footprint, present) {
        return Verdict::Complete;
    }
    if let Some(other) = overlaps {
        return Verdict::Abort(Abort::Overlaps(other));
    }
    if extended(record, footprint, present) || unledgered_touches(footprint, present) {
        return Verdict::Abort(Abort::Extended);
    }
    if severed(record, footprint, present) {
        return Verdict::Abort(Abort::Severed);
    }
    if root_reparented(record, present) {
        return Verdict::Abort(Abort::RootReparented);
    }
    if primary_closed(record, present) {
        return Verdict::Abort(Abort::PrimaryClosed);
    }
    if let Some(member) = retargeted(record, footprint, present) {
        return Verdict::Abort(Abort::MemberRetargeted(member));
    }
    if let Some(tail) = open_tail_behind_history(record, footprint, present) {
        return Verdict::Abort(Abort::AdvancedPastRecord(tail));
    }
    if record.default_branch != present.default_branch {
        return Verdict::Abort(Abort::DefaultBranchChanged {
            created_against: record.default_branch.clone(),
        });
    }
    if present.comments_truncated {
        return Verdict::Abort(Abort::Truncated);
    }
    if let Some(member) = footprint
        .named
        .iter()
        .find(|m| present.unfetchable.contains(m))
    {
        return Verdict::Abort(Abort::Unreachable(*member));
    }
    Verdict::Recover
}

/// The verdict on a record adopted AFTER the crawl — supplementary recovery
/// finding a record ahead of the store's at a train's first evaluation —
/// judged as the crawl judges, against the store as it stands: its
/// footprint against the cached topology, overlaps against the OTHER
/// active trains the store holds (a replacement can name a member another
/// train owns, and `train_involving` returns only one root, so a stop could
/// leave the other running), and `unknown` — PRs the record names that the
/// store does not know — unreachable, the crawl's discovery being over
/// (Codex trains review, P1 twice).
pub(crate) fn judge_replacement(
    state: &RepoState,
    root: PrNumber,
    record: &TrainRecord,
    unknown: &HashSet<PrNumber>,
    barred: Option<CommentId>,
) -> Verdict {
    let merged: HashSet<PrNumber> = state
        .prs
        .values()
        .filter(|p| p.state.is_merged())
        .map(|p| p.number)
        .collect();
    let lineages = HashMap::new();
    let present = Present {
        topology: state,
        default_branch: &state.default_branch,
        merged: &merged,
        unfetchable: unknown,
        comments_truncated: false,
        lineages: &lineages,
        unledgered: &[],
    };
    let footprint = Footprint::of(state, record);
    let others: Vec<(PrNumber, Footprint)> = state
        .active_trains
        .iter()
        .filter(|(other, t)| **other != root && t.state.is_active())
        .map(|(other, t)| (*other, Footprint::of(state, t)))
        .collect();
    let mut live: Vec<(PrNumber, &Footprint)> = vec![(root, &footprint)];
    live.extend(others.iter().map(|(other, fp)| (*other, fp)));
    let overlaps = overlapping(&live).get(&root).copied();
    judge(root, record, &footprint, &present, barred, overlaps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::event::{StateEvent, StateEventPayload};
    use crate::persistence::snapshot::PersistedRepoSnapshot;
    use crate::types::{CascadePhase, DescendantProgress, Sha};

    fn state(events: &[StateEventPayload]) -> RepoState {
        let mut state = RepoState::from_snapshot(PersistedRepoSnapshot::new("main".to_owned()));
        for (seq, payload) in events.iter().enumerate() {
            state.apply_event(&StateEvent {
                seq: seq as u64,
                ts: crate::test_utils::test_timestamp(),
                payload: payload.clone(),
            });
        }
        state
    }

    fn opened(pr: u64, base: &str) -> StateEventPayload {
        StateEventPayload::PrOpened {
            pr: PrNumber(pr),
            head_sha: Sha::parse(pr.to_string().repeat(40)).unwrap(),
            head_ref: format!("pr-{pr}"),
            base_ref: base.to_owned(),
            is_draft: false,
        }
    }

    fn declared(pr: u64, predecessor: u64, comment: u64) -> StateEventPayload {
        StateEventPayload::PredecessorDeclared {
            pr: PrNumber(pr),
            predecessor: PrNumber(predecessor),
            comment_id: CommentId(comment),
        }
    }

    /// A replacement record is judged against the OTHER active trains the
    /// store holds: `1 <- 2 <- 3`, #2 merged; the crawl adopted #1's old
    /// idle record and an independent train on #3 as disjoint; the
    /// replacement for #1 names current #2 and frozen #3. Two active
    /// trains would own #3, and a stop on one would leave the other
    /// running (Codex trains review, P1).
    #[test]
    fn a_replacement_record_overlapping_another_active_train_aborts() {
        let ts = crate::test_utils::test_timestamp();
        let mut three = TrainRecord::new(PrNumber(3), ts);
        three.default_branch = "main".to_owned();
        let s = state(&[
            opened(1, "main"),
            opened(2, "pr-1"),
            opened(3, "pr-2"),
            declared(2, 1, 20),
            declared(3, 2, 30),
            StateEventPayload::PrMerged {
                pr: PrNumber(2),
                merge_sha: Sha::parse("b".repeat(40)).unwrap(),
            },
            StateEventPayload::TrainRecordAdopted {
                root_pr: PrNumber(3),
                record: three,
            },
        ]);
        let mut replacement = TrainRecord::new(PrNumber(1), ts);
        replacement.default_branch = "main".to_owned();
        replacement.current_pr = PrNumber(2);
        replacement.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::with_known_stack(
                vec![PrNumber(3)],
                vec![PrNumber(1), PrNumber(2), PrNumber(3)],
            ),
        };
        assert_eq!(
            judge_replacement(&s, PrNumber(1), &replacement, &HashSet::new(), None),
            Verdict::Abort(Abort::Overlaps(PrNumber(3)))
        );
        // Without the other train, the same record recovers.
        let mut alone = replacement.clone();
        alone.status_comment_id = None;
        let s = state(&[
            opened(1, "main"),
            opened(2, "pr-1"),
            opened(3, "pr-2"),
            declared(2, 1, 20),
            declared(3, 2, 30),
            StateEventPayload::PrMerged {
                pr: PrNumber(2),
                merge_sha: Sha::parse("b".repeat(40)).unwrap(),
            },
        ]);
        assert_eq!(
            judge_replacement(&s, PrNumber(1), &alone, &HashSet::new(), None),
            Verdict::Recover
        );
    }
}
