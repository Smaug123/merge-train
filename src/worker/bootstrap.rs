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
//!   the live command path runs: only the PR author's non-edited
//!   declarations count, the first VALID one on a PR wins, and an invalid
//!   or late-addition (merged-predecessor) edge is dropped exactly as the
//!   handler would drop it — so the crawl persists only edges the live path
//!   would have.
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
use crate::status::parse::parse_status_comment;
use crate::types::{MergeStateStatus, PrNumber, TrainError, TrainErrorKind, TrainRecord};

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
}

/// The PRs a train record involves: its root, current PR, and the frozen
/// descendant set its phase carries.
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

/// Whether the crawled topology extends `record`'s stack beyond what it
/// froze — a new PR declaring a stack member as its predecessor, appearing
/// in the train's descendant closure but absent from its frozen set. In
/// live operation such a declaration fires `topology_change_abort`; the
/// crawl records it as baseline, so the extension has to be detected here.
///
/// Only *extensions* are detected, not pure reorders or removals within the
/// frozen set: the cascade prepares each frozen descendant against
/// `current_pr` (the frozen frontier), not against its live-declared
/// predecessor, so a recovered train's git operations stay self-consistent
/// regardless of intra-set churn — and detecting removal/reorder cannot be
/// done without false-positives on the legitimate mid-cascade state where a
/// merged member blocks traversal to its still-pending children. An
/// extension, by contrast, is unambiguous and matches the reviewer's
/// scenario exactly. An `Idle`-phase train has no frozen set yet (it will
/// freeze against the current topology at its next `Preparing`), so it is
/// never flagged.
fn stack_extended(topology: &RepoState, record: &TrainRecord) -> bool {
    let Some(progress) = record.cascade_phase.progress() else {
        return false;
    };
    let mut frozen: HashSet<PrNumber> = progress.frozen_descendants().iter().copied().collect();
    frozen.insert(record.original_root_pr);
    frozen.insert(record.current_pr);
    [record.original_root_pr, record.current_pr]
        .into_iter()
        .flat_map(|anchor| collect_all_descendants(anchor, &topology.descendants, &topology.prs))
        .any(|d| !frozen.contains(&d))
}

/// Turns crawled facts into state events. Pure: fetching is the caller's;
/// `now` stamps the completion of stale records (see below).
///
/// `comments` pairs each crawled PR with its comments (id-ordered); PRs
/// missing from it simply contribute no declarations or records.
pub(crate) fn crawl_events(
    default_branch: &str,
    crawled_prs: &[PrData],
    comments: &[(PrNumber, Vec<CommentData>)],
    bot_name: &str,
    bot_user_id: u64,
    now: chrono::DateTime<chrono::Utc>,
) -> CrawlOutcome {
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
    for (pr, pr_comments) in comments {
        let Some(&author) = authors.get(pr) else {
            continue;
        };
        for comment in pr_comments {
            if comment.author_id != author || comment.author_id == bot_user_id {
                continue;
            }
            let Some(Command::Predecessor(target)) = parse_command(&comment.body, bot_name) else {
                continue;
            };
            if comment.edited {
                tracing::warn!(
                    %pr, comment = %comment.id,
                    "ignoring an EDITED predecessor declaration during the \
                     crawl (the editor cannot be verified); the author can \
                     re-declare in a fresh comment"
                );
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
    // fetch is for train discovery, not the edge.
    let crawled_numbers: HashSet<PrNumber> = all_prs.iter().map(|p| p.number).collect();
    let mut referenced_uncrawled: Vec<PrNumber> = Vec::new();
    for (_, _, target) in &candidates {
        if !crawled_numbers.contains(target) && !referenced_uncrawled.contains(target) {
            referenced_uncrawled.push(*target);
        }
    }

    // Record only VALID, non-late-addition declarations (Codex crawl review
    // round 5): the crawl bypasses the live command path, so an unvalidated
    // edge — a closed/missing/mismatched predecessor, a cycle — would wedge
    // `is_root` or fabricate a bogus stack extension, and the redelivered
    // webhook would treat the already-owned declaration as idempotent and
    // never reject it. A MERGED predecessor is a late addition the live
    // handler records nothing for (it answers `LateAddition` instead), so
    // it is skipped here too. This also subsumes round 2's uncrawled-target
    // fetch: `ListOpenPrs` returns every open PR, so an unrecorded
    // predecessor is necessarily closed or merged — either way not a valid
    // edge — and never needs a follow-up fetch.
    for (comment_id, pr, predecessor) in candidates {
        let recordable = topology.prs.contains_key(&pr)
            && !topology
                .prs
                .get(&predecessor)
                .is_some_and(|p| p.state.is_merged())
            && {
                let cached = topology.prs.get(&pr).expect("checked contains_key");
                match validate_predecessor_declaration(
                    cached,
                    predecessor,
                    &topology.prs,
                    default_branch,
                ) {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!(
                            %pr, %predecessor, error = %e,
                            "dropping an invalid crawled predecessor declaration"
                        );
                        false
                    }
                }
            };
        if !recordable {
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

    // Train recovery from the bot's status comments. Trust gates: authored
    // by the bot, parseable, and posted on its own root PR. Per root, the
    // latest incarnation at its highest recovery_seq (ties to the later
    // comment) wins.
    let mut best: HashMap<PrNumber, (TrainRecord, crate::types::CommentId)> = HashMap::new();
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
            let candidate_key = (record.started_at, record.recovery_seq, comment.id);
            let supersedes = best
                .get(&record.original_root_pr)
                .is_none_or(|(b, id)| candidate_key > (b.started_at, b.recovery_seq, *id));
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
        let all_members_merged = record.cascade_phase.progress().is_some()
            && members(&record)
                .iter()
                .skip(1) // the root legitimately merges early in the cascade
                .all(|m| merged_numbers.contains(m));
        if record.state.is_active() && all_members_merged {
            record.state = crate::types::TrainState::Completed { ended_at: now };
        }
        // A stack extended during the gap aborts, matching the live
        // topology-change abort (Codex crawl review round 4).
        let extended = record.state.is_active() && stack_extended(&topology, &record);
        if record.state.is_active() && !extended {
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
        }
    }
    referenced_uncrawled.sort_unstable();
    referenced_uncrawled.dedup();

    CrawlOutcome {
        events,
        recovered_roots,
        referenced_uncrawled,
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
        let outcome = crawl_events("main", &open, &comments, "merge-train", BOT, test_now());
        assert_eq!(
            declared(&outcome.events),
            vec![(PrNumber(2), PrNumber(1))],
            "the stranger's comment is ignored; the author's first valid one wins"
        );
    }

    /// The crawl runs the SAME validation the live command path does, and
    /// records only edges the handler would have persisted: a closed
    /// predecessor rejects, a merged one is a late addition (nothing
    /// recorded), a base mismatch rejects. Recording any of these would
    /// wedge `is_root` or fabricate a bogus stack extension (Codex crawl
    /// review round 5).
    #[test]
    fn invalid_declarations_are_dropped() {
        let open = vec![
            pr(1, AUTHOR, PrState::Open),
            child(2, AUTHOR, 1, PrState::Open), // #2 -> #1 (closed): rejected
            child(3, AUTHOR, 4, PrState::Open), // #3 -> #4 (merged): late addition
            pr(5, AUTHOR, PrState::Open),       // #6 -> #5 but base is main: mismatch
            pr(6, AUTHOR, PrState::Open),
        ];
        // #1 closed, #4 merged.
        let mut closed_one = pr(1, AUTHOR, PrState::Closed);
        closed_one.head_ref = "pr-1".to_owned();
        let open = {
            let mut v = open;
            v[0] = closed_one;
            v
        };
        let merged = vec![pr(
            4,
            AUTHOR,
            PrState::Merged {
                merge_commit_sha: Sha::parse("b".repeat(40)).unwrap(),
            },
        )];
        let comments = vec![
            (
                PrNumber(2),
                vec![comment(1, AUTHOR, "@merge-train predecessor #1")],
            ),
            (
                PrNumber(3),
                vec![comment(2, AUTHOR, "@merge-train predecessor #4")],
            ),
            (
                PrNumber(6),
                vec![comment(3, AUTHOR, "@merge-train predecessor #5")],
            ),
        ];
        let mut crawled = open;
        crawled.extend(merged);
        let outcome = crawl_events("main", &crawled, &comments, "merge-train", BOT, test_now());
        assert!(
            declared(&outcome.events).is_empty(),
            "every invalid/late-addition declaration is dropped, got {:?}",
            declared(&outcome.events)
        );
    }

    /// A bot status comment on its own root is adopted (id repaired), and
    /// an ACTIVE record is handed back for recovery marking.
    #[test]
    fn trains_recover_from_bot_status_comments() {
        let ts = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let mut record = TrainRecord::new(PrNumber(1), ts);
        record.recovery_seq = 4;
        let body = format_status_comment(&record, "mid-flight").unwrap();
        let open = vec![pr(1, AUTHOR, PrState::Open)];
        let comments = vec![(PrNumber(1), vec![comment(8, BOT, &body)])];

        let outcome = crawl_events("main", &open, &comments, "merge-train", BOT, test_now());
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
        let record = TrainRecord::new(PrNumber(1), ts);
        let body = format_status_comment(&record, "s").unwrap();
        let open = vec![pr(1, AUTHOR, PrState::Open), pr(2, AUTHOR, PrState::Open)];
        let comments = vec![
            (PrNumber(1), vec![comment(1, STRANGER, &body)]),
            (PrNumber(2), vec![comment(2, BOT, &body)]),
        ];
        let outcome = crawl_events("main", &open, &comments, "merge-train", BOT, test_now());
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
        let mut old = TrainRecord::new(PrNumber(1), t0);
        old.recovery_seq = 50;
        let mut newer = TrainRecord::new(PrNumber(1), t1);
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
        let outcome = crawl_events("main", &open, &comments, "merge-train", BOT, test_now());
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
        let outcome = crawl_events("main", &open, &comments, "merge-train", BOT, test_now());
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
        let outcome = crawl_events("main", &open, &comments, "merge-train", BOT, test_now());
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
        let mut record = TrainRecord::new(PrNumber(1), ts);
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
        let outcome = crawl_events("main", &merged, &comments, "merge-train", BOT, ts);
        assert!(outcome.recovered_roots.is_empty(), "no zombie");
        let adopted = outcome
            .events
            .iter()
            .find_map(|e| match e {
                StateEventPayload::TrainRecordAdopted { record, .. } => Some(record),
                _ => None,
            })
            .expect("adopted (as completed)");
        assert!(
            matches!(adopted.state, crate::types::TrainState::Completed { .. }),
            "adopted as completed, so application removes it"
        );
    }

    /// A genuinely unfinished train — an unmerged member — IS recovered,
    /// and members the crawl did not see (closed-unmerged PRs) are handed
    /// back for individual fetching.
    #[test]
    fn unfinished_trains_recover_and_report_uncrawled_members() {
        use crate::types::{CascadePhase, DescendantProgress, PrState};
        let ts = test_now();
        let mut record = TrainRecord::new(PrNumber(1), ts);
        record.cascade_phase = CascadePhase::Preparing {
            progress: DescendantProgress::new(vec![PrNumber(2), PrNumber(3)]),
        };
        let body = format_status_comment(&record, "mid").unwrap();
        // PR 2 is open; PR 3 was closed unmerged during the outage — the
        // crawl's lists never see it.
        let open = vec![pr(1, AUTHOR, PrState::Open), pr(2, AUTHOR, PrState::Open)];
        let comments = vec![(PrNumber(1), vec![comment(1, BOT, &body)])];
        let outcome = crawl_events("main", &open, &comments, "merge-train", BOT, ts);
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
        let mut record = TrainRecord::new(PrNumber(1), ts);
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
        let outcome = crawl_events("main", &open, &comments, "merge-train", BOT, ts);
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
        let mut record = TrainRecord::new(PrNumber(1), ts);
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
        let outcome = crawl_events("main", &open, &comments, "merge-train", BOT, ts);
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
        let record = TrainRecord::new(PrNumber(1), ts);
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
        let outcome = crawl_events("main", &open, &comments, "merge-train", BOT, test_now());
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
}
