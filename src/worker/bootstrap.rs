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
//! - **PR cache** — open and recently merged PRs, verbatim.
//! - **Predecessor topology** — `@bot predecessor #N` comments, honoring
//!   the same authorization the live pipeline enforces: only the PR
//!   author's declarations count, and the LAST declaration on a PR wins
//!   (comment id order; an edited-away declaration simply is not there).
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

use std::collections::HashMap;

use crate::commands::{Command, parse_command};
use crate::effects::PrData;
use crate::effects::github::CommentData;
use crate::persistence::event::StateEventPayload;
use crate::status::parse::parse_status_comment;
use crate::types::{MergeStateStatus, PrNumber, TrainRecord};

use super::pipeline::cache_fill_events;

/// The crawl's decision: events to append, the roots of adopted ACTIVE
/// trains (the caller marks them for M6 recovery), and adopted-train
/// members absent from the crawl (closed-unmerged PRs — the caller fetches
/// and caches them BEFORE appending, or the resumed train's evaluation
/// errors on a member it cannot see).
pub(crate) struct CrawlOutcome {
    pub events: Vec<StateEventPayload>,
    pub recovered_roots: Vec<PrNumber>,
    pub missing_members: Vec<PrNumber>,
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

/// Turns crawled facts into state events. Pure: fetching is the caller's;
/// `now` stamps the completion of stale records (see below).
///
/// `comments` pairs each crawled PR with its comments (id-ordered); PRs
/// missing from it simply contribute no declarations or records.
pub(crate) fn crawl_events(
    default_branch: &str,
    open_prs: &[PrData],
    merged_prs: &[PrData],
    comments: &[(PrNumber, Vec<CommentData>)],
    bot_name: &str,
    bot_user_id: u64,
    now: chrono::DateTime<chrono::Utc>,
) -> CrawlOutcome {
    let mut events = vec![StateEventPayload::DefaultBranchSet {
        branch: default_branch.to_owned(),
    }];

    // PR cache fills first: declarations and adoptions below refer to them,
    // and `apply_event` skips events about unknown PRs.
    let all_prs: Vec<&PrData> = open_prs.iter().chain(merged_prs.iter()).collect();
    for pr in &all_prs {
        events.extend(cache_fill_events(pr.number, pr, MergeStateStatus::Unknown));
    }
    let authors: HashMap<PrNumber, u64> = all_prs.iter().map(|p| (p.number, p.author_id)).collect();

    // Predecessor declarations: author-only (the live pipeline's rule),
    // last declaration on a PR wins, the bot never declares. EDITED
    // comments are refused outright: the API reports only the original
    // author, never the editor, so an edited body cannot be attributed —
    // honoring it would reopen the edit-impersonation hole the live path
    // closes by authorizing the SENDER (Codex crawl review round 2).
    let mut declared_targets: Vec<PrNumber> = Vec::new();
    for (pr, pr_comments) in comments {
        let Some(&author) = authors.get(pr) else {
            continue;
        };
        let mut last: Option<(PrNumber, crate::types::CommentId)> = None;
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
            last = Some((target, comment.id));
        }
        if let Some((predecessor, comment_id)) = last {
            declared_targets.push(predecessor);
            events.push(StateEventPayload::PredecessorDeclared {
                pr: *pr,
                predecessor,
                comment_id,
            });
        }
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
    let merged_numbers: std::collections::HashSet<PrNumber> = all_prs
        .iter()
        .filter(|p| p.state.is_merged())
        .map(|p| p.number)
        .collect();
    let crawled_numbers: std::collections::HashSet<PrNumber> =
        all_prs.iter().map(|p| p.number).collect();

    let mut recovered_roots = Vec::new();
    // Declaration targets outside the crawl are fetched individually,
    // exactly like adopted-train members: `is_root` must see the target's
    // real state (merged-beyond-the-window, closed) rather than wedge on
    // an invisible PR; the declaration itself persists, matching the live
    // path's record-then-validate-loudly order (Codex crawl review
    // round 2).
    let mut missing_members: Vec<PrNumber> = declared_targets
        .into_iter()
        .filter(|t| !crawled_numbers.contains(t))
        .collect();
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
        if record.state.is_active() {
            recovered_roots.push(root);
            for member in members(&record) {
                if !crawled_numbers.contains(&member) && !missing_members.contains(&member) {
                    missing_members.push(member);
                }
            }
        }
        events.push(StateEventPayload::TrainRecordAdopted {
            root_pr: root,
            record,
        });
    }
    missing_members.sort_unstable();
    missing_members.dedup();

    CrawlOutcome {
        events,
        recovered_roots,
        missing_members,
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
    /// author's comments declare, and the last declaration wins.
    #[test]
    fn declarations_are_author_only_and_last_wins() {
        let open = vec![pr(1, AUTHOR, PrState::Open), pr(2, AUTHOR, PrState::Open)];
        let comments = vec![(
            PrNumber(2),
            vec![
                comment(1, STRANGER, "@merge-train predecessor #9"),
                comment(2, AUTHOR, "@merge-train predecessor #7"),
                comment(3, AUTHOR, "@merge-train predecessor #1"),
            ],
        )];
        let outcome = crawl_events(
            "main",
            &open,
            &[],
            &comments,
            "merge-train",
            BOT,
            test_now(),
        );
        assert_eq!(declared(&outcome.events), vec![(PrNumber(2), PrNumber(1))]);
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

        let outcome = crawl_events(
            "main",
            &open,
            &[],
            &comments,
            "merge-train",
            BOT,
            test_now(),
        );
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
        let outcome = crawl_events(
            "main",
            &open,
            &[],
            &comments,
            "merge-train",
            BOT,
            test_now(),
        );
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
        let outcome = crawl_events(
            "main",
            &open,
            &[],
            &comments,
            "merge-train",
            BOT,
            test_now(),
        );
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
        let open = vec![pr(1, AUTHOR, PrState::Open), pr(2, AUTHOR, PrState::Open)];
        let mut edited = comment(1, AUTHOR, "@merge-train predecessor #1");
        edited.edited = true;
        let comments = vec![(PrNumber(2), vec![edited])];
        let outcome = crawl_events(
            "main",
            &open,
            &[],
            &comments,
            "merge-train",
            BOT,
            test_now(),
        );
        assert!(
            declared(&outcome.events).is_empty(),
            "an edited body has an unknowable author; fail closed"
        );
    }

    /// A declaration target outside the crawl (merged beyond the window,
    /// closed, or a typo) is handed back for individual fetching — exactly
    /// like an adopted train's members — so `is_root` sees the target's
    /// real state instead of wedging on an invisible PR. The declaration
    /// itself persists (the live path records first, validates loudly at
    /// start).
    #[test]
    fn uncrawled_declaration_targets_are_fetched() {
        let open = vec![pr(2, AUTHOR, PrState::Open)];
        let comments = vec![(
            PrNumber(2),
            vec![comment(1, AUTHOR, "@merge-train predecessor #77")],
        )];
        let outcome = crawl_events(
            "main",
            &open,
            &[],
            &comments,
            "merge-train",
            BOT,
            test_now(),
        );
        assert_eq!(declared(&outcome.events), vec![(PrNumber(2), PrNumber(77))]);
        assert_eq!(outcome.missing_members, vec![PrNumber(77)]);
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
        let outcome = crawl_events("main", &[], &merged, &comments, "merge-train", BOT, ts);
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
        let outcome = crawl_events("main", &open, &[], &comments, "merge-train", BOT, ts);
        assert_eq!(outcome.recovered_roots, vec![PrNumber(1)]);
        assert_eq!(outcome.missing_members, vec![PrNumber(3)]);
    }

    /// Cache fills precede declarations and adoptions, so `apply_event`
    /// (which skips unknown PRs) accepts them.
    #[test]
    fn fills_precede_declarations_and_adoptions() {
        let ts = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let record = TrainRecord::new(PrNumber(1), ts);
        let body = format_status_comment(&record, "s").unwrap();
        let open = vec![pr(1, AUTHOR, PrState::Open), pr(2, AUTHOR, PrState::Open)];
        let comments = vec![
            (PrNumber(1), vec![comment(1, BOT, &body)]),
            (
                PrNumber(2),
                vec![comment(2, AUTHOR, "@merge-train predecessor #1")],
            ),
        ];
        let outcome = crawl_events(
            "main",
            &open,
            &[],
            &comments,
            "merge-train",
            BOT,
            test_now(),
        );
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
