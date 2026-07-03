//! Supplementary GitHub recovery (DESIGN §Recovery precedence, §Restart
//! safety step 5): what the status comment on a train's root PR implies for
//! the locally recovered record. Pure decision — the worker fetches the
//! comments ([`crate::effects::github::GitHubEffect::ListComments`]) and
//! applies the verdict; nothing here performs IO.
//!
//! # Why this exists under SQLite
//!
//! During normal operation the store commits state events *before* the
//! batch carrying the status-comment update executes, so the comment's
//! `recovery_seq` can never be ahead of a store that observed every commit.
//! The remote-ahead case is exactly the case where the local store
//! REGRESSED: the state DB was restored from a backup (or the disk rolled
//! back). Adopting the comment's record then prevents re-running an
//! already-landed irreversible operation — the backup's record may still
//! say `SquashPending` for a squash the real world has already seen.
//!
//! # Trust
//!
//! Only comments authored by the bot itself are considered (the author
//! check is DESIGN's defence against forged state injection), and only
//! records for the same *train incarnation*: the same `original_root_pr`
//! AND the same `started_at`. The root PR's comment history legitimately
//! contains status comments from previous trains on the same root
//! (stopped/completed and later restarted); matching on the pair keeps a
//! dead train's high `recovery_seq` from hijacking its successor.

use crate::effects::github::CommentData;
use crate::status::parse::parse_status_comment;
use crate::types::{CommentId, TrainRecord};

/// The verdict on a train's status comment at recovery time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommentRecovery {
    /// The comment's record is ahead (`recovery_seq` strictly greater):
    /// adopt it wholesale. `status_comment_id` is already repaired to the
    /// comment it was found in.
    Adopt(Box<TrainRecord>),
    /// The local record is current and the live comment already embeds it
    /// (same incarnation, same `recovery_seq`): keep both.
    KeepLocal,
    /// The local record is current but the recorded comment's content is
    /// behind it — the common crash shape: events commit before the
    /// best-effort `UpdateComment` runs — or unparseable. The worker must
    /// rewrite the comment body from the local record BEFORE the train
    /// resumes: the comment is the only recovery source if the DB is lost
    /// in the resume window, and a stale `recovery_seq` there cannot
    /// prevent replaying work already performed (Codex M6 review round 3).
    RefreshComment(CommentId),
    /// The local record is current but its `status_comment_id` points at a
    /// comment that no longer exists, while the bot's status comment for
    /// this train lives at this id (a crash between the initial post and
    /// `StatusCommentPosted`, or a restore from a backup predating a
    /// delete-and-repost): record the id, then refresh the body (the found
    /// comment's content is at best as old as the local record).
    RepairCommentId(CommentId),
    /// The status comment is gone entirely: local state stands, but the
    /// worker must re-post the off-disk backup and record the fresh id
    /// (`StatusCommentPosted`) before resuming — the engine's own self-heal
    /// runs only at idle evaluations, which a cascade resumed mid-phase may
    /// never pass. (NOT via `TrainRecordAdopted`: that event is a ledger
    /// boundary, and here the local intent ledger is genuine and needed.)
    RepostBackup,
}

/// Decides recovery for `local` given the root PR's comments.
pub fn decide_comment_recovery(
    local: &TrainRecord,
    comments: &[CommentData],
    bot_user_id: u64,
) -> CommentRecovery {
    // The best candidate: bot-authored, parseable, same train incarnation,
    // maximal recovery_seq (ties resolved toward the later comment id —
    // deterministic, and a re-posted comment supersedes its predecessor).
    let best: Option<(CommentId, TrainRecord)> = comments
        .iter()
        .filter(|c| c.author_id == bot_user_id)
        .filter_map(|c| {
            let record = parse_status_comment(&c.body).ok()?;
            (record.original_root_pr == local.original_root_pr
                && record.started_at == local.started_at)
                .then_some((c.id, record))
        })
        .max_by_key(|(id, record)| (record.recovery_seq, *id));

    if let Some((id, record)) = &best
        && record.recovery_seq > local.recovery_seq
    {
        let mut adopted = record.clone();
        adopted.status_comment_id = Some(*id);
        return CommentRecovery::Adopt(Box::new(adopted));
    }

    match local.status_comment_id {
        Some(local_id) => {
            if let Some(live) = comments.iter().find(|c| c.id == local_id) {
                // Remote-ahead was handled above (Adopt), so the live
                // content is at most as new as the local record. It is a
                // sound backup only if it embeds exactly this record;
                // anything else — behind after a crash-before-update,
                // mangled by an edit — must be rewritten before the train
                // resumes.
                let fresh = parse_status_comment(&live.body).is_ok_and(|r| {
                    r.original_root_pr == local.original_root_pr
                        && r.started_at == local.started_at
                        && r.recovery_seq == local.recovery_seq
                });
                if fresh {
                    CommentRecovery::KeepLocal
                } else {
                    CommentRecovery::RefreshComment(local_id)
                }
            } else if let Some((id, _)) = best {
                // The recorded comment is gone, but the bot's status
                // comment for this train lives elsewhere: point at it.
                CommentRecovery::RepairCommentId(id)
            } else {
                // Gone without replacement: the worker re-posts the backup.
                CommentRecovery::RepostBackup
            }
        }
        // No id recorded, but the bot's comment for this incarnation is
        // live: the crash landed after the initial `PostComment` succeeded
        // and before `StatusCommentPosted` was appended. Attach to it —
        // posting again would leave a duplicate status comment (Codex M6
        // review round 2, P3).
        None if best.is_some() => {
            CommentRecovery::RepairCommentId(best.expect("checked is_some").0)
        }
        // Never posted (crash before the preflight's comment landed): the
        // engine's self-healing posts it; nothing to decide here.
        None => CommentRecovery::KeepLocal,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use proptest::prelude::*;

    use super::*;
    use crate::status::format::format_status_comment;
    use crate::test_utils::arb_train_record;
    use crate::types::PrNumber;

    const BOT: u64 = 424_242;
    const USER: u64 = 7;

    fn comment(id: u64, author_id: u64, record: &TrainRecord) -> CommentData {
        CommentData {
            id: CommentId(id),
            author_id,
            body: format_status_comment(record, "status").unwrap(),
        }
    }

    /// A record that is `local` but `bump` recovery_seq bumps ahead.
    fn ahead_of(local: &TrainRecord, bump: u64) -> TrainRecord {
        let mut r = local.clone();
        r.recovery_seq = local.recovery_seq + bump;
        r
    }

    /// What adopting `record` from a comment must yield: the comment format
    /// truncates unbounded fields (error message/stderr), so the adopted
    /// record is the ROUND-TRIPPED one, not the original.
    fn as_adopted(record: &TrainRecord, id: u64) -> TrainRecord {
        let mut r =
            parse_status_comment(&format_status_comment(record, "status").unwrap()).unwrap();
        r.status_comment_id = Some(CommentId(id));
        r
    }

    proptest! {
        /// Non-bot comments never influence recovery, however far "ahead"
        /// they claim to be: forged state is ignored.
        #[test]
        fn forged_comments_are_ignored(local in arb_train_record()) {
            let forged = ahead_of(&local, 100);
            let comments = vec![comment(1, USER, &forged)];
            let verdict = decide_comment_recovery(&local, &comments, BOT);
            prop_assert!(!matches!(verdict, CommentRecovery::Adopt(_)));
        }

        /// A bot comment for the same train incarnation is adopted iff its
        /// recovery_seq is strictly ahead; the adopted record carries the
        /// comment's id.
        #[test]
        fn strictly_ahead_bot_records_are_adopted(
            local in arb_train_record(),
            bump in 0u64..3,
        ) {
            let remote = ahead_of(&local, bump);
            let comments = vec![comment(9, BOT, &remote)];
            let verdict = decide_comment_recovery(&local, &comments, BOT);
            if bump > 0 {
                let expected = as_adopted(&remote, 9);
                prop_assert_eq!(verdict, CommentRecovery::Adopt(Box::new(expected)));
            } else {
                prop_assert!(!matches!(verdict, CommentRecovery::Adopt(_)));
            }
        }

        /// A previous train's comment on the same root — same PR, different
        /// `started_at` — never hijacks the current train, no matter its
        /// recovery_seq.
        #[test]
        fn a_dead_trains_comment_never_hijacks_its_successor(
            local in arb_train_record(),
        ) {
            let mut dead = ahead_of(&local, 1000);
            dead.started_at = local.started_at + chrono::Duration::seconds(1);
            let comments = vec![comment(1, BOT, &dead)];
            let verdict = decide_comment_recovery(&local, &comments, BOT);
            prop_assert!(!matches!(verdict, CommentRecovery::Adopt(_)));
        }

        /// With several bot candidates, the maximal recovery_seq wins, and
        /// seq ties resolve to the later comment id.
        #[test]
        fn the_maximal_candidate_wins(local in arb_train_record()) {
            let a = ahead_of(&local, 1);
            let b = ahead_of(&local, 2);
            let comments = vec![
                comment(1, BOT, &b),
                comment(2, BOT, &a),
                comment(3, BOT, &b),
            ];
            let verdict = decide_comment_recovery(&local, &comments, BOT);
            let expected = as_adopted(&b, 3);
            prop_assert_eq!(verdict, CommentRecovery::Adopt(Box::new(expected)));
        }
    }

    fn local_at(seq: u64, comment_id: Option<u64>) -> TrainRecord {
        let mut r = TrainRecord::new(
            PrNumber(1),
            Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap(),
        );
        r.recovery_seq = seq;
        r.status_comment_id = comment_id.map(CommentId);
        r
    }

    #[test]
    fn a_current_comment_keeps_local() {
        let local = local_at(5, Some(3));
        let embedded = local_at(5, None);
        let comments = vec![comment(3, BOT, &embedded)];
        assert_eq!(
            decide_comment_recovery(&local, &comments, BOT),
            CommentRecovery::KeepLocal
        );
    }

    /// The common crash shape: events committed, the best-effort update
    /// never ran, so the live comment is behind the store. It must be
    /// rewritten before the train resumes.
    #[test]
    fn a_stale_comment_is_refreshed() {
        let local = local_at(5, Some(3));
        let behind = local_at(4, None);
        let comments = vec![comment(3, BOT, &behind)];
        assert_eq!(
            decide_comment_recovery(&local, &comments, BOT),
            CommentRecovery::RefreshComment(CommentId(3))
        );
    }

    /// A mangled (unparseable) live comment is no backup at all: rewrite it.
    #[test]
    fn a_mangled_comment_is_refreshed() {
        let local = local_at(5, Some(3));
        let comments = vec![CommentData {
            id: CommentId(3),
            author_id: BOT,
            body: "someone edited this".to_owned(),
        }];
        assert_eq!(
            decide_comment_recovery(&local, &comments, BOT),
            CommentRecovery::RefreshComment(CommentId(3))
        );
    }

    #[test]
    fn deleted_comment_with_live_replacement_repairs_the_id() {
        // Local points at comment 3 (gone); the bot's comment lives at 8
        // with a seq no greater than local's.
        let local = local_at(5, Some(3));
        let replacement = local_at(5, None);
        let comments = vec![comment(8, BOT, &replacement)];
        assert_eq!(
            decide_comment_recovery(&local, &comments, BOT),
            CommentRecovery::RepairCommentId(CommentId(8))
        );
    }

    #[test]
    fn deleted_comment_without_replacement_reposts_the_backup() {
        let local = local_at(5, Some(3));
        assert_eq!(
            decide_comment_recovery(&local, &[], BOT),
            CommentRecovery::RepostBackup
        );
    }

    #[test]
    fn never_posted_keeps_local() {
        let local = local_at(0, None);
        assert_eq!(
            decide_comment_recovery(&local, &[], BOT),
            CommentRecovery::KeepLocal
        );
    }

    /// The posted-but-unrecorded crash window: no id in the store, but the
    /// bot's comment for this incarnation is live — attach, don't re-post.
    #[test]
    fn posted_but_unrecorded_comment_is_attached() {
        let local = local_at(0, None);
        let comments = vec![comment(4, BOT, &local)];
        assert_eq!(
            decide_comment_recovery(&local, &comments, BOT),
            CommentRecovery::RepairCommentId(CommentId(4))
        );
    }
}
