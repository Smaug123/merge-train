//! Codex crawl review rounds 1–18 were each one hand-explored corner of a
//! single question: after the state DB is destroyed, does first-contact
//! recovery (`worker/bootstrap::crawl_events` + the M6 resume path) leave
//! the system in a state the live path could defend? This module mechanizes
//! the reviewer, exactly as `interleaving` (above) mechanized the
//! saga-ordering review rounds.
//!
//! - [`a_db_loss_with_a_quiet_gap_is_unobservable`] — the differential
//!   property. One generated history (stack shape; valid, junk, stranger,
//!   restated, and retracted declarations; starts and stops) runs in two
//!   worlds: L never crashes; C crashes at a generated saga depth, loses
//!   the whole DB, and recovers from the crawl. When nothing touched GitHub
//!   during the outage, recovery owes EQUIVALENCE: same cache (only
//!   unreferenced closed-unmerged PRs may be forgotten — neither list
//!   endpoint returns them), same predecessor edges under the same owning
//!   comments, same train outcomes, same merges, plus each world's own
//!   absolutes (≤1 squash, exact store↔GitHub agreement, matched intent
//!   ledgers, empty command backlog).
//!
//! The QUIET gap is the whole scope here: nothing touched GitHub while the
//! database was gone. Comment EDITS are therefore excluded — a pre-loss
//! edit is honored live by authorizing the *editor* (`sender_id`), which
//! the crawl cannot reconstruct from `ListComments` (rounds 2/15), and that
//! divergence is documented, not accidental. Likewise a stranger's comment
//! *deletion*: GitHub loses the comment either way, but live keeps the
//! unauthorized retraction's edge while the crawl cannot see it. A NOISY
//! gap — reality moving while the bot is dead — makes equivalence
//! unattainable by design and is owed the documented envelope instead; it
//! gets its own property.
//!
//! One residual the differential property EXEMPTS rather than excludes:
//! a command acknowledged but not yet answered when the DB dies is gone —
//! GitHub never redelivers an acked webhook — so its effects never happen.
//! The loss is bounded (the addressed stack) and visible (an ack reaction
//! with no follow-up); the user re-issues. See [`command_loss_exemptions`].
//!
//! Every case does real git work; case counts are deliberately small. Raise
//! `PROPTEST_CASES` when touching the crawl or recovery.

use std::collections::{BTreeSet, HashSet};

use proptest::prelude::*;
use proptest::sample::Index;
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;

use super::*;
use crate::commands::{Command, parse_command};
use crate::persistence::event::StateEventPayload;
use crate::state::descendants::collect_all_descendants;
use crate::status::parse::parse_status_comment;
use crate::store::DurableCommand;
use crate::types::{CachedPr, PrState, TrainRecord, TrainState};

// ── World building ──

/// A generated stack on a real repo: `bases[k]` is PR k+1's base — 0
/// for the default branch, otherwise the (1-based) number of an earlier
/// PR, so shapes cover independent roots, linear chains, and fan-out.
/// Every PR with a dependent gets an extra commit after the forks, so
/// preparation does real merge work (as `World::linear_stack` does).
fn build_world(bases: &[usize]) -> (World, Vec<Sha>) {
    let (temp, config, _initial) = create_test_repo_with_origin();
    let mut fake_prs = HashMap::new();
    let mut heads = Vec::new();
    for (k, &base_idx) in bases.iter().enumerate() {
        let number = k + 1;
        let branch = format!("pr-{number}");
        let base = if base_idx == 0 {
            "main".to_owned()
        } else {
            format!("pr-{base_idx}")
        };
        let head = create_branch_with_file(
            &config,
            &branch,
            &format!("pr-{number}.txt"),
            &format!("content {number}"),
            &base,
        );
        create_pr_ref(&config, number as u64, &head);
        fake_prs.insert(
            PrNumber(number as u64),
            FakePr {
                branch,
                base_ref: base,
                state: FakePrState::Open,
                author_id: AUTHOR,
            },
        );
        heads.push(head);
    }
    let mut with_children: Vec<usize> = bases.iter().copied().filter(|b| *b > 0).collect();
    with_children.sort_unstable();
    with_children.dedup();
    for pr in with_children {
        let branch = format!("pr-{pr}");
        let head = create_branch_with_file(
            &config,
            &branch,
            &format!("pr-{pr}-fix.txt"),
            "fix",
            &branch,
        );
        create_pr_ref(&config, pr as u64, &head);
        heads[pr - 1] = head;
    }
    let mut fake = FakeGitHub::new(config.clone(), fake_prs);
    fake.comment_author = TEST_BOT_ID;
    let world = World {
        _temp: temp,
        state_dir: TempDir::new().unwrap(),
        config,
        github: Arc::new(Mutex::new(fake)),
        next_delivery: 0,
        clock: Arc::new(Mutex::new(crate::test_utils::test_timestamp())),
    };
    (world, heads)
}

/// The disaster: the state DB (and its WAL, and the lock) is gone.
fn crash_db(world: &World) {
    let db = world.db_path();
    for path in [
        db.clone(),
        db.with_extension("db-wal"),
        db.with_extension("db-shm"),
        db.with_extension("lock"),
    ] {
        let _ = std::fs::remove_file(path);
    }
}

// ── Mirrored user comments ──
//
// The existing payload builders fold comment ids mod 10 and never touch
// the fake's comment store; the crawl reads that store, so this harness
// keeps webhook and `ListComments` views of every user comment
// identical — as they are on real GitHub.

#[allow(clippy::too_many_arguments)]
fn user_comment_json(
    config: &GitConfig,
    pr: u64,
    text: Option<&str>,
    author: u64,
    author_login: &str,
    sender: u64,
    sender_login: &str,
    comment_id: u64,
    action: &str,
) -> Vec<u8> {
    let body_json = match text {
        Some(t) => format!("\"{t}\""),
        None => "null".to_owned(),
    };
    format!(
        r#"{{
            "action": "{action}",
            "comment": {{
                "id": {comment_id},
                "body": {body_json},
                "user": {{ "id": {author}, "login": "{author_login}" }},
                "updated_at": "2026-07-01T10:{mm:02}:{ss:02}Z"
            }},
            "issue": {{
                "number": {pr},
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {sender}, "login": "{sender_login}" }}
        }}"#,
        mm = (comment_id / 60) % 60,
        ss = comment_id % 60,
        repo = repo_json(config),
    )
    .into_bytes()
}

/// Posts a user comment: into the fake's store AND as a webhook.
fn post_mirrored_comment(
    world: &mut World,
    processor: &mut Processor,
    pr: u64,
    id: u64,
    text: &str,
    author: u64,
    login: &str,
) {
    world.github.lock().unwrap().comments.insert(
        CommentId(id),
        FakeComment {
            pr: PrNumber(pr),
            author_id: author,
            body: text.to_owned(),
            edited: Edited::Never,
        },
    );
    let body = user_comment_json(
        &world.config,
        pr,
        Some(text),
        author,
        login,
        author,
        login,
        id,
        "created",
    );
    world.enqueue(processor, "issue_comment", body);
}

/// Deletes a user comment: from the fake's store AND as a webhook whose
/// sender is the deleter.
fn delete_mirrored_comment(
    world: &mut World,
    processor: &mut Processor,
    id: u64,
    sender: u64,
    sender_login: &str,
) {
    let Some((pr, author)) = ({
        let mut github = world.github.lock().unwrap();
        github
            .comments
            .remove(&CommentId(id))
            .map(|c| (c.pr.0, c.author_id))
    }) else {
        return;
    };
    let login = if author == AUTHOR {
        "author"
    } else {
        "stranger"
    };
    let body = user_comment_json(
        &world.config,
        pr,
        None,
        author,
        login,
        sender,
        sender_login,
        id,
        "deleted",
    );
    world.enqueue(processor, "issue_comment", body);
}

// ── Generated histories ──

/// What the harness posted, for later moves that need to refer back.
#[derive(Default)]
struct History {
    /// Author-declaration comment ids per PR, oldest first.
    decl_ids: HashMap<u64, Vec<u64>>,
    /// The latest declaration target attempted per PR (restatements).
    last_target: HashMap<u64, u64>,
    /// Every user comment id the harness posted.
    user_comments: Vec<u64>,
    /// The ORIGINAL payload of every user comment — (pr, text, author)
    /// — which a redelivery replays verbatim however the comment was
    /// edited or deleted since.
    originals: HashMap<u64, (u64, String, u64)>,
    /// Declarations to make WHILE a train runs — (pr, target, id) —
    /// delivered by the batch driver one per saga boundary, so they
    /// genuinely race the train rather than preceding it.
    late: Vec<(u64, u64, u64)>,
    /// Stops to issue WHILE a train runs — (pr, id) — delivered the
    /// same way. A stop queued with the rest of the history is
    /// consumed at the first observation boundary, where it cancels a
    /// pending start or finds no train, so a history that only queues
    /// them never actually STOPS one and never exercises recovery
    /// after a stop (Codex review of #90, round 3, P2).
    ///
    /// Like the late declarations, these are delivered by the batch
    /// driver, which only the RECOVERED world runs — so they belong to
    /// the envelope property, whose oracle is the documented envelope.
    /// The differential compares two worlds against each other and
    /// owes them identical histories; giving it mid-cascade events
    /// would mean driving its live world through the same driver.
    late_stops: Vec<(u64, u64)>,
    /// The late declarations actually DELIVERED before the DB loss, as
    /// (source, target): mid-train topology changes live accepted and
    /// recovery must abort on, exactly like a gap extension (Codex
    /// harness review round 3, P2).
    delivered_late: Vec<(u64, u64)>,
}

/// PR numbers with a real base PR, with that base — the honest
/// declarations a user would make.
fn stacked(bases: &[usize]) -> Vec<(u64, u64)> {
    bases
        .iter()
        .enumerate()
        .filter(|(_, b)| **b > 0)
        .map(|(k, b)| ((k + 1) as u64, *b as u64))
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn declare(
    world: &mut World,
    processor: &mut Processor,
    h: &mut History,
    pr: u64,
    target: u64,
    id: u64,
    author: u64,
    login: &str,
) {
    let text = format!("@merge-train predecessor #{target}");
    post_mirrored_comment(world, processor, pr, id, &text, author, login);
    h.user_comments.push(id);
    h.originals.insert(id, (pr, text, author));
    if author == AUTHOR {
        h.decl_ids.entry(pr).or_default().push(id);
        h.last_target.insert(pr, target);
    }
}

/// Enqueues the whole generated history: PR announcements, then the
/// declaration phase, then the command phase. Declarations precede
/// commands so the differential property never races a declaration
/// against a running train (recovery is DELIBERATELY stricter there —
/// it aborts on any extension — so that case lives in the envelope
/// property, whose command decoder also emits late declarations).
fn enqueue_history(
    world: &mut World,
    processor: &mut Processor,
    bases: &[usize],
    heads: &[Sha],
    decls: &[(u8, Index, Index)],
    cmds: &[(u8, Index, Index)],
    allow_late_decls: bool,
) -> History {
    let n = bases.len();
    for (k, &b) in bases.iter().enumerate() {
        let base = if b == 0 {
            "main".to_owned()
        } else {
            format!("pr-{b}")
        };
        let body = pr_opened_body(
            &world.config,
            (k + 1) as u64,
            &heads[k],
            &format!("pr-{}", k + 1),
            &base,
        );
        world.enqueue(processor, "pull_request", body);
    }
    // Warm the store before any comment is mirrored: a fresh store's
    // first delivery runs the first-contact crawl, which would otherwise
    // read the ENTIRE mirrored history at once — the live oracle must
    // build its expectation incrementally, through the handlers, or a
    // crawl defect installs the same state in both worlds (Codex
    // harness review round 2, P1).
    process_backlog(world, processor);

    let mut h = History::default();
    let honest = stacked(bases);
    let mut next_honest = 0usize;
    for (i, (kind, a, b)) in decls.iter().enumerate() {
        let id = 1000 + i as u64;
        match kind % 6 {
            // Weighted toward the honest declarations that make stacks.
            0 | 1 => {
                if next_honest < honest.len() {
                    let (pr, base) = honest[next_honest];
                    next_honest += 1;
                    declare(world, processor, &mut h, pr, base, id, AUTHOR, "author");
                }
            }
            // Junk: an arbitrary (pr, target) pair — cycles, mismatched
            // bases, self-references. Validation must answer the same
            // way live and replayed.
            2 => {
                let pr = a.index(n) as u64 + 1;
                let target = b.index(n) as u64 + 1;
                declare(world, processor, &mut h, pr, target, id, AUTHOR, "author");
            }
            // A stranger declares: rejected live, ignored by the crawl.
            3 => {
                let pr = a.index(n) as u64 + 1;
                let target = b.index(n) as u64 + 1;
                declare(
                    world, processor, &mut h, pr, target, id, STRANGER, "stranger",
                );
            }
            // Restatement: ownership transfer to a fresh comment.
            4 => {
                let mut prs: Vec<u64> = h.last_target.keys().copied().collect();
                prs.sort_unstable();
                if !prs.is_empty() {
                    let pr = prs[a.index(prs.len())];
                    let target = h.last_target[&pr];
                    declare(world, processor, &mut h, pr, target, id, AUTHOR, "author");
                }
            }
            // The author retracts PARTIALLY: only their latest
            // declaring comment is deleted, leaving any older
            // declarations standing on GitHub. Live retracts the edge
            // and posts a retraction RECEIPT (owner ruling
            // 2026-07-18); the crawl reads the receipt as a tombstone
            // for the older comments, so recovery must NOT resurrect
            // the edge — the differential property holds this with no
            // allowance, which is the receipt feature's oracle.
            5 => {
                let mut prs: Vec<u64> = h
                    .decl_ids
                    .iter()
                    .filter(|(_, ids)| !ids.is_empty())
                    .map(|(pr, _)| *pr)
                    .collect();
                prs.sort_unstable();
                if !prs.is_empty() {
                    let pr = prs[a.index(prs.len())];
                    let cid = h.decl_ids.get_mut(&pr).unwrap().pop().unwrap();
                    delete_mirrored_comment(world, processor, cid, AUTHOR, "author");
                }
            }
            _ => unreachable!(),
        }
    }

    let roots: Vec<u64> = bases
        .iter()
        .enumerate()
        .filter(|(_, b)| **b == 0)
        .map(|(k, _)| (k + 1) as u64)
        .collect();
    for (i, (kind, a, b)) in cmds.iter().enumerate() {
        let id = 2000 + i as u64;
        let modulus = if allow_late_decls { 5 } else { 3 };
        match kind % modulus {
            // Weighted toward starts that can actually run: a train
            // needs a valid root.
            0 => {
                let pr = roots[a.index(roots.len())];
                post_mirrored_comment(
                    world,
                    processor,
                    pr,
                    id,
                    "@merge-train start",
                    AUTHOR,
                    "author",
                );
                h.user_comments.push(id);
                h.originals
                    .insert(id, (pr, "@merge-train start".to_owned(), AUTHOR));
            }
            1 => {
                let pr = a.index(n) as u64 + 1;
                post_mirrored_comment(
                    world,
                    processor,
                    pr,
                    id,
                    "@merge-train start",
                    AUTHOR,
                    "author",
                );
                h.user_comments.push(id);
                h.originals
                    .insert(id, (pr, "@merge-train start".to_owned(), AUTHOR));
            }
            2 => {
                let pr = a.index(n) as u64 + 1;
                post_mirrored_comment(
                    world,
                    processor,
                    pr,
                    id,
                    "@merge-train stop",
                    AUTHOR,
                    "author",
                );
                h.user_comments.push(id);
                h.originals
                    .insert(id, (pr, "@merge-train stop".to_owned(), AUTHOR));
            }
            // A declaration racing the running train: live records it
            // (a new-PR extension does not abort a live train); the
            // crawl must answer with the conservative abort. Deferred
            // to the batch driver, which delivers it at a saga
            // boundary AFTER the train has started — declared here it
            // would precede the train and its id would sit below the
            // status comment (monolith review, P2).
            3 => {
                let pr = a.index(n) as u64 + 1;
                let target = b.index(n) as u64 + 1;
                h.late.push((pr, target, id));
            }
            // A stop racing the running train: the one way a generated
            // history reaches a train that was really STOPPED, and
            // with it the recovery windows behind a stop — the record
            // committed while its status comment still says active
            // (Codex review of #90, round 3, P2). Delivered by the
            // batch driver, so it belongs to the property whose worlds
            // that driver builds; see `late_stops`.
            4 => {
                let pr = a.index(n) as u64 + 1;
                h.late_stops.push((pr, id));
            }
            _ => unreachable!(),
        }
    }
    h
}

// ── Driving to quiescence ──

/// Reality's merged-close webhooks: GitHub always announces a merged
/// PR the store has not yet heard about. (After a crawl this is nearly
/// always empty — the merged list endpoint already taught the store —
/// but a stopped train's unobserved squash still reconciles this way.)
/// Reality's merged-close webhooks, for the merges GitHub still owes a
/// delivery for: those `announce` names — the bot's own squashes after
/// the loss, and the gap's manual merges. A merge whose webhook was
/// already acked into the destroyed DB gets NO new delivery, so
/// fabricating one would repair a crawl that failed to reconstruct it
/// (Codex harness review round 6, P2).
fn deliver_owed_merges(
    world: &mut World,
    processor: &mut Processor,
    announce: &HashSet<u64>,
) -> usize {
    let unseen: Vec<(PrNumber, Sha, Sha, String, String)> = {
        let github = world.github.lock().unwrap();
        let state = processor.state();
        github
            .prs
            .iter()
            .filter_map(|(pr, fake)| {
                let FakePrState::Merged { squash_sha } = &fake.state else {
                    return None;
                };
                if !announce.contains(&pr.0) {
                    return None;
                }
                let known = state.prs.get(pr).is_some_and(|c| c.state.is_merged());
                (!known).then(|| {
                    (
                        *pr,
                        squash_sha.clone(),
                        github.branch_head(&fake.branch),
                        fake.branch.clone(),
                        fake.base_ref.clone(),
                    )
                })
            })
            .collect()
    };
    let delivered = unseen.len();
    for (pr, merge_sha, head, branch, base) in unseen {
        let body = pr_merged_body(&world.config, pr.0, &head, &branch, &base, &merge_sha);
        world.enqueue(processor, "pull_request", body);
    }
    delivered
}

/// Processes every claimable delivery, letting a RELEASED one (a doubted
/// trigger) retry past the stall cooldown, as the worker's stall-retry
/// timer does. Sagas are NOT run: the harness pumps them itself, counting
/// batches.
fn process_backlog(world: &World, processor: &mut Processor) {
    let mut claims = 0;
    while let Some(delivery) = processor.claim().unwrap() {
        claims += 1;
        assert!(claims < 500, "the backlog did not settle");
        if process(processor, delivery) == PipelineOutcome::Released {
            world.advance_past_cooldown();
        }
    }
}

/// `drain`, with the cooldown treatment of [`process_backlog`].
fn drain_with_releases(world: &World, processor: &mut Processor) {
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(rounds < 100, "drain did not settle");
        let mut did_work = false;
        while let Some(delivery) = processor.claim().unwrap() {
            did_work = true;
            if process(processor, delivery) == PipelineOutcome::Released {
                world.advance_past_cooldown();
            }
        }
        run_sagas(processor);
        if !did_work {
            return;
        }
    }
}

/// Drains, plays reality's parts (CI green for whatever waits,
/// merged-close webhooks for unheard squashes), and repeats until no
/// train is active and nothing new was delivered. Panics if the system
/// will not go quiet — the stuck-train detector.
fn settle(world: &mut World, processor: &mut Processor, announce: &HashSet<u64>) {
    for _round in 0..40 {
        drain_with_releases(world, processor);
        let delivered = deliver_owed_merges(world, processor, announce);
        let waiting: Vec<PrNumber> = processor
            .state()
            .active_trains
            .values()
            .filter(|t| t.state.is_active())
            .map(|t| t.current_pr)
            .collect();
        if waiting.is_empty() && delivered == 0 {
            return;
        }
        for pr in waiting {
            let target = {
                let github = world.github.lock().unwrap();
                github
                    .prs
                    .get(&pr)
                    .map(|fake| (github.branch_head(&fake.branch), world.next_delivery + 900))
            };
            if let Some((head, suite)) = target {
                let body = check_suite_green_body(&world.config, &head, &[pr.0], suite);
                world.enqueue(processor, "check_suite", body);
            }
        }
    }
    panic!("did not settle: {:?}", processor.state().active_trains);
}

/// `run_batches_then_crash`, but hands back what the store held at the
/// moment of death: the state (the envelope property's baseline) and
/// the commands acknowledged but not yet answered (the differential
/// property's exemptions — those die with the DB).
fn run_batches_then_snapshot(
    world: &mut World,
    mut processor: Processor,
    depth: usize,
    history: &mut History,
) -> (RepoState, Vec<DurableCommand>) {
    let mut executed = 0;
    // Nudges that produced no batch: a regression that parks a train
    // for good would otherwise spin here forever instead of failing
    // (Codex harness review round 6, P2).
    let mut idle_rounds = 0;
    'outer: while executed < depth {
        process_backlog(world, &mut processor);
        match processor.pump().unwrap() {
            Some(first) => {
                let mut batch = first;
                loop {
                    let outcomes = execute(&mut processor, &batch);
                    executed += 1;
                    if executed >= depth {
                        break 'outer; // crash: outcomes never observed
                    }
                    // One late declaration per executed batch, once a
                    // train is RUNNING (recorded — the preflight's
                    // `TrainStarted` lands at its boundary, so nothing
                    // is delivered before that): it arrives — and is
                    // processed — while the saga is in flight, before
                    // its outcomes are observed, exactly as the worker
                    // services intake between batches.
                    let train_running = processor
                        .state()
                        .active_trains
                        .values()
                        .any(|t| t.state.is_active());
                    if train_running && !history.late.is_empty() {
                        let (pr, target, planned) = history.late.remove(0);
                        // Allocated NOW, above everything the fake holds
                        // (bot status comments and receipts posted since
                        // the history was planned): GitHub ids are
                        // globally monotonic, and a declaration numbered
                        // below the train's status comment would pass
                        // as pre-train (Codex harness review round 2).
                        let id = {
                            let github = world.github.lock().unwrap();
                            github
                                .comments
                                .keys()
                                .map(|c| c.0 + 1)
                                .max()
                                .map_or(planned, |floor| floor.max(planned))
                        };
                        let before = processor
                            .state()
                            .prs
                            .get(&PrNumber(pr))
                            .and_then(|c| c.predecessor);
                        declare(
                            world,
                            &mut processor,
                            history,
                            pr,
                            target,
                            id,
                            AUTHOR,
                            "author",
                        );
                        process_backlog(world, &mut processor);
                        // Only an EFFECTIVE declaration extends the
                        // stack, and only once its delivery has been
                        // PROCESSED can we tell: live validation
                        // refuses a junk one (wrong base, cycle,
                        // already declared), and recovery is right to
                        // carry on past it (Codex harness review round
                        // 5, P1; round 6, P2 — this used to read the
                        // state before the handler ran).
                        let after = processor
                            .state()
                            .prs
                            .get(&PrNumber(pr))
                            .and_then(|c| c.predecessor);
                        if after == Some(PrNumber(target)) && before != after {
                            history.delivered_late.push((pr, target));
                            coverage::hit("envelope", "late declaration delivered");
                        }
                    }
                    // And one late STOP per executed batch, on the same
                    // terms: it lands while the saga is in flight, so
                    // the train it names is really running when it
                    // arrives.
                    if train_running && !history.late_stops.is_empty() {
                        let (pr, planned) = history.late_stops.remove(0);
                        let id = {
                            let github = world.github.lock().unwrap();
                            github
                                .comments
                                .keys()
                                .map(|c| c.0 + 1)
                                .max()
                                .map_or(planned, |floor| floor.max(planned))
                        };
                        let text = "@merge-train stop";
                        post_mirrored_comment(
                            world,
                            &mut processor,
                            pr,
                            id,
                            text,
                            AUTHOR,
                            "author",
                        );
                        history.user_comments.push(id);
                        history.originals.insert(id, (pr, text.to_owned(), AUTHOR));
                        process_backlog(world, &mut processor);
                    }
                    match processor
                        .on_outcomes(batch.root, outcomes, batch.feedback)
                        .unwrap()
                    {
                        Some(next) => {
                            // A crash point AFTER the boundary
                            // committed and BEFORE the next batch runs:
                            // the store is then ahead of GitHub (the
                            // start consumed, its status comment not
                            // posted; the stop committed while GitHub
                            // still shows an active train), which is
                            // exactly what a DB loss must survive
                            // (Codex harness review round 8, P1).
                            executed += 1;
                            if executed >= depth {
                                break 'outer;
                            }
                            batch = next;
                        }
                        None => {
                            // The same window where the saga ENDED at
                            // that boundary: committed, with nothing
                            // dispatched behind it.
                            executed += 1;
                            if executed >= depth {
                                break 'outer;
                            }
                            break;
                        }
                    }
                }
            }
            None => {
                let waiting: Vec<PrNumber> = processor
                    .state()
                    .active_trains
                    .values()
                    .filter(|t| t.state.is_active())
                    .map(|t| t.current_pr)
                    .collect();
                if waiting.is_empty() {
                    break; // completed before `depth` batches
                }
                idle_rounds += 1;
                assert!(
                    idle_rounds < 40,
                    "a train is stuck before the crash: {:?}",
                    processor.state().active_trains
                );
                for pr in waiting {
                    let (head, suite) = {
                        let github = world.github.lock().unwrap();
                        let branch = github.prs[&pr].branch.clone();
                        (github.branch_head(&branch), world.next_delivery + 900)
                    };
                    let body = check_suite_green_body(&world.config, &head, &[pr.0], suite);
                    world.enqueue(&mut processor, "check_suite", body);
                }
            }
        }
    }
    let at_loss = processor.state().clone();
    let pending = processor
        .store_mut()
        .pending_commands()
        .unwrap()
        .into_iter()
        .map(|(_, command)| command)
        .collect();
    drop(processor); // the crash
    (at_loss, pending)
}

/// Which roots still have their off-disk backup at the moment of the
/// loss, for THIS incarnation: a status comment of the bot's on the
/// root whose record's `started_at` is the at-loss train's. A
/// start/stop/start history leaves the previous incarnation's terminal
/// comment behind, and that is no backup for the train now running
/// (Codex harness review round 14, P2). Read from GitHub, never from
/// the lost record's `status_comment_id`: a `PostComment` that landed
/// before its outcome was recorded leaves the store's field empty while
/// the comment stands, recoverable (Codex harness review round 11, P2).
fn status_roots_at_loss(world: &World, at_loss: &RepoState) -> HashSet<u64> {
    let github = world.github.lock().unwrap();
    github
        .comments
        .values()
        .filter(|c| c.author_id == TEST_BOT_ID)
        .filter_map(|c| {
            let record = parse_status_comment(&c.body).ok()?;
            let current = at_loss
                .active_trains
                .get(&c.pr)
                .is_some_and(|live| live.started_at == record.started_at);
            (record.original_root_pr == c.pr && current).then_some(c.pr.0)
        })
        .collect()
}

/// Everything a DB loss is allowed to change: the three documented
/// residuals, as (PRs, train roots).
fn loss_exemptions(
    world: &World,
    at_loss: &RepoState,
    pending: &[DurableCommand],
) -> (HashSet<PrNumber>, HashSet<u64>) {
    let backed_up = status_roots_at_loss(world, at_loss);
    let (mut prs, mut roots) = command_loss_exemptions(at_loss, pending, &backed_up);
    let (more_prs, more_roots) = refused_declaration_exemptions(world, at_loss);
    prs.extend(more_prs);
    roots.extend(more_roots);
    (prs, roots)
}

/// The THIRD documented residual (see [`command_loss_exemptions`] for
/// the other two): a predecessor-shaped comment by the PR's own author
/// that live REFUSED — a mismatched base, a cycle, a closed target —
/// and that still stands on GitHub, unaccounted for by the PR's ledger.
/// The crawl clears only what cannot have changed since the comment was
/// made: its author, and that a PR is not its own predecessor. A base
/// branch can be put back after live accepted a declaration against it
/// (Codex review of #89, P1), so the crawl cannot tell refused junk
/// from an accepted declaration whose writes were lost, and reads it
/// as an extension. A recovered train the comment touches — sitting on
/// a member, or naming one — is aborted: stop-shaped, and a `start`
/// away. The blast radius is that train's own.
///
/// The comments are exactly the crawl's evidence
/// ([`unledgered_evidence`]), read from GitHub as it stands at the
/// loss. A stranger's comment or an unedited self-declaration is NOT
/// excused: the crawl clears those, and a crawl that aborted on one
/// would still fail.
fn refused_declaration_exemptions(
    world: &World,
    at_loss: &RepoState,
) -> (HashSet<PrNumber>, HashSet<u64>) {
    let refused = unledgered_evidence(&world.github.lock().unwrap());
    let mut prs: HashSet<PrNumber> = HashSet::new();
    let mut roots: HashSet<u64> = HashSet::new();
    for (root, record) in &at_loss.active_trains {
        let stack = train_stack(at_loss, record);
        if refused
            .iter()
            .any(|(source, target)| stack.contains(source) || stack.contains(target))
        {
            roots.insert(root.0);
            prs.extend(train_members(record));
            prs.extend(stack);
        }
    }
    (prs, roots)
}

/// What a DB loss is allowed to change, beyond which everything still
/// owes equivalence. Two documented residuals:
///
/// - An acknowledged-but-UNANSWERED command: the addressed PR,
///   everything below it, and any at-loss train whose stack contains
///   it. GitHub never redelivers an acked webhook, so when the DB dies
///   holding such a command its effects simply never happen — bounded
///   and visible (the user has an ack reaction and no follow-up).
/// - A train with NO status comment ON GITHUB: the comment is the
///   off-disk backup, and the window between `TrainStarted` committing
///   and its `PostComment` landing leaves nothing to recover from. That
///   is the crawl's stated envelope ("a train whose comment is gone is
///   not resurrected"), and it is stop-shaped: the user re-issues
///   `start`. Decided by GitHub, not by the lost record: a post that
///   landed before its outcome was recorded IS a backup.
fn command_loss_exemptions(
    at_loss: &RepoState,
    pending: &[DurableCommand],
    status_roots_at_loss: &HashSet<u64>,
) -> (HashSet<PrNumber>, HashSet<u64>) {
    let mut prs: HashSet<PrNumber> = HashSet::new();
    let mut roots: HashSet<u64> = HashSet::new();
    for (root, record) in &at_loss.active_trains {
        if !status_roots_at_loss.contains(&root.0) {
            roots.insert(root.0);
            prs.extend(train_members(record));
            prs.extend(train_stack(at_loss, record));
        }
    }
    // `start_train` refuses more than a running train: a PR that is
    // not open, or not a root, is answered with a comment and changes
    // nothing — so losing that start excuses nothing either (Codex
    // harness review round 14, P2). "A root" is the engine's own
    // predicate (`is_root`): targeting the default branch, with no
    // unresolved predecessor, so a PR based on a sibling's branch with
    // no declaration is refused too (round 18, P2).
    let startable = |pr: &PrNumber| {
        at_loss.train_involving(*pr).is_none()
            && at_loss.prs.get(pr).is_some_and(|cached| {
                matches!(cached.state, PrState::Open)
                    && crate::state::topology::is_root(
                        cached,
                        &at_loss.default_branch,
                        &at_loss.prs,
                    )
            })
    };
    // WHICH pending commands would actually have changed something,
    // resolved in the order the worker would have run them. A start
    // the engine would refuse changes nothing. A start a later stop
    // CANCELS changes nothing, and neither does that stop: live
    // consumes the queued start without ever running it, so the pair
    // leaves the PR exactly where losing neither would have (Codex
    // review of #90, rounds 1 and 2, P2). A stop naming no train and
    // cancelling no queued start is answered "no active merge train"
    // and touches nothing; exempting its PR, every descendant and its
    // train outcome would let unrelated crawl errors through (Codex
    // harness review round 10, P2).
    let mut queued: Vec<PrNumber> = Vec::new();
    let mut effective: Vec<PrNumber> = Vec::new();
    for command in pending {
        match command {
            // A second start for a PR already queued is refused as a
            // duplicate, so only the first of them can change
            // anything.
            DurableCommand::Start { pr } => {
                if startable(pr) && !queued.contains(pr) {
                    queued.push(*pr);
                }
            }
            DurableCommand::Stop { pr, .. } => {
                if let Some(cancelled) = queued.iter().position(|q| q == pr) {
                    queued.remove(cancelled);
                } else if at_loss.train_involving(*pr).is_some() {
                    effective.push(*pr);
                }
            }
        }
    }
    effective.extend(queued);
    for pr in effective {
        prs.insert(pr);
        prs.extend(collect_all_descendants(
            pr,
            &at_loss.descendants,
            &at_loss.prs,
        ));
        roots.insert(pr.0);
        for (root, record) in &at_loss.active_trains {
            if train_stack(at_loss, record).contains(&pr) {
                roots.insert(root.0);
                prs.extend(train_members(record));
                prs.extend(train_stack(at_loss, record));
            }
        }
    }
    (prs, roots)
}

// ── The differential property ──

fn run_live(
    bases: &[usize],
    decls: &[(u8, Index, Index)],
    cmds: &[(u8, Index, Index)],
) -> (World, Processor) {
    let (mut world, heads) = build_world(bases);
    let mut processor = world.processor();
    enqueue_history(
        &mut world,
        &mut processor,
        bases,
        &heads,
        decls,
        cmds,
        false,
    );
    let announce = all_prs(&world);
    settle(&mut world, &mut processor, &announce);
    (world, processor)
}

/// The recovered world, its processor, the blast radius a lost command
/// excuses, and what the LEDGERS attested at the moment of the loss —
/// `None` when nothing of the bot's survived, which makes the crawl an
/// onboarding rather than a recovery.
type LostRun = (
    World,
    Processor,
    HashSet<PrNumber>,
    HashSet<u64>,
    Option<HashMap<PrNumber, Option<(PrNumber, CommentId)>>>,
);

fn run_lost(
    bases: &[usize],
    decls: &[(u8, Index, Index)],
    cmds: &[(u8, Index, Index)],
    depth: usize,
    property: &'static str,
) -> LostRun {
    let (mut world, heads) = build_world(bases);
    let mut processor = world.processor();
    let mut history = enqueue_history(
        &mut world,
        &mut processor,
        bases,
        &heads,
        decls,
        cmds,
        false,
    );
    let (at_loss, pending) = run_batches_then_snapshot(&mut world, processor, depth, &mut history);
    let (exempt_prs, exempt_roots) = loss_exemptions(&world, &at_loss, &pending);
    // Merges reality had already announced to the dead DB: GitHub will
    // not deliver them again, so recovery must reconstruct them from
    // the crawl alone (Codex harness review round 6, P2).
    let already_announced = merged_prs(&world);
    crash_db(&world);
    // What the ledgers attested AT THE MOMENT OF THE LOSS. Read now,
    // not after: recovery rewrites a ledger it disbelieved, so reading
    // afterwards would let a crawl that wrongly dropped an edge
    // manufacture its own alibi (Codex harness review round 14, P2).
    let attested_at_loss: HashMap<PrNumber, Option<(PrNumber, CommentId)>> = {
        let github = world.github.lock().unwrap();
        github
            .prs
            .keys()
            .map(|pr| (*pr, attested_edge(&github, *pr)))
            .collect()
    };
    // With no trace of the bot at all — not one comment of its own, a
    // refusal reply included (Codex harness review round 11, P2; the
    // crawl counts any as prior contact) — the crawl ONBOARDS: it
    // reads the declarations as the
    // live path would read them and records what it finds. That is a
    // fresh derivation, not a recovery, and the owner's ruling
    // (2026-09-08) accepts that it can differ from a live history of
    // restatements and refusals. The topology comparison does not
    // apply to it.
    let onboarded = {
        let github = world.github.lock().unwrap();
        !github.comments.values().any(|c| c.author_id == TEST_BOT_ID)
    };

    let mut processor = world.processor();
    assert!(
        processor.state().default_branch.is_empty(),
        "precondition: the store really is fresh"
    );
    // Any webhook wakes the repo; the crawl rebuilds everything first.
    let head = world.github.lock().unwrap().branch_head("pr-1");
    let suite = world.next_delivery + 900;
    let body = check_suite_green_body(&world.config, &head, &[1], suite);
    world.enqueue(&mut processor, "check_suite", body);
    let announce: HashSet<u64> = all_prs(&world)
        .difference(&already_announced)
        .copied()
        .collect();
    settle(&mut world, &mut processor, &announce);
    record_lost_run_coverage(property, &at_loss, &mut processor);
    if onboarded {
        coverage::hit(property, "onboarded (no bot records survived)");
    }
    (
        world,
        processor,
        exempt_prs,
        exempt_roots,
        if onboarded {
            None
        } else {
            Some(attested_at_loss)
        },
    )
}

fn pr_kind(state: &PrState) -> &'static str {
    match state {
        PrState::Open => "open",
        PrState::Closed => "closed",
        PrState::Merged { .. } => "merged",
    }
}

fn train_kind(state: &TrainState) -> &'static str {
    match state {
        TrainState::Running | TrainState::WaitingCi => "active",
        TrainState::Stopped { .. } => "stopped",
        TrainState::Completed { .. } => "completed",
        TrainState::Aborted { .. } => "aborted",
        TrainState::NeedsManualReview => "needs-manual-review",
    }
}

fn train_kinds(state: &RepoState) -> std::collections::BTreeMap<u64, &'static str> {
    state
        .active_trains
        .iter()
        .map(|(root, t)| (root.0, train_kind(&t.state)))
        .collect()
}

/// One world's absolutes: ≤1 squash per PR, exact store↔GitHub merge
/// agreement, matched intent ledgers for finished trains (at every
/// boundary, not just the last), and an empty command backlog.
fn assert_consistent(world: &World, processor: &mut Processor, ctx: &str) {
    {
        let github = world.github.lock().unwrap();
        for (pr, count) in &github.squash_count {
            assert!(*count <= 1, "{ctx}: PR #{pr} squashed {count} times");
        }
        for (pr, fake) in &github.prs {
            let store_merge = processor.state().prs.get(pr).and_then(|c| match &c.state {
                PrState::Merged { merge_commit_sha } => Some(merge_commit_sha.clone()),
                _ => None,
            });
            let real_merge = match &fake.state {
                FakePrState::Merged { squash_sha } => Some(squash_sha.clone()),
                _ => None,
            };
            // The SHA too: descendant reconciliation fences on it, so a
            // store that agrees only on "merged" is not in agreement.
            assert_eq!(
                store_merge, real_merge,
                "{ctx}: store and reality disagree about PR #{pr}'s merge"
            );
        }
    }
    // A train that finished — completed, or retired by a fan-out — settled
    // every intent before each boundary that cleared its ledger. Asking
    // `ReplayFacts::for_train` instead would answer for the last phase
    // alone, which the terminal boundary already cleared: it can never
    // fail (Codex review of #90, round 5, P2).
    let events = processor.store_mut().events().unwrap();
    let finished: BTreeSet<PrNumber> = events
        .iter()
        .filter_map(|e| match e.payload {
            StateEventPayload::TrainCompleted { root_pr } => Some(root_pr),
            StateEventPayload::FanOutCompleted { old_root, .. } => Some(old_root),
            _ => None,
        })
        .collect();
    for root in finished {
        let unmatched = crate::cascade::unmatched_at_boundaries(&events, root);
        assert!(
            unmatched.is_empty(),
            "{ctx}: finished train #{root} left intents unmatched: {unmatched:?}"
        );
    }
    assert!(
        processor.store_mut().pending_commands().unwrap().is_empty(),
        "{ctx}: acknowledged commands left unanswered at quiescence"
    );
}

/// The differential oracle: with a quiet gap, the crawl-recovered world
/// must be indistinguishable from the never-crashed one — except inside
/// the blast radius of commands the DB died holding (see
/// [`command_loss_exemptions`]). Worlds have separate repos (different
/// SHAs), so comparison is shape-wise.
fn assert_equivalent(
    lw: &World,
    lp: &mut Processor,
    cw: &World,
    cp: &mut Processor,
    exempt_prs: &HashSet<PrNumber>,
    exempt_roots: &HashSet<u64>,
    attested_at_loss: Option<&HashMap<PrNumber, Option<(PrNumber, CommentId)>>>,
) {
    assert_consistent(lw, lp, "live");
    assert_consistent(cw, cp, "lost-db");
    let live = lp.state().clone();
    let lost = cp.state().clone();
    assert_eq!(live.default_branch, lost.default_branch);
    for (pr, l) in &live.prs {
        let Some(c) = lost.prs.get(pr) else {
            assert!(
                matches!(l.state, PrState::Closed),
                "recovery forgot PR #{pr}, which is {:?} (only unreferenced \
                 closed-unmerged PRs may be forgotten)",
                l.state
            );
            // And UNREFERENCED: a PR the recovered state points at —
            // an edge's target, or a train's member — must be in the
            // cache, or the topology it recovered rests on a PR it
            // never fetched (Codex harness review round 14, P2).
            let referenced_by_edge = lost
                .prs
                .values()
                .any(|other| other.predecessor == Some(*pr));
            let referenced_by_train = lost.active_trains.values().any(|record| {
                record.original_root_pr == *pr
                    || record.current_pr == *pr
                    || record
                        .cascade_phase
                        .progress()
                        .is_some_and(|p| p.frozen_descendants().contains(pr))
            });
            assert!(
                !referenced_by_edge && !referenced_by_train,
                "recovery forgot PR #{pr} while still referring to it"
            );
            continue;
        };
        if !exempt_prs.contains(pr) {
            assert_eq!(
                pr_kind(&l.state),
                pr_kind(&c.state),
                "PR #{pr}: cached state diverged"
            );
            // The ROUTING fields too: root validation and every git
            // operation read them, so a cache that agrees only on the
            // state kind is not equivalent (Codex harness review round
            // 4, P2).
            assert_eq!(
                (&l.head_ref, &l.base_ref, l.is_draft),
                (&c.head_ref, &c.base_ref, c.is_draft),
                "PR #{pr}: routing fields diverged"
            );
        }
        // Each world's cache matches its OWN reality, exemption or not:
        // a lost command excuses the two worlds differing, never a
        // world disagreeing with the GitHub it can see — not its head
        // (Codex review of #90, round 3, P2), and not its state or
        // routing either (round 4, P2). "What GitHub would say" is the
        // fake's own answer to a fetch, not a re-derivation of it.
        for (world, cached, which) in [(lw, l, "live"), (cw, c, "lost-db")] {
            let github = world.github.lock().unwrap();
            if !github.prs.contains_key(pr) {
                continue;
            }
            let (real, _) = github.pr_data(*pr);
            assert_eq!(
                (
                    &cached.state,
                    &cached.head_ref,
                    &cached.base_ref,
                    cached.is_draft
                ),
                (&real.state, &real.head_ref, &real.base_ref, real.is_draft),
                "{which}: PR #{pr}'s cached state or routing disagrees with its GitHub"
            );
            // Only an open PR's head is tracked: a closed or merged PR's
            // branch is no longer the cache's to follow.
            if real.state == PrState::Open {
                assert_eq!(
                    cached.head_sha, real.head_sha,
                    "{which}: PR #{pr}'s cached head is stale"
                );
            }
        }
        // THE ONE ALLOWANCE, and the whole oracle in a sentence: an
        // edge GitHub carries no corroborated LEDGER for is one no
        // crawl can recover, because the ledger is the topology's
        // off-disk backup — the write is best-effort, and a crash can
        // land between the edge committing and its ledger reaching
        // GitHub. That residual is stop-shaped: the missing edge makes
        // a later `start` refuse until the user re-declares; it never
        // drives anything. Everything the ledger DOES attest must come
        // back exactly, and an edge the crawl invents from nowhere is
        // always a bug.
        // THE ORACLE, in one sentence: the recovered topology is
        // exactly what the LEDGERS attest. The ledger is written after
        // the event that changes the edge, so a crash in that window
        // leaves GitHub one write behind the state — and the crawl
        // faithfully reproduces the older value, in either direction:
        // an edge whose ledger never landed is missing, and an edge
        // whose retraction never landed comes back. Both are the
        // documented residual, and both are stop-shaped: the state and
        // the PR's base disagree, so the next `start` refuses until
        // the user re-declares. What is NOT tolerated is the crawl
        // departing from what the ledgers say.
        let Some(attested_at_loss) = attested_at_loss else {
            continue; // an onboarding: its topology is a fresh reading
        };
        let attested = attested_at_loss.get(pr).copied().flatten();
        // Ownership is part of what the ledger attests, and it moves
        // on a restatement: a lost write leaves the older owner
        // exactly as it leaves the older edge.
        let recovered = c.predecessor.zip(c.predecessor_comment_id);
        let live_edge = l.predecessor.zip(l.predecessor_comment_id);
        // A recovered edge the ledgers did not grant — target and
        // owner — is one the crawl derived from a declaration, which
        // only an onboarding may do: a crash before the ledger write
        // leaves the edge MISSING, never rebuilt, even when live holds
        // it (Codex harness review round 16, P2).
        assert!(
            recovered.is_none() || recovered == attested,
            "PR #{pr}: recovered edge {recovered:?} was granted by no ledger at the \
             loss (attested {attested:?})"
        );
        if live_edge != recovered && recovered == attested {
            coverage::hit(
                "differential",
                if recovered.is_none() {
                    "edge lost with its unwritten ledger"
                } else {
                    "edge kept by a ledger one write behind"
                },
            );
            continue;
        }
        assert_eq!(
            l.predecessor, c.predecessor,
            "PR #{pr}: predecessor edge diverged"
        );
        if l.predecessor_comment_id != c.predecessor_comment_id {
            eprintln!(
                "DIAG pr={pr} live={:?}/{:?} lost={:?}/{:?} attested={:?}",
                l.predecessor,
                l.predecessor_comment_id,
                c.predecessor,
                c.predecessor_comment_id,
                attested_at_loss.get(pr)
            );
            let github = cw.github.lock().unwrap();
            for (id, comment) in github.comments.iter().filter(|(_, c)| c.pr == *pr) {
                eprintln!(
                    "DIAG   comment {id} author={} body={:?}",
                    comment.author_id,
                    &comment.body[..comment.body.len().min(110)]
                );
            }
        }
        assert_eq!(
            l.predecessor_comment_id, c.predecessor_comment_id,
            "PR #{pr}: predecessor ownership diverged"
        );
    }
    for pr in lost.prs.keys() {
        assert!(live.prs.contains_key(pr), "recovery invented PR #{pr}");
    }
    let mut live_trains = train_kinds(&live);
    let mut lost_trains = train_kinds(&lost);
    for root in exempt_roots {
        live_trains.remove(root);
        lost_trains.remove(root);
    }
    // A TERMINAL record (stopped/aborted) with no surviving status
    // comment is honestly forgotten: a train stopped at its very
    // first observation boundary dies before its first status post,
    // so after a DB loss there is nothing sound to resurrect it from
    // (the documented envelope) — and a terminal record is
    // post-mortem display state; a fresh `start` behaves identically
    // with or without it. Tolerated ONLY when no comment survives:
    // dropping an adoptable terminal record would still fail here.
    let lost_has_status_comment = |root: u64| {
        let github = cw.github.lock().unwrap();
        github.comments.values().any(|c| {
            c.author_id == TEST_BOT_ID
                && c.pr == PrNumber(root)
                && parse_status_comment(&c.body).is_ok_and(|r| r.original_root_pr == PrNumber(root))
        })
    };
    live_trains.retain(|root, kind| {
        let forgotten_terminal = matches!(*kind, "stopped" | "aborted")
            && !lost_trains.contains_key(root)
            && !lost_has_status_comment(*root);
        !forgotten_terminal
    });
    assert_eq!(live_trains, lost_trains, "train outcomes diverged");
}

/// The PRs reality has already merged — at a crash, exactly the merges
/// whose close webhooks GitHub had already delivered and the dead DB
/// had acked. Nothing re-announces them afterwards.
fn merged_prs(world: &World) -> HashSet<u64> {
    let github = world.github.lock().unwrap();
    github
        .prs
        .iter()
        .filter(|(_, p)| matches!(p.state, FakePrState::Merged { .. }))
        .map(|(n, _)| n.0)
        .collect()
}

/// Every PR in the world: in a world that never crashed, GitHub still
/// owes a delivery for every merge the bot performs.
fn all_prs(world: &World) -> HashSet<u64> {
    let github = world.github.lock().unwrap();
    github.prs.keys().map(|n| n.0).collect()
}

/// Any webhook wakes the repository after the loss; the crawl rebuilds
/// everything before it is handled. A green check-suite for the root's
/// head will do.
fn fallback_wakeup(world: &mut World, processor: &mut Processor) {
    let head = world.github.lock().unwrap().branch_head("pr-1");
    let suite = world.next_delivery + 900;
    let body = check_suite_green_body(&world.config, &head, &[1], suite);
    world.enqueue(processor, "check_suite", body);
}

// ── What a train is made of ──

/// A train's known stack at loss: frozen set + primaries + the recorded
/// descendant closure (mirrors the crawl's own extension definition,
/// computed against the at-loss state the harness trusts).
fn train_stack(at_loss: &RepoState, record: &TrainRecord) -> HashSet<PrNumber> {
    let mut stack: HashSet<PrNumber> = record
        .cascade_phase
        .progress()
        .map(|p| p.frozen_descendants().iter().copied().collect())
        .unwrap_or_default();
    stack.insert(record.original_root_pr);
    stack.insert(record.current_pr);
    for anchor in [record.original_root_pr, record.current_pr] {
        stack.extend(collect_all_descendants(
            anchor,
            &at_loss.descendants,
            &at_loss.prs,
        ));
    }
    stack
}

/// The train's certain members: root, current, frozen set. (The full
/// closure may brush other trains; these are unambiguously this one's.)
fn train_members(record: &TrainRecord) -> HashSet<PrNumber> {
    let mut members: HashSet<PrNumber> = record
        .cascade_phase
        .progress()
        .map(|p| p.frozen_descendants().iter().copied().collect())
        .unwrap_or_default();
    members.insert(record.original_root_pr);
    members.insert(record.current_pr);
    members
}

// ── The retraction-receipt tombstone, end to end ──

/// The face-(b) scenario, deterministic (owner ruling 2026-07-18):
/// declare, restate (ownership moves to the newer comment), author
/// deletes the restatement — a live retraction. The ORIGINAL
/// declaration comment still stands on GitHub, so without a tombstone
/// a lost-DB crawl resurrects the edge and the recovered train MERGES
/// the descendant the user unstacked. The worker's retraction RECEIPT
/// (bot-posted, machine-parseable) outlives the DB and kills every
/// earlier declaration on the PR during the crawl's replay.
#[test]
fn a_partial_retraction_survives_a_db_loss() {
    let (mut world, heads) = build_world(&[0, 1]);
    let mut processor = world.processor();
    for (number, base_name) in [(1u64, "main"), (2, "pr-1")] {
        let body = pr_opened_body(
            &world.config,
            number,
            &heads[number as usize - 1],
            &format!("pr-{number}"),
            base_name,
        );
        world.enqueue(&mut processor, "pull_request", body);
    }
    post_mirrored_comment(
        &mut world,
        &mut processor,
        2,
        1000,
        "@merge-train predecessor #1",
        AUTHOR,
        "author",
    );
    post_mirrored_comment(
        &mut world,
        &mut processor,
        2,
        1001,
        "@merge-train predecessor #1",
        AUTHOR,
        "author",
    );
    delete_mirrored_comment(&mut world, &mut processor, 1001, AUTHOR, "author");
    post_mirrored_comment(
        &mut world,
        &mut processor,
        1,
        2000,
        "@merge-train start",
        AUTHOR,
        "author",
    );
    run_batches_then_snapshot(&mut world, processor, 10, &mut History::default());
    // The train must have got as far as its status comment, or the
    // envelope says it is simply not recoverable and this test would
    // pass for the wrong reason. (Batch counts move whenever the
    // worker gains a step — the stack ledger's writes added several —
    // so this precondition is asserted, not assumed.)
    assert!(
        world
            .github
            .lock()
            .unwrap()
            .comments
            .values()
            .any(|c| c.author_id == TEST_BOT_ID
                && crate::status::parse_status_comment(&c.body).is_ok()),
        "precondition: the train's status comment is on GitHub at the crash"
    );
    let already_announced = merged_prs(&world);
    crash_db(&world);

    let mut processor = world.processor();
    fallback_wakeup(&mut world, &mut processor);
    let announce: HashSet<u64> = all_prs(&world)
        .difference(&already_announced)
        .copied()
        .collect();
    settle(&mut world, &mut processor, &announce);

    {
        let github = world.github.lock().unwrap();
        assert!(
            matches!(
                github.prs.get(&PrNumber(1)).map(|f| &f.state),
                Some(FakePrState::Merged { .. })
            ),
            "the train on #1 must still complete"
        );
        assert!(
            matches!(
                github.prs.get(&PrNumber(2)).map(|f| &f.state),
                Some(FakePrState::Open)
            ),
            "the retracted descendant #2 must NOT be driven after recovery"
        );
    }
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "the surviving older declaration must not resurrect the retracted edge"
    );
}

// ── Generators ──

fn arb_bases() -> impl Strategy<Value = Vec<usize>> {
    (2usize..=4).prop_flat_map(|n| {
        let mut parts: Vec<BoxedStrategy<usize>> = vec![Just(0usize).boxed()];
        for k in 1..n {
            parts.push((0..=k).boxed());
        }
        parts
    })
}

/// Real git per case: 6 by default, but `PROPTEST_CASES` genuinely
/// raises it (a hardcoded `cases:` would silently ignore the env var).
fn cases() -> u32 {
    if std::env::var_os("PROPTEST_CASES").is_some() {
        ProptestConfig::default().cases
    } else {
        6
    }
}

/// What the generated cases actually exercised. Random generation
/// alone does not establish a distribution — commands may be empty,
/// and a gap move with no subject is a no-op — so key regimes are
/// forced BY CONSTRUCTION on a rotating schedule (see the runners) and
/// the floors below are asserted after every run (Codex harness
/// review, P2; monolith review, P2).
///
/// The counters are THREAD-LOCAL: proptest runs a property's cases on
/// the calling thread, while `cargo test` runs tests in parallel — a
/// fixed case in another test driving the same case function would
/// otherwise credit its coverage to this property's depth search, and
/// the search would stop at a depth that reached nothing (Codex
/// harness review round 12, P2).
mod coverage {
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    thread_local! {
        static HITS: RefCell<BTreeMap<(&'static str, &'static str), u32>> =
            const { RefCell::new(BTreeMap::new()) };
    }

    pub(super) fn hit(property: &'static str, regime: &'static str) {
        HITS.with(|hits| *hits.borrow_mut().entry((property, regime)).or_default() += 1);
    }

    pub(super) fn count(property: &'static str, regime: &'static str) -> u32 {
        HITS.with(|hits| hits.borrow().get(&(property, regime)).copied().unwrap_or(0))
    }

    pub(super) fn reset(property: &'static str) {
        HITS.with(|hits| hits.borrow_mut().retain(|(p, _), _| *p != property));
    }

    pub(super) fn report(property: &'static str) -> String {
        HITS.with(|hits| {
            hits.borrow()
                .iter()
                .filter(|((p, _), _)| *p == property)
                .map(|((_, r), n)| format!("{r}: {n}"))
                .collect::<Vec<_>>()
                .join(", ")
        })
    }
}

fn runner_config(regimes: u32) -> ProptestConfig {
    ProptestConfig {
        cases: cases_per_regime(regimes),
        source_file: Some(file!()),
        ..ProptestConfig::default()
    }
}

/// Regimes are forced BY CONSTRUCTION: each property runs once per
/// regime with that regime baked into the strategy, so a failure
/// shrinks and replays within its regime. `PROPTEST_CASES` is the
/// property's TOTAL budget, split across ITS regimes — a fixed divisor
/// would under-run the two-regime property and over-run the
/// eight-regime one (Codex harness review round 4, P2).
fn cases_per_regime(regimes: u32) -> u32 {
    (cases() / regimes).max(1)
}

/// Records what a lost-DB run exercised: adoption, mid-phase crashes,
/// fan-out, extension aborts.
fn record_lost_run_coverage(
    property: &'static str,
    at_loss: &RepoState,
    processor: &mut Processor,
) {
    if at_loss
        .active_trains
        .values()
        .any(|t| t.state.is_active() && t.cascade_phase.progress().is_some())
    {
        coverage::hit(property, "crash mid-phase");
    }
    if at_loss.active_trains.values().any(|t| t.state.is_active()) {
        coverage::hit(property, "crash with an active train");
    }
    // The boundaries past the first squash: a lost squash outcome, and
    // the descendants' reconciliation against it. A depth cap short of
    // them excludes them whatever the case budget (Codex harness
    // review round 12, P2); the fixed cases require both.
    if at_loss.active_trains.values().any(|t| {
        t.state.is_active()
            && (at_loss
                .prs
                .get(&t.original_root_pr)
                .is_some_and(|p| matches!(p.state, PrState::Merged { .. }))
                || t.cascade_phase
                    .progress()
                    .is_some_and(|p| !p.completed().is_empty()))
    }) {
        coverage::hit(property, "crash after a squash");
    }
    if at_loss.active_trains.values().any(|t| {
        t.state.is_active()
            && matches!(
                t.cascade_phase,
                crate::types::CascadePhase::Reconciling { .. }
                    | crate::types::CascadePhase::CatchingUp { .. }
                    | crate::types::CascadePhase::Retargeting { .. }
            )
    }) {
        coverage::hit(property, "crash while reconciling");
    }
    if !at_loss.active_trains.is_empty() {
        coverage::hit(property, "train recorded at loss");
    }
    if at_loss
        .active_trains
        .values()
        .any(|t| matches!(t.state, TrainState::Stopped { .. }))
    {
        coverage::hit(property, "crash with a stopped train");
    }
    let events = processor.store_mut().events().unwrap();
    if events
        .iter()
        .any(|e| matches!(e.payload, StateEventPayload::TrainRecordAdopted { .. }))
    {
        coverage::hit(property, "adoption");
    }
    if events
        .iter()
        .any(|e| matches!(e.payload, StateEventPayload::FanOutCompleted { .. }))
    {
        coverage::hit(property, "fan-out");
    }
    if events.iter().any(|e| {
        matches!(
            &e.payload,
            StateEventPayload::TrainAborted { error, .. }
                if error.kind == crate::types::TrainErrorKind::PredecessorChanged
        )
    }) {
        coverage::hit(property, "extension abort");
    }
}

/// A regime with a train: a start on a real root is forced into the
/// commands, a late declaration follows it, and the first declaration
/// actually posts a comment (an honest one when the shape has a stack,
/// a junk one otherwise — an honest declaration with no stack to
/// declare is a no-op).
fn force_train(
    bases: &[usize],
    decls: &mut [(u8, Index, Index)],
    cmds: &mut Vec<(u8, Index, Index)>,
    late: bool,
) {
    decls[0].0 = if stacked(bases).is_empty() { 2 } else { 0 };
    // Kind 0 is "start on a real root"; kind 3 a late declaration (in
    // the envelope's decoder only). The indexes only pick which PRs.
    let (_, a, b) = decls[0];
    if cmds.is_empty() {
        cmds.push((0, a, b));
    } else {
        cmds[0].0 = 0;
    }
    if late {
        if cmds.len() == 1 {
            cmds.push((3, b, a));
        } else {
            cmds[1].0 = 3;
        }
    }
}

/// Whether `comment`'s body is `author`'s own word, as the live path
/// and the crawl judge it (`CommentData::body_written_by`): unedited
/// and theirs, or last edited by them. An edit by an unknown editor is
/// nobody's word.
fn written_by(comment: &FakeComment, author: u64) -> bool {
    match comment.edited {
        Edited::Never => comment.author_id == author,
        Edited::By { editor } => editor == Some(author),
    }
}

/// The newest stack ledger standing on `pr`, as the crawl selects it.
fn newest_ledger(
    github: &FakeGitHub,
    pr: PrNumber,
) -> Option<(CommentId, crate::status::StackLedger)> {
    github
        .comments
        .iter()
        .filter(|(_, c)| c.pr == pr && c.author_id == TEST_BOT_ID)
        .filter_map(|(id, c)| {
            crate::status::parse_stack_ledger(&c.body)
                .filter(|l| l.pr == pr)
                .map(|l| (*id, l))
        })
        .max_by_key(|(id, _)| *id)
}

/// The unledgered predecessor-shaped comments the crawl reads as
/// evidence — computed from the fake exactly as `crawl_events` does,
/// so the oracle can say what the crawl must follow and what it may
/// abort over. A comment the believed edge accounts for (its owning
/// comment, a restatement of it, or one below the ledger's settled
/// watermark and unedited) is not news; of the rest, only one the
/// live path COULD have accepted is evidence: edited (nobody's word,
/// read conservatively), or unedited, the PR author's own, and not a
/// self-reference.
fn unledgered_evidence(github: &FakeGitHub) -> Vec<(PrNumber, PrNumber)> {
    let mut evidence: Vec<(PrNumber, PrNumber)> = github
        .comments
        .iter()
        .filter_map(|(id, c)| {
            let Some(Command::Predecessor(target)) = parse_command(&c.body, "merge-train") else {
                return None;
            };
            let author = github.prs.get(&c.pr)?.author_id;
            let edited = c.edited != Edited::Never;
            // Owner and restatement are judged against the edge the
            // crawl BELIEVES — the corroborated one — not the ledger's
            // text: an owning comment edited to name another
            // predecessor corroborates nothing and is evidence of its
            // new target (Codex harness review round 13, P2). The
            // settled watermark is the ledger's own.
            let believed = attested_edge(github, c.pr);
            let accounted_for = believed
                .is_some_and(|(believed_target, owner)| owner == *id || believed_target == target)
                || (!edited
                    && newest_ledger(github, c.pr)
                        .and_then(|(_, l)| l.settled_through)
                        .is_some_and(|through| *id <= through));
            let could_have_been_accepted =
                edited || (target != c.pr && c.author_id != 0 && c.author_id == author);
            (!accounted_for && could_have_been_accepted).then_some((c.pr, target))
        })
        .collect();
    evidence.sort_unstable();
    evidence
}

/// The edge GitHub itself attests for `pr`: the newest stack ledger on
/// it, corroborated by the comment that ledger names. This is exactly
/// what `crawl_events` believes, and the harness computes it
/// independently — from the fake's comments — rather than asking the
/// crawl. Corroboration is in the PR AUTHOR'S OWN BYTES: a comment
/// whose text still reads as the declaration but was last edited by
/// an unknown hand corroborates nothing, live or in recovery (Codex
/// harness review round 11, P2).
fn attested_edge(github: &FakeGitHub, pr: PrNumber) -> Option<(PrNumber, CommentId)> {
    let (_, ledger) = newest_ledger(github, pr)?;
    let declared = ledger.declared?;
    let author = github.prs.get(&pr)?.author_id;
    let corroborated = github.comments.get(&declared.owner).is_some_and(|c| {
        c.pr == pr
            && written_by(c, author)
            && matches!(
                parse_command(&c.body, "merge-train"),
                Some(Command::Predecessor(t)) if t == declared.predecessor
            )
    });
    corroborated.then_some((declared.predecessor, declared.owner))
}

/// A deterministic sample of a strategy, for the fixed cases below.
fn fixed_sample<S: Strategy>(strategy: &S) -> S::Value {
    let mut det = TestRunner::deterministic();
    strategy.new_tree(&mut det).expect("a value tree").current()
}

/// Runs one fixed case at increasing crash depths until every
/// `required` coverage key is hit.
///
/// A depth indexes the batches a saga happens to produce, and that
/// sequence changes whenever the worker gains or loses a step — the
/// stack ledger's writes added several. Hand-tuned depths rot silently
/// into cases that no longer reach the state they were chosen for, so
/// the harness searches for one instead of asserting a guess.
fn at_a_depth_hitting(
    property: &'static str,
    what: &str,
    required: &[&'static str],
    mut case: impl FnMut(usize),
) {
    // Deep enough to pass the first squash and the descendants'
    // reconciliation against it for the two-PR fixed cases (the first
    // squash lands around depth 17, reconciliation around 20), with
    // headroom for the worker gaining steps (Codex harness review
    // round 12, P2).
    const MAX_DEPTH: usize = 28;
    for depth in 1..=MAX_DEPTH {
        let before: Vec<u32> = required
            .iter()
            .map(|key| coverage::count(property, key))
            .collect();
        case(depth);
        if required
            .iter()
            .zip(&before)
            .all(|(key, was)| coverage::count(property, key) > *was)
        {
            return;
        }
    }
    panic!(
        "{property}: no crash depth up to {MAX_DEPTH} exercised {required:?} for \
         {what}. Coverage: {}",
        coverage::report(property)
    );
}

type DifferentialInputs = (
    Vec<usize>,
    Vec<(u8, Index, Index)>,
    Vec<(u8, Index, Index)>,
    usize,
);

fn differential_strategy() -> impl Strategy<Value = DifferentialInputs> {
    (
        arb_bases(),
        proptest::collection::vec(any::<(u8, Index, Index)>(), 1..8),
        proptest::collection::vec(any::<(u8, Index, Index)>(), 0..4),
        // Past the first squash and the reconciliation behind it (see
        // `at_a_depth_hitting`'s `MAX_DEPTH`).
        1usize..=24,
    )
}

fn differential_case(regime: &str, (bases, mut decls, mut cmds, depth): DifferentialInputs) {
    const P: &str = "differential";
    if regime == "train" {
        // No late declaration: the differential never races a
        // declaration against a running train (recovery is deliberately
        // stricter there).
        force_train(&bases, &mut decls, &mut cmds, false);
    }
    let (live_world, mut live) = run_live(&bases, &decls, &cmds);
    let (lost_world, mut lost, exempt_prs, exempt_roots, attested_at_loss) =
        run_lost(&bases, &decls, &cmds, depth, P);
    assert_equivalent(
        &live_world,
        &mut live,
        &lost_world,
        &mut lost,
        &exempt_prs,
        &exempt_roots,
        attested_at_loss.as_ref(),
    );
}

/// With a QUIET gap — nothing touched GitHub while the DB was gone —
/// losing the database at any saga depth must be unobservable once the
/// crawl and M6 recovery finish. Two regimes: with a forced train, and
/// free. Each regime runs one FIXED case first — a two-PR stack, an
/// honest declaration, a start on the root, crashed mid-phase — whose
/// coverage is asserted exactly; the random cases' distribution is
/// reported, never asserted (probabilistic floors were flaky at higher
/// case counts: Codex harness review round 2, P2).
#[test]
fn a_db_loss_with_a_quiet_gap_is_unobservable() {
    const P: &str = "differential";
    coverage::reset(P);
    for regime in ["train", "free"] {
        if regime == "train" {
            let (_, sample, _, _) = fixed_sample(&differential_strategy());
            let (_, a, b) = sample[0];
            for required in [
                &["adoption", "crash mid-phase"],
                &["adoption", "crash after a squash"],
                &["adoption", "crash while reconciling"],
            ] {
                at_a_depth_hitting(P, "a forced train", required, |depth| {
                    differential_case(
                        regime,
                        (vec![0, 1], vec![(0, a, b)], vec![(0, a, b)], depth),
                    );
                });
            }
        }
        let mut runner = TestRunner::new(runner_config(2));
        runner
            .run(&differential_strategy(), |inputs| {
                differential_case(regime, inputs);
                Ok(())
            })
            .unwrap_or_else(|e| panic!("differential, regime `{regime}`: {e}"));
    }
    eprintln!("{P} coverage: {}", coverage::report(P));
}

// ── The oracle's own fidelity: cases a review found it wrong on ──

/// A deterministic `Index` that picks `want` out of `len`: the
/// decoders choose subjects by index, and a fixed case must name its
/// subject.
fn index_picking(want: usize, len: usize) -> Index {
    let mut runner = TestRunner::deterministic();
    (0..1000)
        .map(|_| any::<Index>().new_tree(&mut runner).unwrap().current())
        .find(|i| i.index(len) == want)
        .unwrap()
}

/// Every crash depth a fixed envelope case can be cut at: a depth
/// indexes the batches a saga happens to produce, so a case pinned to
/// one depth rots silently into a state it was not chosen for (see
/// `at_a_depth_hitting`); a fixed case that must reach a window sweeps
/// them all instead.
const SWEPT_DEPTHS: std::ops::RangeInclusive<usize> = 1..=24;

/// The no-backup exemption must be decided by GitHub, not by the lost
/// record: a `PostComment` that landed before its outcome was recorded
/// leaves `status_comment_id` empty in the store while a recoverable
/// status comment stands on the root. Exempting that train would hide
/// an abandonment or abort at exactly the boundary recovery exists for
/// (Codex harness review round 11, P2). Swept over every depth.
#[test]
fn a_train_whose_comment_landed_is_not_exempted_for_lacking_one() {
    let (_, sample, _, _) = fixed_sample(&differential_strategy());
    let (_, a, b) = sample[0];
    let mut seen_the_window = false;
    for depth in 1..=14 {
        let (mut world, heads) = build_world(&[0, 1]);
        let mut processor = world.processor();
        let mut history = enqueue_history(
            &mut world,
            &mut processor,
            &[0, 1],
            &heads,
            &[(0, a, b)],
            &[(0, a, b)],
            false,
        );
        let (at_loss, pending) =
            run_batches_then_snapshot(&mut world, processor, depth, &mut history);
        let (_, exempt) = loss_exemptions(&world, &at_loss, &pending);
        let github = world.github.lock().unwrap();
        for (root, record) in &at_loss.active_trains {
            let backup_exists = github.comments.values().any(|c| {
                c.pr == *root
                    && c.author_id == TEST_BOT_ID
                    && parse_status_comment(&c.body)
                        .is_ok_and(|r| r.started_at == record.started_at)
            });
            if backup_exists && record.status_comment_id.is_none() {
                seen_the_window = true;
            }
            assert!(
                !(backup_exists && exempt.contains(&root.0)),
                "depth {depth}: train #{root} has a recoverable status comment yet is exempted"
            );
        }
    }
    assert!(
        seen_the_window,
        "the sweep never reached the post-landed-unrecorded window"
    );
}

/// Only a repository with NOT ONE comment of the bot's is onboarded:
/// the crawl counts any bot comment as prior contact, so a history of
/// refusals alone is a recovery, and the topology comparison applies
/// to it. An oracle that waived the comparison for lack of a ledger or
/// status comment would let recovery invent edges from declarations
/// the bot refused (Codex harness review round 11, P2).
#[test]
fn a_history_of_refusals_alone_is_a_recovery_not_an_onboarding() {
    let (_, sample, _, _) = fixed_sample(&differential_strategy());
    let (_, a, b) = sample[0];
    let (world, _, _, _, attested) = run_lost(&[0, 0], &[(2, a, b)], &[], 1, "probe");
    let github = world.github.lock().unwrap();
    let bot_replies = github
        .comments
        .values()
        .filter(|c| c.author_id == TEST_BOT_ID)
        .count();
    assert!(
        bot_replies > 0,
        "precondition: the junk declaration was refused with a reply"
    );
    assert!(
        attested.is_some(),
        "a bot reply is prior contact: the topology comparison must apply"
    );
}

/// A ledger's OWNING comment edited during the gap to name another
/// predecessor no longer corroborates the ledger, so the crawl reads
/// it as unledgered evidence and follows its new target. The oracle's
/// accounting must say the same: "accounted for" is judged against
/// the CORROBORATED edge, not the ledger's text (Codex harness review
/// round 13, P2).
#[test]
fn an_owning_comment_edited_to_name_another_predecessor_is_evidence() {
    let (mut world, heads) = build_world(&[0, 1, 0]);
    let mut processor = world.processor();
    let z = index_picking(0, 4);
    enqueue_history(
        &mut world,
        &mut processor,
        &[0, 1, 0],
        &heads,
        &[(0, z, z)],
        &[],
        false,
    );
    let announce = all_prs(&world);
    settle(&mut world, &mut processor, &announce);
    let mut github = world.github.lock().unwrap();
    let (_, ledger) = newest_ledger(&github, PrNumber(2)).unwrap();
    let owner = github
        .comments
        .get_mut(&ledger.declared.unwrap().owner)
        .unwrap();
    owner.body = "@merge-train predecessor #3".to_owned();
    owner.edited = Edited::By { editor: None };
    assert_eq!(
        attested_edge(&github, PrNumber(2)),
        None,
        "no longer corroborated"
    );
    assert!(
        unledgered_evidence(&github).contains(&(PrNumber(2), PrNumber(3))),
        "the edited owner is evidence of its new target"
    );
}

/// A lost command excuses the two worlds DIFFERING from each other. It
/// never excuses a world disagreeing with the GitHub that world can
/// see, so every fact the cache holds about an exempt PR must be
/// checked against that world's own GitHub exactly as on any other PR:
/// its head (Codex review of #90, round 3, P2), and its state and
/// routing (round 4, P2). Each corruption must be caught by the check
/// that names it, not incidentally by another.
#[test]
fn a_cache_disagreeing_with_its_own_github_is_caught_under_a_command_loss_exemption() {
    let (_, sample, _, _) = fixed_sample(&differential_strategy());
    let (_, a, b) = sample[0];
    let pr = PrNumber(2);
    let run = |depth| {
        let (live_world, live) = run_live(&[0, 1], &[(0, a, b)], &[(0, a, b)]);
        let (lost_world, lost, exempt_prs, exempt_roots, attested) =
            run_lost(&[0, 1], &[(0, a, b)], &[(0, a, b)], depth, "probe");
        (
            live_world,
            live,
            lost_world,
            lost,
            exempt_prs,
            exempt_roots,
            attested,
        )
    };
    let depth = SWEPT_DEPTHS
        .clone()
        .find(|&depth| {
            let (live_world, mut live, lost_world, mut lost, exempt_prs, exempt_roots, attested) =
                run(depth);
            if !exempt_prs.contains(&pr) {
                return false;
            }
            // As recovered, the two worlds are equivalent.
            assert_equivalent(
                &live_world,
                &mut live,
                &lost_world,
                &mut lost,
                &exempt_prs,
                &exempt_roots,
                attested.as_ref(),
            );
            true
        })
        .expect("no depth left PR #2 exempt through a lost command");

    type Corruption = fn(&CachedPr) -> StateEventPayload;
    let corruptions: [(&str, Corruption, &str); 5] = [
        (
            "a stale head",
            |c| StateEventPayload::PrSynchronized {
                pr: c.number,
                new_head_sha: Sha::parse("0".repeat(40)).unwrap(),
            },
            "cached head is stale",
        ),
        (
            "a wrong base",
            |c| StateEventPayload::PrBaseChanged {
                pr: c.number,
                old_base: c.base_ref.clone(),
                new_base: "wrong-base".to_owned(),
            },
            "cached state or routing disagrees with its GitHub",
        ),
        (
            "a wrong head branch",
            |c| StateEventPayload::PrOpened {
                pr: c.number,
                head_sha: c.head_sha.clone(),
                head_ref: "wrong-head".to_owned(),
                base_ref: c.base_ref.clone(),
                is_draft: c.is_draft,
            },
            "cached state or routing disagrees with its GitHub",
        ),
        (
            "a closure GitHub never saw",
            |c| StateEventPayload::PrClosed { pr: c.number },
            "cached state or routing disagrees with its GitHub",
        ),
        (
            "a draft GitHub never saw",
            |c| StateEventPayload::PrConvertedToDraft { pr: c.number },
            "cached state or routing disagrees with its GitHub",
        ),
    ];
    let mut escaped = Vec::new();
    for (what, corrupt, expected) in corruptions {
        let (live_world, mut live, lost_world, mut lost, exempt_prs, exempt_roots, attested) =
            run(depth);
        assert!(
            exempt_prs.contains(&pr),
            "depth {depth} stopped exempting PR #2"
        );
        let event = corrupt(&lost.state().prs[&pr]);
        lost.store_mut().append(event, Utc::now()).unwrap();
        let verdict = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_equivalent(
                &live_world,
                &mut live,
                &lost_world,
                &mut lost,
                &exempt_prs,
                &exempt_roots,
                attested.as_ref(),
            )
        }));
        match verdict {
            Ok(()) => escaped.push(format!("{what}: accepted")),
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                    .unwrap_or_default();
                if !message.contains(&format!("lost-db: PR #{pr}'s {expected}")) {
                    escaped.push(format!("{what}: rejected by another check: {message}"));
                }
            }
        }
    }
    assert!(
        escaped.is_empty(),
        "depth {depth}: corruptions of an exempt PR's cache the oracle missed:\n{}",
        escaped.join("\n")
    );
}

/// A pending `start` that a later pending `stop` CANCELS changes
/// nothing, and neither does the stop: live consumes the queued start
/// without running it, so losing both leaves the PR exactly where
/// losing neither would have. Exempting it would let the oracle accept
/// that PR merged in one world and open in the other (Codex review of
/// #90, round 2, P2).
#[test]
fn a_lost_start_cancelled_by_a_lost_stop_excuses_nothing() {
    let (_, sample, _, _) = fixed_sample(&differential_strategy());
    let (_, a, _) = sample[0];
    let on_first = index_picking(0, 2);
    let mut checked = false;
    for depth in SWEPT_DEPTHS {
        let (mut world, heads) = build_world(&[0, 1]);
        let mut processor = world.processor();
        let mut history = enqueue_history(
            &mut world,
            &mut processor,
            &[0, 1],
            &heads,
            &[],
            &[(0, a, a), (2, on_first, on_first)],
            false,
        );
        let (at_loss, pending) =
            run_batches_then_snapshot(&mut world, processor, depth, &mut history);
        let queued_in_order: Vec<(&'static str, u64)> = pending
            .iter()
            .map(|c| match c {
                DurableCommand::Start { pr } => ("start", pr.0),
                DurableCommand::Stop { pr, .. } => ("stop", pr.0),
            })
            .collect();
        if queued_in_order != [("start", 1), ("stop", 1)] {
            continue;
        }
        let (exempt_prs, exempt_roots) = loss_exemptions(&world, &at_loss, &pending);
        assert!(
            !exempt_prs.contains(&PrNumber(1)) && !exempt_roots.contains(&1),
            "depth {depth}: the stop cancels the start, so the pair excuses nothing"
        );
        checked = true;
        break;
    }
    assert!(
        checked,
        "no depth left the start and its cancelling stop pending"
    );
}

/// A pending `stop` excuses something only when it would have changed
/// something: it names a train, or it cancels a queued `start` that
/// would itself have run. A stop naming a PR whose queued start the
/// engine would refuse cancels nothing an answer would not have
/// refused anyway, so it excuses nothing either — and exempting that
/// PR would let the oracle accept it merged in one world and open in
/// the other (Codex review of #90, P2).
#[test]
fn a_lost_stop_cancelling_an_ineligible_start_excuses_nothing() {
    let (_, sample, _, _) = fixed_sample(&differential_strategy());
    let (_, a, _) = sample[0];
    let on_second = index_picking(1, 2);
    let mut checked = false;
    for depth in SWEPT_DEPTHS {
        let (mut world, heads) = build_world(&[0, 1]);
        let mut processor = world.processor();
        let mut history = enqueue_history(
            &mut world,
            &mut processor,
            &[0, 1],
            &heads,
            &[],
            &[
                (0, a, a),
                (1, on_second, on_second),
                (2, on_second, on_second),
            ],
            false,
        );
        let (at_loss, pending) =
            run_batches_then_snapshot(&mut world, processor, depth, &mut history);
        let queued = |want: fn(&DurableCommand) -> Option<PrNumber>, pr: u64| {
            pending.iter().filter_map(want).any(|p| p.0 == pr)
        };
        let start_of = |c: &DurableCommand| match c {
            DurableCommand::Start { pr } => Some(*pr),
            DurableCommand::Stop { .. } => None,
        };
        let stop_of = |c: &DurableCommand| match c {
            DurableCommand::Stop { pr, .. } => Some(*pr),
            DurableCommand::Start { .. } => None,
        };
        if !(queued(start_of, 1) && queued(start_of, 2) && queued(stop_of, 2)) {
            continue;
        }
        assert!(
            at_loss.prs.get(&PrNumber(2)).is_some_and(|cached| {
                !crate::state::topology::is_root(cached, &at_loss.default_branch, &at_loss.prs)
            }),
            "precondition: #2 is no root, so its start would be refused"
        );
        let (exempt_prs, exempt_roots) = loss_exemptions(&world, &at_loss, &pending);
        assert!(
            !exempt_prs.contains(&PrNumber(2)) && !exempt_roots.contains(&2),
            "depth {depth}: the stop cancels a start that would never have run, \
             so it excuses nothing"
        );
        checked = true;
        break;
    }
    assert!(checked, "no depth left both starts and the stop pending");
}

/// A pending `start` on a PR that is no root — based on a sibling's
/// branch, with no declaration — is refused by the engine and changes
/// nothing, so losing it excuses nothing: the differential must keep
/// comparing that PR and its train outcome (Codex harness review
/// round 18, P2).
#[test]
fn a_lost_start_on_a_non_root_excuses_nothing() {
    let (_, sample, _, _) = fixed_sample(&differential_strategy());
    let (_, a, _) = sample[0];
    let on_second = index_picking(1, 2);
    let mut checked = false;
    for depth in SWEPT_DEPTHS {
        let (mut world, heads) = build_world(&[0, 1]);
        let mut processor = world.processor();
        let mut history = enqueue_history(
            &mut world,
            &mut processor,
            &[0, 1],
            &heads,
            &[],
            &[(0, a, a), (1, on_second, on_second)],
            false,
        );
        let (at_loss, pending) =
            run_batches_then_snapshot(&mut world, processor, depth, &mut history);
        let both_pending = [1, 2].iter().all(|pr| {
            pending
                .iter()
                .any(|c| matches!(c, DurableCommand::Start { pr: p } if p.0 == *pr))
        });
        if !both_pending {
            continue;
        }
        let (exempt_prs, exempt_roots) = loss_exemptions(&world, &at_loss, &pending);
        assert!(
            !exempt_prs.contains(&PrNumber(2)) && !exempt_roots.contains(&2),
            "depth {depth}: the start on non-root #2 changes nothing and excuses nothing"
        );
        checked = true;
        break;
    }
    assert!(checked, "no depth left both starts pending");
}
