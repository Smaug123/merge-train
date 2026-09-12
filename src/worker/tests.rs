//! Worker integration tests: real git, real Store, fake GitHub.
//!
//! The pipeline/saga tests drive [`Processor`] synchronously — the exact
//! calls the worker thread makes, minus the threads — so every durability
//! boundary is a call boundary and crashes are simulated by dropping the
//! `Store` and reopening it. The registry tests at the bottom exercise the
//! real async intake path end-to-end.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use crate::cascade::{EffectOutcome, ReplayFacts};
use crate::git::interpreter::WorktreeGitInterpreter;
use crate::git::test_support::{
    create_branch_with_file, create_pr_ref, create_test_repo_with_origin,
};
use crate::git::{GitConfig, run_git_stdout};
use crate::github::test_support::{FakeComment, FakeGitHub, FakePr, FakePrState};
use crate::state::RepoState;
use crate::store::Store;
use crate::types::{PrNumber, Sha};

use super::executor::{GitHubExec, SagaBatch, execute_batch};
use super::pipeline::{PipelineOutcome, Processor, WorkerDeps};
use super::test_support::TEST_BOT_ID;
use super::{GitSettings, IntakeDelivery, WorkerMsg};
use crate::effects::Effect;
use crate::effects::github::GitHubEffect;

// ─── Identities ───

/// The PR author in these tests.
const AUTHOR: u64 = 100;
/// A user who is not the PR author and holds no role unless granted.
const STRANGER: u64 = 200;

// ─── Payload builders (the raw JSON the pipeline parses) ───

fn repo_json(config: &GitConfig) -> String {
    format!(
        r#"{{ "owner": {{ "login": "{}" }}, "name": "{}" }}"#,
        config.owner, config.repo
    )
}

fn pr_opened_body(
    config: &GitConfig,
    number: u64,
    head: &Sha,
    branch: &str,
    base: &str,
) -> Vec<u8> {
    format!(
        r#"{{
            "action": "opened",
            "pull_request": {{
                "number": {number},
                "state": "open",
                "draft": false,
                "merged": false,
                "head": {{ "sha": "{head}", "ref": "{branch}" }},
                "base": {{ "sha": "{base_sha}", "ref": "{base}" }},
                "user": {{ "id": {AUTHOR}, "login": "author" }},
                "updated_at": "2026-07-01T10:00:00Z"
            }},
            "repository": {repo}
        }}"#,
        base_sha = "0".repeat(40),
        repo = repo_json(config),
    )
    .into_bytes()
}

fn pr_merged_body(
    config: &GitConfig,
    number: u64,
    head: &Sha,
    branch: &str,
    base: &str,
    merge_sha: &Sha,
) -> Vec<u8> {
    format!(
        r#"{{
            "action": "closed",
            "pull_request": {{
                "number": {number},
                "state": "closed",
                "draft": false,
                "merged": true,
                "merge_commit_sha": "{merge_sha}",
                "head": {{ "sha": "{head}", "ref": "{branch}" }},
                "base": {{ "sha": "{base_sha}", "ref": "{base}" }},
                "user": {{ "id": {AUTHOR}, "login": "author" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "repository": {repo}
        }}"#,
        base_sha = "0".repeat(40),
        repo = repo_json(config),
    )
    .into_bytes()
}

fn comment_body(
    config: &GitConfig,
    pr: u64,
    text: &str,
    commenter_id: u64,
    commenter_login: &str,
    comment_id: u64,
) -> Vec<u8> {
    comment_body_with_action(
        config,
        pr,
        text,
        commenter_id,
        commenter_login,
        comment_id,
        "created",
    )
}

#[allow(clippy::too_many_arguments)]
fn comment_body_with_action(
    config: &GitConfig,
    pr: u64,
    text: &str,
    commenter_id: u64,
    commenter_login: &str,
    comment_id: u64,
    action: &str,
) -> Vec<u8> {
    format!(
        r#"{{
            "action": "{action}",
            "comment": {{
                "id": {comment_id},
                "body": "{text}",
                "user": {{ "id": {commenter_id}, "login": "{commenter_login}" }},
                "updated_at": "2026-07-01T10:00:00Z"
            }},
            "issue": {{
                "number": {pr},
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {commenter_id}, "login": "{commenter_login}" }}
        }}"#,
        repo = repo_json(config),
    )
    .into_bytes()
}

fn check_suite_green_body(config: &GitConfig, head: &Sha, prs: &[u64], suite_id: u64) -> Vec<u8> {
    let prs_json: Vec<String> = prs
        .iter()
        .map(|n| format!("{{ \"number\": {n} }}"))
        .collect();
    format!(
        r#"{{
            "action": "completed",
            "check_suite": {{
                "id": {suite_id},
                "head_sha": "{head}",
                "conclusion": "success",
                "pull_requests": [{prs}],
                "updated_at": "2026-07-01T11:00:00Z"
            }},
            "repository": {repo}
        }}"#,
        prs = prs_json.join(", "),
        repo = repo_json(config),
    )
    .into_bytes()
}

// ─── The world: a real stack on a real remote + a fake GitHub ───

struct World {
    _temp: TempDir,
    state_dir: TempDir,
    config: GitConfig,
    github: Arc<Mutex<FakeGitHub>>,
    /// Monotonic delivery-id source.
    next_delivery: u64,
}

impl World {
    /// A linear stack of `n` PRs on a real repo: `pr-1` targets main, each
    /// `pr-k` targets `pr-(k-1)`. The root's branch gets an extra commit
    /// after descendants fork, so preparation does real merge work.
    fn linear_stack(n: usize) -> (World, Vec<Sha>) {
        let (temp, config, _initial) = create_test_repo_with_origin();
        let mut fake_prs = HashMap::new();
        let mut heads = Vec::new();
        for i in 1..=n {
            let branch = format!("pr-{i}");
            let base = if i == 1 {
                "main".to_owned()
            } else {
                format!("pr-{}", i - 1)
            };
            let head = create_branch_with_file(
                &config,
                &branch,
                &format!("pr-{i}.txt"),
                &format!("content {i}"),
                &base,
            );
            create_pr_ref(&config, i as u64, &head);
            fake_prs.insert(
                PrNumber(i as u64),
                FakePr {
                    branch,
                    base_ref: base,
                    state: FakePrState::Open,
                    author_id: AUTHOR,
                },
            );
            heads.push(head);
        }
        // Advance the root after the fork so preparation is a real merge.
        if n > 1 {
            let head = create_branch_with_file(&config, "pr-1", "pr-1-fix.txt", "fix", "pr-1");
            create_pr_ref(&config, 1, &head);
            heads[0] = head;
        }
        let mut fake = FakeGitHub::new(config.clone(), fake_prs);
        fake.comment_author = TEST_BOT_ID;
        let github = Arc::new(Mutex::new(fake));
        let world = World {
            _temp: temp,
            state_dir: TempDir::new().unwrap(),
            config,
            github,
            next_delivery: 0,
        };
        (world, heads)
    }

    fn db_path(&self) -> PathBuf {
        self.state_dir.path().join("state.db")
    }

    fn deps(&self) -> WorkerDeps {
        WorkerDeps {
            github: GitHubExec::Fake(self.github.clone()),
            git: GitSettings {
                base_dir: self.config.base_dir.clone(),
                owner: self.config.owner.clone(),
                repo: self.config.repo.clone(),
                commit_identity: self.config.commit_identity.clone(),
                worktree_max_age: self.config.worktree_max_age,
                clone_url: None,
            },
            bot_user_id: TEST_BOT_ID,
            bot_name: "merge-train".to_owned(),
            stall_retry_delay: std::time::Duration::from_millis(25),
            poll_interval: std::time::Duration::ZERO,
        }
    }

    fn processor(&self) -> Processor {
        Processor::new(Store::open(&self.db_path()).unwrap(), self.deps()).unwrap()
    }

    /// Durably enqueues a raw delivery (as the intake path would).
    fn enqueue(&mut self, processor: &mut Processor, event_type: &str, body: Vec<u8>) {
        self.next_delivery += 1;
        let id = format!("delivery-{}", self.next_delivery);
        processor
            .store_mut()
            .enqueue(&id, event_type, "{}", &body, chrono::Utc::now())
            .unwrap();
    }

    /// The standard opening moves: every PR announced, predecessors declared
    /// by the author.
    fn enqueue_stack_setup(&mut self, processor: &mut Processor, n: usize, heads: &[Sha]) {
        let config = self.config.clone();
        for i in 1..=n {
            let base = if i == 1 {
                "main".to_owned()
            } else {
                format!("pr-{}", i - 1)
            };
            let body = pr_opened_body(&config, i as u64, &heads[i - 1], &format!("pr-{i}"), &base);
            self.enqueue(processor, "pull_request", body);
        }
        for i in 2..=n {
            let body = comment_body(
                &config,
                i as u64,
                &format!("@merge-train predecessor #{}", i - 1),
                AUTHOR,
                "author",
                i as u64 * 10,
            );
            self.enqueue(processor, "issue_comment", body);
        }
    }
}

// ─── The synchronous drive loop (the worker thread, minus the threads) ───

/// Executes one batch exactly as the executor thread would.
fn execute(processor: &mut Processor, batch: &SagaBatch) -> Vec<EffectOutcome> {
    let interpreter = WorktreeGitInterpreter::new(processor.git_config(), batch.root);
    let result = execute_batch(&interpreter, processor.github(), batch);
    // As the worker does on `SagaOutcomes`: bookkeeping before the boundary.
    processor.note_best_effort(&result.best_effort).unwrap();
    result.observed
}

/// Executes the batches a boundary handed back, to quiescence — what the
/// worker loop does with `on_outcomes`' return value.
fn finish_batches(_world: &mut World, processor: &mut Processor, mut next: Option<SagaBatch>) {
    let mut steps = 0;
    while let Some(batch) = next {
        steps += 1;
        assert!(steps < 200, "batches did not settle");
        let outcomes = execute(processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    }
}

/// Whether a batch is stack-ledger bookkeeping rather than cascade work:
/// the probe that finds a PR's ledger comment, or the write that states
/// its declaration. Tests that step a saga by hand care about neither.
fn is_ledger_bookkeeping(processor: &mut Processor, batch: &SagaBatch) -> bool {
    let writes_a_ledger = batch.best_effort.iter().any(|e| match e {
        Effect::GitHub(
            GitHubEffect::PostComment { body, .. } | GitHubEffect::UpdateComment { body, .. },
        ) => crate::status::parse_stack_ledger(body).is_some(),
        _ => false,
    });
    let probes_a_ledger = batch.effects.len() == 1
        && matches!(
            &batch.effects[0],
            Effect::GitHub(GitHubEffect::ListComments { pr }) if *pr == batch.root
        )
        && processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .iter()
            .any(|o| o.pr == batch.root);
    writes_a_ledger || probes_a_ledger
}

/// Pumps to the next batch the CASCADE asked for, running any stack-ledger
/// bookkeeping it meets on the way. The ledger is written by the same
/// worker loop as everything else, so a test that steps a saga by hand
/// would otherwise have to hand-hold those batches too.
fn pump_cascade(processor: &mut Processor) -> Option<SagaBatch> {
    let mut next = processor.pump().unwrap();
    loop {
        let batch = next?;
        if !is_ledger_bookkeeping(processor, &batch) {
            return Some(batch);
        }
        let outcomes = execute(processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
        if next.is_none() {
            next = processor.pump().unwrap();
        }
    }
}

/// Runs queued sagas to quiescence (Park/Done and no pending work).
fn run_sagas(processor: &mut Processor) {
    let mut steps = 0;
    let mut next = processor.pump().unwrap();
    while let Some(batch) = next {
        steps += 1;
        assert!(steps < 500, "saga did not terminate");
        let outcomes = execute(processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    }
}

/// Processes every pending delivery, then runs sagas, until quiescent.
fn drain(processor: &mut Processor) {
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(rounds < 100, "drain did not settle");
        let mut did_work = false;
        while let Some(delivery) = processor.claim().unwrap() {
            did_work = true;
            assert_eq!(
                processor.process_claimed(delivery).unwrap(),
                PipelineOutcome::Processed,
                "no test in this harness expects a release"
            );
        }
        run_sagas(processor);
        if !did_work {
            return;
        }
    }
}

/// Drives to full train completion, nudging any `WaitingCi` train with a
/// green check-suite webhook for its root's current heads (reality's job).
fn drive_to_completion(world: &mut World, processor: &mut Processor) {
    for _ in 0..20 {
        drain(processor);
        let waiting: Vec<PrNumber> = processor
            .state()
            .active_trains
            .values()
            .filter(|t| t.state.is_active())
            .map(|t| t.current_pr)
            .collect();
        if waiting.is_empty() {
            return;
        }
        for pr in waiting {
            let (head, suite) = {
                let github = world.github.lock().unwrap();
                let branch = github.prs[&pr].branch.clone();
                (github.branch_head(&branch), world.next_delivery + 900)
            };
            let body = check_suite_green_body(&world.config, &head, &[pr.0], suite);
            world.enqueue(processor, "check_suite", body);
        }
    }
    panic!(
        "trains did not complete: {:?}",
        processor.state().active_trains
    );
}

fn start_command(world: &mut World, processor: &mut Processor, pr: u64) {
    let body = comment_body(
        &world.config,
        pr,
        "@merge-train start",
        AUTHOR,
        "author",
        500 + pr,
    );
    world.enqueue(processor, "issue_comment", body);
}

// ─── End-to-end: the money test ───

#[test]
fn start_command_runs_train_to_completion_end_to_end() {
    let (mut world, heads) = World::linear_stack(3);
    let mut processor = world.processor();

    world.enqueue_stack_setup(&mut processor, 3, &heads);
    start_command(&mut world, &mut processor, 1);
    drive_to_completion(&mut world, &mut processor);

    let github = world.github.lock().unwrap();
    let clone_dir = world.config.clone_dir();

    // Every PR squashed exactly once; every train retired.
    for i in 1..=3u64 {
        let pr = PrNumber(i);
        assert!(
            matches!(github.prs[&pr].state, FakePrState::Merged { .. }),
            "PR {pr} did not merge"
        );
        assert_eq!(github.squash_count.get(&pr), Some(&1));
        assert!(processor.state().prs[&pr].state.is_merged());
    }
    assert!(processor.state().active_trains.is_empty());

    // All content — including the root's post-fork fix — reached real main.
    let main_tree =
        run_git_stdout(&clone_dir, &["ls-tree", "--name-only", "refs/heads/main"]).unwrap();
    for name in ["pr-1.txt", "pr-1-fix.txt", "pr-2.txt", "pr-3.txt"] {
        assert!(main_tree.contains(name), "{name} missing from main");
    }

    // Delivery accounting: everything processed and closed.
    assert!(processor.claim().unwrap().is_none());

    // No dangling intents in the durable ledger.
    let events = processor.store_mut().events().unwrap();
    for i in 1..=3u64 {
        let facts = ReplayFacts::for_train(&events, PrNumber(i));
        assert_eq!(facts.unmatched().count(), 0, "unmatched intents on #{i}");
    }
}

// ─── Dedupe ───

#[test]
fn duplicate_content_under_new_delivery_id_is_skipped() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    let declared_events = processor.store_mut().events().unwrap().len();

    // GitHub redelivers the predecessor comment under a fresh delivery id:
    // identical content, so the dedupe key already exists.
    let body = comment_body(
        &world.config,
        2,
        "@merge-train predecessor #1",
        AUTHOR,
        "author",
        20,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);

    assert_eq!(
        processor.store_mut().events().unwrap().len(),
        declared_events,
        "a duplicate delivery must not re-run the handler"
    );
}

/// A retired incarnation's owed sync is discharged BEFORE its successor
/// starts. Once the successor's saga is running its feedback batches chain
/// without returning to the pump, so a sync queued behind it can wait out
/// the whole cascade while the old comment still says ACTIVE — the exact
/// window the obligation exists to close (Codex terminal-sync review round
/// 11, P1). The deferral happens once: an obligation that can never be
/// discharged must not stall the start forever.
#[test]
fn an_owed_sync_is_discharged_before_its_roots_next_start() {
    let (mut world, heads) = World::linear_stack(2);
    world.github.lock().unwrap().blocked.insert(PrNumber(1));
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    drain(&mut processor);

    // The stop's final update cannot land: the obligation stays owed.
    world.github.lock().unwrap().update_comment_broken = true;
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    drain(&mut processor);
    assert_eq!(
        processor.owed_status_comments().len(),
        1,
        "precondition: the retired incarnation owes its comment"
    );

    // The user starts again on the same root.
    let restart = comment_body(
        &world.config,
        1,
        "@merge-train start",
        AUTHOR,
        "author",
        600,
    );
    world.enqueue(&mut processor, "issue_comment", restart);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let first = processor.pump().unwrap().expect("work is queued");
    assert!(
        first.effects.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::ListComments { pr }) if *pr == PrNumber(1)
        )),
        "the owed sync's probe must go first, not the start's preflight: {:?}",
        first.effects
    );

    // And the start is not stalled behind an obligation that can never be
    // discharged: it runs on the next turn.
    let outcomes = execute(&mut processor, &first);
    let next = processor
        .on_outcomes(first.root, outcomes, first.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    drain(&mut processor);
    assert!(
        processor.state().active_trains[&PrNumber(1)]
            .state
            .is_active(),
        "the successor train starts anyway"
    );
}

/// A fan-out's last word names the independent trains it spawned. The
/// record alone says only "completed", so the retry must not rebuild the
/// message from it: the intended text is persisted with the obligation
/// (Codex terminal-sync review round 11, P2).
#[test]
fn a_retried_fan_out_completion_still_names_its_new_roots() {
    let (mut world, heads) = World::linear_stack(1);
    world.github.lock().unwrap().blocked.insert(PrNumber(1));
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);
    start_command(&mut world, &mut processor, 1);
    drain(&mut processor);
    let status_id = processor.state().active_trains[&PrNumber(1)]
        .status_comment_id
        .expect("the train has a status comment");

    // The fan-out lands, and its own status update never runs.
    processor
        .store_mut()
        .append_batch(
            &[
                crate::persistence::event::StateEventPayload::FanOutCompleted {
                    old_root: PrNumber(1),
                    new_roots: vec![PrNumber(2), PrNumber(3)],
                    original_root_pr: PrNumber(1),
                },
            ],
            chrono::Utc::now(),
        )
        .unwrap();
    assert_eq!(
        processor.owed_status_comments().len(),
        1,
        "the fan-out owes the old root's comment its final word"
    );

    // Any delivery re-queues the owed syncs, as the stall-retry timer does.
    let remark = comment_body(&world.config, 1, "a remark", AUTHOR, "author", 9100);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    assert!(
        processor.owed_status_comments().is_empty(),
        "the retry landed"
    );
    let github = world.github.lock().unwrap();
    let body = &github.comments[&status_id].body;
    assert!(
        body.contains("#2") && body.contains("#3"),
        "the retried final comment must still name the spawned roots: {body}"
    );
    assert!(
        body.contains("independent trains"),
        "and say what they are: {body}"
    );
}

/// `GitEffect::CleanupWorktree` is root-relative — the executor resolves
/// it against the batch's root — so an abort's cleanup must ride a batch
/// rooted at ITS train. Anything else cleans one worktree and leaves the
/// other dirty (Codex terminal-sync review round 12, P2).
#[test]
fn abort_cleanup_for_a_foreign_root_is_queued_not_inlined() {
    let (world, _heads) = World::linear_stack(2);
    let mut processor = world.processor();
    let cleanup = vec![
        Effect::Git(crate::effects::GitEffect::CleanupWorktree),
        Effect::GitHub(GitHubEffect::PostComment {
            pr: PrNumber(9),
            body: "aborted".to_owned(),
        }),
    ];

    // Its own root: inlined, and nothing is queued.
    let (mine, roots) =
        processor.cleanup_for_batch(vec![(PrNumber(9), cleanup.clone())], PrNumber(9));
    assert_eq!(mine.len(), 2, "the batch's own root runs inline");
    assert_eq!(roots, [PrNumber(9)].into_iter().collect());
    assert!(!processor.has_queued_work(), "nothing to defer");

    // A foreign root: nothing inlined, and its own cleanup is queued —
    // carrying the effects, so a queued `Start` for that root cannot
    // replace the record they belong to before they run.
    let (mine, roots) = processor.cleanup_for_batch(vec![(PrNumber(9), cleanup)], PrNumber(1));
    assert!(
        mine.is_empty(),
        "another train's root-relative cleanup must not ride this batch"
    );
    assert_eq!(roots, [PrNumber(9)].into_iter().collect());
    let queued = processor.pump().unwrap().expect("the cleanup is queued");
    assert_eq!(queued.root, PrNumber(9), "under its own root");
    assert_eq!(
        queued.best_effort.len(),
        2,
        "with the effects captured when the abort applied: {:?}",
        queued.best_effort
    );
}

/// SEVERAL retired incarnations of one root can owe their comments — a
/// train stopped, restarted and stopped again while GitHub refused the
/// updates. The successor's start waits for each of them once, not just
/// for the oldest, or the newest incarnation's comment stays saying ACTIVE
/// through the whole cascade (Codex terminal-sync review round 12, P2).
#[test]
fn every_owed_incarnation_is_probed_before_the_next_start() {
    let (mut world, heads) = World::linear_stack(2);
    world.github.lock().unwrap().blocked.insert(PrNumber(1));
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    // Two incarnations, each stopped with its final update refused.
    world.github.lock().unwrap().update_comment_broken = true;
    for (start_id, stop_id) in [(501u64, 502u64), (603, 604)] {
        let start = comment_body(
            &world.config,
            1,
            "@merge-train start",
            AUTHOR,
            "author",
            start_id,
        );
        world.enqueue(&mut processor, "issue_comment", start);
        drain(&mut processor);
        let stop = comment_body(
            &world.config,
            1,
            "@merge-train stop",
            AUTHOR,
            "author",
            stop_id,
        );
        world.enqueue(&mut processor, "issue_comment", stop);
        drain(&mut processor);
    }
    assert_eq!(
        processor.owed_status_comments().len(),
        2,
        "precondition: two incarnations owe their comments"
    );

    // A third start must probe BOTH before its preflight runs.
    let restart = comment_body(
        &world.config,
        1,
        "@merge-train start",
        AUTHOR,
        "author",
        705,
    );
    world.enqueue(&mut processor, "issue_comment", restart);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    // Drive every batch the worker hands back, counting the probes and
    // stopping at the start's preflight (the settings reads).
    let mut probes = 0;
    let mut preflight_after = None;
    let mut next = processor.pump().unwrap();
    for step in 0..12 {
        let Some(batch) = next else {
            break;
        };
        if batch.effects.iter().any(|e| {
            matches!(e, Effect::GitHub(GitHubEffect::ListComments { pr }) if *pr == PrNumber(1))
        }) {
            probes += 1;
        }
        if batch
            .effects
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::GetRepoSettings)))
        {
            preflight_after = Some(probes);
            break;
        }
        let outcomes = execute(&mut processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
        if next.is_none() {
            next = processor.pump().unwrap();
        }
        let _ = step;
    }
    assert_eq!(
        preflight_after,
        Some(2),
        "both retired incarnations are probed before the successor's preflight"
    );
}

// ─── The stack ledger: the topology's off-disk backup ───

/// Every stack-ledger comment the bot has on `pr`, newest first.
/// Escapes `s` for embedding inside a JSON string literal in a raw
/// webhook body.
fn json_escaped(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn ledgers_on(
    world: &World,
    pr: u64,
) -> Vec<(crate::types::CommentId, crate::status::StackLedger)> {
    let github = world.github.lock().unwrap();
    let mut found: Vec<(crate::types::CommentId, crate::status::StackLedger)> = github
        .comments
        .iter()
        .filter(|(_, c)| c.pr == PrNumber(pr) && c.author_id == TEST_BOT_ID)
        .filter_map(|(id, c)| crate::status::parse_stack_ledger(&c.body).map(|l| (*id, l)))
        .collect();
    found.sort_by_key(|(id, l)| (l.seq, *id));
    found.reverse();
    found
}

/// The ledger a PR's comments say it has, if exactly one is there.
fn ledger_on(world: &World, pr: u64) -> Option<crate::status::StackLedger> {
    let found = ledgers_on(world, pr);
    assert!(found.len() <= 1, "PR {pr} has {} ledgers", found.len());
    found.first().map(|(_, l)| *l)
}

/// The invariant the ledger exists for: at quiescence, every PR's ledger
/// comment says exactly what the store holds for that PR — and a PR with a
/// declaration has one. (A PR that never had a declaration has no ledger:
/// there is nothing to state, and no obligation was ever created.)
fn assert_ledgers_match_store(world: &World, processor: &Processor) {
    for (pr, cached) in &processor.state().prs {
        let found = ledgers_on(world, pr.0);
        assert!(
            found.len() <= 1,
            "PR {pr} carries {} ledgers; one is a duplicate a crawl would have \
             to arbitrate",
            found.len()
        );
        let expected = cached.predecessor.zip(cached.predecessor_comment_id);
        match found.first() {
            Some((id, ledger)) => {
                assert_eq!(
                    ledger.declared.map(|d| (d.predecessor, d.owner)),
                    expected,
                    "PR {pr}'s ledger disagrees with the store"
                );
                assert_eq!(
                    cached.ledger_comment_id,
                    Some(*id),
                    "PR {pr}'s store does not know where its ledger lives"
                );
            }
            None => assert_eq!(
                expected, None,
                "PR {pr} holds a declaration the ledger does not record"
            ),
        }
    }
}

/// A recorded declaration is written to the PR's ledger comment: the whole
/// point — a crawl reads this back instead of re-deriving the edge from the
/// user's comment.
#[test]
fn a_declaration_writes_its_stack_ledger() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    let ledger = ledger_on(&world, 2).expect("PR 2's ledger");
    assert_eq!(ledger.pr, PrNumber(2));
    assert_eq!(
        ledger.declared.map(|d| d.predecessor),
        Some(PrNumber(1)),
        "the ledger names the predecessor"
    );
    assert_eq!(
        ledger.declared.map(|d| d.owner),
        processor.state().prs[&PrNumber(2)].predecessor_comment_id,
        "and the comment that declared it"
    );
    assert_ledgers_match_store(&world, &processor);
    assert!(
        ledger_on(&world, 1).is_none(),
        "a PR that declares nothing has nothing to record"
    );
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the obligation is discharged"
    );
}

/// A retraction rewrites the SAME comment to say the PR is not stacked. The
/// ledger is state, not a log: there is one per PR, and it is the answer.
#[test]
fn a_retraction_rewrites_the_ledger_in_place() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let before = ledgers_on(&world, 2);
    assert_eq!(before.len(), 1);
    let comment_id = before[0].0;

    let deletion = comment_body_with_action(
        &world.config,
        2,
        "@merge-train predecessor #1",
        AUTHOR,
        "author",
        20,
        "deleted",
    );
    world.enqueue(&mut processor, "issue_comment", deletion);
    drain(&mut processor);

    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "still exactly one ledger");
    assert_eq!(after[0].0, comment_id, "the same comment, rewritten");
    assert_eq!(
        after[0].1.declared, None,
        "the ledger now says the PR declares no predecessor"
    );
    assert!(
        after[0].1.seq > before[0].1.seq,
        "and says so more recently"
    );
    // The watermark is what tells a crawl that the declaration comment
    // still sitting on this PR is one the bot has already settled — not a
    // change that appeared while it was away.
    assert_eq!(
        after[0].1.settled_through,
        processor.state().prs[&PrNumber(2)].declarations_settled_through,
        "the ledger carries the settled watermark"
    );
    assert!(
        after[0].1.settled_through >= before[0].1.settled_through,
        "which only ever moves forward"
    );
    assert_eq!(
        after[0].1.settled_through,
        Some(crate::types::CommentId(20)),
        "and names the declaration it settled"
    );
}

/// A declaration the live path REFUSES leaves no ledger: nothing was
/// decided, so there is nothing to record — and a crawl reading ledgers can
/// never resurrect what was refused.
#[test]
fn a_rejected_declaration_writes_no_ledger() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    // PR 1 is based on the default branch, so declaring PR 2 as its
    // predecessor is refused (the base does not match PR 2's branch).
    let body = comment_body(
        &world.config,
        1,
        "@merge-train predecessor #2",
        AUTHOR,
        "author",
        44,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);

    assert_eq!(
        processor.state().prs[&PrNumber(1)].predecessor,
        None,
        "precondition: the declaration was refused"
    );
    assert!(
        ledger_on(&world, 1).is_none(),
        "a refused declaration records nothing"
    );
}

/// The ledger write is best-effort like every other status write, so it is
/// OWED until it lands: an outage during the write leaves the obligation in
/// the store, and the retry discharges it without a restart.
#[test]
fn a_ledger_write_lost_to_an_outage_is_owed_and_lands_later() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    for i in 1..=2u64 {
        let base = if i == 1 { "main" } else { "pr-1" };
        let body = pr_opened_body(
            &world.config,
            i,
            &heads[i as usize - 1],
            &format!("pr-{i}"),
            base,
        );
        world.enqueue(&mut processor, "pull_request", body);
    }
    drain(&mut processor);

    world.github.lock().unwrap().unavailable = true;
    let body = comment_body(
        &world.config,
        2,
        "@merge-train predecessor #1",
        AUTHOR,
        "author",
        20,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1)),
        "the edge is recorded even though the ledger write failed"
    );
    assert_eq!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .iter()
            .map(|o| o.pr)
            .collect::<Vec<_>>(),
        vec![PrNumber(2)],
        "the ledger is owed"
    );

    world.github.lock().unwrap().unavailable = false;
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 21);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);

    assert_eq!(
        ledger_on(&world, 2)
            .and_then(|l| l.declared)
            .map(|d| d.predecessor),
        Some(PrNumber(1)),
        "the retry wrote the ledger"
    );
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty()
    );
}

/// A crash between posting the ledger and recording where it went leaves
/// an orphan the store has never heard of. The next write must ADOPT it,
/// not post a second: two ledgers on one PR is a duplicate a crawl would
/// then have to arbitrate.
#[test]
fn an_orphaned_ledger_comment_is_adopted_not_duplicated() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    for i in 1..=2u64 {
        let base = if i == 1 { "main" } else { "pr-1" };
        let body = pr_opened_body(
            &world.config,
            i,
            &heads[i as usize - 1],
            &format!("pr-{i}"),
            base,
        );
        world.enqueue(&mut processor, "pull_request", body);
    }
    drain(&mut processor);

    // Declare, and run as far as the batch that POSTS the ledger — then
    // die without observing its outcome: GitHub has the comment, the store
    // has no id for it.
    let body = comment_body(
        &world.config,
        2,
        "@merge-train predecessor #1",
        AUTHOR,
        "author",
        20,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = processor.pump().unwrap().expect("the ledger probe");
    let outcomes = execute(&mut processor, &probe);
    let post = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap()
        .expect("the batch that posts the ledger");
    let interpreter = WorktreeGitInterpreter::new(processor.git_config(), post.root);
    let _ = execute_batch(&interpreter, processor.github(), &post);
    drop(processor);

    let orphan = {
        let found = ledgers_on(&world, 2);
        assert_eq!(found.len(), 1, "precondition: the comment IS on GitHub");
        found[0].0
    };
    let mut processor = world.processor();
    assert_eq!(
        processor.state().prs[&PrNumber(2)].ledger_comment_id,
        None,
        "precondition: the store never learned where it went"
    );
    assert_eq!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .iter()
            .map(|o| o.pr)
            .collect::<Vec<_>>(),
        vec![PrNumber(2)],
        "precondition: still owed"
    );

    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 21);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);

    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "the orphan was adopted, not duplicated");
    assert_eq!(after[0].0, orphan);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].ledger_comment_id,
        Some(orphan),
        "and the store now knows where its ledger lives"
    );
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty()
    );
}

/// A maintainer deletes the bot's ledger comment. No topology event says
/// so, and the ledger is the topology's only off-disk backup — so the
/// deletion itself dirties it, and the bot posts a fresh one.
#[test]
fn a_deleted_ledger_comment_is_written_again() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let original = ledgers_on(&world, 2)[0].0;

    // The comment is gone from GitHub, and the webhook says so: authored
    // by the bot, deleted by a person.
    world.github.lock().unwrap().comments.remove(&original);
    let body = format!(
        r#"{{
            "action": "deleted",
            "comment": {{
                "id": {original},
                "body": "",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", body.into_bytes());
    drain(&mut processor);

    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "a fresh ledger was posted");
    assert_ne!(after[0].0, original, "at a new comment");
    assert_eq!(
        after[0].1.declared.map(|d| d.predecessor),
        Some(PrNumber(1)),
        "saying what the store still holds"
    );
    assert_ledgers_match_store(&world, &processor);
}

/// An obligation that outlived the process is picked up at construction.
/// On a quiet repository nothing else would ever ask — no delivery, no
/// saga, no train — and the topology's backup would stay missing
/// indefinitely (Codex ledger review round 1, P1).
#[test]
fn owed_ledgers_are_seeded_when_the_processor_starts() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    for i in 1..=2u64 {
        let base = if i == 1 { "main" } else { "pr-1" };
        let body = pr_opened_body(
            &world.config,
            i,
            &heads[i as usize - 1],
            &format!("pr-{i}"),
            base,
        );
        world.enqueue(&mut processor, "pull_request", body);
    }
    drain(&mut processor);

    // The declaration lands; its ledger write does not.
    world.github.lock().unwrap().unavailable = true;
    let body = comment_body(
        &world.config,
        2,
        "@merge-train predecessor #1",
        AUTHOR,
        "author",
        20,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);
    assert_eq!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .iter()
            .map(|o| o.pr)
            .collect::<Vec<_>>(),
        vec![PrNumber(2)],
        "precondition: the ledger is owed"
    );
    drop(processor);

    // A fresh process, a healthy GitHub, and NOTHING else happening: the
    // obligation is picked up at construction and probed straight away.
    world.github.lock().unwrap().unavailable = false;
    let mut processor = world.processor();
    drain(&mut processor);
    assert!(
        processor.take_retry_request(),
        "the restart probed for the ledger with no traffic to prompt it, and asked \
         to be retried"
    );
    // The obligation outlived a process, so a post may already have landed
    // unobserved: absence is believed only on the second look, a stall
    // cycle later.
    std::thread::sleep(std::time::Duration::from_millis(30));
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    assert_eq!(
        ledger_on(&world, 2)
            .and_then(|l| l.declared)
            .map(|d| d.predecessor),
        Some(PrNumber(1)),
        "and then wrote it"
    );
    // The obligation outlived a dead process whose own post may still be
    // out there unobserved, so the replacement went out unbound: a fresh
    // listing must account for any orphan before discharge (Codex ledger
    // review round 23, P2).
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty()
    );
}

/// A maintainer can EDIT the bot's own comment, and GitHub reports the bot
/// as its author — so the handler's self-guard ignores the event and a
/// doctored machine block would stay trusted until a crawl read it. The
/// edit re-owes the ledger, exactly as a deletion does (Codex ledger
/// review round 1, P1).
#[test]
fn an_edited_ledger_comment_is_rewritten() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let ledger_id = ledgers_on(&world, 2)[0].0;

    // Someone rewrites the machine block to claim a different edge.
    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: Some(crate::status::Declaration {
            predecessor: PrNumber(9),
            owner: crate::types::CommentId(1),
        }),
        seq: 9999,
        settled_through: Some(crate::types::CommentId(1)),
    });
    world
        .github
        .lock()
        .unwrap()
        .comments
        .get_mut(&ledger_id)
        .unwrap()
        .body = forged;
    let body = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": {ledger_id},
                "body": "doctored",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", body.into_bytes());
    drain(&mut processor);

    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "still one ledger, rewritten in place");
    assert_eq!(after[0].0, ledger_id);
    assert_eq!(
        after[0].1.declared.map(|d| d.predecessor),
        Some(PrNumber(1)),
        "the forged edge is overwritten with what the store holds"
    );
    assert_ledgers_match_store(&world, &processor);
}

/// A ledger POST that landed while its `StackLedgerPosted` did not leaves
/// the store with no id — and GitHub's listings are eventually consistent,
/// so the comment can be missing from the next one. Posting a replacement
/// then leaves a permanent duplicate, so absence must be STABLE first
/// (Codex ledger review round 1, P2).
#[test]
fn a_ledger_absent_from_one_listing_is_not_duplicated() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let orphan = ledgers_on(&world, 2)[0].0;

    // Forget where it went (the crash), and hide it from listings.
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(orphan);
    drop(processor);
    let mut processor = world.processor();

    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "one absent listing must not produce a second ledger"
    );
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the obligation is kept for another look"
    );

    // Once the listing catches up, the existing ledger is adopted.
    world.github.lock().unwrap().hidden_from_listings.clear();
    std::thread::sleep(std::time::Duration::from_millis(30));
    let remark = comment_body(&world.config, 2, "another remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "still exactly one");
    assert_eq!(after[0].0, orphan, "the original, adopted");
}

/// A ledger dirtied WHILE a write is in flight is not discharged by that
/// write: the write clears the obligation it was made for, and anything
/// later holds a newer generation (Codex ledger review round 13, P1).
#[test]
fn a_ledger_dirtied_mid_write_is_not_cleared_by_it() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    // Doctor the comment so a write is actually needed, then dirty it and
    // run as far as the batch that writes it.
    let ledger_id = ledgers_on(&world, 2)[0].0;
    world
        .github
        .lock()
        .unwrap()
        .comments
        .get_mut(&ledger_id)
        .unwrap()
        .body = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: None,
        seq: 1,
        settled_through: None,
    });
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = processor.pump().unwrap().expect("the ledger probe");
    let outcomes = execute(&mut processor, &probe);
    let write = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap()
        .expect("the ledger write");

    // A maintainer edits the comment while that write is out.
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    let outcomes = execute(&mut processor, &write);
    processor
        .on_outcomes(write.root, outcomes, write.feedback)
        .unwrap();
    assert_eq!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .iter()
            .map(|o| o.pr)
            .collect::<Vec<_>>(),
        vec![PrNumber(2)],
        "the newer invalidation outlives the write that was already in flight"
    );
}

/// A probe's listing is evidence about the moment it was TAKEN, not the
/// moment it is processed. If a maintainer doctors the ledger while the
/// listing is in flight — the "changed under us" webhook bumps the
/// obligation's generation — the pre-edit body that listing shows must
/// not discharge the newer obligation, or the corruption is never
/// repaired (Codex ledger review round 14, P1).
#[test]
fn a_stale_probe_listing_does_not_satisfy_a_newer_invalidation() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let ledger_id = ledgers_on(&world, 2)[0].0;

    // Something dirties the obligation while the comment still matches,
    // and the probe's listing is taken in that state.
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = processor.pump().unwrap().expect("the ledger probe");
    let outcomes = execute(&mut processor, &probe);

    // The maintainer doctors the comment while that listing is in
    // flight, and the webhook's `mark_ledger_owed` lands first.
    world
        .github
        .lock()
        .unwrap()
        .comments
        .get_mut(&ledger_id)
        .unwrap()
        .body = "doctored".to_string();
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();

    let next = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap();
    assert_eq!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .iter()
            .map(|o| o.pr)
            .collect::<Vec<_>>(),
        vec![PrNumber(2)],
        "a listing taken before the invalidation must not discharge it"
    );

    // And the repair itself lands: the stale listing still names the
    // right comment, so the fall-through write fixes it in place — but
    // only a FRESH listing may discharge the newer obligation (Codex
    // ledger review round 18, P2).
    finish_batches(&mut world, &mut processor, next);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "repaired in place, not duplicated");
    assert_eq!(after[0].0, ledger_id);
    assert_ledgers_match_store(&world, &processor);
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the newer invalidation still wants a fresh look"
    );
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the fresh listing discharges it"
    );
}

/// `ledger_posts_attempted` guards the absence heuristic: a PR a post may
/// already have happened for needs STABLE absence before another. An id
/// the store RECORDED is the strongest such evidence there is — but a
/// clean restart used to forget it, because only open obligations seeded
/// the set. One transiently-short listing after the next topology change
/// then posted a second ledger (Codex ledger review round 14, P2).
#[test]
fn a_recorded_ledger_guards_reposting_across_a_restart() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let recorded = ledgers_on(&world, 2)[0].0;

    // A clean restart: every obligation discharged, the id recorded.
    drop(processor);
    let mut processor = world.processor();

    // The ledger is dirtied again, and the next listing happens not to
    // show the recorded comment yet.
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(recorded);
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "one absent listing after a clean restart must not produce a second ledger"
    );
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the obligation is kept for another look"
    );

    // Once the listing shows it again, the recorded ledger satisfies the
    // obligation — still exactly one comment.
    world.github.lock().unwrap().hidden_from_listings.clear();
    std::thread::sleep(std::time::Duration::from_millis(30));
    let remark = comment_body(&world.config, 2, "another remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "still exactly one");
    assert_eq!(after[0].0, recorded, "the original, not a replacement");
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the recorded ledger discharged the obligation"
    );
}

/// One missed listing, then a probe that FINDS the ledger (whose repair
/// write fails, keeping the obligation open), then another miss: two
/// misses in total, but the find in between proved the comment exists.
/// The absence streak must restart at every find, or the second miss
/// counts as "stable absence" and posts a duplicate over a comment that
/// was seen alive in between (Codex ledger review round 14, P2).
#[test]
fn a_found_ledger_resets_the_absence_streak() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let ledger_id = ledgers_on(&world, 2)[0].0;

    // Owed again, and the first listing misses the comment.
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(ledger_id);
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "one miss: waiting, not posting"
    );

    // The second listing shows it — forged, so a repair is needed — but
    // the repair write fails, leaving the obligation open.
    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: Some(crate::status::Declaration {
            predecessor: PrNumber(9),
            owner: crate::types::CommentId(1),
        }),
        seq: 9999,
        settled_through: None,
    });
    {
        let mut github = world.github.lock().unwrap();
        github.hidden_from_listings.clear();
        github.comments.get_mut(&ledger_id).unwrap().body = forged;
        github.update_comment_broken = true;
    }
    let remark = comment_body(&world.config, 2, "another remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the failed repair keeps the obligation open"
    );

    // A third listing misses it again, past the absence cooldown.
    {
        let mut github = world.github.lock().unwrap();
        github.hidden_from_listings.insert(ledger_id);
        github.update_comment_broken = false;
    }
    std::thread::sleep(std::time::Duration::from_millis(30));
    let remark = comment_body(&world.config, 2, "a third remark", AUTHOR, "author", 9003);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(
        after.len(),
        1,
        "an intervening find restarts the absence count; no replacement is posted"
    );
    assert_eq!(after[0].0, ledger_id);
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "still owed: the repair has yet to land"
    );
}

/// Deleting an UNRELATED bot reply on a PR that has a ledger must not
/// clear the duplicate-post guard: the `ledger_posts_attempted`
/// catch-all classifies any deleted bot comment on the PR as
/// possibly-the-ledger, and dropping the guard on that guess let one
/// transiently-short listing post a second ledger (Codex ledger review
/// round 15, P2).
#[test]
fn deleting_an_unrelated_bot_reply_does_not_license_an_immediate_repost() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let ledger_id = ledgers_on(&world, 2)[0].0;

    // The next listing happens not to show the ledger...
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(ledger_id);
    // ...while a maintainer deletes some OTHER bot comment on the PR.
    let deletion = format!(
        r#"{{
            "action": "deleted",
            "comment": {{
                "id": 4242,
                "body": "An old train notice.",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", deletion.into_bytes());
    drain(&mut processor);

    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "an unidentifiable deletion plus one short listing must not post a second ledger"
    );
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the conservative re-probe stays owed until the ledger is seen"
    );

    // The listing catches up: the untouched ledger discharges the probe.
    world.github.lock().unwrap().hidden_from_listings.clear();
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "still exactly one");
    assert_eq!(after[0].0, ledger_id);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the real ledger, once seen, discharges the obligation"
    );
}

/// After a database loss, the probe ADOPTS the ledger it finds on
/// GitHub — but nothing put the PR in `ledger_posts_attempted`, so the
/// next obligation treated absence as "simply new" and posted on the
/// first short listing (Codex ledger review round 15, P2). An adoption
/// is proof a post landed; it must guard like one.
#[test]
fn a_ledger_adopted_after_a_db_loss_still_guards_against_reposting() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let ledger_id = ledgers_on(&world, 2)[0].0;

    // The database is lost; the declarations are redelivered, and the
    // probe adopts the ledger the previous life posted.
    let db = world.db_path();
    drop(processor);
    std::fs::remove_file(&db).unwrap();
    for sidecar in ["state.db-wal", "state.db-shm"] {
        let _ = std::fs::remove_file(db.with_file_name(sidecar));
    }
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let adopted = ledgers_on(&world, 2);
    assert_eq!(adopted.len(), 1, "adopted, not duplicated");
    assert_eq!(adopted[0].0, ledger_id);

    // The ledger is dirtied again, and the next listing happens not to
    // show the adopted comment yet.
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(ledger_id);
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "one short listing after an adoption must not produce a second ledger"
    );
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the obligation is kept for another look"
    );

    // Once the listing shows it again, the adopted ledger discharges it.
    world.github.lock().unwrap().hidden_from_listings.clear();
    let remark = comment_body(&world.config, 2, "another remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "still exactly one");
    assert_eq!(after[0].0, ledger_id);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the adopted ledger discharged the obligation"
    );
}

/// A maintainer edits ANOTHER bot reply into a "ledger" whose sequence
/// number outranks everything for ever. The recorded-id-first selection
/// finds the real ledger satisfied and used to clear the obligation with
/// the forgery still standing — and the forgery, having the highest
/// sequence number, wins a crawl's duplicate arbitration over the truth
/// (Codex ledger review round 16, P2). Forged siblings are neutralized,
/// and the obligation stays open until a listing confirms the set.
#[test]
fn a_forged_sibling_ledger_is_neutralized_not_ignored() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let real = ledgers_on(&world, 2)[0].0;

    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: None,
        seq: u64::MAX,
        settled_through: None,
    });
    world.github.lock().unwrap().comments.insert(
        crate::types::CommentId(4242),
        FakeComment {
            pr: PrNumber(2),
            author_id: TEST_BOT_ID,
            body: forged.clone(),
            edited: true,
        },
    );
    let forged_json = json_escaped(&forged);
    let edit = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": 4242,
                "body": "{forged_json}",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", edit.into_bytes());
    drain(&mut processor);

    let after = ledgers_on(&world, 2);
    assert_eq!(
        after.len(),
        1,
        "the forged sibling must be neutralized, not left to win arbitration"
    );
    assert_eq!(after[0].0, real, "the real ledger is the one that stands");
    let neutralized = world.github.lock().unwrap().comments[&crate::types::CommentId(4242)]
        .body
        .clone();
    assert!(
        crate::status::parse_stack_ledger(&neutralized).is_none(),
        "the forgery no longer parses as a ledger"
    );

    // The obligation is discharged only once a listing confirms the
    // whole set is clean, at the stall cadence.
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the verifying probe discharges the obligation"
    );
    assert_ledgers_match_store(&world, &processor);
}

/// Ledger A is deleted; stable absence posts replacement B — and A's
/// deletion webhook, delayed, lands while B's POST is still in flight.
/// The recorded id is still A, so the deletion looked positively
/// identified and cleared the repost guard even though B exists; one
/// listing omitting B then posted a third ledger (Codex ledger review
/// round 16, P2).
#[test]
fn a_late_deletion_webhook_does_not_unguard_an_in_flight_replacement() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let old = ledgers_on(&world, 2)[0].0;

    // A maintainer deletes the ledger, but the webhook is delayed: only
    // the listings can notice. Something else dirties the obligation.
    world.github.lock().unwrap().comments.remove(&old);
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();

    // Two stable absences later, the replacement POST is dispatched.
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let remark = comment_body(&world.config, 2, "another remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = processor.pump().unwrap().expect("the second absence probe");
    let outcomes = execute(&mut processor, &probe);
    let post = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap()
        .expect("the replacement post");
    assert!(
        post.best_effort
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::PostComment { .. }))),
        "stable absence dispatches the replacement"
    );

    // The delayed deletion webhook lands while that POST is in flight.
    let deletion = format!(
        r#"{{
            "action": "deleted",
            "comment": {{
                "id": {old},
                "body": "the old ledger",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", deletion.into_bytes());
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // The POST lands and is recorded — and one listing transiently
    // omits the fresh replacement.
    let outcomes = execute(&mut processor, &post);
    let next = processor
        .on_outcomes(post.root, outcomes, post.feedback)
        .unwrap();
    let replacement = ledgers_on(&world, 2)[0].0;
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(replacement);
    finish_batches(&mut world, &mut processor, next);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "one short listing while the deletion webhook is late must not post a third ledger"
    );
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the obligation is kept for another look"
    );

    // The listing catches up; the replacement discharges the obligation.
    world.github.lock().unwrap().hidden_from_listings.clear();
    // 9005, not 9003: the replacement POST took the fake's next
    // monotonic id (one past the seeded remarks), and a remark reusing
    // it would overwrite the replacement in harnesses that seed webhook
    // comments into the fake.
    let remark = comment_body(&world.config, 2, "a third remark", AUTHOR, "author", 9005);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "still exactly one");
    assert_eq!(after[0].0, replacement);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the replacement, once seen, discharges the obligation"
    );
}

/// A rewrite fails; the retry discovers a previously hidden duplicate
/// and enters duplicate cleanup, which deliberately binds no generation
/// — but the FAILED attempt's `ledger_write_gen` entry was still there,
/// so the successful rewrite inside the cleanup batch cleared the
/// obligation through it, abandoning any duplicate the cleanup had not
/// yet seen (Codex ledger review round 17, P2).
#[test]
fn a_stale_write_binding_does_not_discharge_duplicate_cleanup() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let real = ledgers_on(&world, 2)[0].0;

    // Two duplicates exist but are hidden from listings; the real
    // ledger is doctored, so a rewrite is owed — and the first attempt
    // fails.
    {
        let mut github = world.github.lock().unwrap();
        for (id, seq) in [(4242u64, 5u64), (4243, 6)] {
            github.comments.insert(
                crate::types::CommentId(id),
                FakeComment {
                    pr: PrNumber(2),
                    author_id: TEST_BOT_ID,
                    body: crate::status::format_stack_ledger(&crate::status::StackLedger {
                        pr: PrNumber(2),
                        declared: None,
                        seq,
                        settled_through: None,
                    }),
                    edited: true,
                },
            );
            github
                .hidden_from_listings
                .insert(crate::types::CommentId(id));
        }
        github.comments.get_mut(&real).unwrap().body = "doctored".to_string();
        github.update_comment_broken = true;
    }
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the failed rewrite keeps the obligation open"
    );

    // The retry finds ONE of the duplicates; its cleanup batch lands —
    // the rewrite and the neutralization both succeed.
    {
        let mut github = world.github.lock().unwrap();
        github.update_comment_broken = false;
        github
            .hidden_from_listings
            .remove(&crate::types::CommentId(4242));
    }
    let remark = comment_body(&world.config, 2, "another remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "a rewrite landing amid duplicate cleanup must not clear the obligation \
         through the failed attempt's stale binding"
    );

    // The second duplicate surfaces; the still-open obligation cleans
    // it up too, and only a clean listing discharges it.
    world.github.lock().unwrap().hidden_from_listings.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "both duplicates neutralized");
    assert_eq!(after[0].0, real);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "a clean listing discharges the obligation"
    );
    assert_ledgers_match_store(&world, &processor);
}

/// The bot can have replies on a PR it never cached (authorization
/// rejections land before precaching). A maintainer edits such a reply
/// into a forged ledger; the tampering webhook schedules repair — and
/// the probe used to drop the obligation because the PR was not in the
/// cache, leaving a bot-authored forgery standing for ever (Codex
/// ledger review round 17, P2). No cache entry means nothing to record,
/// not nothing to repair.
#[test]
fn a_forged_ledger_on_an_uncached_pr_is_neutralized_not_abandoned() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    // PR 3 was never opened here: the bot's reply on it predates this
    // process. A maintainer edits that reply into a "ledger".
    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(3),
        declared: Some(crate::status::Declaration {
            predecessor: PrNumber(1),
            owner: crate::types::CommentId(1),
        }),
        seq: 9999,
        settled_through: None,
    });
    world.github.lock().unwrap().comments.insert(
        crate::types::CommentId(4242),
        FakeComment {
            pr: PrNumber(3),
            author_id: TEST_BOT_ID,
            body: forged.clone(),
            edited: true,
        },
    );
    let forged_json = json_escaped(&forged);
    let edit = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": 4242,
                "body": "{forged_json}",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 3,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", edit.into_bytes());
    drain(&mut processor);

    assert_eq!(
        ledgers_on(&world, 3).len(),
        0,
        "a forged ledger on an uncached PR is neutralized, not abandoned"
    );
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the obligation waits for a listing that confirms nothing is left"
    );

    // The verifying probe finds nothing ledger-shaped and lets go.
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "a clean listing discharges the obligation"
    );
}

/// A rewrite is owed and its probe's listing shows no siblings — and
/// while that listing is in flight, another bot reply is edited into a
/// forged ledger, advancing the obligation. Binding the rewrite to the
/// ADVANCED generation let its success discharge the obligation without
/// the forgery ever being seen: queued retries found nothing owed, and
/// the forgery stood for ever (Codex ledger review round 18, P2).
#[test]
fn a_write_from_a_stale_listing_does_not_discharge_the_newer_obligation() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let real = ledgers_on(&world, 2)[0].0;

    // The real ledger is doctored, so a rewrite is owed; the probe's
    // listing is taken while no sibling exists yet.
    world
        .github
        .lock()
        .unwrap()
        .comments
        .get_mut(&real)
        .unwrap()
        .body = "doctored".to_string();
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = processor.pump().unwrap().expect("the ledger probe");
    let outcomes = execute(&mut processor, &probe);

    // While that listing is in flight, another bot reply becomes a
    // forged ledger; its webhook advances the obligation.
    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: None,
        seq: u64::MAX,
        settled_through: None,
    });
    world.github.lock().unwrap().comments.insert(
        crate::types::CommentId(4242),
        FakeComment {
            pr: PrNumber(2),
            author_id: TEST_BOT_ID,
            body: forged.clone(),
            edited: true,
        },
    );
    let edit = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": 4242,
                "body": "{body}",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        body = json_escaped(&forged),
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", edit.into_bytes());
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // The stale-listing callback dispatches the rewrite; it and
    // everything it queues run to quiescence.
    let next = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "the forgery created mid-probe must be neutralized, not stranded by \
         the stale write's discharge"
    );
    assert_eq!(ledgers_on(&world, 2)[0].0, real);
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "only a fresh listing discharges the newer obligation"
    );

    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the fresh listing discharges it"
    );
    assert_ledgers_match_store(&world, &processor);
}

/// The uncached-PR face of the same race: the obligation's probe sees an
/// EMPTY listing, and while it is in flight a bot reply is edited into a
/// forged ledger, advancing the obligation. Clearing the advanced
/// generation on the stale empty listing stranded the forgery for ever
/// (Codex ledger review round 18, P2).
#[test]
fn a_stale_empty_listing_on_an_uncached_pr_keeps_the_newer_obligation() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(3),
        declared: Some(crate::status::Declaration {
            predecessor: PrNumber(1),
            owner: crate::types::CommentId(1),
        }),
        seq: 9999,
        settled_through: None,
    });
    // PR 3 owes a ledger look from an earlier life; the probe's listing
    // will show nothing at all. (A deletion webhook cannot carry the
    // dirtying here: the parser blanks a deleted comment's body.)
    processor.store_mut().mark_ledger_owed(PrNumber(3)).unwrap();
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = processor.pump().unwrap().expect("the ledger probe");
    let outcomes = execute(&mut processor, &probe);

    // While it is in flight, ANOTHER bot reply on PR 3 is edited into a
    // forged ledger; its webhook advances the obligation.
    world.github.lock().unwrap().comments.insert(
        crate::types::CommentId(4242),
        FakeComment {
            pr: PrNumber(3),
            author_id: TEST_BOT_ID,
            body: forged.clone(),
            edited: true,
        },
    );
    let edit = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": 4242,
                "body": "{body}",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 3,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        body = json_escaped(&forged),
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", edit.into_bytes());
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    let next = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    assert_eq!(
        ledgers_on(&world, 3).len(),
        0,
        "the forgery created mid-probe must be neutralized, not stranded by \
         the stale empty listing's discharge"
    );
    // The repair lands directly by id, and the probe the advancing
    // webhook queued discharges the obligation — one quiescence run.
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "repair and discharge both land once the tampering is named"
    );
}

/// The webhook NAMES the comment that was edited into a forged ledger —
/// and an eventually-consistent listing can still omit that comment
/// after the webhook is processed. Generations match, the real ledger is
/// satisfied, no extras are visible, and the obligation used to be
/// discharged with the known forgery never seen; once listings caught
/// up, it stood with no retry owed (Codex ledger review round 19, P2).
#[test]
fn a_known_ledger_edit_missing_from_the_listing_blocks_discharge() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let real = ledgers_on(&world, 2)[0].0;

    // A bot reply becomes a forged ledger; the webhook lands, but the
    // next listing has not caught up and omits the comment.
    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: None,
        seq: u64::MAX,
        settled_through: None,
    });
    {
        let mut github = world.github.lock().unwrap();
        github.comments.insert(
            crate::types::CommentId(4242),
            FakeComment {
                pr: PrNumber(2),
                author_id: TEST_BOT_ID,
                body: forged.clone(),
                edited: true,
            },
        );
        github
            .hidden_from_listings
            .insert(crate::types::CommentId(4242));
    }
    let edit = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": 4242,
                "body": "{body}",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        body = json_escaped(&forged),
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", edit.into_bytes());
    drain(&mut processor);

    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "a comment the webhook NAMED is still unseen; the obligation must wait \
         for it or its confirmed absence"
    );

    // The listing catches up: the forgery is neutralized, then a clean
    // listing discharges the obligation.
    world.github.lock().unwrap().hidden_from_listings.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "the forgery is neutralized once listed");
    assert_eq!(after[0].0, real);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "a listing that accounts for everything discharges the obligation"
    );
    assert_ledgers_match_store(&world, &processor);
}

/// Replacement B is posted for deleted ledger A, and B's POST response
/// is lost: the comment exists, nothing acknowledged it, and the failed
/// attempt's generation binding is gone. A's DELAYED deletion webhook
/// then found the recorded id still naming A with no write in flight,
/// cleared the repost guard, and one listing omitting B posted a third
/// ledger and discharged the obligation — duplicates, no retry owed
/// (Codex ledger review round 19, P2).
#[test]
fn an_unacknowledged_replacement_still_guards_after_a_late_deletion() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let old = ledgers_on(&world, 2)[0].0;

    // A is deleted, webhook delayed: only listings can notice.
    world.github.lock().unwrap().comments.remove(&old);
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let remark = comment_body(&world.config, 2, "another remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = processor.pump().unwrap().expect("the second absence probe");
    let outcomes = execute(&mut processor, &probe);

    // Stable absence dispatches replacement B — whose response is lost.
    world.github.lock().unwrap().post_comment_response_lost = true;
    let post = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap()
        .expect("the replacement post");
    assert!(
        post.best_effort
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::PostComment { .. }))),
        "stable absence dispatches the replacement"
    );
    let outcomes = execute(&mut processor, &post);
    world.github.lock().unwrap().post_comment_response_lost = false;
    let next = processor
        .on_outcomes(post.root, outcomes, post.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    let replacement = ledgers_on(&world, 2)[0].0;
    assert_ne!(replacement, old, "B exists; nothing acknowledged it");

    // A's DELAYED deletion webhook lands only now...
    let deletion = format!(
        r#"{{
            "action": "deleted",
            "comment": {{
                "id": {old},
                "body": "the old ledger",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", deletion.into_bytes());
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    // ...and one listing transiently omits the unacknowledged B.
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(replacement);
    drain(&mut processor);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "an unacknowledged replacement keeps the repost guard: one short \
         listing must not post another ledger"
    );
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the obligation is kept for another look"
    );

    // The listing catches up; B is adopted and discharges the obligation.
    world.github.lock().unwrap().hidden_from_listings.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "still exactly one");
    assert_eq!(after[0].0, replacement);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the adopted replacement discharges the obligation"
    );
}

/// Seeing a suspect is not the same as repairing it: the reconciliation
/// used to forget a suspect the moment a listing showed it, so a FAILED
/// neutralization followed by one short listing let the satisfied
/// primary discharge the obligation with the known forgery standing
/// (Codex ledger review round 20, P2). A suspect leaves the books only
/// seen in a good state — inert, or as the satisfied ledger itself — or
/// stably absent.
#[test]
fn a_suspect_outlives_a_failed_neutralization() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let real = ledgers_on(&world, 2)[0].0;

    // A bot reply becomes a forged ledger; the webhook names it.
    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: None,
        seq: u64::MAX,
        settled_through: None,
    });
    {
        let mut github = world.github.lock().unwrap();
        github.comments.insert(
            crate::types::CommentId(4242),
            FakeComment {
                pr: PrNumber(2),
                author_id: TEST_BOT_ID,
                body: forged.clone(),
                edited: true,
            },
        );
        // The first repair attempt will fail.
        github.update_comment_broken = true;
    }
    let edit = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": 4242,
                "body": "{body}",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        body = json_escaped(&forged),
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", edit.into_bytes());
    drain(&mut processor);

    // The neutralization failed; the next listing transiently omits the
    // forgery, and the (valid) primary looks satisfied.
    {
        let mut github = world.github.lock().unwrap();
        github.update_comment_broken = false;
        github
            .hidden_from_listings
            .insert(crate::types::CommentId(4242));
    }
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "a suspect whose repair has not landed still blocks discharge"
    );

    // The listing shows the forgery again: neutralized, verified, done.
    world.github.lock().unwrap().hidden_from_listings.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "the forgery is neutralized once repairable");
    assert_eq!(after[0].0, real);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "a listing that accounts for everything discharges the obligation"
    );
    assert_ledgers_match_store(&world, &processor);
}

/// The ambiguous-post caution must survive a restart: replacement B
/// lands unacknowledged, the worker restarts, and only THEN does A's
/// delayed deletion webhook arrive. The in-memory set was empty, the
/// fast path dropped the conservatively seeded repost guard, and one
/// listing omitting B posted a third ledger and discharged the
/// obligation (Codex ledger review round 20, P2). An obligation owed at
/// startup now seeds the same caution the dead process held.
#[test]
fn a_restart_does_not_forget_an_unacknowledged_replacement() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let old = ledgers_on(&world, 2)[0].0;

    // A is deleted, webhook delayed; stable absence posts replacement B,
    // whose response is lost.
    world.github.lock().unwrap().comments.remove(&old);
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let remark = comment_body(&world.config, 2, "another remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = processor.pump().unwrap().expect("the second absence probe");
    let outcomes = execute(&mut processor, &probe);
    world.github.lock().unwrap().post_comment_response_lost = true;
    let post = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap()
        .expect("the replacement post");
    let outcomes = execute(&mut processor, &post);
    world.github.lock().unwrap().post_comment_response_lost = false;
    let next = processor
        .on_outcomes(post.root, outcomes, post.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    let replacement = ledgers_on(&world, 2)[0].0;
    assert_ne!(replacement, old, "B exists; nothing acknowledged it");

    // The worker restarts, forgetting everything in memory.
    drop(processor);
    let mut processor = world.processor();

    // A's delayed deletion webhook arrives, and one listing transiently
    // omits the unacknowledged B.
    let deletion = format!(
        r#"{{
            "action": "deleted",
            "comment": {{
                "id": {old},
                "body": "the old ledger",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", deletion.into_bytes());
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(replacement);
    drain(&mut processor);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "the restart must not forget that a replacement may already stand"
    );
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the obligation is kept for another look"
    );

    // The listing catches up; B is adopted and discharges the obligation.
    world.github.lock().unwrap().hidden_from_listings.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "still exactly one");
    assert_eq!(after[0].0, replacement);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the adopted replacement discharges the obligation"
    );
}

/// GitHub's listing cache can serve a PRE-EDIT body for a comment the
/// webhook already reported edited: the listing looks satisfied, and the
/// obligation used to be discharged with the tampered comment untouched
/// and no retry owed (Codex ledger review round 21, P2). The webhook
/// names the comment and its content — the repair targets it directly
/// and owes its own acknowledgement, so listing staleness is irrelevant.
#[test]
fn a_tampered_ledger_is_repaired_even_when_the_listing_lags_the_edit() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let real = ledgers_on(&world, 2)[0].0;
    let pre_edit = world.github.lock().unwrap().comments[&real].body.clone();

    // The maintainer forges the recorded ledger; the webhook reports it,
    // but listings still serve the pre-edit body.
    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: None,
        seq: u64::MAX,
        settled_through: None,
    });
    {
        let mut github = world.github.lock().unwrap();
        github.comments.get_mut(&real).unwrap().body = forged.clone();
        github.stale_listing_bodies.insert(real, pre_edit);
    }
    let edit = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": {real},
                "body": "{body}",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        body = json_escaped(&forged),
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", edit.into_bytes());
    drain(&mut processor);

    // The listing cache catches up; whatever the stale listing said, the
    // tampered comment itself must have been repaired (or still be owed).
    world.github.lock().unwrap().stale_listing_bodies.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "exactly one ledger");
    assert_eq!(after[0].0, real);
    assert_ledgers_match_store(&world, &processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "everything repaired, nothing owed"
    );
}

/// A duplicate the PROBE discovers (an orphan from a crash, never named
/// by any webhook) got its neutralization dispatched and forgotten: a
/// failed write plus one short listing abandoned it permanently (Codex
/// ledger review round 21, P2). Discovery persists the repair before
/// dispatching it, so it is owed until acknowledged.
#[test]
fn a_probe_discovered_duplicate_is_owed_until_neutralized() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let real = ledgers_on(&world, 2)[0].0;

    // An orphan duplicate from a previous life; nothing ever named it.
    world.github.lock().unwrap().comments.insert(
        crate::types::CommentId(4242),
        FakeComment {
            pr: PrNumber(2),
            author_id: TEST_BOT_ID,
            body: crate::status::format_stack_ledger(&crate::status::StackLedger {
                pr: PrNumber(2),
                declared: None,
                seq: 7,
                settled_through: None,
            }),
            edited: false,
        },
    );
    // The probe discovers it, and the neutralization FAILS.
    world.github.lock().unwrap().update_comment_broken = true;
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);

    // Updates heal, but the next listing happens to omit the duplicate.
    {
        let mut github = world.github.lock().unwrap();
        github.update_comment_broken = false;
        github
            .hidden_from_listings
            .insert(crate::types::CommentId(4242));
    }
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);

    // Whatever the listings said, the discovered duplicate must have
    // been neutralized (its id was known; no listing was needed).
    world.github.lock().unwrap().hidden_from_listings.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(
        after.len(),
        1,
        "a discovered duplicate is owed until neutralized, not forgotten \
         with its failed write"
    );
    assert_eq!(after[0].0, real);
    assert_ledgers_match_store(&world, &processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "everything repaired, nothing owed"
    );
}

/// A replacement POST can be in flight UNBOUND — its probe declined the
/// discharge binding because repairs were open — and not yet failed, so
/// neither the write binding nor the failure-driven tracking knows about
/// it. A's delayed deletion webhook then dropped the repost guard
/// mid-flight, and after B landed, one short listing posted a second
/// replacement (Codex ledger review round 21, P2). Every dispatched POST
/// is tracked from the moment it exists.
#[test]
fn a_post_in_flight_unbound_still_blocks_the_deletion_fast_path() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let old = ledgers_on(&world, 2)[0].0;

    // A forged sibling is named while updates are broken: its repair
    // stays owed, so later probe writes go out unbound.
    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: None,
        seq: u64::MAX,
        settled_through: None,
    });
    {
        let mut github = world.github.lock().unwrap();
        github.comments.insert(
            crate::types::CommentId(4242),
            FakeComment {
                pr: PrNumber(2),
                author_id: TEST_BOT_ID,
                body: forged.clone(),
                edited: true,
            },
        );
        github
            .hidden_from_listings
            .insert(crate::types::CommentId(4242));
        github.update_comment_broken = true;
    }
    let edit = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": 4242,
                "body": "{body}",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        body = json_escaped(&forged),
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", edit.into_bytes());
    drain(&mut processor);

    // A is deleted, webhook delayed; two spaced misses reach stable
    // absence, and the replacement POST goes out — UNBOUND, because the
    // sibling's repair is still owed.
    world.github.lock().unwrap().comments.remove(&old);
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let remark = comment_body(&world.config, 2, "another remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = processor.pump().unwrap().expect("the second absence probe");
    let outcomes = execute(&mut processor, &probe);
    let post = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap()
        .expect("the replacement post");
    assert!(
        post.best_effort
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::PostComment { .. }))),
        "stable absence dispatches the replacement"
    );

    // A's delayed deletion webhook lands while that POST is in flight.
    let deletion = format!(
        r#"{{
            "action": "deleted",
            "comment": {{
                "id": {old},
                "body": "the old ledger",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", deletion.into_bytes());
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // B lands fine — and one listing transiently omits it while the
    // repairs and probes queued behind it run to quiescence.
    world.github.lock().unwrap().update_comment_broken = false;
    let outcomes = execute(&mut processor, &post);
    // The replacement is the fresh post, not the (still-forged,
    // max-sequence) sibling the repair has yet to reach.
    let replacement = ledgers_on(&world, 2)
        .into_iter()
        .map(|(id, _)| id)
        .find(|id| *id != crate::types::CommentId(4242))
        .expect("the replacement exists");
    assert_ne!(replacement, old);
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(replacement);
    let next = processor
        .on_outcomes(post.root, outcomes, post.feedback)
        .unwrap();
    // One quiescence run only: a single sub-cooldown miss. (A second
    // SPACED miss would make the absence stable, and posting then is the
    // designed behavior, not the bug.)
    finish_batches(&mut world, &mut processor, next);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "a POST in flight, even unbound, blocks the deletion fast path: one \
         short listing must not post a second replacement"
    );

    // The listing catches up and the replacement discharges everything.
    world.github.lock().unwrap().hidden_from_listings.clear();
    std::thread::sleep(std::time::Duration::from_millis(30));
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "still exactly one");
    assert_eq!(after[0].0, replacement);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the replacement, once seen, discharges the obligation"
    );
}

/// A deletion webhook is PROOF the recorded ledger is gone — but a
/// stale listing can keep serving the dead comment's old body, and the
/// probe used to accept that ghost as a satisfied ledger and discharge
/// without ever posting a replacement, leaving the declaration with no
/// backup (Codex ledger review round 22, P2). A confirmed deletion is
/// remembered, and the ghost is never selected again.
#[test]
fn a_ghost_of_a_confirmed_deleted_ledger_is_never_believed() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let old = ledgers_on(&world, 2)[0].0;

    // The maintainer deletes the ledger; the listing cache keeps
    // serving it.
    {
        let mut github = world.github.lock().unwrap();
        let ghost = github.comments.remove(&old).unwrap();
        github.stale_listing_ghosts.insert(old, ghost);
    }
    let deletion = format!(
        r#"{{
            "action": "deleted",
            "comment": {{
                "id": {old},
                "body": "gone",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", deletion.into_bytes());
    drain(&mut processor);

    // The webhook proved the deletion: the ghost in the listing must not
    // satisfy anything, and the replacement must actually exist.
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "a replacement is posted even while the listing serves the ghost"
    );
    world.github.lock().unwrap().stale_listing_ghosts.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert_ledgers_match_store(&world, &processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "nothing owed once the replacement stands"
    );
}

/// A ledger POST lands with its response lost, so its id is unrecorded —
/// and a maintainer then edits the marker away. The webhook matches only
/// the catch-all, which used to record nothing; a stale listing serving
/// the PRE-EDIT body then adopted the comment and discharged, leaving
/// the real comment vandalized with no retry owed (Codex ledger review
/// round 22, P2). The named-but-unidentified edit now taints the id: a
/// selection of it must rewrite, never accept a cached body.
#[test]
fn an_unidentified_edit_taints_a_cached_body_until_rewritten() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    // The declaration's ledger POST lands, but its response is lost.
    world.github.lock().unwrap().post_comment_response_lost = true;
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    world.github.lock().unwrap().post_comment_response_lost = false;
    let unrecorded = ledgers_on(&world, 2)[0].0;
    let pre_edit = world.github.lock().unwrap().comments[&unrecorded]
        .body
        .clone();

    // The maintainer edits the marker away; the listing cache still
    // serves the pre-edit ledger body.
    {
        let mut github = world.github.lock().unwrap();
        github.comments.get_mut(&unrecorded).unwrap().body = "vandalized".to_string();
        github.stale_listing_bodies.insert(unrecorded, pre_edit);
    }
    let edit = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": {unrecorded},
                "body": "vandalized",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", edit.into_bytes());
    drain(&mut processor);

    // Whatever the stale listing said, the REAL comment must carry the
    // ledger again (the taint forces an acknowledged rewrite by id).
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "the vandalized ledger is rewritten, not trusted from a cached body"
    );
    world.github.lock().unwrap().stale_listing_bodies.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert_ledgers_match_store(&world, &processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "nothing owed once the rewrite lands"
    );
}

/// Two spaced listings omit a ledger that still EXISTS, so stable
/// absence posts a replacement — displacing the recorded id on evidence,
/// not proof. Once listings recover, both comments used to stand for
/// ever with nothing owed (Codex ledger review round 22, P2). Displacing
/// a recorded ledger now leaves a cleanup obligation behind: neutralize
/// it, or learn from the 404 that it really was gone.
#[test]
fn a_displaced_ledger_is_owed_cleanup_not_forgotten() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let old = ledgers_on(&world, 2)[0].0;

    // The listings go blind to the (existing!) ledger long enough for
    // absence to become stable.
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(old);
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let remark = comment_body(&world.config, 2, "another remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);

    // The replacement stands; the displaced original still exists. Once
    // the listings recover, exactly one ledger may remain.
    world.github.lock().unwrap().hidden_from_listings.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(
        after.len(),
        1,
        "displacing a recorded ledger owes its cleanup; two must not stand"
    );
    assert_ne!(after[0].0, old, "the replacement is the one that stands");
    assert_ledgers_match_store(&world, &processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "nothing owed once the displaced ledger is cleaned up"
    );
}

/// Retract, let the "not stacked" write land, restore the declaration
/// in the same comment: two distinct ledger bodies both match the
/// restored declaration, and only the sequence number tells them apart.
/// A stale listing serving the ORIGINAL stacked body used to satisfy the
/// obligation — its seq was older but under the tamper cap — while the
/// real comment still said "not stacked" (Codex ledger review round 23,
/// P2). A body older than the last acknowledged write is never accepted.
#[test]
fn a_body_older_than_the_last_acknowledged_write_is_not_believed() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let ledger = ledgers_on(&world, 2)[0].0;
    let stacked_body = world.github.lock().unwrap().comments[&ledger].body.clone();

    // The author retracts by editing the declaration away; the
    // "not stacked" rewrite lands.
    let config = world.config.clone();
    let edited_declaration = move |text: &str| {
        format!(
            r#"{{
                "action": "edited",
                "comment": {{
                    "id": 20,
                    "body": "{text}",
                    "user": {{ "id": {AUTHOR}, "login": "author" }},
                    "updated_at": "2026-07-01T12:00:00Z"
                }},
                "issue": {{
                    "number": 2,
                    "pull_request": {{ "url": "..." }},
                    "user": {{ "id": {AUTHOR}, "login": "author" }}
                }},
                "repository": {repo},
                "sender": {{ "id": {AUTHOR}, "login": "author" }}
            }}"#,
            repo = repo_json(&config),
        )
        .into_bytes()
    };
    world.enqueue(
        &mut processor,
        "issue_comment",
        edited_declaration("never mind"),
    );
    drain(&mut processor);

    // ...and restores the declaration in the same comment, while the
    // listing cache still serves the ORIGINAL stacked body.
    world
        .github
        .lock()
        .unwrap()
        .stale_listing_bodies
        .insert(ledger, stacked_body);
    world.enqueue(
        &mut processor,
        "issue_comment",
        edited_declaration("@merge-train predecessor #1"),
    );
    drain(&mut processor);

    // Whatever the stale listing said, the real comment must state the
    // restored declaration once the cache heals.
    world.github.lock().unwrap().stale_listing_bodies.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert_ledgers_match_store(&world, &processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "nothing owed once the rewrite lands"
    );
}

/// GitHub answers 404 for comments that EXIST when repository access is
/// temporarily revoked. Treating that as proven deletion marked a forged
/// sibling dead and dropped its repair; after access was restored, the
/// dead-filter hid the forgery from every listing and the obligation
/// cleared over it (Codex ledger review round 23, P2). A write 404 alone
/// now only flags the repair; death needs a successful listing that
/// omits the comment too.
#[test]
fn an_auth_glitch_404_does_not_bury_a_forged_sibling() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let real = ledgers_on(&world, 2)[0].0;

    // A forged sibling is named while comment writes 404 spuriously.
    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: None,
        seq: u64::MAX,
        settled_through: None,
    });
    {
        let mut github = world.github.lock().unwrap();
        github.comments.insert(
            crate::types::CommentId(4242),
            FakeComment {
                pr: PrNumber(2),
                author_id: TEST_BOT_ID,
                body: forged.clone(),
                edited: true,
            },
        );
        github.update_comment_notfound = true;
    }
    let edit = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": 4242,
                "body": "{body}",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        body = json_escaped(&forged),
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", edit.into_bytes());
    drain(&mut processor);

    // Access is restored: the forgery is still there, still listed, and
    // must still be repaired.
    world.github.lock().unwrap().update_comment_notfound = false;
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(
        after.len(),
        1,
        "a spurious 404 must not bury the forgery for ever"
    );
    assert_eq!(after[0].0, real);
    assert_ledgers_match_store(&world, &processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "nothing owed once the repair lands"
    );
}

/// The first ledger POST lands with its response lost, so no id is
/// recorded. Two spaced listings omit the orphan and a replacement is
/// posted — displacing NOTHING on record, so no cleanup was owed, and
/// both ledgers stood for ever with empty obligation and repair tables
/// (Codex ledger review round 23, P2). A replacement over an earlier
/// unacknowledged post goes out unbound: only a fresh listing that
/// accounts for the orphan may discharge.
#[test]
fn a_replacement_over_an_unacknowledged_orphan_keeps_reconciling() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.github.lock().unwrap().post_comment_response_lost = true;
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    world.github.lock().unwrap().post_comment_response_lost = false;
    let orphan = ledgers_on(&world, 2)[0].0;

    // Two spaced listings omit the orphan; the replacement lands.
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(orphan);
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let remark = comment_body(&world.config, 2, "another remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);

    // The listings recover: the orphan must be reconciled away, not
    // stand for ever beside the replacement with nothing owed.
    world.github.lock().unwrap().hidden_from_listings.clear();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(
        after.len(),
        1,
        "a replacement over an unacknowledged orphan owes reconciliation"
    );
    assert_ne!(after[0].0, orphan, "the replacement is the one that stands");
    assert_ledgers_match_store(&world, &processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "nothing owed once the orphan is reconciled"
    );
}

/// The id transition and the displaced comment's cleanup are one store
/// transaction: whichever moment a crash picks, either both survive or
/// neither does (Codex ledger review round 23, P2). Functionally: one
/// call records the new id AND owes the old comment its repair.
#[test]
fn recording_a_replacement_owes_the_displaced_cleanup_atomically() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let old = ledgers_on(&world, 2)[0].0;

    let store = processor.store_mut();
    store
        .record_stack_ledger_posted(
            PrNumber(2),
            crate::types::CommentId(9999),
            Some(old),
            chrono::Utc::now(),
        )
        .unwrap();
    assert_eq!(
        store.state().prs[&PrNumber(2)].ledger_comment_id,
        Some(crate::types::CommentId(9999)),
        "the id transition landed"
    );
    let repairs = store.ledger_repairs(PrNumber(2)).unwrap();
    assert_eq!(repairs.len(), 1, "the displaced comment is owed cleanup");
    assert_eq!(repairs[0].comment_id, old);
    assert!(!repairs[0].rewrite, "cleanup neutralizes, not rewrites");
}

/// A doctored ledger that also LOOKS like a command must still be
/// repaired. Command authorization rejects the apparent command and
/// returns early, so the repair has to happen before it (Codex ledger
/// review round 13, P1).
#[test]
fn a_doctored_ledger_carrying_a_command_is_still_repaired() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let ledger_id = ledgers_on(&world, 2)[0].0;

    let forged = format!(
        "{}\n@merge-train predecessor #9",
        crate::status::format_stack_ledger(&crate::status::StackLedger {
            pr: PrNumber(2),
            declared: Some(crate::status::Declaration {
                predecessor: PrNumber(9),
                owner: crate::types::CommentId(1),
            }),
            seq: 9999,
            settled_through: Some(crate::types::CommentId(1)),
        })
    );
    world
        .github
        .lock()
        .unwrap()
        .comments
        .get_mut(&ledger_id)
        .unwrap()
        .body = forged.clone();
    // Edited by a STRANGER: the apparent command is unauthorized.
    let body = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": {ledger_id},
                "body": "@merge-train predecessor #9",
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {STRANGER}, "login": "stranger" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", body.into_bytes());
    drain(&mut processor);

    assert_eq!(
        ledger_on(&world, 2)
            .and_then(|l| l.declared)
            .map(|d| d.predecessor),
        Some(PrNumber(1)),
        "the forged edge is overwritten even though the command was refused"
    );
    assert_ledgers_match_store(&world, &processor);
}

/// A ledger whose DECLARATION still matches but whose other
/// recovery-significant fields were changed is rewritten: a bogus `seq`
/// would win a crawl's duplicate arbitration for ever, and a changed `pr`
/// makes the comment meaningless where it sits (Codex ledger review round
/// 13, P1).
#[test]
fn a_tampered_sequence_number_is_rewritten() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let (ledger_id, before) = ledgers_on(&world, 2)[0];

    let doctored = crate::status::StackLedger {
        seq: u64::MAX,
        ..before
    };
    world
        .github
        .lock()
        .unwrap()
        .comments
        .get_mut(&ledger_id)
        .unwrap()
        .body = crate::status::format_stack_ledger(&doctored);
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9002);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);

    let after = ledger_on(&world, 2).expect("still one ledger");
    assert_eq!(
        after.declared, before.declared,
        "the declaration is unchanged"
    );
    assert!(
        after.seq < u64::MAX,
        "but the sequence number is back to something the store could have written"
    );
}

/// The ledger's convergence property: whatever the users do to their
/// declarations, and whatever GitHub refuses along the way, the ledger
/// comments end up stating exactly what the store holds.
mod ledger_property {
    use proptest::prelude::*;

    use super::*;

    /// One change to the declared topology, as a user makes it.
    #[derive(Debug, Clone, Copy)]
    enum TopologyAction {
        /// PR `pr` declares `target` as its predecessor, in a fresh comment.
        Declare { pr: u64, target: u64 },
        /// The author deletes the comment that last declared on `pr`.
        Retract { pr: u64 },
        /// The author edits that comment into something that declares nothing.
        EditAway { pr: u64 },
    }

    fn arb_topology_action() -> impl Strategy<Value = (TopologyAction, bool)> {
        let action = prop_oneof![
            // Weighted towards the edge that VALIDATES (a PR's base is its
            // predecessor's branch), or nothing downstream ever holds a
            // declaration for a retraction to retract.
            3 => (2u64..=3).prop_map(|pr| TopologyAction::Declare { pr, target: pr - 1 }),
            1 => (2u64..=3, 1u64..=3)
                .prop_map(|(pr, target)| TopologyAction::Declare { pr, target }),
            2 => (2u64..=3).prop_map(|pr| TopologyAction::Retract { pr }),
            1 => (2u64..=3).prop_map(|pr| TopologyAction::EditAway { pr }),
        ];
        // Whether GitHub refuses comment edits while this action is processed:
        // the ledger write then fails and stays owed.
        (action, any::<bool>())
    }

    /// A comment webhook with an EXACT id (`comment_body` reduces ids modulo
    /// ten to keep its timestamp well-formed, which collapses the ids this
    /// property needs to keep apart).
    fn topology_comment(
        config: &GitConfig,
        pr: u64,
        text: &str,
        comment_id: u64,
        action: &str,
    ) -> Vec<u8> {
        format!(
            r#"{{
                "action": "{action}",
                "comment": {{
                    "id": {comment_id},
                    "body": "{text}",
                    "user": {{ "id": {AUTHOR}, "login": "author" }},
                    "updated_at": "2026-07-01T10:00:00Z"
                }},
                "issue": {{
                    "number": {pr},
                    "pull_request": {{ "url": "..." }},
                    "user": {{ "id": {AUTHOR}, "login": "author" }}
                }},
                "repository": {repo},
                "sender": {{ "id": {AUTHOR}, "login": "author" }}
            }}"#,
            repo = repo_json(config),
        )
        .into_bytes()
    }

    /// Runs one generated history of topology changes and asserts the ledger
    /// invariant at the end of it.
    fn ledger_property_case(actions: &[(TopologyAction, bool)]) {
        let (mut world, heads) = World::linear_stack(3);
        let mut processor = world.processor();
        for i in 1..=3u64 {
            let base = if i == 1 {
                "main".to_owned()
            } else {
                format!("pr-{}", i - 1)
            };
            let body = pr_opened_body(
                &world.config,
                i,
                &heads[i as usize - 1],
                &format!("pr-{i}"),
                &base,
            );
            world.enqueue(&mut processor, "pull_request", body);
        }
        drain(&mut processor);

        // The comment each PR last declared in, so retractions and edits have
        // something real to name.
        let mut last_comment: HashMap<u64, u64> = HashMap::new();
        let mut next_comment = 500u64;
        for (action, broken) in actions {
            world.github.lock().unwrap().update_comment_broken = *broken;
            let delivery = match *action {
                TopologyAction::Declare { pr, target } => {
                    next_comment += 10;
                    last_comment.insert(pr, next_comment);
                    Some(topology_comment(
                        &world.config,
                        pr,
                        &format!("@merge-train predecessor #{target}"),
                        next_comment,
                        "created",
                    ))
                }
                TopologyAction::Retract { pr } => last_comment.get(&pr).map(|id| {
                    topology_comment(
                        &world.config,
                        pr,
                        "@merge-train predecessor #1",
                        *id,
                        "deleted",
                    )
                }),
                TopologyAction::EditAway { pr } => last_comment
                    .get(&pr)
                    .map(|id| topology_comment(&world.config, pr, "never mind", *id, "edited")),
            };
            if let Some(body) = delivery {
                world.enqueue(&mut processor, "issue_comment", body);
                drain(&mut processor);
            }
        }

        // The worker restarts — obligations are durable, so a ledger owed
        // when the process died is still owed when it comes back — GitHub
        // answers again, and it gets one more turn: whatever the failures
        // were, the ledgers must converge on what the store holds.
        drop(processor);
        let mut processor = world.processor();
        world.github.lock().unwrap().update_comment_broken = false;
        next_comment += 10;
        let remark = topology_comment(&world.config, 2, "a remark", next_comment, "created");
        world.enqueue(&mut processor, "issue_comment", remark);
        drain(&mut processor);

        assert!(
            processor
                .store_mut()
                .owed_stack_ledgers()
                .unwrap()
                .is_empty(),
            "every ledger obligation is discharged once GitHub answers"
        );
        assert_ledgers_match_store(&world, &processor);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 12,
            ..ProptestConfig::default()
        })]

        /// However the declarations are made, retracted, edited away and
        /// remade — and however many of the ledger writes GitHub refuses while
        /// it happens — once GitHub answers again every PR's ledger comment
        /// states exactly the declaration the store holds, and there is at most
        /// one of them per PR.
        ///
        /// This is the property a lost-DB crawl depends on: it READS these
        /// comments instead of re-deriving the topology from the users'.
        #[test]
        fn ledgers_converge_on_the_declarations_the_store_holds(
            actions in proptest::collection::vec(arb_topology_action(), 1..6),
        ) {
            ledger_property_case(&actions);
        }
    }

    /// The shrunk cases worth keeping as examples: a declaration remade after a
    /// retraction, and a write that fails before the one that lands.
    #[test]
    fn ledger_property_fixed_cases() {
        ledger_property_case(&[
            (TopologyAction::Declare { pr: 2, target: 1 }, false),
            (TopologyAction::Retract { pr: 2 }, false),
            (TopologyAction::Declare { pr: 2, target: 1 }, false),
        ]);
        ledger_property_case(&[
            (TopologyAction::Declare { pr: 2, target: 1 }, false),
            (TopologyAction::Retract { pr: 2 }, true),
            (TopologyAction::Declare { pr: 3, target: 2 }, true),
        ]);
        ledger_property_case(&[
            (TopologyAction::Declare { pr: 3, target: 2 }, false),
            (TopologyAction::EditAway { pr: 3 }, false),
        ]);
    }

    /// One adversarial move against the ledger machinery: tampering,
    /// GitHub misbehaving, or a process death.
    #[derive(Debug, Clone, Copy)]
    enum AdversarialAction {
        /// A user (re)declares `pr`'s predecessor in a fresh comment.
        Declare { pr: u64 },
        /// A maintainer edits `pr`'s highest-ranked ledger comment into
        /// a forgery with an unbeatable sequence number. When `stale`,
        /// the listing cache keeps serving the pre-edit body.
        TamperLedger { pr: u64, stale: bool },
        /// A maintainer edits the marker out of `pr`'s ledger comment,
        /// leaving plain text. When `stale`, the listing cache keeps
        /// serving the pre-edit body.
        EditAwayLedger { pr: u64, stale: bool },
        /// A maintainer edits some other bot reply on `pr` into a
        /// forged ledger.
        ForgeSibling { pr: u64 },
        /// A maintainer deletes `pr`'s ledger comment; the webhook may
        /// arrive only after everything else, and the listing cache may
        /// keep serving the dead comment as a ghost.
        DeleteLedger {
            pr: u64,
            delayed_webhook: bool,
            ghost: bool,
        },
        /// GitHub's listings transiently omit every comment currently
        /// on `pr` (they still exist; writes by id still reach them).
        HideListings { pr: u64 },
        /// The listings catch up.
        UnhideAll,
        /// Comment edits start failing (a token outage)...
        BreakUpdates,
        /// ...and recover.
        HealUpdates,
        /// Posts land, but their responses are lost on the wire.
        LosePostResponses,
        /// Repository access flickers: comment writes 404 although the
        /// comments exist...
        AuthGlitch404,
        /// ...and access returns.
        HealAuth,
        /// The worker dies; a fresh one takes over the same store.
        Restart,
        /// Time passes mid-adversity: the stall timer fires and the
        /// machinery retries WHILE the world is still lying to it.
        Tick,
    }

    fn arb_adversarial_action() -> impl Strategy<Value = AdversarialAction> {
        prop_oneof![
            3 => (2u64..=3).prop_map(|pr| AdversarialAction::Declare { pr }),
            2 => ((2u64..=3), proptest::bool::ANY)
                .prop_map(|(pr, stale)| AdversarialAction::TamperLedger { pr, stale }),
            2 => ((2u64..=3), proptest::bool::ANY)
                .prop_map(|(pr, stale)| AdversarialAction::EditAwayLedger { pr, stale }),
            2 => (2u64..=3).prop_map(|pr| AdversarialAction::ForgeSibling { pr }),
            2 => ((2u64..=3), proptest::bool::ANY, proptest::bool::ANY).prop_map(
                |(pr, delayed_webhook, ghost)| AdversarialAction::DeleteLedger {
                    pr,
                    delayed_webhook,
                    ghost,
                },
            ),
            1 => (2u64..=3).prop_map(|pr| AdversarialAction::HideListings { pr }),
            1 => Just(AdversarialAction::UnhideAll),
            1 => Just(AdversarialAction::BreakUpdates),
            1 => Just(AdversarialAction::HealUpdates),
            1 => Just(AdversarialAction::LosePostResponses),
            1 => Just(AdversarialAction::AuthGlitch404),
            1 => Just(AdversarialAction::HealAuth),
            1 => Just(AdversarialAction::Restart),
            2 => Just(AdversarialAction::Tick),
        ]
    }

    fn forged_body(pr: u64) -> String {
        crate::status::format_stack_ledger(&crate::status::StackLedger {
            pr: PrNumber(pr),
            declared: None,
            seq: u64::MAX,
            settled_through: None,
        })
    }

    fn edited_comment_webhook(config: &GitConfig, pr: u64, comment_id: u64, body: &str) -> Vec<u8> {
        format!(
            r#"{{
                "action": "edited",
                "comment": {{
                    "id": {comment_id},
                    "body": "{body}",
                    "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                    "updated_at": "2026-07-01T12:00:00Z"
                }},
                "issue": {{
                    "number": {pr},
                    "pull_request": {{ "url": "..." }},
                    "user": {{ "id": {AUTHOR}, "login": "author" }}
                }},
                "repository": {repo},
                "sender": {{ "id": {AUTHOR}, "login": "author" }}
            }}"#,
            body = json_escaped(body),
            repo = repo_json(config),
        )
        .into_bytes()
    }

    fn deleted_comment_webhook(config: &GitConfig, pr: u64, comment_id: u64) -> Vec<u8> {
        format!(
            r#"{{
                "action": "deleted",
                "comment": {{
                    "id": {comment_id},
                    "body": "gone",
                    "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                    "updated_at": "2026-07-01T12:00:00Z"
                }},
                "issue": {{
                    "number": {pr},
                    "pull_request": {{ "url": "..." }},
                    "user": {{ "id": {AUTHOR}, "login": "author" }}
                }},
                "repository": {repo},
                "sender": {{ "id": {AUTHOR}, "login": "author" }}
            }}"#,
            repo = repo_json(config),
        )
        .into_bytes()
    }

    /// The convergence oracle under adversity. Every review round from 14
    /// through 21 was an instance of the same failure: some interleaving
    /// of tampering, listing staleness, failed or unacknowledged writes,
    /// delayed webhooks and restarts left a forged or duplicate ledger
    /// standing WITH NOTHING OWED — no retry would ever fix it. So the
    /// property is exactly that: once the adversary goes home (GitHub
    /// heals, the delayed webhooks arrive) and the machinery gets its
    /// retries, every obligation and repair drains, and the ledger
    /// comments state precisely what the store holds — at most one per PR.
    fn adversarial_case(actions: &[AdversarialAction]) {
        let (mut world, heads) = World::linear_stack(3);
        let mut processor = world.processor();
        for i in 1..=3u64 {
            let base = if i == 1 {
                "main".to_owned()
            } else {
                format!("pr-{}", i - 1)
            };
            let body = pr_opened_body(
                &world.config,
                i,
                &heads[i as usize - 1],
                &format!("pr-{i}"),
                &base,
            );
            world.enqueue(&mut processor, "pull_request", body);
        }
        drain(&mut processor);

        let mut next_comment = 500u64;
        let mut next_forgery = 700_000u64;
        let mut delayed: Vec<Vec<u8>> = Vec::new();
        for action in actions {
            match *action {
                AdversarialAction::Declare { pr } => {
                    next_comment += 10;
                    let body = topology_comment(
                        &world.config,
                        pr,
                        &format!("@merge-train predecessor #{}", pr - 1),
                        next_comment,
                        "created",
                    );
                    world.enqueue(&mut processor, "issue_comment", body);
                }
                AdversarialAction::TamperLedger { pr, stale } => {
                    let Some((id, _)) = ledgers_on(&world, pr).into_iter().next() else {
                        continue;
                    };
                    let forged = forged_body(pr);
                    {
                        let mut github = world.github.lock().unwrap();
                        let comment = github.comments.get_mut(&id).unwrap();
                        let pre_edit = std::mem::replace(&mut comment.body, forged.clone());
                        if stale {
                            github.stale_listing_bodies.insert(id, pre_edit);
                        }
                    }
                    let hook = edited_comment_webhook(&world.config, pr, id.0, &forged);
                    world.enqueue(&mut processor, "issue_comment", hook);
                }
                AdversarialAction::EditAwayLedger { pr, stale } => {
                    let Some((id, _)) = ledgers_on(&world, pr).into_iter().next() else {
                        continue;
                    };
                    {
                        let mut github = world.github.lock().unwrap();
                        let comment = github.comments.get_mut(&id).unwrap();
                        let pre_edit =
                            std::mem::replace(&mut comment.body, "vandalized".to_string());
                        if stale {
                            github.stale_listing_bodies.insert(id, pre_edit);
                        }
                    }
                    let hook = edited_comment_webhook(&world.config, pr, id.0, "vandalized");
                    world.enqueue(&mut processor, "issue_comment", hook);
                }
                AdversarialAction::ForgeSibling { pr } => {
                    let forged = forged_body(pr);
                    let id = {
                        let mut github = world.github.lock().unwrap();
                        while github
                            .comments
                            .contains_key(&crate::types::CommentId(next_forgery))
                        {
                            next_forgery += 1;
                        }
                        let id = crate::types::CommentId(next_forgery);
                        github.comments.insert(
                            id,
                            FakeComment {
                                pr: PrNumber(pr),
                                author_id: TEST_BOT_ID,
                                body: forged.clone(),
                                edited: true,
                            },
                        );
                        id
                    };
                    let hook = edited_comment_webhook(&world.config, pr, id.0, &forged);
                    world.enqueue(&mut processor, "issue_comment", hook);
                }
                AdversarialAction::DeleteLedger {
                    pr,
                    delayed_webhook,
                    ghost,
                } => {
                    let Some((id, _)) = ledgers_on(&world, pr).into_iter().next() else {
                        continue;
                    };
                    {
                        let mut github = world.github.lock().unwrap();
                        let removed = github.comments.remove(&id).unwrap();
                        if ghost {
                            github.stale_listing_ghosts.insert(id, removed);
                        }
                    }
                    let hook = deleted_comment_webhook(&world.config, pr, id.0);
                    if delayed_webhook {
                        delayed.push(hook);
                    } else {
                        world.enqueue(&mut processor, "issue_comment", hook);
                    }
                }
                AdversarialAction::HideListings { pr } => {
                    let mut github = world.github.lock().unwrap();
                    let ids: Vec<crate::types::CommentId> = github
                        .comments
                        .iter()
                        .filter(|(_, c)| c.pr == PrNumber(pr))
                        .map(|(id, _)| *id)
                        .collect();
                    for id in ids {
                        github.hidden_from_listings.insert(id);
                    }
                }
                AdversarialAction::UnhideAll => {
                    world.github.lock().unwrap().hidden_from_listings.clear();
                }
                AdversarialAction::BreakUpdates => {
                    world.github.lock().unwrap().update_comment_broken = true;
                }
                AdversarialAction::HealUpdates => {
                    world.github.lock().unwrap().update_comment_broken = false;
                }
                AdversarialAction::LosePostResponses => {
                    world.github.lock().unwrap().post_comment_response_lost = true;
                }
                AdversarialAction::AuthGlitch404 => {
                    world.github.lock().unwrap().update_comment_notfound = true;
                }
                AdversarialAction::HealAuth => {
                    world.github.lock().unwrap().update_comment_notfound = false;
                }
                AdversarialAction::Restart => {
                    drop(processor);
                    processor = world.processor();
                }
                AdversarialAction::Tick => {
                    std::thread::sleep(std::time::Duration::from_millis(30));
                    processor.requeue_marked_recoveries().unwrap();
                    run_sagas(&mut processor);
                }
            }
            drain(&mut processor);
        }

        // The adversary goes home: GitHub heals and the delayed webhooks
        // arrive.
        {
            let mut github = world.github.lock().unwrap();
            github.hidden_from_listings.clear();
            github.stale_listing_bodies.clear();
            github.stale_listing_ghosts.clear();
            github.update_comment_broken = false;
            github.update_comment_notfound = false;
            github.post_comment_response_lost = false;
        }
        for hook in delayed {
            world.enqueue(&mut processor, "issue_comment", hook);
        }
        drain(&mut processor);

        // The machinery gets its retries, spaced past the absence
        // cooldown, until nothing is owed.
        for _ in 0..8 {
            let owed = !processor
                .store_mut()
                .owed_stack_ledgers()
                .unwrap()
                .is_empty();
            let repairs = (1..=3u64).any(|pr| {
                !processor
                    .store_mut()
                    .ledger_repairs(PrNumber(pr))
                    .unwrap()
                    .is_empty()
            });
            if !owed && !repairs {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(30));
            processor.requeue_marked_recoveries().unwrap();
            run_sagas(&mut processor);
        }

        assert!(
            processor
                .store_mut()
                .owed_stack_ledgers()
                .unwrap()
                .is_empty(),
            "every ledger obligation drains once the adversary goes home"
        );
        for pr in 1..=3u64 {
            assert!(
                processor
                    .store_mut()
                    .ledger_repairs(PrNumber(pr))
                    .unwrap()
                    .is_empty(),
                "every repair on PR {pr} drains once the adversary goes home"
            );
        }
        assert_ledgers_match_store(&world, &processor);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 10,
            ..ProptestConfig::default()
        })]

        /// However the declarations, tamperings, forgeries, deletions,
        /// stale listings, failed and unacknowledged writes, delayed
        /// webhooks and restarts interleave: once GitHub heals and every
        /// webhook has arrived, the system converges — nothing owed,
        /// nothing standing that a lost-DB crawl would misread.
        #[test]
        fn ledgers_converge_under_adversity(
            actions in proptest::collection::vec(arb_adversarial_action(), 1..8),
        ) {
            adversarial_case(&actions);
        }
    }

    /// The review-round shapes, pinned: tampering behind a stale listing
    /// (rounds 19 and 21), a forged sibling behind a broken then healed
    /// token (rounds 20 and 21), and a deletion whose webhook outlives
    /// both a lost post response and the process (rounds 19 and 20).
    #[test]
    fn adversarial_fixed_cases() {
        adversarial_case(&[
            AdversarialAction::Declare { pr: 2 },
            AdversarialAction::TamperLedger { pr: 2, stale: true },
            AdversarialAction::HideListings { pr: 2 },
        ]);
        adversarial_case(&[
            AdversarialAction::Declare { pr: 2 },
            AdversarialAction::BreakUpdates,
            AdversarialAction::ForgeSibling { pr: 2 },
            AdversarialAction::HideListings { pr: 2 },
            AdversarialAction::HealUpdates,
        ]);
        adversarial_case(&[
            AdversarialAction::Declare { pr: 2 },
            AdversarialAction::DeleteLedger {
                pr: 2,
                delayed_webhook: true,
                ghost: false,
            },
            AdversarialAction::LosePostResponses,
            AdversarialAction::Restart,
        ]);
        // The round-22 shapes: a ghost of a confirmed deletion, an
        // unidentified edit-away behind a stale body after a lost post
        // response, and a displacement by listing blindness with time
        // passing mid-adversity.
        adversarial_case(&[
            AdversarialAction::Declare { pr: 2 },
            AdversarialAction::DeleteLedger {
                pr: 2,
                delayed_webhook: false,
                ghost: true,
            },
            AdversarialAction::Tick,
        ]);
        adversarial_case(&[
            AdversarialAction::LosePostResponses,
            AdversarialAction::Declare { pr: 2 },
            AdversarialAction::HealUpdates,
            AdversarialAction::EditAwayLedger { pr: 2, stale: true },
            AdversarialAction::Tick,
        ]);
        adversarial_case(&[
            AdversarialAction::Declare { pr: 2 },
            AdversarialAction::HideListings { pr: 2 },
            AdversarialAction::Tick,
            AdversarialAction::Tick,
            AdversarialAction::Tick,
            AdversarialAction::UnhideAll,
            AdversarialAction::Tick,
        ]);
        // The round-23 shapes: a forgery behind an auth-glitch 404, and
        // a replacement posted over an orphan whose response was lost.
        adversarial_case(&[
            AdversarialAction::Declare { pr: 2 },
            AdversarialAction::AuthGlitch404,
            AdversarialAction::ForgeSibling { pr: 2 },
            AdversarialAction::Tick,
            AdversarialAction::HealAuth,
            AdversarialAction::Tick,
        ]);
        adversarial_case(&[
            AdversarialAction::LosePostResponses,
            AdversarialAction::Declare { pr: 2 },
            AdversarialAction::HealUpdates,
            AdversarialAction::HideListings { pr: 2 },
            AdversarialAction::Tick,
            AdversarialAction::Tick,
            AdversarialAction::UnhideAll,
            AdversarialAction::Tick,
        ]);
    }
}

// ─── Authorization ───

#[test]
fn unauthorized_start_is_rejected_with_a_comment_and_no_train() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);

    let body = comment_body(
        &world.config,
        1,
        "@merge-train start",
        STRANGER,
        "stranger",
        77,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);

    assert!(processor.state().active_trains.is_empty());
    let github = world.github.lock().unwrap();
    assert!(
        github
            .posted_comments
            .iter()
            .any(|(pr, text)| *pr == PrNumber(1) && text.contains("Only the PR author")),
        "expected a rejection comment, got {:?}",
        github.posted_comments
    );
}

/// Commands run only from *created* comments. On `issue_comment.edited`,
/// GitHub keeps `comment.user` as the original author while the editor is in
/// `sender` — accepting commands from edits lets anyone who can edit another
/// user's comment impersonate them to the authorization gates (Codex M5
/// round 2, P1). Edits also re-key dedupe by `updated_at`, so an accepted
/// edit would re-run the command on every unrelated edit.
#[test]
fn edited_comment_commands_are_ignored() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);

    let body = comment_body_with_action(
        &world.config,
        1,
        "@merge-train start",
        AUTHOR,
        "author",
        88,
        "edited",
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);

    assert!(
        processor.state().active_trains.is_empty(),
        "an edited comment must never run a command"
    );
    assert_eq!(
        world
            .github
            .lock()
            .unwrap()
            .squash_count
            .values()
            .sum::<u32>(),
        0,
        "the edited comment's command ran a train to completion"
    );
}

/// The sender pin: predecessor declarations stay live under edits, so those
/// ARE authorized on edits — against the *editor* (`sender`), never the
/// original comment author.
#[test]
fn edited_predecessor_authorizes_the_editor_not_the_comment_author() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    // A stranger edits the author's comment into a predecessor declaration:
    // comment.user stays the author, sender is the editor.
    let body = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": 61,
                "body": "@merge-train predecessor #1",
                "user": {{ "id": {AUTHOR}, "login": "author" }},
                "updated_at": "2026-07-01T12:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {STRANGER}, "login": "stranger" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", body.into_bytes());
    drain(&mut processor);

    let github = world.github.lock().unwrap();
    assert!(
        github
            .posted_comments
            .iter()
            .any(|(pr, text)| *pr == PrNumber(2) && text.contains("Only the PR author")),
        "the editing stranger must be denied, got {:?}",
        github.posted_comments
    );
}

/// Retracting a predecessor declaration — editing it away or deleting the
/// declaring comment — is a topology change and is author-only, exactly like
/// declaring one. Without authorization, anyone with comment edit/delete
/// rights could reshape the stack and abort an active train (Codex M5
/// round 3, P1).
#[test]
fn stranger_cannot_retract_a_predecessor_declaration() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    // enqueue_stack_setup declared #1 as #2's predecessor via comment id 0
    // (its ids are taken mod 10).
    let declared = |p: &Processor| p.state().prs[&PrNumber(2)].predecessor;
    assert_eq!(declared(&processor), Some(PrNumber(1)));

    // A stranger deletes the author's declaring comment. (A comment can only
    // be deleted once on real GitHub, so each scenario uses its own comment.)
    let repo = repo_json(&world.config);
    let delete_body = move |comment_id: u64, sender_id: u64, sender_login: &str| {
        format!(
            r#"{{
                "action": "deleted",
                "comment": {{
                    "id": {comment_id},
                    "body": null,
                    "user": {{ "id": {AUTHOR}, "login": "author" }},
                    "updated_at": "2026-07-01T13:00:00Z"
                }},
                "issue": {{
                    "number": 2,
                    "pull_request": {{ "url": "..." }},
                    "user": {{ "id": {AUTHOR}, "login": "author" }}
                }},
                "repository": {repo},
                "sender": {{ "id": {sender_id}, "login": "{sender_login}" }}
            }}"#,
        )
        .into_bytes()
    };
    world.enqueue(
        &mut processor,
        "issue_comment",
        delete_body(20, STRANGER, "stranger"),
    );
    drain(&mut processor);

    assert_eq!(
        declared(&processor),
        Some(PrNumber(1)),
        "a stranger's deletion must not retract the declaration"
    );
    assert!(
        world
            .github
            .lock()
            .unwrap()
            .posted_comments
            .iter()
            .any(|(pr, text)| *pr == PrNumber(2) && text.contains("Only the PR author")),
        "expected a denial comment"
    );

    // The author re-declares in a fresh comment (id 5), then deletes it:
    // their own retraction proceeds.
    let body = comment_body(
        &world.config,
        2,
        "@merge-train predecessor #1",
        AUTHOR,
        "author",
        900,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);
    world.enqueue(
        &mut processor,
        "issue_comment",
        delete_body(900, AUTHOR, "author"),
    );
    drain(&mut processor);
    assert_eq!(
        declared(&processor),
        None,
        "the author's own deletion must retract the declaration"
    );
}

/// An authorized retraction leaves no trace in GitHub's present — the
/// declaring comment is gone — so the worker posts a durable RECEIPT on
/// the PR naming the retracted comment's id. A lost-DB crawl reads it as
/// a tombstone for that declaration and everything it had superseded. A
/// denied retraction posts none: nothing was retracted.
#[test]
fn an_authorized_retraction_posts_a_receipt_naming_the_retracted_comment() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    let repo = repo_json(&world.config);
    let delete_body = move |comment_id: u64, sender_id: u64, sender_login: &str| {
        format!(
            r#"{{
                "action": "deleted",
                "comment": {{
                    "id": {comment_id},
                    "body": null,
                    "user": {{ "id": {AUTHOR}, "login": "author" }},
                    "updated_at": "2026-07-01T13:00:00Z"
                }},
                "issue": {{
                    "number": 2,
                    "pull_request": {{ "url": "..." }},
                    "user": {{ "id": {AUTHOR}, "login": "author" }}
                }},
                "repository": {repo},
                "sender": {{ "id": {sender_id}, "login": "{sender_login}" }}
            }}"#,
        )
        .into_bytes()
    };
    let receipts = |world: &World| -> Vec<(PrNumber, crate::types::CommentId)> {
        world
            .github
            .lock()
            .unwrap()
            .posted_comments
            .iter()
            .filter_map(|(pr, text)| match crate::status::parse_receipt(text) {
                Some(crate::status::Receipt::Retraction {
                    pr: named,
                    retracted,
                }) => {
                    assert_eq!(*pr, named, "a receipt sits on the PR it names");
                    Some((named, retracted))
                }
                _ => None,
            })
            .collect()
    };

    // A stranger's deletion is denied: no retraction, no receipt.
    world.enqueue(
        &mut processor,
        "issue_comment",
        delete_body(20, STRANGER, "stranger"),
    );
    drain(&mut processor);
    assert_eq!(
        receipts(&world),
        vec![],
        "a denied retraction posts no receipt"
    );

    // The author re-declares in a fresh comment (id 5) and deletes it.
    let body = comment_body(
        &world.config,
        2,
        "@merge-train predecessor #1",
        AUTHOR,
        "author",
        900,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);
    world.enqueue(
        &mut processor,
        "issue_comment",
        delete_body(900, AUTHOR, "author"),
    );
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "the author's own deletion retracts the declaration"
    );
    assert_eq!(
        receipts(&world),
        vec![(PrNumber(2), crate::types::CommentId(900))],
        "exactly one receipt, on the retracting PR, anchored at the RETRACTED comment"
    );
}

/// A terminal train's status comment is the off-disk backup's last word:
/// left saying "active" because a GitHub outage swallowed the final
/// update, a later DB loss would resurrect the train the user stopped
/// (monolith review, P1). So the update is OWED — written with the
/// terminal event, surviving a restart — and retried at the stall cadence
/// until it is confirmed to have landed.
#[test]
fn a_failed_terminal_status_update_is_owed_until_it_lands() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    let claim_all = |p: &mut Processor| {
        while let Some(delivery) = p.claim().unwrap() {
            p.process_claimed(delivery).unwrap();
        }
    };
    claim_all(&mut processor);
    // Preflight lands the train and its status comment.
    let preflight = pump_cascade(&mut processor).expect("the start's preflight");
    let outcomes = execute(&mut processor, &preflight);
    let step = processor
        .on_outcomes(preflight.root, outcomes, preflight.feedback)
        .unwrap()
        .expect("the first cascade step");
    assert!(
        processor.state().active_trains[&PrNumber(1)]
            .state
            .is_active()
    );

    // The user stops while that step is in flight; it applies at the
    // boundary, whose cleanup batch carries the final status update.
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    claim_all(&mut processor);
    let outcomes = execute(&mut processor, &step);
    let cleanup = processor
        .on_outcomes(step.root, outcomes, step.feedback)
        .unwrap()
        .expect("the stop's cleanup batch");
    assert!(cleanup.effects.is_empty(), "cleanup is best-effort only");
    assert!(
        !processor.state().active_trains[&PrNumber(1)]
            .state
            .is_active()
    );

    // GitHub is down when the cleanup runs: the update fails.
    world.github.lock().unwrap().unavailable = true;
    let outcomes = execute(&mut processor, &cleanup);
    processor
        .on_outcomes(cleanup.root, outcomes, cleanup.feedback)
        .unwrap();
    let embedded = |world: &World| -> crate::types::TrainState {
        let github = world.github.lock().unwrap();
        let record = github
            .comments
            .values()
            .filter(|c| c.author_id == TEST_BOT_ID && c.pr == PrNumber(1))
            .find_map(|c| crate::status::parse_status_comment(&c.body).ok())
            .expect("the status comment");
        record.state
    };
    assert!(
        embedded(&world).is_active(),
        "precondition: the comment still says active — what a lost-DB crawl would resurrect"
    );
    assert_eq!(processor.owed_status_syncs(), vec![PrNumber(1)]);
    assert!(
        processor.take_retry_request(),
        "the worker must arm the stall timer for the retry"
    );

    // The owed sync survives a restart.
    drop(processor);
    let mut processor = world.processor();
    assert_eq!(processor.owed_status_syncs(), vec![PrNumber(1)]);

    // The outage ends; the timer's retry lands the update.
    world.github.lock().unwrap().unavailable = false;
    processor.requeue_marked_recoveries().unwrap();
    drain(&mut processor);
    assert!(
        matches!(embedded(&world), crate::types::TrainState::Stopped { .. }),
        "the comment must now say stopped"
    );
    assert!(processor.owed_status_syncs().is_empty());
    drop(processor);
    assert!(
        world.processor().owed_status_syncs().is_empty(),
        "nothing owed after a restart"
    );
}

/// Completion REMOVES the train's record from the state, so the owed sync
/// must carry the record as the train ended: a completion update lost to
/// an outage is still retried and lands (Codex terminal-sync review, P1).
#[test]
fn a_failed_completion_status_update_is_owed_until_it_lands() {
    let (mut world, _heads) = World::linear_stack(1);
    let mut processor = world.processor();
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    // Step until the batch that carries the completion update: effects
    // empty, and the train already gone from the state.
    let mut batch = pump_cascade(&mut processor).expect("the start's preflight");
    let cleanup = loop {
        let outcomes = execute(&mut processor, &batch);
        let next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap()
            .expect("the train has more to do");
        if next.effects.is_empty() && !processor.state().active_trains.contains_key(&PrNumber(1)) {
            break next;
        }
        batch = next;
    };
    assert_eq!(
        processor.owed_status_syncs(),
        vec![PrNumber(1)],
        "owed with the completion"
    );
    world.github.lock().unwrap().unavailable = true;
    let outcomes = execute(&mut processor, &cleanup);
    let next = processor
        .on_outcomes(cleanup.root, outcomes, cleanup.feedback)
        .unwrap();
    // The worker executes whatever the boundary hands back (here the owed
    // sync's probe, which fails in the outage); a test that drops it would
    // leave the saga slot occupied.
    finish_batches(&mut world, &mut processor, next);
    assert_eq!(processor.owed_status_syncs(), vec![PrNumber(1)]);
    assert!(processor.take_retry_request());

    world.github.lock().unwrap().unavailable = false;
    processor.requeue_marked_recoveries().unwrap();
    drain(&mut processor);
    let github = world.github.lock().unwrap();
    let record = github
        .comments
        .values()
        .filter(|c| c.author_id == TEST_BOT_ID && c.pr == PrNumber(1))
        .find_map(|c| crate::status::parse_status_comment(&c.body).ok())
        .expect("the status comment");
    assert!(
        matches!(record.state, crate::types::TrainState::Completed { .. }),
        "the comment must now say completed: {:?}",
        record.state
    );
    drop(github);
    assert!(processor.owed_status_syncs().is_empty());
}

/// An owed sync whose comment turns out to be GONE is cleared by the
/// retry's probes — nothing stale survives — and nothing is reposted. A
/// permanent failure of the update itself (revoked credentials, say) does
/// NOT clear it: only a probe may conclude absence (Codex terminal-sync
/// review, P1), and only once that absence is STABLE across two of them
/// (round 9, P1 — see `a_stably_absent_comment_clears_its_owed_sync`).
#[test]
fn an_owed_sync_for_a_deleted_comment_is_cleared_by_the_probe() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    let claim_all = |p: &mut Processor| {
        while let Some(delivery) = p.claim().unwrap() {
            p.process_claimed(delivery).unwrap();
        }
    };
    claim_all(&mut processor);
    let preflight = pump_cascade(&mut processor).expect("the start's preflight");
    let outcomes = execute(&mut processor, &preflight);
    let step = processor
        .on_outcomes(preflight.root, outcomes, preflight.feedback)
        .unwrap()
        .expect("the first cascade step");
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    claim_all(&mut processor);
    let outcomes = execute(&mut processor, &step);
    let cleanup = processor
        .on_outcomes(step.root, outcomes, step.feedback)
        .unwrap()
        .expect("the stop's cleanup batch");
    // The user deleted the bot's status comment before the update ran:
    // the update 404s (permanent), and the obligation stays owed.
    let status_id = {
        let github = world.github.lock().unwrap();
        *github
            .comments
            .iter()
            .find(|(_, c)| c.author_id == TEST_BOT_ID && c.pr == PrNumber(1))
            .map(|(id, _)| id)
            .expect("the status comment")
    };
    world.github.lock().unwrap().comments.remove(&status_id);
    let posted_before = world.github.lock().unwrap().posted_comments.len();
    let outcomes = execute(&mut processor, &cleanup);
    let next = processor
        .on_outcomes(cleanup.root, outcomes, cleanup.feedback)
        .unwrap();
    // The update 404s (permanent) — which never clears the obligation on
    // its own; only the probes the boundary hands back may conclude the
    // comment is gone, and they do, once they agree.
    finish_batches(&mut world, &mut processor, next);
    drain(&mut processor);
    assert_eq!(
        processor.owed_status_syncs().len(),
        1,
        "one absent listing is not yet proof"
    );
    nudge(&mut world, &mut processor, 8);
    assert!(
        processor.owed_status_syncs().is_empty(),
        "confirmed gone: cleared"
    );
    assert_eq!(
        world.github.lock().unwrap().posted_comments.len(),
        posted_before,
        "nothing is reposted for a comment the user removed"
    );
}

/// An owed update's outcome is matched by COMMENT, never by the batch it
/// rode: an observation boundary appends one train's terminal cleanup to
/// whichever batch is in flight, so the update can arrive under a foreign
/// root — or, as here, under no batch at all (Codex terminal-sync review
/// round 3, P1). Per-incarnation independence is pinned by the store's own
/// `a_second_incarnation_does_not_overwrite_the_first_obligation`.
#[test]
fn an_owed_update_is_matched_by_comment_not_by_batch_root() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    let claim_all = |p: &mut Processor| {
        while let Some(delivery) = p.claim().unwrap() {
            p.process_claimed(delivery).unwrap();
        }
    };
    claim_all(&mut processor);
    let preflight = pump_cascade(&mut processor).expect("the start's preflight");
    let outcomes = execute(&mut processor, &preflight);
    let step = processor
        .on_outcomes(preflight.root, outcomes, preflight.feedback)
        .unwrap()
        .expect("the first cascade step");
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    claim_all(&mut processor);
    let outcomes = execute(&mut processor, &step);
    let cleanup = processor
        .on_outcomes(step.root, outcomes, step.feedback)
        .unwrap()
        .expect("the stop's cleanup batch");

    // The terminal update fails in an outage: owed, with its comment.
    world.github.lock().unwrap().unavailable = true;
    let outcomes = execute(&mut processor, &cleanup);
    let next = processor
        .on_outcomes(cleanup.root, outcomes, cleanup.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    let owed = processor.owed_status_comments();
    assert_eq!(owed.len(), 1, "one obligation");
    let comment_id = owed[0].expect("its comment id is known");

    // A successful update for that comment, arriving with no batch of its
    // own, clears the obligation; one for another comment does not.
    processor
        .note_best_effort(&[crate::cascade::EffectOutcome {
            effect: Effect::GitHub(GitHubEffect::UpdateComment {
                comment_id: crate::types::CommentId(comment_id.0 + 1000),
                body: "someone else's".to_owned(),
            }),
            result: Ok(crate::cascade::EffectResponse::GitHub(
                crate::effects::GitHubResponse::CommentUpdated,
            )),
        }])
        .unwrap();
    assert_eq!(
        processor.owed_status_comments(),
        vec![Some(comment_id)],
        "an unrelated comment's update clears nothing"
    );
    processor
        .note_best_effort(&[crate::cascade::EffectOutcome {
            effect: Effect::GitHub(GitHubEffect::UpdateComment {
                comment_id,
                body: "the owed rewrite".to_owned(),
            }),
            result: Ok(crate::cascade::EffectResponse::GitHub(
                crate::effects::GitHubResponse::CommentUpdated,
            )),
        }])
        .unwrap();
    assert!(
        processor.owed_status_comments().is_empty(),
        "the owed comment's update clears it, whatever batch it rode"
    );
}

/// A live comment at the OWED id whose body someone mangled is still this
/// train's backup: the retry rewrites it rather than reading it as gone
/// (the M6 recovery path calls this `RefreshComment`). Reading it as
/// absent would skip the final update forever (Codex terminal-sync review
/// round 6, P2).
#[test]
fn an_owed_sync_rewrites_a_mangled_comment_at_the_known_id() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    let claim_all = |p: &mut Processor| {
        while let Some(delivery) = p.claim().unwrap() {
            p.process_claimed(delivery).unwrap();
        }
    };
    claim_all(&mut processor);
    let preflight = pump_cascade(&mut processor).expect("the start's preflight");
    let outcomes = execute(&mut processor, &preflight);
    let step = processor
        .on_outcomes(preflight.root, outcomes, preflight.feedback)
        .unwrap()
        .expect("the first cascade step");
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    claim_all(&mut processor);
    let outcomes = execute(&mut processor, &step);
    let cleanup = processor
        .on_outcomes(step.root, outcomes, step.feedback)
        .unwrap()
        .expect("the stop's cleanup batch");

    let status_id = processor.state().active_trains[&PrNumber(1)]
        .status_comment_id
        .expect("a status comment");
    world.github.lock().unwrap().unavailable = true;
    let outcomes = execute(&mut processor, &cleanup);
    let next = processor
        .on_outcomes(cleanup.root, outcomes, cleanup.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    assert_eq!(processor.owed_status_syncs(), vec![PrNumber(1)]);

    // Someone mangles the comment's body: no parseable record at all.
    world.github.lock().unwrap().unavailable = false;
    world
        .github
        .lock()
        .unwrap()
        .comments
        .get_mut(&status_id)
        .unwrap()
        .body = "(mangled)".to_owned();
    processor.requeue_marked_recoveries().unwrap();
    drain(&mut processor);

    assert!(processor.owed_status_syncs().is_empty(), "the sync landed");
    let github = world.github.lock().unwrap();
    let record = crate::status::parse_status_comment(&github.comments[&status_id].body)
        .expect("the mangled comment was rewritten from the record");
    assert!(matches!(
        record.state,
        crate::types::TrainState::Stopped { .. }
    ));
}

/// The crash window between `PostComment` succeeding and its
/// `StatusCommentPosted` committing: the comment is LIVE on GitHub while
/// the record has no id for it. A terminal event there still owes a sync,
/// and the retry resolves the comment by the INCARNATION embedded in it —
/// otherwise that comment stays saying active and a later DB loss
/// resurrects the stopped train (Codex terminal-sync review round 4, P1).
#[test]
fn a_comment_posted_before_the_crash_is_synced_by_incarnation() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    // Run until the batch that POSTS the status comment, execute it, and
    // die before observing it: GitHub has the comment, the store does not.
    let mut batch = pump_cascade(&mut processor).expect("the start's preflight");
    loop {
        let posts = batch
            .effects
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::PostComment { .. })));
        let outcomes = execute(&mut processor, &batch);
        if posts {
            break; // the crash: outcomes never observed
        }
        batch = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap()
            .expect("the train has more to do");
    }
    drop(processor);

    let mut processor = world.processor();
    let record = processor.state().active_trains[&PrNumber(1)].clone();
    assert_eq!(
        record.status_comment_id, None,
        "precondition: the id never committed"
    );
    let live = {
        let github = world.github.lock().unwrap();
        github
            .comments
            .iter()
            .find(|(_, c)| c.author_id == TEST_BOT_ID && c.pr == PrNumber(1))
            .map(|(id, _)| *id)
            .expect("precondition: the comment IS on GitHub")
    };

    // The user stops the train. The obligation is owed with no id.
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    drain(&mut processor);
    assert!(
        !processor.state().active_trains[&PrNumber(1)]
            .state
            .is_active(),
        "the train is stopped"
    );

    // The obligation was created mid-process by the terminal event, and
    // its cleanup carried no `UpdateComment` (the record had no id), so
    // NOTHING refers to it: the worker must schedule it anyway, without a
    // restart or a stall-timer nudge (Codex terminal-sync review round 5,
    // P1). `drain` alone must land it.
    drain(&mut processor);
    assert!(
        processor.owed_status_comments().is_empty(),
        "the sync landed"
    );
    let github = world.github.lock().unwrap();
    let embedded = crate::status::parse_status_comment(&github.comments[&live].body).unwrap();
    assert!(
        matches!(embedded.state, crate::types::TrainState::Stopped { .. }),
        "the orphaned comment must say stopped: {:?}",
        embedded.state
    );
    assert_eq!(
        embedded.started_at, record.started_at,
        "the same incarnation"
    );
}

/// The crash window in which `PostComment` landed but
/// `StatusCommentPosted` never committed: GitHub holds the train's status
/// comment while the store has no id for it. Returns the live comment.
fn train_with_orphaned_status_comment() -> (World, Processor, crate::types::CommentId) {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let mut batch = pump_cascade(&mut processor).expect("the start's preflight");
    loop {
        let posts = batch
            .effects
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::PostComment { .. })));
        let outcomes = execute(&mut processor, &batch);
        if posts {
            break; // the crash: outcomes never observed
        }
        batch = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap()
            .expect("the train has more to do");
    }
    drop(processor);
    let processor = world.processor();
    let live = {
        let github = world.github.lock().unwrap();
        github
            .comments
            .iter()
            .find(|(_, c)| c.author_id == TEST_BOT_ID && c.pr == PrNumber(1))
            .map(|(id, _)| *id)
            .expect("the comment IS on GitHub")
    };
    (world, processor, live)
}

/// Any delivery re-queues the owed syncs, as the worker's stall-retry
/// timer does: a probe that concluded nothing gets another look. The sleep
/// clears the absence cooldown — this harness sets the stall delay to
/// 25ms, and two listings closer together than that count as one.
fn nudge(world: &mut World, processor: &mut Processor, comment_id: u64) {
    std::thread::sleep(std::time::Duration::from_millis(30));
    let body = comment_body(
        &world.config,
        1,
        "just a remark, not a command",
        AUTHOR,
        "author",
        comment_id,
    );
    world.enqueue(processor, "issue_comment", body);
    drain(processor);
}

/// GitHub is not read-after-write consistent: a comment posted moments ago
/// can be missing from a listing that later returns it. An owed sync whose
/// comment id is UNKNOWN resolves the comment by listing, so one absent
/// listing must not be read as proof of deletion — clearing the obligation
/// there leaves that comment saying ACTIVE forever, and a later DB loss
/// resurrects the stopped train (Codex terminal-sync review round 9, P1).
#[test]
fn an_eventually_consistent_listing_does_not_clear_an_unknown_id_sync() {
    let (mut world, mut processor, live) = train_with_orphaned_status_comment();
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(live);

    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    drain(&mut processor);
    assert_eq!(
        processor.owed_status_comments().len(),
        1,
        "one absent listing is not proof of deletion"
    );

    // The comment becomes visible; the retry rewrites it.
    world.github.lock().unwrap().hidden_from_listings.clear();
    nudge(&mut world, &mut processor, 8);
    assert!(
        processor.owed_status_comments().is_empty(),
        "the sync landed once the listing caught up"
    );
    let github = world.github.lock().unwrap();
    let embedded = crate::status::parse_status_comment(&github.comments[&live].body).unwrap();
    assert!(
        matches!(embedded.state, crate::types::TrainState::Stopped { .. }),
        "the orphaned comment must say stopped: {:?}",
        embedded.state
    );
}

/// Absence is believed once it is STABLE: a comment missing from two
/// consecutive probes really is deleted, and the obligation is cleared
/// rather than retried forever (bounded uncertainty — an unbounded
/// obligation is one nobody can characterize).
#[test]
fn a_stably_absent_comment_clears_its_owed_sync() {
    let (mut world, mut processor, live) = train_with_orphaned_status_comment();
    world.github.lock().unwrap().comments.remove(&live);

    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    drain(&mut processor);
    assert_eq!(
        processor.owed_status_comments().len(),
        1,
        "the first absence is not yet believed"
    );
    let posted_before = world.github.lock().unwrap().posted_comments.len();
    nudge(&mut world, &mut processor, 8);
    assert!(
        processor.owed_status_comments().is_empty(),
        "a second absent probe confirms the deletion"
    );
    assert_eq!(
        world.github.lock().unwrap().posted_comments.len(),
        posted_before,
        "nothing is reposted for a comment the user removed"
    );
}

/// The terminal update landed and the process died before its outcomes
/// were handled, so the obligation survives against a comment that ALREADY
/// says what it owes. The probe reads that, and clears — it must not issue
/// another write, which a token that has lost edit rights would fail
/// forever, retrying an obligation that is already satisfied (Codex
/// terminal-sync review round 9, P2).
#[test]
fn a_probe_that_finds_the_terminal_record_clears_without_rewriting() {
    let (mut world, heads) = World::linear_stack(2);
    // The frontier PR is not mergeable, so the train parks `WaitingCi`
    // rather than running to completion: it is still stoppable.
    world.github.lock().unwrap().blocked.insert(PrNumber(1));
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    drain(&mut processor);

    // Stop the train, and run the cleanup that carries the terminal
    // update WITHOUT observing its outcomes: the update lands on GitHub,
    // the obligation stays owed.
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let cleanup = pump_cascade(&mut processor).expect("the stop's cleanup batch");
    assert!(
        cleanup
            .best_effort
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { .. }))),
        "the cleanup carries the terminal update"
    );
    let interpreter = WorktreeGitInterpreter::new(processor.git_config(), cleanup.root);
    let _ = execute_batch(&interpreter, processor.github(), &cleanup);
    drop(processor);

    // Restart: the obligation is durable, and the comment already says it.
    let mut processor = world.processor();
    assert_eq!(
        processor.owed_status_comments().len(),
        1,
        "precondition: still owed after the crash"
    );
    // The token can still read comments but has lost the right to edit
    // them: a rewrite would fail forever.
    let updates_before = {
        let mut github = world.github.lock().unwrap();
        github.update_comment_broken = true;
        github.comment_updates
    };
    drain(&mut processor);
    assert!(
        processor.owed_status_comments().is_empty(),
        "an obligation the comment already satisfies is cleared, not retried"
    );
    assert_eq!(
        world.github.lock().unwrap().comment_updates,
        updates_before,
        "nothing is rewritten"
    );
}

/// A stop queued while a status-sync probe is in flight applies at THAT
/// boundary. The probe's outcomes are an observation boundary like any
/// other: nothing is in flight behind them, so letting the rewrite jump
/// ahead leaves an acknowledged stop unapplied across arbitrarily many
/// comment-only batches while GitHub retries (Codex terminal-sync review
/// round 9, P2).
#[test]
fn a_queued_stop_preempts_an_owed_sync_probe() {
    let (mut world, heads) = World::linear_stack(2);
    // The frontier PR is not mergeable, so the train parks `WaitingCi`
    // rather than running to completion: it is still stoppable.
    world.github.lock().unwrap().blocked.insert(PrNumber(1));
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    drain(&mut processor);

    // The token loses the right to edit comments: the first train's stop
    // owes a terminal update that can never land.
    world.github.lock().unwrap().update_comment_broken = true;
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    drain(&mut processor);
    assert_eq!(
        processor.owed_status_comments().len(),
        1,
        "precondition: an obligation that keeps retrying"
    );

    // The user starts a fresh train on the same root (a new comment: the
    // first `start` is deduped by its id).
    let restart = comment_body(
        &world.config,
        1,
        "@merge-train start",
        AUTHOR,
        "author",
        600,
    );
    world.enqueue(&mut processor, "issue_comment", restart);
    drain(&mut processor);
    assert!(
        processor.state().active_trains[&PrNumber(1)]
            .state
            .is_active(),
        "precondition: the second incarnation is running"
    );

    // A probe for the old obligation goes in flight (any delivery
    // re-queues the owed syncs, as the stall-retry timer does)...
    let remark = comment_body(&world.config, 1, "just a remark", AUTHOR, "author", 8);
    world.enqueue(&mut processor, "issue_comment", remark);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = loop {
        let batch = processor.pump().unwrap().expect("the owed sync's probe");
        if batch
            .effects
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::ListComments { .. })))
        {
            break batch;
        }
        let outcomes = execute(&mut processor, &batch);
        processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    };

    // ... and the user stops the running train while it is out.
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 9);
    world.enqueue(&mut processor, "issue_comment", stop);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let outcomes = execute(&mut processor, &probe);
    let next = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap();
    assert!(
        !processor.state().active_trains[&PrNumber(1)]
            .state
            .is_active(),
        "the stop applies at the probe's boundary, not behind its rewrites"
    );
    finish_batches(&mut world, &mut processor, next);
}

/// However many owed syncs fail, the worker arms at most ONE outstanding
/// stall-retry timer; the timer's landing (`requeue_marked_recoveries`)
/// re-opens the gate. One timer per failed probe amplified instead: every
/// `RetryStalled` requeues ALL obligations, each failing probe armed
/// another timer, and traffic grew with the number of timers in flight
/// rather than respecting `stall_retry_delay` (Codex terminal-sync review
/// round 15, P2).
#[test]
fn failing_probes_arm_one_stall_timer_not_one_each() {
    let (mut world, heads) = World::linear_stack(2);
    {
        let mut github = world.github.lock().unwrap();
        github.prs.get_mut(&PrNumber(2)).unwrap().base_ref = "main".to_owned();
        github.blocked.insert(PrNumber(1));
        github.blocked.insert(PrNumber(2));
    }
    let mut processor = world.processor();
    // Both PRs are independent roots whose trains park (not mergeable).
    for i in 1..=2u64 {
        let body = pr_opened_body(
            &world.config,
            i,
            &heads[(i - 1) as usize],
            &format!("pr-{i}"),
            "main",
        );
        world.enqueue(&mut processor, "pull_request", body);
    }
    start_command(&mut world, &mut processor, 1);
    start_command(&mut world, &mut processor, 2);
    drain(&mut processor);

    // Both stops owe terminal updates that cannot land.
    world.github.lock().unwrap().update_comment_broken = true;
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    let stop = comment_body(&world.config, 2, "@merge-train stop", AUTHOR, "author", 76);
    world.enqueue(&mut processor, "issue_comment", stop);
    drain(&mut processor);
    assert_eq!(
        processor.owed_status_syncs().len(),
        2,
        "precondition: two obligations that keep retrying"
    );
    assert!(
        processor.take_retry_request(),
        "the first request arms the stall timer"
    );

    // While that timer is outstanding, another delivery re-queues the owed
    // syncs and both probes fail again: the failures must coalesce into
    // the timer already out, not arm a second one.
    let remark = comment_body(&world.config, 1, "just a remark", AUTHOR, "author", 8);
    world.enqueue(&mut processor, "issue_comment", remark);
    drain(&mut processor);
    assert!(
        !processor.take_retry_request(),
        "failures while a timer is outstanding must coalesce into it"
    );

    // The timer lands and its requeued probes fail again: the gate has
    // re-opened, and exactly one fresh timer is armed.
    processor.requeue_marked_recoveries().unwrap();
    drain(&mut processor);
    assert!(
        processor.take_retry_request(),
        "the landed timer's failed probes arm one fresh timer"
    );
    assert!(!processor.take_retry_request(), "and only one");
}

/// A completed terminal REWRITE is an observation boundary like the probe
/// that preceded it: a deferred abort acts there, before queued work runs.
/// Pumping straight from the rewrite's `feedback: false` completion let a
/// queued restart run against a successor train a review dismissal had
/// already doomed — the start was durably rejected as "already running",
/// and the abort then left no train at all (Codex terminal-sync review
/// round 16, P2).
#[test]
fn a_rewrite_landing_applies_deferred_aborts_before_queued_starts() {
    let (mut world, heads) = World::linear_stack(2);
    world.github.lock().unwrap().blocked.insert(PrNumber(1));
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    drain(&mut processor);

    // The first incarnation's stop owes a terminal update that cannot
    // land while the token has lost the right to edit comments.
    world.github.lock().unwrap().update_comment_broken = true;
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    drain(&mut processor);
    assert_eq!(
        processor.owed_status_comments().len(),
        1,
        "precondition: an obligation that keeps retrying"
    );

    // A second incarnation runs on the same root.
    let restart = comment_body(
        &world.config,
        1,
        "@merge-train start",
        AUTHOR,
        "author",
        600,
    );
    world.enqueue(&mut processor, "issue_comment", restart);
    drain(&mut processor);
    assert!(
        processor.state().active_trains[&PrNumber(1)]
            .state
            .is_active(),
        "precondition: the second incarnation is running"
    );

    // The old obligation's probe runs, and hands back the REWRITE, which
    // goes in flight.
    let remark = comment_body(&world.config, 1, "just a remark", AUTHOR, "author", 8);
    world.enqueue(&mut processor, "issue_comment", remark);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = loop {
        let batch = processor.pump().unwrap().expect("the owed sync's probe");
        if batch
            .effects
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::ListComments { .. })))
        {
            break batch;
        }
        let outcomes = execute(&mut processor, &batch);
        processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    };
    let outcomes = execute(&mut processor, &probe);
    let rewrite = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap()
        .expect("the terminal rewrite batch");
    assert!(
        rewrite
            .best_effort
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::UpdateComment { .. }))),
        "precondition: the rewrite is what went in flight"
    );

    // While it is out: the user queues a restart, and a review dismissal
    // then aborts the second incarnation (deferred — the rewrite holds
    // this root's saga slot). The outage also ends, so the rewrite lands.
    world.github.lock().unwrap().update_comment_broken = false;
    let restart = comment_body(&world.config, 1, "@merge-train start", AUTHOR, "author", 9);
    world.enqueue(&mut processor, "issue_comment", restart);
    let dismissal = format!(
        r#"{{
            "action": "dismissed",
            "review": {{
                "id": 779,
                "user": {{ "id": 555, "login": "reviewer" }},
                "state": "dismissed",
                "body": null
            }},
            "pull_request": {{ "number": 1 }},
            "repository": {repo}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(
        &mut processor,
        "pull_request_review",
        dismissal.into_bytes(),
    );
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // The rewrite's completion is the boundary where the abort must act.
    let outcomes = execute(&mut processor, &rewrite);
    let next = processor
        .on_outcomes(rewrite.root, outcomes, rewrite.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    drain(&mut processor);

    assert!(
        world
            .github
            .lock()
            .unwrap()
            .posted_comments
            .iter()
            .any(|(pr, text)| *pr == PrNumber(1) && text.contains("Merge train aborted")),
        "the dismissed incarnation still posts its abort notice"
    );
    assert!(
        processor.state().active_trains[&PrNumber(1)]
            .state
            .is_active(),
        "the queued restart must start a fresh incarnation, not be \
         rejected against the successor the dismissal doomed"
    );
}

/// A successor train aborted while an older incarnation's status-sync
/// probe is in flight must not lose its cleanup to a queued restart. The
/// probe's boundary applies the deferred abort, and the cleanup computed
/// there must ride the boundary's own batch: queued as a recompute behind
/// the waiting `start`, the start replaces the aborted record first and
/// the recompute then finds nothing — no worktree removal, no abort
/// notice (Codex terminal-sync review round 14, P2).
#[test]
fn an_abort_at_a_probe_boundary_survives_a_queued_restart() {
    let (mut world, heads) = World::linear_stack(2);
    world.github.lock().unwrap().blocked.insert(PrNumber(1));
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    drain(&mut processor);

    // The first incarnation's stop owes a terminal update that cannot
    // land while the token has lost the right to edit comments.
    world.github.lock().unwrap().update_comment_broken = true;
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    drain(&mut processor);
    assert_eq!(
        processor.owed_status_comments().len(),
        1,
        "precondition: an obligation that keeps retrying"
    );

    // A second incarnation runs on the same root.
    let restart = comment_body(
        &world.config,
        1,
        "@merge-train start",
        AUTHOR,
        "author",
        600,
    );
    world.enqueue(&mut processor, "issue_comment", restart);
    drain(&mut processor);
    assert!(
        processor.state().active_trains[&PrNumber(1)]
            .state
            .is_active(),
        "precondition: the second incarnation is running"
    );

    // The old obligation's probe goes in flight.
    let remark = comment_body(&world.config, 1, "just a remark", AUTHOR, "author", 8);
    world.enqueue(&mut processor, "issue_comment", remark);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let probe = loop {
        let batch = processor.pump().unwrap().expect("the owed sync's probe");
        if batch
            .effects
            .iter()
            .any(|e| matches!(e, Effect::GitHub(GitHubEffect::ListComments { .. })))
        {
            break batch;
        }
        let outcomes = execute(&mut processor, &batch);
        processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    };

    // While it is out: a review dismissal aborts the second incarnation
    // (deferred — the probe holds this root's saga slot), and the user
    // queues a THIRD start behind it.
    world.github.lock().unwrap().update_comment_broken = false;
    let dismissal = format!(
        r#"{{
            "action": "dismissed",
            "review": {{
                "id": 777,
                "user": {{ "id": 555, "login": "reviewer" }},
                "state": "dismissed",
                "body": null
            }},
            "pull_request": {{ "number": 1 }},
            "repository": {repo}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(
        &mut processor,
        "pull_request_review",
        dismissal.into_bytes(),
    );
    let restart = comment_body(&world.config, 1, "@merge-train start", AUTHOR, "author", 9);
    world.enqueue(&mut processor, "issue_comment", restart);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // The probe's boundary applies the abort; everything runs to rest.
    let outcomes = execute(&mut processor, &probe);
    let next = processor
        .on_outcomes(probe.root, outcomes, probe.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    drain(&mut processor);

    assert!(
        world
            .github
            .lock()
            .unwrap()
            .posted_comments
            .iter()
            .any(|(pr, text)| *pr == PrNumber(1) && text.contains("Merge train aborted")),
        "the aborted incarnation's notice must not be lost to the queued restart"
    );
    assert!(
        processor.state().active_trains[&PrNumber(1)]
            .state
            .is_active(),
        "the queued start still runs, as a fresh incarnation"
    );
}

/// A comment event *performed by the bot* (sender == bot) must not reach
/// the handler at all: the handler's self-guard keys off the comment
/// author, so a bot-performed edit of a USER's comment (author == user)
/// would run predecessor declaration/retraction with no authorization gate
/// (Codex M5 round 18).
#[test]
fn bot_performed_comment_edits_are_closed_without_handling() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1))
    );

    // The bot "edits" the author's declaring comment (id 0) away:
    // author stays the user, sender is the bot.
    let body = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": 0,
                "body": "nothing here now",
                "user": {{ "id": {AUTHOR}, "login": "author" }},
                "updated_at": "2026-07-01T14:00:00Z"
            }},
            "issue": {{
                "number": 2,
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "issue_comment", body.into_bytes());
    drain(&mut processor);

    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1)),
        "a bot-performed edit must not retract the declaration"
    );
}

#[test]
fn maintainer_stop_is_authorized_via_role_lookup() {
    let (mut world, heads) = World::linear_stack(2);
    world.github.lock().unwrap().roles.insert(
        "maintainer".to_owned(),
        crate::effects::github::CollaboratorRole::Maintain,
    );
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);

    // Process the backlog only up to the first saga batch, then stop mid-way
    // is covered elsewhere; here the train may even complete — a stop on a
    // finished train is the "no active train" comment, so instead stop a
    // *running* train: process deliveries but no sagas yet.
    while let Some(delivery) = processor.claim().unwrap() {
        assert_eq!(
            processor.process_claimed(delivery).unwrap(),
            PipelineOutcome::Processed
        );
    }

    // The maintainer (not the author) asks for a stop before the saga runs.
    let body = comment_body(
        &world.config,
        1,
        "@merge-train stop",
        STRANGER,
        "maintainer",
        88,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);

    // The train never became active (stopped before/at start) or was stopped;
    // either way nothing merged and no train is active.
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active()),
        "stop must terminate the train"
    );
    assert_eq!(
        world
            .github
            .lock()
            .unwrap()
            .squash_count
            .values()
            .sum::<u32>(),
        0,
        "nothing may merge after a pre-run stop"
    );
}

#[test]
fn stranger_without_role_cannot_stop() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);

    let body = comment_body(
        &world.config,
        1,
        "@merge-train stop",
        STRANGER,
        "stranger",
        99,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);

    let github = world.github.lock().unwrap();
    assert!(
        github
            .posted_comments
            .iter()
            .any(|(_, text)| text.contains("admin/maintainer")),
        "expected a role rejection, got {:?}",
        github.posted_comments
    );
}

/// A *permanent* permission-lookup failure (bad token scope, API changes)
/// must fail closed — deny the command and close the delivery — not release
/// it: a released delivery goes back to the front of the queue and retries
/// forever, wedging every later delivery for the repo (Codex M5 round 4).
/// Transient failures still release (the stall-retry loop covers them).
#[test]
fn permanent_permission_lookup_failure_fails_closed() {
    let (mut world, heads) = World::linear_stack(2);
    world.github.lock().unwrap().permission_lookup_broken = true;
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);

    // A stranger's stop needs a role lookup, which fails Permanent.
    let body = comment_body(
        &world.config,
        1,
        "@merge-train stop",
        STRANGER,
        "stranger",
        98,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    while let Some(delivery) = processor.claim().unwrap() {
        assert_eq!(
            processor.process_claimed(delivery).unwrap(),
            PipelineOutcome::Processed,
            "a permanent lookup failure must close the delivery, not release it"
        );
    }

    let github = world.github.lock().unwrap();
    assert!(
        github
            .posted_comments
            .iter()
            .any(|(_, text)| text.contains("cannot verify")),
        "expected a fail-closed denial, got {:?}",
        github.posted_comments
    );
}

/// A start whose preflight batch FAILS (e.g. 5xx on branch protection after
/// retries) dies before its train exists, so the engine has nowhere to feed
/// the failure — the acked start used to vanish with only a log line (Codex
/// M5 round 14, P1). The user must be told to re-issue, and the worker must
/// remain fully operable.
#[test]
fn failed_start_preflight_answers_the_user_instead_of_vanishing() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let batch = pump_cascade(&mut processor).expect("start plans preflight");

    // GitHub goes down for the preflight execution, then recovers.
    world.github.lock().unwrap().unavailable = true;
    let outcomes = execute(&mut processor, &batch);
    assert!(outcomes.iter().any(|o| o.result.is_err()));
    world.github.lock().unwrap().unavailable = false;

    let mut next = processor
        .on_outcomes(batch.root, outcomes, batch.feedback)
        .unwrap();
    while let Some(batch) = next {
        let outcomes = execute(&mut processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    }

    assert!(processor.state().active_trains.is_empty());
    {
        let github = world.github.lock().unwrap();
        assert!(
            github
                .posted_comments
                .iter()
                .any(|(pr, text)| *pr == PrNumber(1) && text.contains("re-issue")),
            "the user must learn the start failed, got {:?}",
            github.posted_comments
        );
    }

    // The worker is fully operable: a re-issued start completes.
    let body = comment_body(&world.config, 1, "@merge-train start", AUTHOR, "author", 8);
    world.enqueue(&mut processor, "issue_comment", body);
    drive_to_completion(&mut world, &mut processor);
    assert!(processor.state().prs[&PrNumber(1)].state.is_merged());
}

// ─── Stop honored at an observation boundary ───

#[test]
fn stop_mid_saga_takes_effect_at_the_next_observation_boundary() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);

    // Process all deliveries; take the FIRST saga batch but do not feed its
    // outcomes back yet — its effects are "in flight".
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let batch = pump_cascade(&mut processor).expect("start plans a saga");
    let outcomes = execute(&mut processor, &batch);

    // The author's stop arrives while those effects execute.
    let body = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 66);
    world.enqueue(&mut processor, "issue_comment", body);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // At the observation boundary the stop preempts the next plan.
    let mut next = processor
        .on_outcomes(batch.root, outcomes, batch.feedback)
        .unwrap();
    let mut steps = 0;
    while let Some(batch) = next {
        steps += 1;
        assert!(steps < 10, "post-stop cleanup must terminate");
        assert!(
            batch.effects.is_empty(),
            "no further observed (cascade-advancing) effects may run after \
             the stop, got {:?}",
            batch.effects
        );
        let outcomes = execute(&mut processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    }

    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active())
    );
    assert_eq!(
        world
            .github
            .lock()
            .unwrap()
            .squash_count
            .values()
            .sum::<u32>(),
        0,
        "the squash must never run once a stop preempts the boundary"
    );
}

/// A stop that lands while an *irreversible* effect is in flight must not
/// discard that effect's record: the squash already merged the PR on GitHub,
/// so its outcome must be integrated before the stop retires the train, or
/// the store diverges from reality (Codex M5 review, P1).
#[test]
fn stop_during_inflight_squash_records_the_merge_before_stopping() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Drive batches, pausing at the boundary right after the squash executed
    // (a root-only train squashes without waiting on CI in this harness).
    let mut next = processor.pump().unwrap();
    let mut in_flight = None;
    while let Some(batch) = next {
        let outcomes = execute(&mut processor, &batch);
        let squashed = world
            .github
            .lock()
            .unwrap()
            .squash_count
            .values()
            .sum::<u32>()
            == 1;
        if squashed {
            in_flight = Some((batch, outcomes));
            break;
        }
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    }
    let (batch, outcomes) = in_flight.expect("the squash batch never executed");

    // The author's stop lands while the squash outcomes are in flight.
    let body = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", body);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Feed the outcomes back and run everything to quiescence.
    let mut next = processor
        .on_outcomes(batch.root, outcomes, batch.feedback)
        .unwrap();
    while let Some(batch) = next {
        let outcomes = execute(&mut processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    }
    drain(&mut processor);

    // GitHub performed the merge; the store must know.
    assert!(matches!(
        world.github.lock().unwrap().prs[&PrNumber(1)].state,
        FakePrState::Merged { .. }
    ));
    assert!(
        processor.state().prs[&PrNumber(1)].state.is_merged(),
        "the executed squash was never recorded in the store"
    );
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active())
    );
    let events = processor.store_mut().events().unwrap();
    let facts = ReplayFacts::for_train(&events, PrNumber(1));
    assert_eq!(
        facts.unmatched().count(),
        0,
        "IntentSquash must have its Done record even though a stop was queued"
    );
}

/// A handler abort (here: review dismissed) landing while an irreversible
/// effect is in flight must not discard that effect's outcome. Handler
/// `TrainAborted` events for the in-flight saga root are deferred to the
/// observation boundary — the same ordering queued stops get (Codex M5
/// round 2, P1); committed mid-saga they made `advance` see an inactive
/// train and drop the executed squash's record.
#[test]
fn handler_abort_during_inflight_squash_records_the_merge_first() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Drive batches, pausing right after the squash executed.
    let mut next = processor.pump().unwrap();
    let mut in_flight = None;
    while let Some(batch) = next {
        let outcomes = execute(&mut processor, &batch);
        let squashed = world
            .github
            .lock()
            .unwrap()
            .squash_count
            .values()
            .sum::<u32>()
            == 1;
        if squashed {
            in_flight = Some((batch, outcomes));
            break;
        }
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    }
    let (batch, outcomes) = in_flight.expect("the squash batch never executed");

    // A review dismissal arrives while the squash outcomes are in flight:
    // the handler aborts the train.
    let body = format!(
        r#"{{
            "action": "dismissed",
            "review": {{
                "id": 777,
                "user": {{ "id": 555, "login": "reviewer" }},
                "state": "dismissed",
                "body": null
            }},
            "pull_request": {{ "number": 1 }},
            "repository": {repo}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(&mut processor, "pull_request_review", body.into_bytes());
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Feed the outcomes back and run to quiescence.
    let mut next = processor
        .on_outcomes(batch.root, outcomes, batch.feedback)
        .unwrap();
    while let Some(batch) = next {
        let outcomes = execute(&mut processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    }
    drain(&mut processor);

    // GitHub performed the merge; the store must know, abort or no abort.
    assert!(
        processor.state().prs[&PrNumber(1)].state.is_merged(),
        "the executed squash was never recorded in the store"
    );
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active())
    );
    let events = processor.store_mut().events().unwrap();
    let facts = ReplayFacts::for_train(&events, PrNumber(1));
    assert_eq!(
        facts.unmatched().count(),
        0,
        "IntentSquash must have its Done record despite the handler abort"
    );
}

/// The mirror pin: a stop at an observation boundary of an *existing* train
/// still suppresses the planned continuation — nothing irreversible that has
/// not yet run may start after the stop.
#[test]
fn stop_mid_saga_on_an_existing_train_suppresses_the_continuation() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Complete the preflight so the train record exists, leaving the next
    // batch (preparation work) in flight.
    let preflight = pump_cascade(&mut processor).expect("start plans preflight");
    let outcomes = execute(&mut processor, &preflight);
    let batch = processor
        .on_outcomes(preflight.root, outcomes, preflight.feedback)
        .unwrap()
        .expect("preflight completion continues the saga");
    assert!(
        processor.state().active_trains.contains_key(&PrNumber(1)),
        "the train must exist before the stop for this test to bite"
    );
    let outcomes = execute(&mut processor, &batch);
    let remote_head_before = world.github.lock().unwrap().branch_head("pr-2");

    // The author's stop lands while that batch's outcomes are in flight.
    let body = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 8);
    world.enqueue(&mut processor, "issue_comment", body);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    let mut next = processor
        .on_outcomes(batch.root, outcomes, batch.feedback)
        .unwrap();
    let mut steps = 0;
    while let Some(batch) = next {
        steps += 1;
        assert!(steps < 10, "post-stop cleanup must terminate");
        assert!(
            batch.effects.is_empty(),
            "no observed effects may run after the stop, got {:?}",
            batch.effects
        );
        let outcomes = execute(&mut processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    }
    drain(&mut processor);

    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active())
    );
    let github = world.github.lock().unwrap();
    assert_eq!(github.squash_count.values().sum::<u32>(), 0);
    assert_eq!(
        github.branch_head("pr-2"),
        remote_head_before,
        "the suppressed continuation must not have pushed"
    );
}

/// An acknowledged stop must survive a crash. The stop delivery closes
/// (exactly-once intake) while its trigger waits for the saga's observation
/// boundary — if that intention lives only in RAM, a crash silently drops
/// the stop and the train keeps merging (Codex M5 round 2, P1). Stops are
/// therefore persisted in the close transaction and reloaded at startup.
#[test]
fn acknowledged_stop_survives_a_crash_before_its_boundary() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Preflight completes (the train now exists); the next batch is in
    // flight when the stop arrives, so the stop queues for the boundary.
    let preflight = pump_cascade(&mut processor).expect("start plans preflight");
    let outcomes = execute(&mut processor, &preflight);
    let _batch = processor
        .on_outcomes(preflight.root, outcomes, preflight.feedback)
        .unwrap()
        .expect("preflight completion continues the saga");
    assert!(processor.state().active_trains.contains_key(&PrNumber(1)));

    let body = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 55);
    world.enqueue(&mut processor, "issue_comment", body);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Crash before the boundary: the saga outcomes and the RAM queue die.
    drop(processor);

    // On restart the persisted stop must still retire the train, with no
    // further webhook traffic.
    let mut processor = world.processor();
    drain(&mut processor);
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active()),
        "the acknowledged stop was lost across the crash"
    );
}

/// An acknowledged START must survive a crash, exactly like a stop (Codex
/// M5 round 19, P1 — the mirror of round 2): the delivery closes (and
/// dedupes) while the start waits in RAM for the saga slot, so a crash in
/// that window lost the command and redelivery was skipped as a duplicate.
/// The window is wide — a start queued behind another train's multi-minute
/// saga sits in RAM the whole time.
#[test]
fn acknowledged_start_survives_a_crash_while_queued() {
    let (mut world, heads) = World::linear_stack(2);
    world
        .github
        .lock()
        .unwrap()
        .prs
        .get_mut(&PrNumber(2))
        .unwrap()
        .base_ref = "main".to_owned();
    let mut processor = world.processor();
    // Two independent roots.
    for i in 1..=2u64 {
        let body = pr_opened_body(
            &world.config,
            i,
            &heads[(i - 1) as usize],
            &format!("pr-{i}"),
            "main",
        );
        world.enqueue(&mut processor, "pull_request", body);
    }
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Train 1's preflight occupies the saga slot; start 2 is acked and
    // closed while it runs, waiting in the queue.
    let batch = pump_cascade(&mut processor).expect("start 1 plans preflight");
    let _outcomes = execute(&mut processor, &batch);
    let body = comment_body(&world.config, 2, "@merge-train start", AUTHOR, "author", 3);
    world.enqueue(&mut processor, "issue_comment", body);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Crash: in-flight outcomes and the RAM queue die.
    drop(processor);

    // On restart the acked start must still run, with no re-issued start.
    // (Train 1 died mid-preflight; stop it so the completion driver is not
    // held up by its inherited-refusal — that path is pinned elsewhere.)
    let mut processor = world.processor();
    let body = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 4);
    world.enqueue(&mut processor, "issue_comment", body);
    drive_to_completion(&mut world, &mut processor);
    let events = processor.store_mut().events().unwrap();
    assert!(
        events.iter().any(|e| matches!(
            e.payload,
            crate::persistence::event::StateEventPayload::TrainStarted {
                root_pr: PrNumber(2),
                ..
            }
        )),
        "the acknowledged start was lost across the crash"
    );
}

/// Command order must survive a restart: a reloaded command was uttered
/// before every command the backlog can still deliver, so it must APPLY
/// before them too. Deferring reloaded commands behind the backlog drain
/// (round 19's first shape) inverted this: a post-restart `stop` — a fresh
/// backlog delivery — was answered "no active merge train" first, and the
/// older reloaded start then started (and merged!) the train the user had
/// just refused.
#[test]
fn post_restart_stop_cancels_a_reloaded_start() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    // The start is acked and durable but never pumped: crash.
    drop(processor);

    // Restart. The user (having watched the start do nothing) refuses it
    // before the reloaded command gets to run.
    let mut processor = world.processor();
    let body = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 63);
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);

    let events = processor.store_mut().events().unwrap();
    assert!(
        !events.iter().any(|e| matches!(
            e.payload,
            crate::persistence::event::StateEventPayload::TrainStarted {
                root_pr: PrNumber(1),
                ..
            }
        )),
        "the reloaded start outran the fresh stop and started the train"
    );
    let github = world.github.lock().unwrap();
    assert!(
        github
            .posted_comments
            .iter()
            .any(|(pr, text)| *pr == PrNumber(1) && text.contains("start cancelled")),
        "expected a start-cancelled answer"
    );
    assert!(
        !github
            .posted_comments
            .iter()
            .any(|(pr, text)| *pr == PrNumber(1) && text.contains("No active merge train")),
        "the stop must consume the pending start, not deny a train exists"
    );
    drop(github);
    assert!(
        processor.store_mut().pending_commands().unwrap().is_empty(),
        "both commands must be answered"
    );
}

/// The evaluate half of trigger-work recovery: an active train whose pending
/// evaluation died with the process (e.g. an acknowledged CI success whose
/// trigger was queued but not yet run) must be re-evaluated at startup, not
/// wait for unrelated webhook traffic (Codex M5 round 2, P1). Evaluation is
/// derived state — `Evaluate { facts }` re-reads everything — so startup
/// simply queues one for every active train.
#[test]
fn active_train_is_evaluated_at_startup_without_new_traffic() {
    let (mut world, heads) = World::linear_stack(1);
    {
        let mut processor = world.processor();
        world.enqueue_stack_setup(&mut processor, 1, &heads);
        drain(&mut processor);
        // Manufacture the crash artifact: an active, phase-Idle train whose
        // evaluation trigger no longer exists anywhere.
        processor
            .store_mut()
            .append_batch(
                &[crate::persistence::event::StateEventPayload::TrainStarted {
                    root_pr: PrNumber(1),
                    current_pr: PrNumber(1),
                }],
                chrono::Utc::now(),
            )
            .unwrap();
    } // crash

    let mut processor = world.processor();
    drain(&mut processor);

    assert!(
        processor.state().prs[&PrNumber(1)].state.is_merged(),
        "the startup evaluation must advance the train; state: {:?}",
        processor.state().active_trains
    );
}

/// The worker loop blocks on its mailbox when a turn does no work — but a
/// turn whose `claim` came up empty may have just queued the owed startup
/// evaluations, and with no saga in flight nothing else will ever pump them
/// (Codex M5 round 7, P1: an active train on a traffic-less repo was never
/// recovered). `has_queued_work` is the loop's don't-block signal.
#[test]
fn startup_evaluation_is_not_stranded_without_traffic() {
    let (mut world, heads) = World::linear_stack(1);
    {
        let mut processor = world.processor();
        world.enqueue_stack_setup(&mut processor, 1, &heads);
        drain(&mut processor);
        processor
            .store_mut()
            .append_batch(
                &[crate::persistence::event::StateEventPayload::TrainStarted {
                    root_pr: PrNumber(1),
                    current_pr: PrNumber(1),
                }],
                chrono::Utc::now(),
            )
            .unwrap();
    } // crash; no deliveries pending

    // The worker loop's exact turn: pump (nothing queued yet), claim
    // (empty backlog — queues the owed evaluations)...
    let mut processor = world.processor();
    assert!(processor.pump().unwrap().is_none());
    assert!(processor.claim().unwrap().is_none());
    // ...and now it must NOT block: the owed work must be visible.
    assert!(
        !processor.saga_in_flight() && processor.has_queued_work(),
        "the loop would block forever despite owing a startup evaluation"
    );
    assert!(
        processor.pump().unwrap().is_some(),
        "the next pump must start the evaluation"
    );
}

/// Startup evaluations must not overtake the durable backlog: a pending
/// (already-acked) delivery may carry a train-terminating fact — here a
/// review dismissal — and evaluating first would plan (and possibly execute
/// irreversible) effects against state that predates it (Codex M5 round 6,
/// P1). The evaluations queue only once the backlog first drains.
#[test]
fn startup_evaluations_do_not_overtake_the_acked_backlog() {
    let (mut world, heads) = World::linear_stack(1);
    {
        let mut processor = world.processor();
        world.enqueue_stack_setup(&mut processor, 1, &heads);
        drain(&mut processor);
        // Crash artifact: an active train plus an acked-but-unprocessed
        // delivery that dismisses its review (the handler aborts on it).
        processor
            .store_mut()
            .append_batch(
                &[crate::persistence::event::StateEventPayload::TrainStarted {
                    root_pr: PrNumber(1),
                    current_pr: PrNumber(1),
                }],
                chrono::Utc::now(),
            )
            .unwrap();
        let body = format!(
            r#"{{
                "action": "dismissed",
                "review": {{
                    "id": 778,
                    "user": {{ "id": 555, "login": "reviewer" }},
                    "state": "dismissed",
                    "body": null
                }},
                "pull_request": {{ "number": 1 }},
                "repository": {repo}
            }}"#,
            repo = repo_json(&world.config),
        );
        world.enqueue(&mut processor, "pull_request_review", body.into_bytes());
    } // crash

    let mut processor = world.processor();
    // Mirror the worker loop's ordering: sagas pump BEFORE backlog claims.
    assert!(
        processor.pump().unwrap().is_none(),
        "no saga may start before the acked backlog has been applied"
    );
    drain(&mut processor);

    assert_eq!(
        world
            .github
            .lock()
            .unwrap()
            .squash_count
            .values()
            .sum::<u32>(),
        0,
        "the acked dismissal must retire the train before any evaluation acts"
    );
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active())
    );
}

// ─── Clone-on-first-use ───

/// The clone must never see the GitHub token: the origin URL stays clean and
/// auth comes from a credential helper that reads `GITHUB_TOKEN` from the
/// process environment at each fetch/push — otherwise any failed git command
/// prints the secret into logs and `ps` output (Codex M5 review, P1).
#[test]
fn ensure_clone_uses_an_env_credential_helper_and_a_credential_free_origin() {
    let dir = TempDir::new().unwrap();
    // A local bare repo stands in for GitHub's remote.
    let remote = dir.path().join("remote.git");
    run_git_stdout(dir.path(), &["init", "--bare", remote.to_str().unwrap()]).unwrap();

    let config = crate::worker::test_support::test_git_config(dir.path());
    super::executor::ensure_clone(&config, Some(remote.to_str().unwrap())).unwrap();

    let clone_dir = config.clone_dir();
    let helper = run_git_stdout(&clone_dir, &["config", "credential.helper"]).unwrap();
    assert!(
        helper.contains("${GITHUB_TOKEN}"),
        "the helper must defer to the environment, got: {helper}"
    );
    assert!(
        !helper.contains("ghp_") && !helper.contains("x-access-token:"),
        "no literal secret material in the helper: {helper}"
    );
    let origin = run_git_stdout(&clone_dir, &["remote", "get-url", "origin"]).unwrap();
    assert!(
        !origin.contains('@'),
        "origin URL must carry no credentials, got: {origin}"
    );

    // A PRE-EXISTING clone from before the auth change (credential-bearing
    // origin, no helper) is normalized on the next ensure_clone — otherwise
    // the auth change silently misses exactly the repos that predate it
    // (Codex M5 round 17).
    run_git_stdout(&clone_dir, &["config", "--unset", "credential.helper"]).unwrap();
    run_git_stdout(
        &clone_dir,
        &[
            "remote",
            "set-url",
            "origin",
            "https://x-access-token:ghp_old@github.com/o/r.git",
        ],
    )
    .unwrap();
    super::executor::ensure_clone(&config, Some(remote.to_str().unwrap())).unwrap();
    let helper = run_git_stdout(&clone_dir, &["config", "credential.helper"]).unwrap();
    assert!(
        helper.contains("${GITHUB_TOKEN}"),
        "an existing clone must get the helper installed, got: {helper}"
    );
    let origin = run_git_stdout(&clone_dir, &["remote", "get-url", "origin"]).unwrap();
    assert!(
        !origin.contains('@'),
        "an existing clone's credentialed origin must be replaced, got: {origin}"
    );
}

/// A stop for a train whose `start` is still *queued* (waiting for the saga
/// slot behind another train) must suppress that queued start — otherwise
/// the stop answers "no active train" and the start then runs anyway
/// (Codex M5 round 6; the same race as stop-during-preflight, one step
/// earlier in the queue).
#[test]
fn stop_cancels_a_queued_not_yet_started_start() {
    let (mut world, heads) = World::linear_stack(2);
    world
        .github
        .lock()
        .unwrap()
        .prs
        .get_mut(&PrNumber(2))
        .unwrap()
        .base_ref = "main".to_owned();
    let mut processor = world.processor();
    // Announce both PRs as *independent roots*: base `main`, no
    // predecessors — so `start 2` is a legitimate queued start.
    for i in 1..=2u64 {
        let body = pr_opened_body(
            &world.config,
            i,
            &heads[(i - 1) as usize],
            &format!("pr-{i}"),
            "main",
        );
        world.enqueue(&mut processor, "pull_request", body);
    }
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Train 1's preflight saga occupies the slot.
    let batch = pump_cascade(&mut processor).expect("start 1 plans preflight");
    let outcomes = execute(&mut processor, &batch);

    // While it runs: start 2 (queues behind the slot), then stop 2.
    start_command(&mut world, &mut processor, 2);
    let body = comment_body(&world.config, 2, "@merge-train stop", AUTHOR, "author", 71);
    world.enqueue(&mut processor, "issue_comment", body);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Feed train 1's outcomes; run everything to quiescence.
    let mut next = processor
        .on_outcomes(batch.root, outcomes, batch.feedback)
        .unwrap();
    while let Some(batch) = next {
        let outcomes = execute(&mut processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    }
    drain(&mut processor);

    // Train 2 must never have started.
    let events = processor.store_mut().events().unwrap();
    assert!(
        !events.iter().any(|e| matches!(
            e.payload,
            crate::persistence::event::StateEventPayload::TrainStarted {
                root_pr: PrNumber(2),
                ..
            }
        )),
        "the stopped queued start must never run"
    );
    assert!(
        world
            .github
            .lock()
            .unwrap()
            .posted_comments
            .iter()
            .any(|(pr, text)| *pr == PrNumber(2) && text.contains("start cancelled")),
        "expected a start-cancelled answer"
    );
}

/// A handler abort committed while ANOTHER root's saga holds the slot
/// must not lose its cleanup to a start already queued for the aborted
/// root: consumed first, the start replaces the aborted record, and a
/// recompute queued behind it finds nothing — no worktree removal, no
/// abort notice. The cleanup is captured when the abort commits and
/// queued ahead of the waiting start (Codex terminal-sync review round
/// 14, P2 — the `process_claimed` face of the probe-boundary finding).
#[test]
fn a_handler_abort_behind_a_queued_start_still_notifies() {
    let (mut world, heads) = World::linear_stack(2);
    {
        let mut github = world.github.lock().unwrap();
        github.prs.get_mut(&PrNumber(2)).unwrap().base_ref = "main".to_owned();
        github.blocked.insert(PrNumber(2));
    }
    let mut processor = world.processor();
    // Announce both PRs as *independent roots*: base `main`, no
    // predecessors.
    for i in 1..=2u64 {
        let body = pr_opened_body(
            &world.config,
            i,
            &heads[(i - 1) as usize],
            &format!("pr-{i}"),
            "main",
        );
        world.enqueue(&mut processor, "pull_request", body);
    }
    // Train 2 runs first and parks (its PR is not mergeable): an active
    // train that a dismissal can abort.
    start_command(&mut world, &mut processor, 2);
    drain(&mut processor);
    assert!(
        processor.state().active_trains[&PrNumber(2)]
            .state
            .is_active(),
        "precondition: train 2 is running"
    );

    // Train 1's preflight occupies the saga slot...
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let batch = processor.pump().unwrap().expect("start 1 plans preflight");
    let outcomes = execute(&mut processor, &batch);

    // ... while a restart of 2 queues behind it, and a review dismissal
    // then aborts train 2 — committed with its delivery, since root 2's
    // saga is NOT the one in flight.
    let restart = comment_body(&world.config, 2, "@merge-train start", AUTHOR, "author", 76);
    world.enqueue(&mut processor, "issue_comment", restart);
    let dismissal = format!(
        r#"{{
            "action": "dismissed",
            "review": {{
                "id": 778,
                "user": {{ "id": 555, "login": "reviewer" }},
                "state": "dismissed",
                "body": null
            }},
            "pull_request": {{ "number": 2 }},
            "repository": {repo}
        }}"#,
        repo = repo_json(&world.config),
    );
    world.enqueue(
        &mut processor,
        "pull_request_review",
        dismissal.into_bytes(),
    );
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Train 1's outcomes come back; everything runs to rest.
    let next = processor
        .on_outcomes(batch.root, outcomes, batch.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    drain(&mut processor);

    assert!(
        world
            .github
            .lock()
            .unwrap()
            .posted_comments
            .iter()
            .any(|(pr, text)| *pr == PrNumber(2) && text.contains("Merge train aborted")),
        "the aborted train's notice must not be lost to the queued restart"
    );
    assert!(
        processor.state().active_trains[&PrNumber(2)]
            .state
            .is_active(),
        "the queued start still runs, as a fresh incarnation"
    );
}

/// Command sequences must respect utterance order: `start → stop → start`
/// issued while another saga holds the slot means the stop cancels the
/// *first* start only, and the final start runs. Two former bugs collapsed
/// this (Codex M5 round 8): `queue()` deduped the second identical start
/// away, and stop-cancellation removed queued starts regardless of whether
/// they were queued before or after the stop.
#[test]
fn start_stop_start_sequence_runs_the_final_start() {
    let (mut world, heads) = World::linear_stack(2);
    world
        .github
        .lock()
        .unwrap()
        .prs
        .get_mut(&PrNumber(2))
        .unwrap()
        .base_ref = "main".to_owned();
    let mut processor = world.processor();
    for i in 1..=2u64 {
        let body = pr_opened_body(
            &world.config,
            i,
            &heads[(i - 1) as usize],
            &format!("pr-{i}"),
            "main",
        );
        world.enqueue(&mut processor, "pull_request", body);
    }
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Train 1's preflight saga occupies the slot.
    let batch = pump_cascade(&mut processor).expect("start 1 plans preflight");
    let outcomes = execute(&mut processor, &batch);

    // While it runs: start 2, stop 2, start 2 again. (Distinct single-digit
    // comment ids: comment_body takes them mod 10, and a collision would
    // dedupe the later comment away.)
    for (text, id) in [
        ("@merge-train start", 4),
        ("@merge-train stop", 5),
        ("@merge-train start", 6),
    ] {
        let body = comment_body(&world.config, 2, text, AUTHOR, "author", id);
        world.enqueue(&mut processor, "issue_comment", body);
    }
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }

    // Feed train 1's outcomes and run everything to quiescence.
    let mut next = processor
        .on_outcomes(batch.root, outcomes, batch.feedback)
        .unwrap();
    while let Some(batch) = next {
        let outcomes = execute(&mut processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
    }
    drive_to_completion(&mut world, &mut processor);

    // The final start ran: PR 2 merged.
    assert!(
        processor.state().prs[&PrNumber(2)].state.is_merged(),
        "the user's final start was lost; trains: {:?}",
        processor.state().active_trains
    );
}

/// Saga outcomes always PARK at arrival: the observation boundary may only
/// run once the acked backlog has been applied (an unprocessed delivery may
/// be a stop or topology change — rounds 7/8), and the *main loop* does that
/// draining one delivery per turn so intake acks stay prompt however deep
/// the backlog is (round 10). The loop resumes the boundary when its claim
/// finds the backlog empty; the saga slot stays occupied until then.
#[test]
fn saga_outcomes_park_until_the_backlog_drains() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let batch = pump_cascade(&mut processor).expect("start plans preflight");
    let outcomes = execute(&mut processor, &batch);

    // An acked delivery (a stranger's stop, which will need GitHub when
    // processed) is still waiting when the outcomes arrive.
    let body = comment_body(
        &world.config,
        1,
        "@merge-train stop",
        STRANGER,
        "stranger",
        7,
    );
    world.enqueue(&mut processor, "issue_comment", body);

    let mut parked = None;
    let next = super::handle_msg(
        &mut processor,
        WorkerMsg::SagaOutcomes {
            root: batch.root,
            outcomes,
            best_effort: Vec::new(),
            feedback: batch.feedback,
        },
        &mut parked,
    )
    .unwrap();

    assert!(
        next.is_none(),
        "no batch may dispatch before the backlog is applied"
    );
    assert!(parked.is_some(), "the boundary must wait for the backlog");
    assert!(
        processor.saga_in_flight(),
        "the saga slot stays occupied while the boundary is parked"
    );
}

/// A command whose own PR is *permanently* unfetchable (bad token access,
/// deleted PR) must be denied with an explanation — proceeding uncached
/// turned the acknowledged command into a silently-logged engine error, and
/// with the delivery closed it would never retry (Codex M5 round 12).
#[test]
fn command_on_an_unfetchable_pr_is_denied_not_dropped() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);

    // PR 99 exists on neither the fake nor the store: the fake answers the
    // fetch with a permanent 404.
    let body = comment_body(&world.config, 99, "@merge-train start", AUTHOR, "author", 9);
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);

    assert!(processor.state().active_trains.is_empty());
    let github = world.github.lock().unwrap();
    assert!(
        github
            .posted_comments
            .iter()
            .any(|(pr, text)| *pr == PrNumber(99) && text.contains("cannot fetch")),
        "expected an explanatory denial, got {:?}",
        github.posted_comments
    );
}

/// The fan-out stop expansion is durable BEFORE the fan-out integrates
/// (Codex M5 round 15): a crash between them must reload stop rows naming
/// the spawned roots, whose trains then stop at startup with no further
/// traffic. This drives the post-crash state directly: fan-out applied,
/// expanded rows persisted, expanded stops never applied.
#[test]
fn expanded_fanout_stops_survive_a_crash() {
    let (mut world, heads) = World::linear_stack(1);
    {
        let mut processor = world.processor();
        world.enqueue_stack_setup(&mut processor, 1, &heads);
        drain(&mut processor);
        // The crash artifact: the fan-out landed, the expansion's rows are
        // durable, the stops were never applied.
        processor
            .store_mut()
            .append_batch(
                &[
                    crate::persistence::event::StateEventPayload::TrainStarted {
                        root_pr: PrNumber(1),
                        current_pr: PrNumber(1),
                    },
                    crate::persistence::event::StateEventPayload::FanOutCompleted {
                        old_root: PrNumber(1),
                        new_roots: vec![PrNumber(2), PrNumber(3)],
                        original_root_pr: PrNumber(1),
                    },
                ],
                chrono::Utc::now(),
            )
            .unwrap();
        processor
            .store_mut()
            .replace_pending_stop(0, &[(PrNumber(2), false), (PrNumber(3), false)])
            .unwrap();
    } // crash

    let mut processor = world.processor();
    drain(&mut processor);

    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active()),
        "the persisted expanded stops must retire the spawned trains; {:?}",
        processor.state().active_trains
    );
    assert!(
        processor.store_mut().pending_commands().unwrap().is_empty(),
        "applied stops must consume their rows"
    );
}

/// A stop for the original root racing the *fan-out boundary* must retire
/// the trains the fan-out spawns: integrated first, `FanOutCompleted`
/// removes the old root, the stop resolves to "no active train", and the
/// continuations the user refused run anyway (Codex M5 round 13). The sweep
/// injects the stop at every boundary up to and including the fan-out.
#[test]
fn stop_on_the_root_retires_fanned_out_trains_at_every_boundary() {
    // A fan-shaped stack: pr-2 and pr-3 both stack on pr-1, so completing
    // pr-1 fans out into two new roots.
    fn fan_world() -> (World, Processor) {
        let (mut world, heads) = World::linear_stack(1);
        let mut heads = heads;
        for i in [2u64, 3] {
            let head = create_branch_with_file(
                &world.config,
                &format!("pr-{i}"),
                &format!("pr-{i}.txt"),
                &format!("content {i}"),
                "pr-1",
            );
            create_pr_ref(&world.config, i, &head);
            world.github.lock().unwrap().prs.insert(
                PrNumber(i),
                FakePr {
                    author_id: AUTHOR,
                    branch: format!("pr-{i}"),
                    base_ref: "pr-1".to_owned(),
                    state: FakePrState::Open,
                },
            );
            heads.push(head);
        }
        let mut processor = world.processor();
        for i in 1..=3u64 {
            let base = if i == 1 { "main" } else { "pr-1" };
            let body = pr_opened_body(
                &world.config,
                i,
                &heads[(i - 1) as usize],
                &format!("pr-{i}"),
                base,
            );
            world.enqueue(&mut processor, "pull_request", body);
        }
        for i in [2u64, 3] {
            let body = comment_body(
                &world.config,
                i,
                "@merge-train predecessor #1",
                AUTHOR,
                "author",
                i,
            );
            world.enqueue(&mut processor, "issue_comment", body);
        }
        start_command(&mut world, &mut processor, 1);
        (world, processor)
    }

    /// Drives to quiescence, injecting a stop for pr-1 while the batch at
    /// `stop_at` is in flight. Returns the first boundary index at which
    /// `FanOutCompleted` had been integrated (`None` if never).
    fn run(world: &mut World, processor: &mut Processor, stop_at: usize) -> Option<usize> {
        while let Some(delivery) = processor.claim().unwrap() {
            processor.process_claimed(delivery).unwrap();
        }
        let mut fan_boundary = None;
        let mut boundary = 0;
        let mut next = processor.pump().unwrap();
        while let Some(batch) = next {
            assert!(boundary < 200, "did not quiesce");
            let outcomes = execute(processor, &batch);
            if boundary == stop_at {
                let body = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 9);
                world.enqueue(processor, "issue_comment", body);
                while let Some(delivery) = processor.claim().unwrap() {
                    processor.process_claimed(delivery).unwrap();
                }
            }
            next = processor
                .on_outcomes(batch.root, outcomes, batch.feedback)
                .unwrap();
            if fan_boundary.is_none() {
                let events = processor.store_mut().events().unwrap();
                if events.iter().any(|e| {
                    matches!(
                        e.payload,
                        crate::persistence::event::StateEventPayload::FanOutCompleted { .. }
                    )
                }) {
                    fan_boundary = Some(boundary);
                }
            }
            boundary += 1;
        }
        fan_boundary
    }

    // Discover the fan-out boundary with no stop at all.
    let (mut world, mut processor) = fan_world();
    let fan_boundary = run(&mut world, &mut processor, usize::MAX)
        .expect("the fan stack must fan out when undisturbed");

    for stop_at in 0..=fan_boundary {
        let (mut world, mut processor) = fan_world();
        run(&mut world, &mut processor, stop_at);
        drain(&mut processor);

        assert!(
            processor
                .state()
                .active_trains
                .values()
                .all(|t| !t.state.is_active()),
            "active trains survived a stop at boundary {stop_at}"
        );
        let github = world.github.lock().unwrap();
        for pr in [2u64, 3] {
            assert_eq!(
                github.squash_count.get(&PrNumber(pr)).copied().unwrap_or(0),
                0,
                "PR #{pr} squashed despite the stop at boundary {stop_at}"
            );
        }
    }
}

// ─── Referenced-PR precache ───

#[test]
fn predecessor_command_precaches_the_unknown_target() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();

    // Only PR #2 is announced by webhook; #1 exists solely on GitHub's side.
    let body = pr_opened_body(&world.config, 2, &heads[1], "pr-2", "pr-1");
    world.enqueue(&mut processor, "pull_request", body);
    let body = comment_body(
        &world.config,
        2,
        "@merge-train predecessor #1",
        AUTHOR,
        "author",
        30,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);

    // The unknown target was fetched, cached, and the declaration validated.
    assert!(processor.state().prs.contains_key(&PrNumber(1)));
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1))
    );
}

// ─── Crash-point sweep over the delivery pipeline ───

/// Replays the same delivery workload with a crash (drop + reopen of the
/// `Store`) injected at every claim/process boundary; the final materialized
/// state must equal the no-crash run and no delivery may be lost.
#[test]
fn crash_at_every_pipeline_boundary_loses_nothing() {
    fn seed_and_enqueue(world: &mut World, heads: &[Sha]) -> Processor {
        let mut processor = world.processor();
        world.enqueue_stack_setup(&mut processor, 2, heads);
        let body = check_suite_green_body(&world.config, &heads[1], &[2], 700);
        world.enqueue(&mut processor, "check_suite", body);
        processor
    }

    fn run(world: &World, mut processor: Processor, crash_at: Option<usize>) -> RepoState {
        let mut boundary = 0;
        loop {
            let crash = |p: Processor| -> Processor {
                drop(p);
                world.processor()
            };
            if Some(boundary) == crash_at {
                processor = crash(processor);
            }
            boundary += 1;
            let Some(delivery) = processor.claim().unwrap() else {
                break;
            };
            if Some(boundary) == crash_at {
                // Crash with the delivery claimed but unprocessed: reopen
                // requeues it.
                processor = crash(processor);
                boundary += 1;
                continue;
            }
            boundary += 1;
            assert_eq!(
                processor.process_claimed(delivery).unwrap(),
                PipelineOutcome::Processed
            );
        }
        assert!(processor.claim().unwrap().is_none(), "no delivery stranded");
        processor.state().clone()
    }

    // The no-crash baseline. (Separate worlds: each has its own repo/state.)
    let (mut world, heads) = World::linear_stack(2);
    let processor = seed_and_enqueue(&mut world, &heads);
    let baseline = run(&world, processor, None);

    // ~2 boundaries per delivery; sweep generously past the end.
    for crash_at in 0..12 {
        let (mut world, heads) = World::linear_stack(2);
        let processor = seed_and_enqueue(&mut world, &heads);
        let state = run(&world, processor, Some(crash_at));
        // Heads differ across worlds (fresh repos), so compare shape:
        // same PRs cached, same predecessor edges, same train set.
        assert_eq!(
            state.prs.keys().collect::<std::collections::BTreeSet<_>>(),
            baseline
                .prs
                .keys()
                .collect::<std::collections::BTreeSet<_>>(),
            "crash at boundary {crash_at} lost a cached PR"
        );
        assert_eq!(
            state.prs[&PrNumber(2)].predecessor,
            baseline.prs[&PrNumber(2)].predecessor,
            "crash at boundary {crash_at} lost the predecessor declaration"
        );
        assert_eq!(state.default_branch, baseline.default_branch);
    }
}

// ─── M6: inherited mid-flight trains recover and resume ───

/// Advances the world until exactly `depth` saga batches have *executed*
/// (their effects hit reality), nudging CI whenever the train parks, then
/// crashes WITHOUT observing the final batch's outcomes — reality is ahead
/// of the store by up to one batch, the widest recovery window. Returns
/// early if the train completes in fewer batches.
fn run_batches_then_crash(world: &mut World, processor: Processor, depth: usize) {
    let mut processor = processor;
    let mut executed = 0;
    'outer: while executed < depth {
        while let Some(delivery) = processor.claim().unwrap() {
            processor.process_claimed(delivery).unwrap();
        }
        match processor.pump().unwrap() {
            Some(first) => {
                let mut batch = first;
                loop {
                    // Stack-ledger bookkeeping runs but does not COUNT: a
                    // depth names a step of the cascade, and the comments
                    // that pick depths describe those steps.
                    let counts = !is_ledger_bookkeeping(&mut processor, &batch);
                    let outcomes = execute(&mut processor, &batch);
                    if counts {
                        executed += 1;
                    }
                    if executed >= depth {
                        break 'outer; // crash: outcomes never observed
                    }
                    match processor
                        .on_outcomes(batch.root, outcomes, batch.feedback)
                        .unwrap()
                    {
                        Some(next) => batch = next,
                        None => break,
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
    drop(processor); // the crash
}

/// The M6 recovery invariants, asserted at quiescence after a
/// crash-restart-recover run of a 2-PR train.
fn assert_recovered_exactly_once(world: &World, processor: &mut Processor, context: &str) {
    let github = world.github.lock().unwrap();
    for (pr, count) in &github.squash_count {
        assert!(*count <= 1, "{context}: PR #{pr} squashed {count} times");
    }
    for i in 1..=2u64 {
        let real_merged = matches!(
            github.prs.get(&PrNumber(i)).map(|f| &f.state),
            Some(FakePrState::Merged { .. })
        );
        assert!(real_merged, "{context}: PR #{i} not merged on GitHub");
    }
    drop(github);
    for i in 1..=2u64 {
        assert!(
            processor.state().prs[&PrNumber(i)].state.is_merged(),
            "{context}: store does not believe PR #{i} merged"
        );
    }
    let events = processor.store_mut().events().unwrap();
    let completed: Vec<PrNumber> = events
        .iter()
        .filter_map(|e| match e.payload {
            crate::persistence::event::StateEventPayload::TrainCompleted { root_pr } => {
                Some(root_pr)
            }
            _ => None,
        })
        .collect();
    assert!(!completed.is_empty(), "{context}: no train completed");
    for root in completed {
        let facts = ReplayFacts::for_train(&events, root);
        assert_eq!(
            facts.unmatched().count(),
            0,
            "{context}: completed train #{root} has unmatched intents"
        );
    }
}

/// The M6 headline: a train inherited mid-cascade from a dead process
/// RESUMES — worktree cleaned, state recovered via the evaluate path — and
/// completes with every PR squashed exactly once. (Before M6 the worker
/// refused these trains and demanded a stop-and-restart.)
#[test]
fn inherited_mid_flight_train_recovers_and_completes() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    run_batches_then_crash(&mut world, processor, 4);

    // A fresh process inherits the mid-flight train and must resume it —
    // no stop, no re-issued start; drive_to_completion supplies only CI.
    let mut processor = world.processor();
    drive_to_completion(&mut world, &mut processor);
    assert_recovered_exactly_once(&world, &mut processor, "depth 4");
}

/// The class oracle: the same recovery must hold with the crash at EVERY
/// saga depth — each depth leaves a different phase mid-flight, and the
/// final batch's effects have always run unobserved (reality ahead of the
/// store by one batch).
#[test]
fn crash_at_every_saga_depth_recovers_to_exactly_once_completion() {
    for depth in 1..=12usize {
        let (mut world, heads) = World::linear_stack(2);
        let mut processor = world.processor();
        world.enqueue_stack_setup(&mut processor, 2, &heads);
        start_command(&mut world, &mut processor, 1);
        run_batches_then_crash(&mut world, processor, depth);

        let mut processor = world.processor();
        drive_to_completion(&mut world, &mut processor);
        assert_recovered_exactly_once(&world, &mut processor, &format!("depth {depth}"));
    }
}

/// `stop` on an inherited mid-flight train still works — recovery must not
/// have cost the emergency brake.
#[test]
fn inherited_mid_flight_train_still_stops() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    run_batches_then_crash(&mut world, processor, 2);

    let mut processor = world.processor();
    let body = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 44);
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active()),
        "the stop must retire the inherited train"
    );
}

/// GitHub down at recovery time: the train stays active and UNADVANCED
/// (correctness over availability — resuming unverified risks the exact
/// double-squash supplementary recovery exists to prevent), the worker asks
/// for the stall-retry timer, and the timer's re-queue recovers the train
/// once GitHub returns.
#[test]
fn recovery_parks_and_retries_when_github_is_unavailable() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    run_batches_then_crash(&mut world, processor, 4);

    world.github.lock().unwrap().unavailable = true;
    let mut processor = world.processor();
    drain(&mut processor);
    assert!(
        processor.take_retry_request(),
        "a parked recovery must arm the stall-retry timer"
    );
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .any(|t| t.state.is_active()),
        "the unrecovered train must stay active"
    );
    assert!(
        !processor.has_queued_work(),
        "the parked evaluation must not spin the worker loop"
    );

    // The outage lifts and the timer fires (`WorkerMsg::RetryStalled`).
    world.github.lock().unwrap().unavailable = false;
    processor.requeue_marked_recoveries().unwrap();
    drive_to_completion(&mut world, &mut processor);
    assert_recovered_exactly_once(&world, &mut processor, "outage-then-retry");
}

/// A timer-driven recovery retry must not overtake the acked backlog: the
/// worker loop pumps before it claims, and the backlog may hold a released
/// delivery carrying exactly the command recovery must not outrun — here a
/// maintainer's stop whose role lookup released during the outage. The
/// retry therefore routes through the same backlog-drain gate as the
/// startup evaluations (Codex M6 review, P1; the round-6 rule again).
#[test]
fn recovery_retry_does_not_overtake_the_acked_backlog() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    run_batches_then_crash(&mut world, processor, 4);

    // Restart mid-outage. A maintainer (not the author: the role lookup is
    // what needs GitHub) says stop; the delivery releases.
    {
        let mut github = world.github.lock().unwrap();
        github.roles.insert(
            "maintainer".to_owned(),
            crate::effects::github::CollaboratorRole::Maintain,
        );
        github.unavailable = true;
    }
    let mut processor = world.processor();
    let body = comment_body(&world.config, 1, "@merge-train stop", 999, "maintainer", 66);
    world.enqueue(&mut processor, "issue_comment", body);
    assert!(processor.pump().unwrap().is_none(), "nothing owed yet");
    let delivery = processor.claim().unwrap().expect("the stop is queued");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released
    );

    // GitHub returns; the stall-retry timer fires.
    world.github.lock().unwrap().unavailable = false;
    processor.requeue_marked_recoveries().unwrap();

    // The worker loop pumps BEFORE it re-claims the released delivery: the
    // retried recovery must not produce work ahead of the acked stop.
    assert!(
        processor.pump().unwrap().is_none(),
        "recovery must not overtake the acked backlog"
    );

    drain(&mut processor);
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active()),
        "the acked stop must retire the train"
    );
    assert_eq!(
        world
            .github
            .lock()
            .unwrap()
            .squash_count
            .values()
            .sum::<u32>(),
        0,
        "nothing may squash after the acked stop"
    );
}

/// Restore-from-backup (DESIGN §Recovery precedence): the status comment's
/// `recovery_seq` is ahead of the local record — the ONLY way that happens
/// under SQLite is local state regressing — so the comment's record is
/// adopted wholesale, id repaired to the comment it was found in.
#[test]
fn a_status_comment_ahead_of_the_store_is_adopted() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    // Depth 4: the crash lands mid-`Preparing` (batches 1–3 are the
    // Idle-phase preflight/comment/refetch steps).
    run_batches_then_crash(&mut world, processor, 4);

    // Simulate the backup restore: doctor the status comment to carry the
    // same train at a recovery_seq the (restored) store has never reached.
    let doctored_seq = {
        let processor = world.processor();
        let local = processor
            .state()
            .active_trains
            .values()
            .find(|t| t.state.is_active())
            .expect("the mid-flight train survives the crash")
            .clone();
        drop(processor);
        let mut ahead = local.clone();
        ahead.recovery_seq += 10;
        let body =
            crate::status::format::format_status_comment(&ahead, "doctored (backup restore)")
                .unwrap();
        let mut github = world.github.lock().unwrap();
        let id = local
            .status_comment_id
            .expect("mid-flight train has a status comment");
        github.comments.get_mut(&id).expect("comment exists").body = body;
        ahead.recovery_seq
    };

    let mut processor = world.processor();
    drive_to_completion(&mut world, &mut processor);
    let events = processor.store_mut().events().unwrap();
    let adopted_seq = events.iter().find_map(|e| match &e.payload {
        crate::persistence::event::StateEventPayload::TrainRecordAdopted { record, .. } => {
            Some(record.recovery_seq)
        }
        _ => None,
    });
    assert_eq!(
        adopted_seq,
        Some(doctored_seq),
        "the ahead comment record must be adopted"
    );
    assert_recovered_exactly_once(&world, &mut processor, "comment-ahead");
}

/// A crash between an append and its best-effort status update — the
/// COMMON crash shape, since events commit before effects run — leaves the
/// live comment behind the store. Recovery must refresh the backup BEFORE
/// the train resumes: the comment is the only recovery source if the DB is
/// lost in the resume window, and a stale `recovery_seq` there cannot
/// prevent replaying work already performed (Codex M6 review round 3, P2).
#[test]
fn recovery_refreshes_a_stale_status_comment_before_resuming() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    // Execute through batch 3 (preflight → status post → refetch), then let
    // boundary 3 plan the Preparing phase: its PhaseTransition (seq bump)
    // is appended, but the batch — carrying the status update — never runs.
    let b1 = pump_cascade(&mut processor).expect("preflight");
    let o1 = execute(&mut processor, &b1);
    let b2 = processor
        .on_outcomes(b1.root, o1, b1.feedback)
        .unwrap()
        .expect("status post");
    let o2 = execute(&mut processor, &b2);
    let b3 = processor
        .on_outcomes(b2.root, o2, b2.feedback)
        .unwrap()
        .expect("refetch");
    let o3 = execute(&mut processor, &b3);
    let _unexecuted = processor
        .on_outcomes(b3.root, o3, b3.feedback)
        .unwrap()
        .expect("preparing batch");
    drop(processor); // crash

    let mut processor = world.processor();
    let local = processor.state().active_trains[&PrNumber(1)].clone();
    let comment_id = local.status_comment_id.expect("comment recorded");
    let embedded = |world: &World| {
        let github = world.github.lock().unwrap();
        crate::status::parse::parse_status_comment(&github.comments[&comment_id].body)
            .expect("status comment parses")
            .recovery_seq
    };
    assert!(
        embedded(&world) < local.recovery_seq,
        "precondition: the crash left the backup behind the store"
    );

    // Loop-faithfully reach the recovery pump; the returned batch is NOT
    // executed — the backup must already be current by then.
    assert!(processor.pump().unwrap().is_none(), "evaluates deferred");
    assert!(processor.claim().unwrap().is_none());
    let _resumed = processor.pump().unwrap();
    assert_eq!(
        embedded(&world),
        local.recovery_seq,
        "the backup must be refreshed before any resumed effect runs"
    );
}

/// A crash after the initial status comment posts but before
/// `StatusCommentPosted` lands leaves a live comment the store has no id
/// for. Recovery must ATTACH to it, not post a duplicate — which requires
/// the comment's embedded `started_at` to equal the store record's (one
/// clock read per decision, not one per plan-then-append), and the
/// no-id-recorded case to consult the comment scan (Codex M6 review
/// round 2, P3).
#[test]
fn recovery_attaches_to_a_posted_but_unrecorded_status_comment() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    // Depth 2: batch 2 posts the status comment; the crash drops its
    // outcomes, so `StatusCommentPosted` is never appended.
    run_batches_then_crash(&mut world, processor, 2);

    let mut processor = world.processor();
    drive_to_completion(&mut world, &mut processor);
    assert_recovered_exactly_once(&world, &mut processor, "posted-but-unrecorded");
    let github = world.github.lock().unwrap();
    let status_comments = github
        .comments
        .values()
        .filter(|c| c.pr == PrNumber(1) && c.body.contains("merge-train-state"))
        .count();
    assert_eq!(
        status_comments, 1,
        "recovery must attach to the live status comment, not duplicate it"
    );
}

/// A deleted status comment must not strand recovery: the dangling id is
/// cleared and the engine re-posts its off-disk backup as it resumes.
#[test]
fn a_deleted_status_comment_is_reposted_during_recovery() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    // Depth 4: mid-`Preparing`, so recovery (not the engine's idle-phase
    // self-heal) must be the thing that re-establishes the backup.
    run_batches_then_crash(&mut world, processor, 4);

    // The user deletes the bot's status comment while the process is down.
    {
        let processor = world.processor();
        let id = processor
            .state()
            .active_trains
            .values()
            .find(|t| t.state.is_active())
            .and_then(|t| t.status_comment_id)
            .expect("mid-flight train has a status comment");
        drop(processor);
        world.github.lock().unwrap().comments.remove(&id);
    }

    let mut processor = world.processor();
    drive_to_completion(&mut world, &mut processor);
    assert_recovered_exactly_once(&world, &mut processor, "comment-deleted");
    // The off-disk backup was re-established at some point post-recovery.
    let github = world.github.lock().unwrap();
    assert!(
        github
            .comments
            .values()
            .any(|c| c.pr == PrNumber(1) && c.body.contains("merge-train-state")),
        "the status comment must be re-posted after deletion"
    );
}

/// Life continues over a compacted log: after a completed train's history
/// is summarized into a checkpoint, a NEW train — including one whose PR
/// arrived after the checkpoint — plans, runs, and completes exactly as it
/// would over the full log, and the replay oracle still holds.
#[test]
fn a_second_train_runs_over_a_compacted_log() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    drive_to_completion(&mut world, &mut processor);

    let compacted = processor
        .store_mut()
        .compact(0, chrono::Utc::now())
        .unwrap();
    assert!(
        compacted.is_some(),
        "an idle log past the threshold compacts"
    );
    assert_eq!(processor.store_mut().events().unwrap().len(), 1);
    let replayed = processor.store_mut().replay().unwrap();
    assert_eq!(
        processor.state(),
        &replayed,
        "the replay oracle survives compaction"
    );

    // A brand-new PR arrives after the checkpoint; its train must run to
    // completion over checkpoint-plus-suffix history.
    let head = create_branch_with_file(&world.config, "pr-3", "pr-3.txt", "content 3", "main");
    create_pr_ref(&world.config, 3, &head);
    world.github.lock().unwrap().prs.insert(
        PrNumber(3),
        FakePr {
            author_id: AUTHOR,
            branch: "pr-3".to_owned(),
            base_ref: "main".to_owned(),
            state: FakePrState::Open,
        },
    );
    let body = pr_opened_body(&world.config, 3, &head, "pr-3", "main");
    world.enqueue(&mut processor, "pull_request", body);
    start_command(&mut world, &mut processor, 3);
    drive_to_completion(&mut world, &mut processor);

    assert!(
        processor.state().prs[&PrNumber(3)].state.is_merged(),
        "the post-compaction train must complete"
    );
    let events = processor.store_mut().events().unwrap();
    let facts = ReplayFacts::for_train(&events, PrNumber(3));
    assert_eq!(facts.unmatched().count(), 0, "matched intent ledger");
    let replayed = processor.store_mut().replay().unwrap();
    assert_eq!(processor.state(), &replayed);
}

// ─── cache_fill_events: the unknown-PR upsert oracle ───

mod cache_fill {
    use chrono::Utc;
    use proptest::prelude::*;

    use crate::effects::PrData;
    use crate::persistence::event::StateEvent;
    use crate::persistence::snapshot::PersistedRepoSnapshot;
    use crate::state::RepoState;
    use crate::types::{MergeStateStatus, PrNumber, PrState, Sha};
    use crate::worker::pipeline::cache_fill_events;

    fn arb_pr_state() -> impl Strategy<Value = PrState> {
        prop_oneof![
            Just(PrState::Open),
            Just(PrState::Closed),
            "[0-9a-f]{40}".prop_map(|s| PrState::Merged {
                merge_commit_sha: Sha::parse(s).unwrap()
            }),
        ]
    }

    proptest! {
        /// Applying the fill events to a state that has never seen the PR
        /// caches exactly the fetched facts.
        #[test]
        fn fill_events_materialize_the_fetched_pr(
            number in 1u64..10000,
            head in "[0-9a-f]{40}",
            state in arb_pr_state(),
            is_draft in any::<bool>(),
        ) {
            let pr = PrNumber(number);
            let data = PrData {
                number: pr,
                head_sha: Sha::parse(head).unwrap(),
                head_ref: "feature".to_owned(),
                base_ref: "main".to_owned(),
                state: state.clone(),
                is_draft,
                author_id: 7,
            };
            let mut repo = RepoState::from_snapshot(PersistedRepoSnapshot::new("main"));
            for (i, payload) in cache_fill_events(pr, &data, MergeStateStatus::Clean)
                .into_iter()
                .enumerate()
            {
                repo.apply_event(&StateEvent { seq: i as u64, ts: Utc::now(), payload });
            }

            let cached = &repo.prs[&pr];
            prop_assert_eq!(&cached.head_sha, &data.head_sha);
            prop_assert_eq!(&cached.base_ref, &data.base_ref);
            // Merged/closed states survive the upsert.
            match &state {
                PrState::Open => prop_assert!(cached.state.is_open()),
                PrState::Closed => prop_assert_eq!(&cached.state, &PrState::Closed),
                PrState::Merged { .. } => prop_assert!(cached.state.is_merged()),
            }
        }
    }
}

// ─── The async intake path (registry + worker thread) ───

mod registry {
    use super::*;
    use crate::worker::test_support::fake_shared_deps;
    use crate::worker::{EnqueueOutcome, WorkerRegistry};
    use tokio::sync::oneshot;

    // A minimal valid `pull_request` payload for repo o/r. The SHAs must be
    // real 40-hex-char values or the parser rejects the payload as malformed
    // and the pipeline closes it before doing any work.
    fn pull_request_body() -> Vec<u8> {
        br#"{
            "action": "synchronize",
            "number": 7,
            "pull_request": {
                "number": 7,
                "state": "open",
                "draft": false,
                "merged": false,
                "head": { "sha": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", "ref": "feature" },
                "base": { "sha": "cafef00dcafef00dcafef00dcafef00dcafef00d", "ref": "main" },
                "user": { "id": 1, "login": "u" },
                "updated_at": "2026-07-01T10:00:00Z"
            },
            "repository": { "name": "r", "owner": { "login": "o" } }
        }"#
        .to_vec()
    }

    async fn send_delivery(
        registry: &WorkerRegistry,
        sender: &tokio::sync::mpsc::Sender<WorkerMsg>,
        id: &str,
        body: Vec<u8>,
    ) -> Result<EnqueueOutcome, crate::store::StoreError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        sender
            .send(WorkerMsg::Enqueue {
                delivery: IntakeDelivery {
                    delivery_id: id.into(),
                    event_type: "pull_request".into(),
                    headers: "{}".into(),
                    body: body.clone(),
                },
                ack: ack_tx,
                permit: registry.reserve_intake(body.len()).await,
            })
            .await
            .unwrap();
        ack_rx.await.unwrap()
    }

    #[tokio::test]
    async fn registry_routes_enqueue_and_processes() {
        let dir = tempfile::tempdir().unwrap();
        let (deps, _fake) = fake_shared_deps(dir.path(), Default::default());
        let registry = WorkerRegistry::new(dir.path(), deps);

        let sender = registry.sender_for("o", "r").await.unwrap();
        assert_eq!(
            send_delivery(&registry, &sender, "d1", pull_request_body())
                .await
                .unwrap(),
            EnqueueOutcome::Enqueued
        );
        // Redelivery of the same id is reported as a duplicate.
        assert_eq!(
            send_delivery(&registry, &sender, "d1", pull_request_body())
                .await
                .unwrap(),
            EnqueueOutcome::Duplicate
        );
    }

    #[tokio::test]
    async fn services_new_intake_with_backlog_present() {
        let dir = tempfile::tempdir().unwrap();
        let db_dir = dir.path().join("o").join("r");
        std::fs::create_dir_all(&db_dir).unwrap();
        // Pre-seed a backlog of pending deliveries, then release the lock so
        // the worker can open the DB and find them waiting.
        {
            let mut store = Store::open(&db_dir.join("state.db")).unwrap();
            for i in 0..50 {
                store
                    .enqueue(
                        &format!("backlog-{i}"),
                        "pull_request",
                        "{}",
                        &pull_request_body(),
                        chrono::Utc::now(),
                    )
                    .unwrap();
            }
        }

        let (deps, _fake) = fake_shared_deps(dir.path(), Default::default());
        let registry = WorkerRegistry::new(dir.path(), deps);
        let sender = registry.sender_for("o", "r").await.unwrap();

        // A fresh delivery is durably enqueued and acked despite the backlog.
        assert_eq!(
            send_delivery(&registry, &sender, "fresh", pull_request_body())
                .await
                .unwrap(),
            EnqueueOutcome::Enqueued
        );
    }

    #[tokio::test]
    async fn recover_existing_spawns_workers_for_queued_repos() {
        let dir = tempfile::tempdir().unwrap();
        let db_dir = dir.path().join("o").join("r");
        std::fs::create_dir_all(&db_dir).unwrap();
        {
            let mut store = Store::open(&db_dir.join("state.db")).unwrap();
            store
                .enqueue(
                    "d1",
                    "pull_request",
                    "{}",
                    &pull_request_body(),
                    chrono::Utc::now(),
                )
                .unwrap();
        }

        let (deps, _fake) = fake_shared_deps(dir.path(), Default::default());
        let registry = WorkerRegistry::new(dir.path(), deps);
        registry.recover_existing().await;

        // That worker now owns the repo's Store: re-enqueuing the same id is
        // seen as a duplicate (the recovered worker opened the existing DB).
        let sender = registry.sender_for("o", "r").await.unwrap();
        assert_eq!(
            send_delivery(&registry, &sender, "d1", pull_request_body())
                .await
                .unwrap(),
            EnqueueOutcome::Duplicate
        );
    }

    #[tokio::test]
    async fn recover_existing_is_a_noop_without_state_dir() {
        let dir = tempfile::tempdir().unwrap();
        let (deps, _fake) = fake_shared_deps(dir.path(), Default::default());
        let registry = WorkerRegistry::new(dir.path().join("does-not-exist"), deps);
        registry.recover_existing().await; // must not panic
    }

    #[tokio::test]
    async fn registry_returns_same_sender_for_same_repo() {
        let dir = tempfile::tempdir().unwrap();
        let (deps, _fake) = fake_shared_deps(dir.path(), Default::default());
        let registry = WorkerRegistry::new(dir.path(), deps);
        let a = registry.sender_for("o", "r").await.unwrap();
        let b = registry.sender_for("o", "r").await.unwrap();
        assert!(a.same_channel(&b), "one worker (one Store) per repo");
    }

    /// A released delivery (GitHub down during a pipeline step) must retry on
    /// the worker's own timer: the webhook was already acked, so no external
    /// party will redeliver it (Codex M5 review).
    #[tokio::test]
    async fn released_delivery_retries_without_new_webhook_traffic() {
        let dir = tempfile::tempdir().unwrap();
        let (deps, fake) = fake_shared_deps(dir.path(), Default::default());
        fake.lock().unwrap().unavailable = true;
        let registry = WorkerRegistry::new(dir.path(), deps);
        let sender = registry.sender_for("o", "r").await.unwrap();

        // First contact requires default-branch discovery (GetRepoSettings);
        // with GitHub down the delivery is acked, claimed, and released.
        assert_eq!(
            send_delivery(&registry, &sender, "d1", pull_request_body())
                .await
                .unwrap(),
            EnqueueOutcome::Enqueued
        );

        // A second discovery attempt can only come from the stall-retry
        // timer: no further messages are sent.
        wait_for(
            &fake,
            |f| f.settings_fetches >= 1,
            "discovery never attempted",
        )
        .await;
        wait_for(
            &fake,
            |f| f.settings_fetches >= 2,
            "no stall retry happened",
        )
        .await;

        // GitHub recovers; the retry loop completes the delivery on its own.
        let lifted_at = fake.lock().unwrap().settings_fetches;
        fake.lock().unwrap().unavailable = false;
        wait_for(
            &fake,
            |f| f.settings_fetches > lifted_at,
            "no retry after the outage lifted",
        )
        .await;
    }

    /// Polls `cond` against the fake until it holds (10s deadline).
    async fn wait_for(
        fake: &Arc<Mutex<FakeGitHub>>,
        cond: impl Fn(&FakeGitHub) -> bool,
        msg: &str,
    ) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if cond(&fake.lock().unwrap()) {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "{msg}");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn startup_prunes_expired_intake_bookkeeping() {
        let dir = tempfile::tempdir().unwrap();
        let db_dir = dir.path().join("o").join("r");
        std::fs::create_dir_all(&db_dir).unwrap();
        // Seed a delivery fully processed well past the retention window,
        // with its dedupe key, then release the lock.
        let old = chrono::Utc::now() - chrono::Duration::days(30);
        let key = crate::webhooks::dedupe::DedupeKey::issue_comment_created(
            PrNumber(7),
            crate::types::CommentId(1),
        );
        {
            let mut store = Store::open(&db_dir.join("state.db")).unwrap();
            store
                .enqueue("ancient", "pull_request", "{}", &pull_request_body(), old)
                .unwrap();
            let claimed = store.claim_next_delivery().unwrap().unwrap();
            assert_eq!(claimed.delivery_id, "ancient");
            store
                .commit_delivery("ancient", &[], Some(&key), &[], old)
                .unwrap();
            assert!(store.is_duplicate(&key).unwrap());
        }

        // Spawning the worker prunes at startup, so redelivering the ancient
        // id is accepted as new (the idempotency row expired with the window).
        let (deps, _fake) = fake_shared_deps(dir.path(), Default::default());
        let registry = WorkerRegistry::new(dir.path(), deps);
        let sender = registry.sender_for("o", "r").await.unwrap();
        assert_eq!(
            send_delivery(&registry, &sender, "ancient", pull_request_body())
                .await
                .unwrap(),
            EnqueueOutcome::Enqueued,
            "the expired done-delivery row should have been pruned at startup"
        );
    }

    #[tokio::test]
    async fn open_failure_surfaces_as_open_error() {
        // Hold the repo lock with a Store; a worker open must fail Locked.
        let dir = tempfile::tempdir().unwrap();
        let db_dir = dir.path().join("o").join("r");
        std::fs::create_dir_all(&db_dir).unwrap();
        let _held = Store::open(&db_dir.join("state.db")).unwrap();

        let (deps, _fake) = fake_shared_deps(dir.path(), Default::default());
        let registry = WorkerRegistry::new(dir.path(), deps);
        let err = registry.sender_for("o", "r").await.unwrap_err();
        assert!(
            matches!(
                err,
                crate::worker::WorkerError::Open(crate::store::StoreError::Locked(_))
            ),
            "expected Open(Locked), got {err:?}"
        );
    }
}

// ─── Polling fallback: missed-webhook recovery ───

/// The production idle wait runs on a plain OS thread and borrows the
/// server's runtime for its timer. It must time out (the poll tick) and
/// deliver a message that arrives first — and, above all, not panic for
/// constructing the timer outside the runtime (Codex polling review round
/// 3, P1: the fake-backed tests never take this path).
#[test]
fn timed_recv_times_out_and_delivers_from_a_plain_thread() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let handle = runtime.handle().clone();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(4);
    let outcome = std::thread::spawn(move || {
        let timed_out = super::timed_recv(&handle, &mut rx, std::time::Duration::from_millis(20));
        tx.blocking_send(7).unwrap();
        let delivered = super::timed_recv(&handle, &mut rx, std::time::Duration::from_secs(5));
        (timed_out.is_err(), delivered)
    })
    .join()
    .unwrap();
    assert!(outcome.0, "an empty mailbox times out at the poll deadline");
    assert!(
        matches!(outcome.1, Ok(Some(7))),
        "a message arriving first is delivered"
    );
}

/// The headline: a train parked `WaitingCi` because its frontier PR is not
/// mergeable will resume when readiness changes — even if the webhook that
/// would have announced it (a `check_suite`/`status`) is never delivered.
/// The poll re-evaluates, the engine re-fetches merge state, and progress
/// is made. (Before polling, a missed webhook stranded the train forever.)
#[test]
fn poll_recovers_a_train_stranded_by_a_missed_ci_webhook() {
    let (mut world, _heads) = World::linear_stack(1);
    world.github.lock().unwrap().blocked.insert(PrNumber(1));
    let mut processor = world.processor();
    start_command(&mut world, &mut processor, 1);
    drain(&mut processor);

    // The frontier PR is blocked, so the train parked without merging.
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .any(|t| t.state.is_active()),
        "the train must be parked (active, WaitingCi), not gone"
    );
    assert!(
        !processor.state().prs[&PrNumber(1)].state.is_merged(),
        "a blocked frontier PR must not have merged"
    );

    // A poll while still blocked re-evaluates but makes no progress.
    processor.poll_active_trains();
    drain(&mut processor);
    assert_eq!(
        world
            .github
            .lock()
            .unwrap()
            .squash_count
            .values()
            .sum::<u32>(),
        0,
        "a poll while still blocked must not merge anything"
    );
    // And persists nothing: an unchanged observation is not an event.
    // Compaction refuses active trains and every evaluation replays the
    // train's log, so a parked train polled forever must not grow the log
    // (Codex polling review, P2).
    let events_after_first_poll = processor.store_mut().events().unwrap().len();
    processor.poll_active_trains();
    drain(&mut processor);
    assert_eq!(
        processor.store_mut().events().unwrap().len(),
        events_after_first_poll,
        "a poll that observes nothing new must append nothing"
    );

    // Readiness flips with NO webhook delivered; the next poll recovers it.
    // `drain` (not `drive_to_completion`) makes NO CI nudge, so only the
    // poll's re-evaluation can complete the train.
    world.github.lock().unwrap().blocked.clear();
    processor.poll_active_trains();
    drain(&mut processor);
    assert!(
        processor.state().prs[&PrNumber(1)].state.is_merged(),
        "the poll alone (no webhook) must recover the stranded train"
    );
}

/// A poll must not evaluate ahead of the acked backlog: its evaluations
/// route through the same backlog-drain gate as the startup ones, so a
/// pending stop/topology delivery is applied first (the round-6 rule).
#[test]
fn poll_does_not_overtake_the_acked_backlog() {
    let (mut world, heads) = World::linear_stack(1);
    // Block #1 so the train stays parked (active) after start.
    world.github.lock().unwrap().blocked.insert(PrNumber(1));
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);
    start_command(&mut world, &mut processor, 1);
    drain(&mut processor);
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .any(|t| t.state.is_active())
    );

    // A delivery is waiting in the backlog; a poll now must not jump it.
    let head = {
        let github = world.github.lock().unwrap();
        github.branch_head("pr-1")
    };
    let body = check_suite_green_body(&world.config, &head, &[1], 800);
    world.enqueue(&mut processor, "check_suite", body);
    processor.poll_active_trains();
    assert!(
        processor.pump().unwrap().is_none(),
        "poll evaluations must wait behind the pending delivery"
    );
    assert!(
        !processor.has_queued_work(),
        "nothing is queued until the backlog drains"
    );
}

/// A poll with no active trains queues nothing — pure overhead avoidance.
#[test]
fn poll_with_no_active_trains_is_a_noop() {
    let (world, _heads) = World::linear_stack(1);
    let mut processor = world.processor();
    processor.poll_active_trains();
    assert!(processor.claim().unwrap().is_none());
    assert!(
        !processor.has_queued_work(),
        "a poll with no active trains queues no work"
    );
}

// ─── The interleaving model check ───

/// Codex review rounds 1, 2, 6, 7, 8, 10, 13, 15, and 19 were all ordering
/// bugs in the worker's command/saga/backlog machinery — each round a human
/// reviewer exploring one more schedule by hand. This harness mechanizes
/// that: proptest generates schedules interleaving delivery processing,
/// observation boundaries, command injection, and (in the second property)
/// crashes, then asserts the invariants every schedule must preserve:
///
/// - **≤ 1 squash per PR** — the cascade's core idempotency guarantee;
/// - **the store is never ahead of reality** — a PR the store calls merged
///   is merged on (fake) GitHub;
/// - **every acknowledged command is answered** — `pending_commands` is
///   empty at quiescence and no train is left active;
/// - **store and GitHub agree exactly, and every completed train's intent
///   ledger is fully matched** — for crash schedules too: M6's recovery
///   (worktree cleanup + supplementary GitHub recovery + the evaluate
///   path) plus reality's merged-close webhooks must reconcile every
///   crash window the schedule can produce.
///
/// Each case does real git work, so the case counts are deliberately small;
/// raise them locally (`PROPTEST_CASES`) when touching worker ordering.
mod interleaving {
    use proptest::prelude::*;

    use super::*;

    /// One scheduler decision.
    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// Claim and process one delivery, if any.
        ProcessOne,
        /// Feed held saga outcomes back (an observation boundary).
        Boundary,
        /// Start the next queued saga if the slot is free.
        Pump,
        /// A user posts `@merge-train stop` on PR 1.
        InjectStop,
        /// CI goes green for every active train's current head.
        InjectCiGreen,
        /// The process dies and restarts.
        Crash,
    }

    /// Weighted: processing and boundaries dominate real schedules, and a
    /// uniform decoder rarely reached deep states (the mutation check on the
    /// reload path caught the harness being too shallow).
    fn decode(code: u8, allow_crash: bool) -> Step {
        match code % if allow_crash { 10 } else { 9 } {
            0..=3 => Step::ProcessOne,
            4..=5 => Step::Boundary,
            6..=7 => Step::Pump,
            8 if code & 1 == 0 => Step::InjectStop,
            8 => Step::InjectCiGreen,
            _ => Step::Crash,
        }
    }

    struct Run {
        world: World,
        /// `None` only transiently during a crash (the old processor must
        /// drop — releasing the Store lock — before the successor opens).
        processor: Option<Processor>,
        /// A dispatched batch whose effects have run (reality has mutated)
        /// but whose outcomes are not yet observed — the real race window.
        held: Option<(SagaBatch, Vec<EffectOutcome>)>,
        /// Comment ids fold mod 10 (see `comment_body`) and 0/1 are used by
        /// setup, so injected stops draw from 2..=9 and then stop.
        next_stop_comment: u64,
        next_suite: u64,
    }

    impl Run {
        fn new() -> Run {
            let (mut world, heads) = World::linear_stack(2);
            let mut processor = world.processor();
            world.enqueue_stack_setup(&mut processor, 2, &heads);
            start_command(&mut world, &mut processor, 1);
            Run {
                world,
                processor: Some(processor),
                held: None,
                next_stop_comment: 2,
                next_suite: 900,
            }
        }

        fn processor(&mut self) -> &mut Processor {
            self.processor.as_mut().expect("processor present")
        }

        /// Executes `batch` against reality now; holds the outcomes.
        fn dispatch(&mut self, batch: SagaBatch) {
            let outcomes = execute(self.processor.as_mut().unwrap(), &batch);
            self.held = Some((batch, outcomes));
        }

        fn apply(&mut self, step: Step) {
            match step {
                Step::ProcessOne => {
                    if let Some(delivery) = self.processor().claim().unwrap() {
                        assert_eq!(
                            self.processor().process_claimed(delivery).unwrap(),
                            PipelineOutcome::Processed,
                            "the fake is up; nothing may release"
                        );
                    }
                }
                Step::Boundary => {
                    if let Some((batch, outcomes)) = self.held.take()
                        && let Some(next) = self
                            .processor()
                            .on_outcomes(batch.root, outcomes, batch.feedback)
                            .unwrap()
                    {
                        self.dispatch(next);
                    }
                }
                Step::Pump => {
                    if self.held.is_none()
                        && let Some(batch) = self.processor().pump().unwrap()
                    {
                        self.dispatch(batch);
                    }
                }
                Step::InjectStop => {
                    if self.next_stop_comment <= 9 {
                        let id = self.next_stop_comment;
                        self.next_stop_comment += 1;
                        let body = comment_body(
                            &self.world.config,
                            1,
                            "@merge-train stop",
                            AUTHOR,
                            "author",
                            id,
                        );
                        let processor = self.processor.as_mut().unwrap();
                        self.world.enqueue(processor, "issue_comment", body);
                    }
                }
                Step::InjectCiGreen => self.nudge_ci(),
                Step::Crash => {
                    self.held = None;
                    self.processor = None; // drop first: releases the lock
                    self.processor = Some(self.world.processor());
                }
            }
        }

        fn nudge_ci(&mut self) {
            let targets: Vec<(Sha, u64)> = {
                let github = self.world.github.lock().unwrap();
                self.processor
                    .as_ref()
                    .unwrap()
                    .state()
                    .active_trains
                    .values()
                    .filter(|t| t.state.is_active())
                    .filter_map(|t| {
                        github
                            .prs
                            .get(&t.current_pr)
                            .map(|fake| (github.branch_head(&fake.branch), t.current_pr.0))
                    })
                    .collect()
            };
            for (head, pr) in targets {
                self.next_suite += 1;
                let body =
                    check_suite_green_body(&self.world.config, &head, &[pr], self.next_suite);
                let processor = self.processor.as_mut().unwrap();
                self.world.enqueue(processor, "check_suite", body);
            }
        }

        /// Runs the deterministic tail to quiescence: settle everything,
        /// then play reality's parts — CI goes green for whatever is
        /// waiting, and GitHub delivers the merged-close webhook for every
        /// squash the store has not yet heard about (that webhook, not
        /// recovery, is how a train stopped after an unobserved squash
        /// reconciles). M6: trains inherited mid-cascade recover and finish
        /// on their own — no stop crutch. Panics if the system will not go
        /// quiet.
        fn finish(&mut self) {
            for round in 0..40 {
                // Settle: boundaries, deliveries, pumps, until a fixpoint.
                loop {
                    let mut progressed = false;
                    if self.held.is_some() {
                        self.apply(Step::Boundary);
                        progressed = true;
                    }
                    while let Some(delivery) = self.processor().claim().unwrap() {
                        self.processor().process_claimed(delivery).unwrap();
                        progressed = true;
                    }
                    if self.held.is_none()
                        && let Some(batch) = self.processor().pump().unwrap()
                    {
                        self.dispatch(batch);
                        progressed = true;
                    }
                    if !progressed {
                        break;
                    }
                }
                let delivered = self.deliver_unseen_merges();
                let any_active = self
                    .processor()
                    .state()
                    .active_trains
                    .values()
                    .any(|t| t.state.is_active());
                if !any_active && delivered == 0 {
                    return;
                }
                if round < 20 {
                    self.nudge_ci();
                }
            }
            panic!(
                "did not quiesce: {:?}",
                self.processor().state().active_trains
            );
        }

        /// Reality's merged-close webhooks: GitHub always announces a
        /// merged PR, which is how the store hears about a squash whose
        /// observation died with the process on a train that was then
        /// stopped (recovery never evaluates a retired train). Returns how
        /// many it enqueued.
        fn deliver_unseen_merges(&mut self) -> usize {
            let unseen: Vec<(PrNumber, Sha, Sha, String, String)> = {
                let github = self.world.github.lock().unwrap();
                let state = self.processor.as_ref().unwrap().state();
                github
                    .prs
                    .iter()
                    .filter_map(|(pr, fake)| {
                        let FakePrState::Merged { squash_sha } = &fake.state else {
                            return None;
                        };
                        let store_merged = state.prs.get(pr).is_some_and(|c| c.state.is_merged());
                        (!store_merged).then(|| {
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
                let body =
                    pr_merged_body(&self.world.config, pr.0, &head, &branch, &base, &merge_sha);
                let processor = self.processor.as_mut().unwrap();
                self.world.enqueue(processor, "pull_request", body);
            }
            delivered
        }

        /// The invariants every schedule must preserve.
        fn assert_safety(&mut self) {
            let github = self.world.github.clone();
            let github = github.lock().unwrap();
            for (pr, count) in &github.squash_count {
                assert!(*count <= 1, "PR #{pr} squashed {count} times");
            }
            for (pr, cached) in &self.processor.as_ref().unwrap().state().prs {
                if cached.state.is_merged() {
                    assert!(
                        matches!(
                            github.prs.get(pr).map(|f| &f.state),
                            Some(FakePrState::Merged { .. })
                        ),
                        "store claims PR #{pr} merged but reality disagrees"
                    );
                }
            }
            drop(github);
            assert!(
                self.processor()
                    .store_mut()
                    .pending_commands()
                    .unwrap()
                    .is_empty(),
                "acknowledged commands left unanswered at quiescence"
            );
            assert!(
                self.processor()
                    .state()
                    .active_trains
                    .values()
                    .all(|t| !t.state.is_active()),
                "active trains left at quiescence"
            );
        }

        /// The additional invariants no-crash schedules must preserve.
        fn assert_full_consistency(&mut self) {
            let github = self.world.github.clone();
            let github = github.lock().unwrap();
            for (pr, fake) in &github.prs {
                let store_merged = self
                    .processor
                    .as_ref()
                    .unwrap()
                    .state()
                    .prs
                    .get(pr)
                    .is_some_and(|c| c.state.is_merged());
                let real_merged = matches!(fake.state, FakePrState::Merged { .. });
                assert_eq!(
                    store_merged, real_merged,
                    "store and reality disagree about PR #{pr}"
                );
            }
            drop(github);
            let events = self.processor().store_mut().events().unwrap();
            let completed: Vec<PrNumber> = events
                .iter()
                .filter_map(|e| match e.payload {
                    crate::persistence::event::StateEventPayload::TrainCompleted { root_pr } => {
                        Some(root_pr)
                    }
                    _ => None,
                })
                .collect();
            for root in completed {
                let facts = ReplayFacts::for_train(&events, root);
                assert_eq!(
                    facts.unmatched().count(),
                    0,
                    "completed train #{root} has unmatched intents"
                );
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 8,
            ..ProptestConfig::default()
        })]

        #[test]
        fn schedules_without_crashes_preserve_all_invariants(
            codes in proptest::collection::vec(any::<u8>(), 6..24),
        ) {
            let mut run = Run::new();
            for code in codes {
                run.apply(decode(code, false));
            }
            run.finish();
            run.assert_safety();
            run.assert_full_consistency();
        }

        #[test]
        fn schedules_with_crashes_preserve_safety(
            codes in proptest::collection::vec(any::<u8>(), 6..24),
        ) {
            let mut run = Run::new();
            for code in codes {
                run.apply(decode(code, true));
            }
            // Every case also crashes at wherever the schedule left the
            // system — a crash-at-random-point sweep — so recovery is
            // exercised from arbitrary depths, not only when the random
            // codes happen to include a crash.
            run.apply(Step::Crash);
            run.finish();
            run.assert_safety();
            // M6: recovery closes the reality-ahead windows, so crash
            // schedules must reach the same exact store/GitHub agreement
            // no-crash schedules do.
            run.assert_full_consistency();
        }
    }
}
