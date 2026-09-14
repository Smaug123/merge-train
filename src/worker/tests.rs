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
use chrono::Utc;

use crate::github::test_support::{FakeComment, FakeGitHub, FakePr, FakePrState};
use crate::persistence::StateEventPayload;
use crate::state::RepoState;
use crate::store::Store;
use crate::types::{CommentId, PrNumber, Sha};

use super::executor::{GitHubExec, SagaBatch, execute_batch};
use super::pipeline::{PipelineOutcome, Processor, WorkerDeps};
use super::test_support::TEST_BOT_ID;
use super::{GitSettings, IntakeDelivery, WorkerMsg};
use crate::effects::Effect;
use crate::effects::github::{Edited, GitHubEffect};

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

fn pr_closed_body(
    config: &GitConfig,
    number: u64,
    head: &Sha,
    branch: &str,
    base: &str,
) -> Vec<u8> {
    format!(
        r#"{{
            "action": "closed",
            "pull_request": {{
                "number": {number},
                "state": "closed",
                "draft": false,
                "merged": false,
                "head": {{ "sha": "{head}", "ref": "{branch}" }},
                "base": {{ "sha": "{base_sha}", "ref": "{base}" }},
                "user": {{ "id": {AUTHOR}, "login": "author" }},
                "updated_at": "2026-07-01T12:30:00Z"
            }},
            "repository": {repo}
        }}"#,
        base_sha = "0".repeat(40),
        repo = repo_json(config),
    )
    .into_bytes()
}

fn pr_reopened_body(
    config: &GitConfig,
    number: u64,
    head: &Sha,
    branch: &str,
    base: &str,
) -> Vec<u8> {
    format!(
        r#"{{
            "action": "reopened",
            "pull_request": {{
                "number": {number},
                "state": "open",
                "draft": false,
                "merged": false,
                "head": {{ "sha": "{head}", "ref": "{branch}" }},
                "base": {{ "sha": "{base_sha}", "ref": "{base}" }},
                "user": {{ "id": {AUTHOR}, "login": "author" }},
                "updated_at": "2026-07-01T12:30:00Z"
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

/// The harness's stall cadence — production's, since the clock the
/// processor measures it against is the harness's own and never waited on.
const STALL_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(30);

struct World {
    _temp: TempDir,
    state_dir: TempDir,
    config: GitConfig,
    github: Arc<Mutex<FakeGitHub>>,
    /// Monotonic delivery-id source.
    next_delivery: u64,
    /// The processor's clock. Tests ADVANCE it to state that a cooldown has
    /// passed; nothing here sleeps.
    clock: Arc<Mutex<chrono::DateTime<chrono::Utc>>>,
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
            clock: Arc::new(Mutex::new(crate::test_utils::test_timestamp())),
        };
        (world, heads)
    }

    /// Time passes: the next cooldown comparison sees `by` more of it.
    fn advance(&self, by: chrono::Duration) {
        *self.clock.lock().unwrap() += by;
    }

    /// Time passes beyond any cooldown the processor measures.
    fn advance_past_cooldown(&self) {
        self.advance(chrono::Duration::from_std(STALL_RETRY_DELAY).unwrap() * 2);
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
            stall_retry_delay: STALL_RETRY_DELAY,
            poll_interval: std::time::Duration::ZERO,
            clock: super::pipeline::Clock::Manual(self.clock.clone()),
        }
    }

    fn processor(&self) -> Processor {
        Processor::new(Store::open(&self.db_path()).unwrap(), self.deps()).unwrap()
    }

    /// Durably enqueues a raw delivery (as the intake path would). A comment
    /// webhook describes a comment that exists on GitHub at that moment, so
    /// it is mirrored into the fake's comment store — the crawl and
    /// recovery list comments, and a `created` delivery for a comment the
    /// listing cannot see is exactly the stale-redelivery shape the
    /// pipeline closes.
    fn enqueue(&mut self, processor: &mut Processor, event_type: &str, body: Vec<u8>) {
        self.enqueue_received_at(processor, event_type, body, chrono::Utc::now());
    }

    /// As `enqueue`, with the time the webhook was RECEIVED: a delivery
    /// received while a crawl was fetching reaches the store only after
    /// the crawl landed, and must be judged against its present all the
    /// same.
    fn enqueue_received_at(
        &mut self,
        processor: &mut Processor,
        event_type: &str,
        body: Vec<u8>,
        received_at: chrono::DateTime<chrono::Utc>,
    ) {
        if event_type == "issue_comment" {
            self.mirror_comment(&body);
        }
        self.next_delivery += 1;
        let id = format!("delivery-{}", self.next_delivery);
        processor
            .store_mut()
            .enqueue(&id, event_type, "{}", &body, received_at)
            .unwrap();
    }

    fn mirror_comment(&self, body: &[u8]) {
        let Ok(json) = serde_json::from_slice::<serde_json::Value>(body) else {
            return;
        };
        let Some(id) = json["comment"]["id"].as_u64() else {
            return;
        };
        let mut github = self.github.lock().unwrap();
        match json["action"].as_str() {
            Some("deleted") => {
                github.comments.remove(&CommentId(id));
            }
            Some(action @ ("created" | "edited")) => {
                let Some(pr) = json["issue"]["number"].as_u64() else {
                    return;
                };
                let author_id = json["comment"]["user"]["id"].as_u64().unwrap_or(0);
                let text = json["comment"]["body"].as_str().unwrap_or("").to_owned();
                // An edit's bytes are the SENDER's, whoever authored the
                // comment (GitHub's `editor`).
                let edited = if action == "edited" {
                    Edited::By {
                        editor: json["sender"]["id"].as_u64(),
                    }
                } else {
                    github
                        .comments
                        .get(&CommentId(id))
                        .map_or(Edited::Never, |c| c.edited)
                };
                github.comments.insert(
                    CommentId(id),
                    FakeComment {
                        pr: PrNumber(pr),
                        author_id,
                        body: text,
                        edited,
                    },
                );
            }
            _ => {}
        }
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

/// One stall cycle: time passes beyond the absence cooldown, the retry
/// timer fires, and the machinery gets its look.
fn tick(world: &World, processor: &mut Processor) {
    world.advance_past_cooldown();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(processor);
}

/// A distinct `updated_at` per call: GitHub's edit webhooks carry the
/// comment's last-updated time, and the dedupe key includes it, so two
/// identical edits of one comment must not look like one redelivered.
fn unique_updated_at() -> String {
    static NEXT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    chrono::DateTime::<chrono::Utc>::from_timestamp(1_780_000_000 + n, 0)
        .unwrap()
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
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

/// A webhook for a change SOMEONE ELSE made to a bot comment on `pr`:
/// GitHub reports the bot as the author of its own comments however a
/// maintainer edits them, and the sender is who acted.
fn bot_comment_webhook(
    config: &GitConfig,
    pr: u64,
    comment_id: u64,
    action: &str,
    body: &str,
) -> Vec<u8> {
    format!(
        r#"{{
            "action": "{action}",
            "comment": {{
                "id": {comment_id},
                "body": {body},
                "user": {{ "id": {TEST_BOT_ID}, "login": "merge-train" }},
                "updated_at": "{updated_at}"
            }},
            "issue": {{
                "number": {pr},
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {AUTHOR}, "login": "author" }}
        }}"#,
        body = serde_json::to_string(body).unwrap(),
        updated_at = unique_updated_at(),
        repo = repo_json(config),
    )
    .into_bytes()
}

/// A ledger for `pr` claiming an edge the store never recorded, with a
/// sequence number no honest write could reach.
fn forged_ledger_body(pr: u64) -> String {
    crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(pr),
        declared: Some(crate::status::Declaration {
            predecessor: PrNumber(9),
            owner: crate::types::CommentId(1),
        }),
        seq: u64::MAX,
        settled_through: None,
    })
}

/// Plants a bot comment on `pr` with `body`, as a maintainer editing an old
/// reply of the bot's into it would leave it, and returns its id. The edit
/// webhook is the caller's to deliver. The id comes from the fake's own
/// allocator, as a posted comment's would: GitHub's comment ids are
/// globally monotonic, and a deleted id never comes back.
fn plant_bot_comment(world: &World, pr: u64, body: &str) -> crate::types::CommentId {
    let mut github = world.github.lock().unwrap();
    let floor = github
        .comments
        .keys()
        .next_back()
        .map_or(0, |max| max.0 + 1);
    let id = crate::types::CommentId(github.next_comment.max(floor));
    github.next_comment = id.0 + 1;
    github.comments.insert(
        id,
        FakeComment {
            pr: PrNumber(pr),
            author_id: TEST_BOT_ID,
            body: body.to_owned(),
            edited: Edited::By {
                editor: Some(TEST_BOT_ID),
            },
        },
    );
    id
}

/// The bot's own write of `body` into comment `id`, as GitHub would then
/// list it: the bytes are the bot's, and the bot is the last editor.
/// Models the delayed-webhook shape, where the bot's update has already
/// restored a comment whose edit webhook is still in the queue.
fn restored_by_bot(world: &World, id: crate::types::CommentId, body: &str) {
    let mut github = world.github.lock().unwrap();
    let comment = github.comments.get_mut(&id).unwrap();
    comment.body = body.to_owned();
    comment.edited = Edited::By {
        editor: Some(TEST_BOT_ID),
    };
}

/// Whether the ledger machinery has anything left to do for `pr`.
fn ledger_pending(processor: &mut Processor, pr: u64) -> bool {
    processor.store_mut().ledger_pending(PrNumber(pr)).unwrap()
}

/// Processes everything queued, but dies the moment a batch that writes a
/// comment best-effort has EXECUTED — before its outcome is observed.
/// Returns the successor process, which has picked up whatever the dead
/// one left owed. With no such write on the way, this is `drain`.
fn drain_crashing_before_ack(world: &World, mut processor: Processor) -> Processor {
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let mut next = processor.pump().unwrap();
    let mut steps = 0;
    while let Some(batch) = next {
        steps += 1;
        assert!(steps < 500, "saga did not terminate");
        let writes_a_comment = batch.best_effort.iter().any(|e| {
            matches!(
                e,
                Effect::GitHub(
                    GitHubEffect::PostComment { .. } | GitHubEffect::UpdateComment { .. }
                )
            )
        });
        if writes_a_comment {
            let interpreter = WorktreeGitInterpreter::new(processor.git_config(), batch.root);
            let _ = execute_batch(&interpreter, processor.github(), &batch);
            drop(processor);
            let mut successor = world.processor();
            drain(&mut successor);
            return successor;
        }
        let outcomes = execute(&mut processor, &batch);
        next = processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap();
        if next.is_none() {
            next = processor.pump().unwrap();
        }
    }
    processor
}

/// What a maintainer does to a freshly written ledger comment in the
/// window between the write landing and its acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Interference {
    /// Deletes it; the deletion webhook is processed before the
    /// acknowledgement.
    Delete,
    /// Edits it into a forgery; the edit webhook is processed before the
    /// acknowledgement.
    Tamper,
}

/// Processes everything queued, but the moment the FIRST batch that writes
/// a ledger has EXECUTED, a maintainer interferes with the comment it
/// wrote and that webhook is processed — only then is the batch's outcome
/// observed. Everything after runs normally. With no such write on the
/// way, this is `drain`.
fn drain_interfering_before_ack(
    world: &mut World,
    processor: &mut Processor,
    interference: Interference,
) {
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let mut next = processor.pump().unwrap();
    let mut steps = 0;
    let mut interfered = false;
    while let Some(batch) = next {
        steps += 1;
        assert!(steps < 500, "saga did not terminate");
        let writes_a_ledger = batch.best_effort.iter().any(|e| match e {
            Effect::GitHub(
                GitHubEffect::PostComment { body, .. } | GitHubEffect::UpdateComment { body, .. },
            ) => crate::status::parse_stack_ledger(body).is_some(),
            _ => false,
        });
        if !writes_a_ledger || interfered {
            let outcomes = execute(processor, &batch);
            next = processor
                .on_outcomes(batch.root, outcomes, batch.feedback)
                .unwrap();
            if next.is_none() {
                next = processor.pump().unwrap();
            }
            continue;
        }
        interfered = true;
        let interpreter = WorktreeGitInterpreter::new(processor.git_config(), batch.root);
        let landed = execute_batch(&interpreter, processor.github(), &batch);
        // Whatever ledger the batch wrote, interfere with it and process
        // the webhook before the outcome is observed.
        if let Some((id, _)) = ledgers_on(world, batch.root.0).into_iter().next() {
            let hook = match interference {
                Interference::Delete => {
                    world.github.lock().unwrap().comments.remove(&id);
                    bot_comment_webhook(&world.config, batch.root.0, id.0, "deleted", "")
                }
                Interference::Tamper => {
                    let forged = forged_ledger_body(batch.root.0);
                    world
                        .github
                        .lock()
                        .unwrap()
                        .comments
                        .get_mut(&id)
                        .unwrap()
                        .body = forged.clone();
                    bot_comment_webhook(&world.config, batch.root.0, id.0, "edited", &forged)
                }
            };
            world.enqueue(processor, "issue_comment", hook);
            while let Some(delivery) = processor.claim().unwrap() {
                processor.process_claimed(delivery).unwrap();
            }
        }
        processor.note_best_effort(&landed.best_effort).unwrap();
        next = processor
            .on_outcomes(batch.root, landed.observed, batch.feedback)
            .unwrap();
        if next.is_none() {
            next = processor.pump().unwrap();
        }
    }
    drain(processor);
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
    // Well clear of the ids the bot's own comments take: this harness
    // mirrors user comments into the fake, and a collision would overwrite
    // the very ledger under test.
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
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

    // Well clear of the ids the bot's own comments take: this harness
    // mirrors user comments into the fake, and a collision would overwrite
    // the very ledger under test.
    let remark = comment_body(&world.config, 2, "a remark", AUTHOR, "author", 9001);
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

/// The post landed, and a maintainer deleted the new comment before the
/// post's acknowledgement was processed. The id is not recorded yet, so
/// the deletion names nothing the store knows — but ignoring it lets the
/// acknowledgement record a dead comment and clear the obligation, and
/// the ledger stays missing for good. A change to any bot comment while
/// the ledger is owed re-owes it at a fresh generation, which the
/// in-flight acknowledgement cannot clear.
#[test]
fn a_deletion_racing_the_posts_acknowledgement_is_not_discharged_by_it() {
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

    // Declare, and run as far as the batch that POSTS the ledger; execute
    // it without observing its outcome yet.
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
    let landed = execute_batch(&interpreter, processor.github(), &post);
    let (posted, _) = ledgers_on(&world, 2)[0];

    // The deletion webhook for the new comment is processed BEFORE the
    // post's acknowledgement.
    world.github.lock().unwrap().comments.remove(&posted);
    let deletion = format!(
        r#"{{
            "action": "deleted",
            "comment": {{
                "id": {posted},
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
    world.enqueue(&mut processor, "issue_comment", deletion.into_bytes());
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    // Now the acknowledgement.
    processor.note_best_effort(&landed.best_effort).unwrap();
    assert!(
        !processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty(),
        "the acknowledgement of a post deleted under it must not discharge the ledger"
    );

    // The boundary completes; the retry writes to the recorded (dead)
    // comment, 404s, and posts afresh.
    let next = processor
        .on_outcomes(post.root, landed.observed, post.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    drain(&mut processor);
    tick(&world, &mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "a fresh ledger");
    assert_ne!(after[0].0, posted);
    assert_ledgers_match_store(&world, &processor);
    assert!(
        processor
            .store_mut()
            .owed_stack_ledgers()
            .unwrap()
            .is_empty()
    );
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
    // obligation is picked up at construction and written straight away.
    world.github.lock().unwrap().unavailable = false;
    let mut processor = world.processor();
    drain(&mut processor);
    assert_eq!(
        ledger_on(&world, 2)
            .and_then(|l| l.declared)
            .map(|d| d.predecessor),
        Some(PrNumber(1)),
        "the restart wrote the ledger with no traffic to prompt it"
    );
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

/// An editor reapplies the SAME tampered body within a second of the
/// repair. GitHub's `updated_at` is second-resolution, so the two edit
/// webhooks share a dedupe key — and the second must still re-owe the
/// ledger, or the corruption stands with nothing owed. The invalidation
/// is idempotent, so it runs before the duplicate-content check.
#[test]
fn a_repeated_identical_edit_within_one_second_is_still_repaired() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let ledger_id = ledgers_on(&world, 2)[0].0;
    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: Some(crate::status::Declaration {
            predecessor: PrNumber(9),
            owner: crate::types::CommentId(1),
        }),
        seq: 9999,
        settled_through: None,
    });
    let webhook = {
        format!(
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
        )
        .into_bytes()
    };
    for round in 1..=2 {
        world
            .github
            .lock()
            .unwrap()
            .comments
            .get_mut(&ledger_id)
            .unwrap()
            .body = forged.clone();
        world.enqueue(&mut processor, "issue_comment", webhook.clone());
        drain(&mut processor);
        assert_eq!(
            ledger_on(&world, 2)
                .and_then(|l| l.declared)
                .map(|d| d.predecessor),
            Some(PrNumber(1)),
            "round {round}: the forged edge is overwritten"
        );
    }
    assert_ledgers_match_store(&world, &processor);
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
    // The store knows the comment, so the write needs no probe first.
    let write = processor.pump().unwrap().expect("the ledger write");
    assert!(
        matches!(
            write.best_effort.first(),
            Some(Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. })) if *comment_id == ledger_id
        ),
        "a rewrite at the recorded id: {:?}",
        write.best_effort
    );

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

// ─── GitHub lies: eventually consistent listings, fallible 404s, forgeries ───

/// A listing never stands in for a write. The shape (Codex ledger review
/// round 24, P2): a rewrite to "not stacked" lands, the process dies
/// before acknowledging it, the declaration is restored in the same owning
/// comment, and the listing cache serves the OLD stacked body — which
/// matches the restored state on every field but its sequence number.
/// The recorded comment is rewritten by id regardless of what any listing
/// shows, so the ledger ends up saying what the store holds.
#[test]
fn a_stale_listing_body_never_stands_in_for_a_write() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let (ledger_id, stacked) = ledgers_on(&world, 2)[0];
    let stacked_body = world.github.lock().unwrap().comments[&ledger_id]
        .body
        .clone();

    // The author edits the declaration away; the retraction's rewrite
    // lands, and its acknowledgement does not.
    let edit_away = comment_body_with_action(
        &world.config,
        2,
        "never mind",
        AUTHOR,
        "author",
        20,
        "edited",
    );
    world.enqueue(&mut processor, "issue_comment", edit_away);
    let mut processor = drain_crashing_before_ack(&world, processor);
    assert_eq!(
        ledgers_on(&world, 2)[0].1.declared,
        None,
        "precondition: GitHub holds the not-stacked rewrite"
    );

    // The author edits the SAME comment back into the declaration, and the
    // listing cache still serves the stacked body from before.
    world
        .github
        .lock()
        .unwrap()
        .stale_listing_bodies
        .insert(ledger_id, stacked_body);
    let edit_back = comment_body_with_action(
        &world.config,
        2,
        "@merge-train predecessor #1",
        AUTHOR,
        "author",
        20,
        "edited",
    );
    world.enqueue(&mut processor, "issue_comment", edit_back);
    drain(&mut processor);

    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1)),
        "precondition: the store took the restored declaration"
    );
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].0, ledger_id);
    assert_eq!(
        after[0].1.declared, stacked.declared,
        "the comment itself says stacked again"
    );
    assert!(
        after[0].1.seq > stacked.seq,
        "written afresh, not believed from the cache"
    );
    assert!(!ledger_pending(&mut processor, 2));
    assert_ledgers_match_store(&world, &processor);
}

/// A post whose `StackLedgerPosted` never committed leaves a comment the
/// store has no id for — and GitHub's listings are eventually consistent,
/// so it can be missing from the next one. Posting on the first miss
/// leaves a permanent duplicate: absence must be STABLE across spaced
/// listings first (Codex ledger review round 1, P2), which the unresolved
/// row written before the post asks for.
#[test]
fn a_ledger_absent_from_one_listing_is_not_duplicated() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let (orphan, _) = ledgers_on(&world, 2)[0];

    // Back to the moment after the post landed and before its record: the
    // store has no id and the obligation, and the listing hides the comment.
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    let orphan_seq = ledgers_on(&world, 2)[0].1.seq;
    {
        let mut github = world.github.lock().unwrap();
        github.hidden_from_listings.insert(orphan);
    }
    processor
        .store_mut()
        .append_batch(
            &[StateEventPayload::StackLedgerRetired {
                pr: PrNumber(2),
                comment_id: orphan,
            }],
            Utc::now(),
        )
        .unwrap();
    processor
        .store_mut()
        .add_unresolved_ledger(PrNumber(2), None, orphan_seq, true)
        .unwrap();
    drop(processor);
    let mut processor = world.processor();
    drain(&mut processor);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "one absent listing must not produce a second ledger"
    );
    assert!(
        ledger_pending(&mut processor, 2),
        "the question is kept for another look"
    );

    // Once the listing catches up, the existing ledger is adopted.
    world.github.lock().unwrap().hidden_from_listings.clear();
    tick(&world, &mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "still exactly one");
    assert_eq!(after[0].0, orphan, "the original, adopted");
    assert_eq!(
        processor.state().prs[&PrNumber(2)].ledger_comment_id,
        Some(orphan)
    );
    assert!(!ledger_pending(&mut processor, 2));
}

/// A post concluded lost after stable absence, then shown after all: the
/// replacement is the recorded ledger, and the reappearing original is a
/// duplicate — neutralized the moment a listing shows it.
#[test]
fn a_post_concluded_lost_is_neutralized_when_it_reappears() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let (orphan, first) = ledgers_on(&world, 2)[0];
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(orphan);
    processor
        .store_mut()
        .append_batch(
            &[StateEventPayload::StackLedgerRetired {
                pr: PrNumber(2),
                comment_id: orphan,
            }],
            Utc::now(),
        )
        .unwrap();
    processor
        .store_mut()
        .add_unresolved_ledger(PrNumber(2), None, first.seq, true)
        .unwrap();
    drop(processor);
    let mut processor = world.processor();
    drain(&mut processor); // absence 1
    tick(&world, &mut processor); // absence 2: concluded lost, replacement posted
    let github = world.github.lock().unwrap();
    let both: Vec<_> = github
        .comments
        .iter()
        .filter(|(_, c)| c.pr == PrNumber(2) && c.author_id == TEST_BOT_ID)
        .filter(|(_, c)| crate::status::parse_stack_ledger(&c.body).is_some())
        .map(|(id, _)| *id)
        .collect();
    drop(github);
    assert_eq!(
        both.len(),
        2,
        "a replacement was posted over the hidden original"
    );
    let replacement = *both.iter().find(|id| **id != orphan).unwrap();
    assert_eq!(
        processor.state().prs[&PrNumber(2)].ledger_comment_id,
        Some(replacement)
    );

    // The original reappears: a duplicate, neutralized.
    world.github.lock().unwrap().hidden_from_listings.clear();
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    tick(&world, &mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "one ledger again");
    assert_eq!(after[0].0, replacement);
    assert!(!ledger_pending(&mut processor, 2));
    assert_ledgers_match_store(&world, &processor);
}

/// A post whose response is lost on the wire created the comment. The
/// next listing shows a ledger of ours carrying the sequence number that
/// post was made with: it is adopted, not duplicated.
#[test]
fn a_lost_post_response_does_not_duplicate_the_ledger() {
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

    world.github.lock().unwrap().post_comment_response_lost = true;
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
        ledgers_on(&world, 2).len(),
        1,
        "precondition: the post landed"
    );
    assert_eq!(processor.state().prs[&PrNumber(2)].ledger_comment_id, None);
    assert!(ledger_pending(&mut processor, 2));

    world.github.lock().unwrap().post_comment_response_lost = false;
    tick(&world, &mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "adopted, not duplicated");
    assert_eq!(
        processor.state().prs[&PrNumber(2)].ledger_comment_id,
        Some(after[0].0)
    );
    assert!(!ledger_pending(&mut processor, 2));
    assert_ledgers_match_store(&world, &processor);
}

/// A maintainer edits some other reply of the bot's into a forged ledger
/// with an unbeatable sequence number. Left alone it wins a crawl's
/// duplicate arbitration for ever (Codex ledger review round 16): the
/// webhook names it, it is neutralized by id, and the real ledger is not
/// touched.
#[test]
fn a_forged_sibling_is_neutralized_and_the_ledger_left_alone() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let (ledger_id, before) = ledgers_on(&world, 2)[0];
    let updates_before = world.github.lock().unwrap().comment_updates;

    let forged = forged_ledger_body(2);
    let sibling = plant_bot_comment(&world, 2, &forged);
    let hook = bot_comment_webhook(&world.config, 2, sibling.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    drain(&mut processor);

    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "the forgery no longer reads as a ledger");
    assert_eq!(
        after[0],
        (ledger_id, before),
        "and the real ledger is untouched"
    );
    assert_eq!(
        world.github.lock().unwrap().comment_updates,
        updates_before + 1,
        "exactly one write: the neutralization"
    );
    assert!(!ledger_pending(&mut processor, 2));
}

/// The bot has replies on PRs it never cached. A forged ledger there has
/// nothing to be rewritten to, but a crawl would still believe it: it is
/// neutralized (Codex ledger review round 17).
#[test]
fn a_forged_ledger_on_an_uncached_pr_is_neutralized() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    // PR 9 exists on GitHub; the bot has simply never heard of it.
    let head = create_branch_with_file(&world.config, "pr-9", "pr-9.txt", "content 9", "main");
    create_pr_ref(&world.config, 9, &head);
    world.github.lock().unwrap().prs.insert(
        PrNumber(9),
        FakePr {
            branch: "pr-9".to_owned(),
            base_ref: "main".to_owned(),
            state: FakePrState::Open,
            author_id: AUTHOR,
        },
    );
    let forged = forged_ledger_body(9);
    let planted = plant_bot_comment(&world, 9, &forged);
    let hook = bot_comment_webhook(&world.config, 9, planted.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    drain(&mut processor);

    assert!(ledgers_on(&world, 9).is_empty(), "neutralized");
    assert!(!ledger_pending(&mut processor, 9));
}

/// A 404 is evidence, not proof: temporarily losing repository access 404s
/// comments that exist, and the next listing may omit the comment too.
/// Concluding death from the two buried a live forgery for ever (Codex
/// ledger review round 24, P2). Here the 404 leaves a question, and the
/// listing that finally shows the comment raises the repair again.
#[test]
fn an_auth_glitch_404_does_not_bury_a_forged_sibling() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    let forged = forged_ledger_body(2);
    let sibling = plant_bot_comment(&world, 2, &forged);
    {
        let mut github = world.github.lock().unwrap();
        github.update_comment_notfound = true;
        github.hidden_from_listings.insert(sibling);
    }
    let hook = bot_comment_webhook(&world.config, 2, sibling.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    drain(&mut processor);
    tick(&world, &mut processor); // a second listing still omits it
    assert_eq!(
        ledgers_on(&world, 2).len(),
        2,
        "precondition: the forgery still stands"
    );

    {
        let mut github = world.github.lock().unwrap();
        github.update_comment_notfound = false;
        github.hidden_from_listings.clear();
    }
    // Nothing else happens on the PR: the next topology change on it is
    // the next time anyone looks.
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
    assert_eq!(
        after.len(),
        1,
        "the forgery was found again and neutralized"
    );
    assert_eq!(after[0].1.declared, None);
    assert!(!ledger_pending(&mut processor, 2));
    assert_ledgers_match_store(&world, &processor);
}

/// The shape of Codex ledger review round 24's third finding: the recorded
/// ledger is missing from two spaced listings, a replacement is posted and
/// its response lost, and the next listing shows ONLY the original. The
/// original was concluded never to have landed, so its reappearance is
/// indistinguishable from a forgery and is neutralized; the replacement
/// stays an open question until a listing shows it, and is then adopted.
/// Never a duplicate left standing with nothing owed.
#[test]
fn an_unacknowledged_replacement_is_found_and_adopted() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let (original, first) = ledgers_on(&world, 2)[0];

    // The original becomes an unrecorded question that two listings miss.
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(original);
    processor
        .store_mut()
        .append_batch(
            &[StateEventPayload::StackLedgerRetired {
                pr: PrNumber(2),
                comment_id: original,
            }],
            Utc::now(),
        )
        .unwrap();
    processor
        .store_mut()
        .add_unresolved_ledger(PrNumber(2), None, first.seq, true)
        .unwrap();
    drop(processor);
    let mut processor = world.processor();
    drain(&mut processor); // absence 1
    world.github.lock().unwrap().post_comment_response_lost = true;
    tick(&world, &mut processor); // absence 2: replacement posted, response lost
    let replacement = {
        let github = world.github.lock().unwrap();
        *github
            .comments
            .iter()
            .filter(|(id, c)| c.pr == PrNumber(2) && c.author_id == TEST_BOT_ID && **id != original)
            .map(|(id, _)| id)
            .next()
            .expect("the replacement exists")
    };
    assert_eq!(processor.state().prs[&PrNumber(2)].ledger_comment_id, None);

    // The listing now shows only the original.
    {
        let mut github = world.github.lock().unwrap();
        github.post_comment_response_lost = false;
        github.hidden_from_listings.clear();
        github.hidden_from_listings.insert(replacement);
    }
    tick(&world, &mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].ledger_comment_id,
        None,
        "the reappearing original is not adopted: the store cannot tell it from a forgery"
    );
    assert!(
        crate::status::parse_stack_ledger(&world.github.lock().unwrap().comments[&original].body)
            .is_none(),
        "it is neutralized"
    );
    assert!(
        ledger_pending(&mut processor, 2),
        "and the unacknowledged replacement is still an open question, so nothing is posted"
    );
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "the hidden replacement is the only ledger-shaped comment"
    );

    world.github.lock().unwrap().hidden_from_listings.clear();
    tick(&world, &mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "the replacement is adopted");
    assert_eq!(after[0].0, replacement);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].ledger_comment_id,
        Some(replacement)
    );
    assert!(!ledger_pending(&mut processor, 2));
    assert_ledgers_match_store(&world, &processor);
}

/// A forgery's edit webhook names it while the listing cache still serves
/// the comment's original, harmless body; the neutralization answers 404
/// (access flickers). The listing's body is not evidence about the
/// forgery — the webhook was — so seeing the comment again raises the
/// repair again, and it is neutralized once access returns. Left to the
/// listing's word, the forgery stood with nothing pending (Codex ledger
/// review of the hardening, P2).
#[test]
fn a_forgery_behind_a_stale_listing_body_is_not_forgotten_after_a_404() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    let forged = forged_ledger_body(2);
    let sibling = plant_bot_comment(&world, 2, &forged);
    {
        let mut github = world.github.lock().unwrap();
        github
            .stale_listing_bodies
            .insert(sibling, "an old reply of the bot's".to_owned());
        github.update_comment_notfound = true;
    }
    let hook = bot_comment_webhook(&world.config, 2, sibling.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    drain(&mut processor);
    tick(&world, &mut processor);
    assert!(
        ledger_pending(&mut processor, 2),
        "the forgery is watched or owed, never forgotten"
    );
    assert_eq!(
        ledgers_on(&world, 2).len(),
        2,
        "precondition: the forgery still stands"
    );

    world.github.lock().unwrap().update_comment_notfound = false;
    tick(&world, &mut processor);
    tick(&world, &mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(
        after.len(),
        1,
        "the forgery was neutralized on the listing cache's word alone"
    );
    assert!(!ledger_pending(&mut processor, 2));
    assert_ledgers_match_store(&world, &processor);
}

/// A neutralization lands, and a maintainer re-forges the comment before
/// the acknowledgement is processed. The acknowledgement clears only the
/// repair it was dispatched for and settles nothing: the re-raised repair
/// runs, 404s under an access flicker, is watched, and is neutralized
/// when the comment is seen again (Codex ledger review of the hardening,
/// P2).
#[test]
fn a_comment_reforged_after_its_neutralization_lands_is_neutralized_again() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    let forged = forged_ledger_body(2);
    let sibling = plant_bot_comment(&world, 2, &forged);
    let hook = bot_comment_webhook(&world.config, 2, sibling.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let batch = processor
        .pump()
        .unwrap()
        .expect("the sync that neutralizes");
    assert!(
        batch.best_effort.iter().any(|e| matches!(
            e,
            Effect::GitHub(GitHubEffect::UpdateComment { comment_id, .. }) if *comment_id == sibling
        )),
        "precondition: the neutralization is dispatched"
    );
    let interpreter = WorktreeGitInterpreter::new(processor.git_config(), batch.root);
    let landed = execute_batch(&interpreter, processor.github(), &batch);
    assert!(
        crate::status::parse_stack_ledger(&world.github.lock().unwrap().comments[&sibling].body)
            .is_none(),
        "precondition: the neutralization landed"
    );

    // Re-forged, and the webhook processed, before the acknowledgement.
    world
        .github
        .lock()
        .unwrap()
        .comments
        .get_mut(&sibling)
        .unwrap()
        .body = forged.clone();
    let hook = bot_comment_webhook(&world.config, 2, sibling.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    processor.note_best_effort(&landed.best_effort).unwrap();
    assert!(
        processor
            .store_mut()
            .settled_ledger_comments(PrNumber(2))
            .unwrap()
            .is_empty(),
        "the stale acknowledgement settles nothing"
    );
    let next = processor
        .on_outcomes(batch.root, landed.observed, batch.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);

    // The re-raised repair meets an access flicker, then access returns.
    world.github.lock().unwrap().update_comment_notfound = true;
    tick(&world, &mut processor);
    world.github.lock().unwrap().update_comment_notfound = false;
    tick(&world, &mut processor);
    tick(&world, &mut processor);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "the re-forged comment is neutralized"
    );
    assert!(!ledger_pending(&mut processor, 2));
    assert_ledgers_match_store(&world, &processor);
}

/// A train's status comment is its durable record; a maintainer editing it
/// into a ledger shape must not get it rewritten into inert text by the
/// ledger machinery — least of all from a delayed webhook, after the
/// terminal update restored it (Codex ledger review of the hardening, P2).
#[test]
fn a_status_comment_edited_into_a_ledger_is_never_neutralized() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);
    start_command(&mut world, &mut processor, 1);
    drain(&mut processor);
    let (status_id, status_body) = {
        let github = world.github.lock().unwrap();
        github
            .comments
            .iter()
            .find(|(_, c)| {
                c.author_id == TEST_BOT_ID && crate::status::parse_status_comment(&c.body).is_ok()
            })
            .map(|(id, c)| (*id, c.body.clone()))
            .expect("the train posted a status comment")
    };
    let updates_before = world.github.lock().unwrap().comment_updates;

    // The edit's webhook names it as a ledger. The harness mirrors the
    // edit into the fake; the terminal update has since restored the
    // comment (the delayed-webhook shape).
    let forged = forged_ledger_body(1);
    let hook = bot_comment_webhook(&world.config, 1, status_id.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    restored_by_bot(&world, status_id, &status_body);
    drain(&mut processor);
    tick(&world, &mut processor);

    assert_eq!(
        world.github.lock().unwrap().comments[&status_id].body,
        status_body,
        "the status comment is untouched"
    );
    assert_eq!(
        world.github.lock().unwrap().comment_updates,
        updates_before,
        "no write went to it"
    );
    assert!(!ledger_pending(&mut processor, 1));
}

/// A status comment the store learned of only by LISTING — its post's
/// acknowledgement was lost, and the terminal sync resolved it — is a
/// status comment all the same: never neutralized.
#[test]
fn a_status_comment_resolved_by_the_terminal_sync_is_never_neutralized() {
    let (mut world, mut processor, live) = train_with_orphaned_status_comment();
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    drain(&mut processor);
    assert!(
        processor.owed_status_comments().is_empty(),
        "precondition: the terminal sync resolved and rewrote the comment"
    );
    let restored = world.github.lock().unwrap().comments[&live].body.clone();

    // The harness mirrors the edit into the fake; the terminal update has
    // since restored the comment (the delayed-webhook shape).
    let forged = forged_ledger_body(1);
    let hook = bot_comment_webhook(&world.config, 1, live.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    restored_by_bot(&world, live, &restored);
    drain(&mut processor);
    tick(&world, &mut processor);
    assert_eq!(
        world.github.lock().unwrap().comments[&live].body,
        restored,
        "the terminal record is untouched"
    );
    assert!(!ledger_pending(&mut processor, 1));
}

/// A maintainer edits a status comment into a ledger before the post's
/// outcome is processed, so the edit webhook names a comment the store
/// does not yet know is a status comment, and queues a repair. Recording
/// the post cancels that repair; nothing neutralizes the comment.
#[test]
fn a_status_comment_named_before_its_post_is_recorded_is_never_neutralized() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);
    start_command(&mut world, &mut processor, 1);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    // Run the cascade up to and through the batch that POSTS the status
    // comment; in the window before its outcome is observed, the edit
    // webhook arrives.
    let mut next = pump_cascade(&mut processor);
    let mut status_id = None;
    let mut steps = 0;
    while let Some(batch) = next {
        steps += 1;
        assert!(steps < 200, "no status post");
        let posts_status = batch.effects.iter().any(|e| {
            matches!(e, Effect::GitHub(GitHubEffect::PostComment { body, .. })
                if crate::status::parse_status_comment(body).is_ok())
        });
        if !posts_status {
            let outcomes = execute(&mut processor, &batch);
            next = processor
                .on_outcomes(batch.root, outcomes, batch.feedback)
                .unwrap();
            continue;
        }
        let interpreter = WorktreeGitInterpreter::new(processor.git_config(), batch.root);
        let landed = execute_batch(&interpreter, processor.github(), &batch);
        let id = {
            let github = world.github.lock().unwrap();
            *github
                .comments
                .iter()
                .find(|(_, c)| {
                    c.author_id == TEST_BOT_ID
                        && crate::status::parse_status_comment(&c.body).is_ok()
                })
                .map(|(id, _)| id)
                .expect("the status comment landed")
        };
        let forged = forged_ledger_body(1);
        let hook = bot_comment_webhook(&world.config, 1, id.0, "edited", &forged);
        world.enqueue(&mut processor, "issue_comment", hook);
        while let Some(delivery) = processor.claim().unwrap() {
            processor.process_claimed(delivery).unwrap();
        }
        assert_eq!(
            processor
                .store_mut()
                .ledger_repairs(PrNumber(1))
                .unwrap()
                .len(),
            1,
            "precondition: the edit queued a repair against a comment not yet known"
        );
        processor.note_best_effort(&landed.best_effort).unwrap();
        next = processor
            .on_outcomes(batch.root, landed.observed, batch.feedback)
            .unwrap();
        status_id = Some(id);
        break;
    }
    let status_id = status_id.expect("the status comment was posted");
    finish_batches(&mut world, &mut processor, next);
    let recorded = world.github.lock().unwrap().comments[&status_id]
        .body
        .clone();
    assert!(
        crate::status::parse_status_comment(&recorded).is_ok(),
        "precondition: the comment reads as a status comment"
    );
    drain(&mut processor);
    tick(&world, &mut processor);
    assert!(
        processor
            .store_mut()
            .ledger_repairs(PrNumber(1))
            .unwrap()
            .is_empty(),
        "recording the post cancelled the repair"
    );
    assert_eq!(
        world.github.lock().unwrap().comments[&status_id].body,
        recorded,
        "the status comment is untouched"
    );
}

/// A status comment whose post landed while the process died before the
/// event recording it: the store knows no such comment yet, and a delayed
/// edit webhook names it as a ledger. Nothing is neutralized on that PR
/// until the status machinery has resolved the id — and once it has, the
/// repair is cancelled.
#[test]
fn an_orphaned_status_comment_is_never_neutralized() {
    let (mut world, mut processor, live) = train_with_orphaned_status_comment();
    let before = world.github.lock().unwrap().comments[&live].body.clone();
    let forged = forged_ledger_body(1);
    let hook = bot_comment_webhook(&world.config, 1, live.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    // The harness mirrors the edit into the fake; the status machinery
    // has since restored the comment (the delayed-webhook shape).
    restored_by_bot(&world, live, &before);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    assert_eq!(
        processor
            .store_mut()
            .ledger_repairs(PrNumber(1))
            .unwrap()
            .len(),
        1,
        "precondition: the edit queued a repair against a comment the store cannot place"
    );
    // The train runs on (its status machinery rewrites the comment as it
    // goes) and retires; the terminal sync resolves the comment by
    // incarnation and registers it. At no point is it neutralized.
    drain(&mut processor);
    tick(&world, &mut processor);
    tick(&world, &mut processor);
    assert!(
        crate::status::parse_status_comment(&world.github.lock().unwrap().comments[&live].body)
            .is_ok(),
        "the status comment reads as a status comment throughout"
    );
    assert!(processor.owed_status_comments().is_empty());
    assert!(
        processor
            .store_mut()
            .ledger_repairs(PrNumber(1))
            .unwrap()
            .is_empty(),
        "registering the comment cancelled the repair"
    );
    assert!(!ledger_pending(&mut processor, 1));
}

/// A train stopped after its status comment landed but before the event
/// recording it: the terminal record lingers with no id, and the terminal
/// sync resolves the comment without touching that record. Ledger repairs
/// on the PR must not stay deferred behind it for ever.
#[test]
fn a_retired_trains_unrecorded_status_comment_does_not_defer_repairs_for_ever() {
    let (mut world, mut processor, live) = train_with_orphaned_status_comment();
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    drain(&mut processor);
    tick(&world, &mut processor);
    assert!(
        processor.owed_status_comments().is_empty(),
        "precondition: the sync landed"
    );
    assert!(
        crate::status::parse_status_comment(&world.github.lock().unwrap().comments[&live].body)
            .is_ok()
    );

    let forged = forged_ledger_body(1);
    let sibling = plant_bot_comment(&world, 1, &forged);
    let hook = bot_comment_webhook(&world.config, 1, sibling.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    drain(&mut processor);
    tick(&world, &mut processor);
    assert!(
        crate::status::parse_stack_ledger(&world.github.lock().unwrap().comments[&sibling].body)
            .is_none(),
        "the forgery is neutralized, not deferred behind a retired train"
    );
    assert!(!ledger_pending(&mut processor, 1));
}

/// A sync is a rewrite and a discovery. The process dies after the
/// rewrite that rode along with the probe acknowledged, before the listing
/// is processed: the obligation must still stand, so the restart lists
/// again and finds what that listing would have shown.
#[test]
fn a_crash_between_the_rewrites_acknowledgement_and_the_listing_keeps_the_sync_owed() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let forged = forged_ledger_body(2);
    let sibling = plant_bot_comment(&world, 2, &forged); // no webhook for it
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    processor.requeue_marked_recoveries().unwrap();
    let batch = processor.pump().unwrap().expect("the sync");
    assert!(
        !batch.best_effort.is_empty() && !batch.effects.is_empty(),
        "precondition: the rewrite rides along with the probe"
    );
    let interpreter = WorktreeGitInterpreter::new(processor.git_config(), batch.root);
    let landed = execute_batch(&interpreter, processor.github(), &batch);
    processor.note_best_effort(&landed.best_effort).unwrap();
    assert!(
        ledger_pending(&mut processor, 2),
        "the rewrite's acknowledgement alone discharges nothing"
    );
    drop(processor);

    let mut processor = world.processor();
    drain(&mut processor);
    assert!(
        crate::status::parse_stack_ledger(&world.github.lock().unwrap().comments[&sibling].body)
            .is_none(),
        "the restart's sync discovered and neutralized the forgery"
    );
    assert!(!ledger_pending(&mut processor, 2));
    assert_ledgers_match_store(&world, &processor);
}

/// A repair-only sync: the neutralization that rode along acknowledges
/// and clears the PR's last repair, and the process dies before the
/// listing is processed. The discovery is durably owed, so the restart
/// lists again and finds the second forgery that listing would have shown.
#[test]
fn a_repair_only_sync_keeps_its_discovery_owed_across_a_crash() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let forged = forged_ledger_body(2);
    let named = plant_bot_comment(&world, 2, &forged);
    let unnamed = plant_bot_comment(&world, 2, &forged); // its webhook never comes
    let hook = bot_comment_webhook(&world.config, 2, named.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let batch = processor.pump().unwrap().expect("the sync");
    let interpreter = WorktreeGitInterpreter::new(processor.git_config(), batch.root);
    let landed = execute_batch(&interpreter, processor.github(), &batch);
    processor.note_best_effort(&landed.best_effort).unwrap();
    assert!(
        processor
            .store_mut()
            .ledger_repairs(PrNumber(2))
            .unwrap()
            .is_empty(),
        "precondition: the named forgery's repair acknowledged"
    );
    assert!(
        ledger_pending(&mut processor, 2),
        "the discovery is still owed"
    );
    drop(processor);

    let mut processor = world.processor();
    drain(&mut processor);
    for id in [named, unnamed] {
        assert!(
            crate::status::parse_stack_ledger(&world.github.lock().unwrap().comments[&id].body)
                .is_none(),
            "neutralized"
        );
    }
    assert!(!ledger_pending(&mut processor, 2));
}

/// Absence evidence is dated by the listing, not by the end of the batch
/// it rode in: the neutralizations beside a listing can take longer than
/// the cooldown, and two listings taken within one consistency window are
/// one piece of evidence however long their batches ran.
#[test]
fn absence_evidence_is_dated_by_the_listing_not_by_the_batchs_end() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let (orphan, first) = ledgers_on(&world, 2)[0];
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(orphan);
    processor
        .store_mut()
        .append_batch(
            &[StateEventPayload::StackLedgerRetired {
                pr: PrNumber(2),
                comment_id: orphan,
            }],
            Utc::now(),
        )
        .unwrap();
    processor
        .store_mut()
        .add_unresolved_ledger(PrNumber(2), None, first.seq, true)
        .unwrap();
    drop(processor);
    let mut processor = world.processor();
    drain(&mut processor); // absence 1, dated now

    // A second listing dispatched within the same window, whose batch
    // takes longer than the cooldown to complete.
    processor.requeue_marked_recoveries().unwrap();
    let batch = processor.pump().unwrap().expect("the second look");
    world.advance_past_cooldown();
    let outcomes = execute(&mut processor, &batch);
    let next = processor
        .on_outcomes(batch.root, outcomes, batch.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "two listings in one window are one absence: no replacement posted"
    );
    assert!(ledger_pending(&mut processor, 2));

    world.github.lock().unwrap().hidden_from_listings.clear();
    tick(&world, &mut processor);
    assert_eq!(ledgers_on(&world, 2)[0].0, orphan, "adopted once shown");
    assert!(!ledger_pending(&mut processor, 2));
}

/// A forgery named while a train inherited mid-flight has not yet been
/// recovered — its recorded status comment id may be stale — is not
/// neutralized until recovery has run; then it is.
#[test]
fn a_forgery_named_during_inherited_recovery_is_neutralized_after_it() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    run_batches_then_crash(&mut world, processor, 4);

    let mut processor = world.processor();
    let forged = forged_ledger_body(1);
    let sibling = plant_bot_comment(&world, 1, &forged);
    let hook = bot_comment_webhook(&world.config, 1, sibling.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    drive_to_completion(&mut world, &mut processor);
    tick(&world, &mut processor);
    let github = world.github.lock().unwrap();
    assert!(
        crate::status::parse_stack_ledger(&github.comments[&sibling].body).is_none(),
        "the forgery is neutralized once recovery has run"
    );
    assert!(
        github.comments.values().any(|c| c.pr == PrNumber(1)
            && c.author_id == TEST_BOT_ID
            && crate::status::parse_status_comment(&c.body).is_ok()),
        "the train's status comment stands"
    );
    drop(github);
    assert!(!ledger_pending(&mut processor, 1));
}

/// A ledger neutralized (its id retired by a passing 404, then named by an
/// edit) and adopted back keeps no "reads as nothing" verdict: adopting
/// it lifts the verdict, so a later listing can still find it — otherwise
/// a second retirement and a replacement would leave it standing as a
/// duplicate that discovery skips for ever.
#[test]
fn re_adopting_a_neutralized_ledger_lifts_its_verdict() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let (ledger_id, _) = ledgers_on(&world, 2)[0];
    let forged = forged_ledger_body(2);

    // Retired by a passing 404 on a rewrite...
    {
        let mut github = world.github.lock().unwrap();
        github.comments.get_mut(&ledger_id).unwrap().body = forged.clone();
        github.update_comment_notfound = true;
    }
    let hook = bot_comment_webhook(&world.config, 2, ledger_id.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    drain(&mut processor);
    assert_eq!(processor.state().prs[&PrNumber(2)].ledger_comment_id, None);
    world.github.lock().unwrap().update_comment_notfound = false;
    // ...then named by an edit while unrecorded (a repair), and adopted
    // back through its own row on the next look.
    let hook = bot_comment_webhook(&world.config, 2, ledger_id.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    drain(&mut processor);
    tick(&world, &mut processor);
    tick(&world, &mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].ledger_comment_id,
        Some(ledger_id),
        "precondition: adopted back"
    );
    assert!(
        !processor
            .store_mut()
            .settled_ledger_comments(PrNumber(2))
            .unwrap()
            .iter()
            .any(|(id, _)| *id == ledger_id),
        "no verdict stands against the ledger"
    );

    // Retired again, absent from two spaced listings, replaced — and then
    // shown again: a duplicate, found and neutralized.
    {
        let mut github = world.github.lock().unwrap();
        github.comments.get_mut(&ledger_id).unwrap().body = forged.clone();
        github.update_comment_notfound = true;
        github.hidden_from_listings.insert(ledger_id);
    }
    let hook = bot_comment_webhook(&world.config, 2, ledger_id.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    drain(&mut processor);
    world.github.lock().unwrap().update_comment_notfound = false;
    tick(&world, &mut processor);
    tick(&world, &mut processor);
    let replacement = processor.state().prs[&PrNumber(2)]
        .ledger_comment_id
        .expect("a replacement was posted");
    assert_ne!(replacement, ledger_id);
    world.github.lock().unwrap().hidden_from_listings.clear();
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    tick(&world, &mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "the reappearing original is neutralized");
    assert_eq!(after[0].0, replacement);
    assert!(!ledger_pending(&mut processor, 2));
}

/// Absence spacing is measured from the previous listing's PROCESSING to
/// the next one's dispatch: a first listing delayed on the wire past the
/// cooldown, and a second dispatched the moment it is processed, are one
/// consistency window, not two.
#[test]
fn a_delayed_first_listing_does_not_make_the_next_one_independent() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let (orphan, first) = ledgers_on(&world, 2)[0];
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    world
        .github
        .lock()
        .unwrap()
        .hidden_from_listings
        .insert(orphan);
    processor
        .store_mut()
        .append_batch(
            &[StateEventPayload::StackLedgerRetired {
                pr: PrNumber(2),
                comment_id: orphan,
            }],
            Utc::now(),
        )
        .unwrap();
    processor
        .store_mut()
        .add_unresolved_ledger(PrNumber(2), None, first.seq, true)
        .unwrap();
    drop(processor);
    let mut processor = world.processor();

    // The first listing: dispatched now, processed a long time later.
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let batch = processor.pump().unwrap().expect("the first look");
    world.advance_past_cooldown();
    let outcomes = execute(&mut processor, &batch);
    let next = processor
        .on_outcomes(batch.root, outcomes, batch.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    // The second, dispatched the moment the first was processed.
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        1,
        "two looks within one window are one absence: no replacement posted"
    );
    assert!(ledger_pending(&mut processor, 2));

    world.github.lock().unwrap().hidden_from_listings.clear();
    tick(&world, &mut processor);
    assert_eq!(ledgers_on(&world, 2)[0].0, orphan, "adopted once shown");
    assert!(!ledger_pending(&mut processor, 2));
}

/// A terminal sync still owed for a root means its status comment's
/// identity is unverified, whatever id the sync carries: repairs on that
/// PR wait for the sync to land, then proceed.
#[test]
fn repairs_wait_for_an_owed_terminal_sync_whatever_id_it_carries() {
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
    // The user stops mid-step. The step posts the status comment; the
    // stop's terminal update then fails once, so the sync is owed and
    // names that comment.
    let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 7);
    world.enqueue(&mut processor, "issue_comment", stop);
    claim_all(&mut processor);
    let outcomes = execute(&mut processor, &step);
    let status_id = {
        let github = world.github.lock().unwrap();
        *github
            .comments
            .iter()
            .find(|(_, c)| {
                c.author_id == TEST_BOT_ID && crate::status::parse_status_comment(&c.body).is_ok()
            })
            .map(|(id, _)| id)
            .expect("the step posted the status comment")
    };
    world.github.lock().unwrap().update_comment_broken = true;
    let cleanup = processor
        .on_outcomes(step.root, outcomes, step.feedback)
        .unwrap()
        .expect("the stop's cleanup batch");
    finish_batches(&mut world, &mut processor, Some(cleanup));
    assert_eq!(
        processor.owed_status_comments(),
        vec![Some(status_id)],
        "precondition: the sync is owed and names the comment"
    );
    // Writes work again, but the listing hides the status comment, so the
    // sync's retry keeps missing it and stays owed.
    {
        let mut github = world.github.lock().unwrap();
        github.update_comment_broken = false;
        github.hidden_from_listings.insert(status_id);
    }

    // A forgery named while the sync is owed: deferred, not neutralized,
    // however many looks the ledger machinery takes meanwhile.
    let forged = forged_ledger_body(1);
    let sibling = plant_bot_comment(&world, 1, &forged);
    let hook = bot_comment_webhook(&world.config, 1, sibling.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    drain(&mut processor);
    assert_eq!(
        processor.owed_status_comments(),
        vec![Some(status_id)],
        "precondition: the sync is still owed (one absent look is not proof)"
    );
    assert_eq!(
        world.github.lock().unwrap().comments[&sibling].body,
        forged,
        "untouched while the terminal sync is owed"
    );
    assert!(
        ledger_pending(&mut processor, 1),
        "the repair is deferred, not dropped"
    );

    // The listing shows the status comment again; the sync lands; then
    // the forgery is neutralized.
    world.github.lock().unwrap().hidden_from_listings.clear();
    tick(&world, &mut processor);
    tick(&world, &mut processor);
    assert!(
        processor.owed_status_comments().is_empty(),
        "the sync landed"
    );
    assert!(
        crate::status::parse_stack_ledger(&world.github.lock().unwrap().comments[&sibling].body)
            .is_none(),
        "and then the forgery was neutralized"
    );
    assert!(!ledger_pending(&mut processor, 1));
}

/// A listed forgery carrying the very sequence number the post about to
/// be made will carry must not claim the post's unresolved row: the row
/// is written after the discovery, so a lost response still leaves the
/// question rather than a duplicate.
#[test]
fn a_forgery_with_the_posts_sequence_number_cannot_claim_its_row() {
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
    // A forgery whose sequence number is exactly what the post will state.
    let seq = processor.store_mut().next_seq().saturating_sub(1);
    let forged = crate::status::format_stack_ledger(&crate::status::StackLedger {
        pr: PrNumber(2),
        declared: Some(crate::status::Declaration {
            predecessor: PrNumber(9),
            owner: crate::types::CommentId(1),
        }),
        seq,
        settled_through: None,
    });
    let forgery = plant_bot_comment(&world, 2, &forged); // no webhook for it
    world.github.lock().unwrap().post_comment_response_lost = true;
    run_sagas(&mut processor);
    let posted = {
        let github = world.github.lock().unwrap();
        *github
            .comments
            .iter()
            .filter(|(id, c)| c.pr == PrNumber(2) && c.author_id == TEST_BOT_ID && **id != forgery)
            .filter(|(_, c)| crate::status::parse_stack_ledger(&c.body).is_some())
            .map(|(id, _)| id)
            .next()
            .expect("the post landed, its response lost")
    };
    assert!(
        processor
            .store_mut()
            .unresolved_ledgers(PrNumber(2))
            .unwrap()
            .iter()
            .any(|r| r.comment_id.is_none() && r.seq == seq && r.ours),
        "the post's own row stands: the forgery did not claim it"
    );

    // The next listing omits the post: no second post.
    {
        let mut github = world.github.lock().unwrap();
        github.post_comment_response_lost = false;
        github.hidden_from_listings.insert(posted);
    }
    tick(&world, &mut processor);
    assert!(
        !world.github.lock().unwrap().comments.iter().any(|(id, c)| {
            *id != posted
                && *id != forgery
                && c.pr == PrNumber(2)
                && c.author_id == TEST_BOT_ID
                && crate::status::parse_stack_ledger(&c.body).is_some()
        }),
        "no duplicate posted on one absent listing"
    );
    world.github.lock().unwrap().hidden_from_listings.clear();
    tick(&world, &mut processor);
    tick(&world, &mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "one ledger; the forgery neutralized");
    assert_eq!(after[0].0, posted);
    assert!(!ledger_pending(&mut processor, 2));
}

/// The recorded ledger is doctored; its rewrite answers a passing 404, so
/// the id is retired and the comment watched as the store's own. The
/// listing then shows it with a STALE plain-text body. The body is the
/// listing's word, not evidence: the comment is adopted back and rewritten
/// by id — never concluded harmless and left standing as a forgery.
#[test]
fn a_retired_ledger_shown_with_a_stale_body_is_adopted_back_not_believed() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let (ledger_id, before) = ledgers_on(&world, 2)[0];

    let forged = forged_ledger_body(2);
    {
        let mut github = world.github.lock().unwrap();
        github.comments.get_mut(&ledger_id).unwrap().body = forged.clone();
        github
            .stale_listing_bodies
            .insert(ledger_id, "vandalized".to_owned());
        github.update_comment_notfound = true;
    }
    let hook = bot_comment_webhook(&world.config, 2, ledger_id.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].ledger_comment_id,
        None,
        "precondition: the 404 retired the id"
    );

    world.github.lock().unwrap().update_comment_notfound = false;
    tick(&world, &mut processor);
    tick(&world, &mut processor);
    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "one ledger, no replacement posted");
    assert_eq!(after[0].0, ledger_id, "the same comment, adopted back");
    assert_eq!(
        after[0].1.declared, before.declared,
        "and rewritten to the truth"
    );
    assert!(!ledger_pending(&mut processor, 2));
    assert_ledgers_match_store(&world, &processor);
}

/// A sync is a rewrite AND a discovery. When the listing fails but the
/// rewrite that rode along lands, the obligation it cleared is owed again
/// so the discovery happens: a forgery whose webhook never came is found
/// once listings work.
#[test]
fn a_failed_listing_owes_the_sync_again_even_when_the_rewrite_landed() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let forged = forged_ledger_body(2);
    let _sibling = plant_bot_comment(&world, 2, &forged); // no webhook for it
    world.github.lock().unwrap().list_comments_broken = true;
    processor.store_mut().mark_ledger_owed(PrNumber(2)).unwrap();
    processor.requeue_marked_recoveries().unwrap();
    run_sagas(&mut processor);
    assert_eq!(
        ledgers_on(&world, 2).len(),
        2,
        "precondition: the forgery is undiscovered"
    );
    assert!(
        ledger_pending(&mut processor, 2),
        "the failed discovery keeps the sync owed"
    );

    world.github.lock().unwrap().list_comments_broken = false;
    tick(&world, &mut processor);
    assert_eq!(ledgers_on(&world, 2).len(), 1, "discovered and neutralized");
    assert!(!ledger_pending(&mut processor, 2));
}

/// A neutralization's 404 outcome is evidence about the dispatch it
/// answers, not about a repair an edit webhook re-raised meanwhile: the
/// newer repair stands, is retried, and lands (Codex ledger review of the
/// hardening, P2).
#[test]
fn a_404_outcome_does_not_discard_a_repair_re_raised_meanwhile() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);

    let forged = forged_ledger_body(2);
    let sibling = plant_bot_comment(&world, 2, &forged);
    let hook = bot_comment_webhook(&world.config, 2, sibling.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let batch = processor
        .pump()
        .unwrap()
        .expect("the sync that neutralizes");
    world.github.lock().unwrap().update_comment_notfound = true;
    let interpreter = WorktreeGitInterpreter::new(processor.git_config(), batch.root);
    let landed = execute_batch(&interpreter, processor.github(), &batch);
    world.github.lock().unwrap().update_comment_notfound = false;

    // Re-forged and reported before the 404 outcome is processed.
    let hook = bot_comment_webhook(&world.config, 2, sibling.0, "edited", &forged);
    world.enqueue(&mut processor, "issue_comment", hook);
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    processor.note_best_effort(&landed.best_effort).unwrap();
    assert_eq!(
        processor
            .store_mut()
            .ledger_repairs(PrNumber(2))
            .unwrap()
            .len(),
        1,
        "the re-raised repair survives the older dispatch's 404"
    );
    assert!(
        processor
            .store_mut()
            .unresolved_ledgers(PrNumber(2))
            .unwrap()
            .is_empty(),
        "and is not demoted to a watched comment"
    );
    let next = processor
        .on_outcomes(batch.root, landed.observed, batch.feedback)
        .unwrap();
    finish_batches(&mut world, &mut processor, next);
    tick(&world, &mut processor);
    assert_eq!(ledgers_on(&world, 2).len(), 1, "neutralized on the retry");
    assert!(!ledger_pending(&mut processor, 2));
}

/// A deleted comment can linger in GitHub's listing cache. Its deletion
/// webhook is proof it is gone: the ghost is neither adopted nor repaired,
/// and a fresh ledger is posted (Codex ledger review round 22).
#[test]
fn a_ghost_of_a_deleted_ledger_is_not_adopted() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let (original, _) = ledgers_on(&world, 2)[0];
    {
        let mut github = world.github.lock().unwrap();
        let removed = github.comments.remove(&original).unwrap();
        github.stale_listing_ghosts.insert(original, removed);
    }
    let hook = bot_comment_webhook(&world.config, 2, original.0, "deleted", "");
    world.enqueue(&mut processor, "issue_comment", hook);
    drain(&mut processor);

    let after = ledgers_on(&world, 2);
    assert_eq!(after.len(), 1, "a fresh ledger");
    assert_ne!(after[0].0, original, "not the ghost");
    assert_eq!(
        processor.state().prs[&PrNumber(2)].ledger_comment_id,
        Some(after[0].0)
    );
    assert!(!ledger_pending(&mut processor, 2));
}

/// The ledger's convergence property: whatever the users do to their
/// declarations and to the ledger comments themselves, whatever GitHub
/// refuses along the way, and however often the process dies between a
/// write landing and its acknowledgement, the ledger comments end up
/// stating exactly what the store holds — one per PR.
mod ledger_property {
    use proptest::prelude::*;

    use super::*;

    /// One change to the declared topology or to a ledger comment, as a
    /// user makes it.
    #[derive(Debug, Clone, Copy)]
    enum TopologyAction {
        /// PR `pr` declares `target` as its predecessor, in a fresh comment.
        Declare { pr: u64, target: u64 },
        /// The author deletes the comment that last declared on `pr`.
        Retract { pr: u64 },
        /// The author edits that comment into something that declares nothing.
        EditAway { pr: u64 },
        /// A maintainer deletes the bot's ledger comment on `pr`. Without
        /// the webhook, the store learns of it only when a write to it
        /// 404s — so a silent deletion is restored by the NEXT write to
        /// that PR, not before, and the generator does not produce one:
        /// the property's premise is that webhooks arrive. The pinned case
        /// below follows a silent deletion with the declaration that
        /// heals it.
        DeleteLedger { pr: u64, webhook: bool },
        /// A maintainer edits the bot's ledger comment on `pr` into a forgery.
        EditLedger { pr: u64 },
    }

    /// What goes wrong while an action is processed.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Fault {
        None,
        /// GitHub refuses comment edits: the ledger write fails and stays owed.
        RefusedWrites,
        /// The ledger write LANDS, and the process dies before observing its
        /// outcome: the obligation is still on the books, and a post's
        /// comment id was never recorded.
        CrashBeforeAck,
        /// The ledger write LANDS, and a maintainer deletes the comment it
        /// wrote — webhook processed — before its outcome is observed.
        DeleteBeforeAck,
    }

    fn arb_step() -> impl Strategy<Value = (TopologyAction, Fault)> {
        let action = prop_oneof![
            // Weighted towards the edge that VALIDATES (a PR's base is its
            // predecessor's branch), or nothing downstream ever holds a
            // declaration for a retraction to retract.
            3 => (2u64..=3).prop_map(|pr| TopologyAction::Declare { pr, target: pr - 1 }),
            1 => (2u64..=3, 1u64..=3)
                .prop_map(|(pr, target)| TopologyAction::Declare { pr, target }),
            2 => (2u64..=3).prop_map(|pr| TopologyAction::Retract { pr }),
            1 => (2u64..=3).prop_map(|pr| TopologyAction::EditAway { pr }),
            1 => (2u64..=3).prop_map(|pr| TopologyAction::DeleteLedger { pr, webhook: true }),
            1 => (2u64..=3).prop_map(|pr| TopologyAction::EditLedger { pr }),
        ];
        let fault = prop_oneof![
            3 => Just(Fault::None),
            1 => Just(Fault::RefusedWrites),
            2 => Just(Fault::CrashBeforeAck),
            2 => Just(Fault::DeleteBeforeAck),
        ];
        (action, fault)
    }

    /// A comment webhook with an EXACT id, by the author.
    fn topology_comment(
        config: &GitConfig,
        pr: u64,
        text: &str,
        comment_id: u64,
        action: &str,
    ) -> Vec<u8> {
        comment_webhook(config, pr, text, comment_id, action, AUTHOR, AUTHOR)
    }

    /// A comment webhook for a comment AUTHORED by `author_id`, whose change
    /// `sender_id` made — GitHub reports the bot as the author of its own
    /// comments however a maintainer edits them.
    fn comment_webhook(
        config: &GitConfig,
        pr: u64,
        text: &str,
        comment_id: u64,
        action: &str,
        author_id: u64,
        sender_id: u64,
    ) -> Vec<u8> {
        format!(
            r#"{{
                "action": "{action}",
                "comment": {{
                    "id": {comment_id},
                    "body": "{text}",
                    "user": {{ "id": {author_id}, "login": "someone" }},
                    "updated_at": "{updated_at}"
                }},
                "issue": {{
                    "number": {pr},
                    "pull_request": {{ "url": "..." }},
                    "user": {{ "id": {AUTHOR}, "login": "author" }}
                }},
                "repository": {repo},
                "sender": {{ "id": {sender_id}, "login": "someone" }}
            }}"#,
            updated_at = unique_updated_at(),
            repo = repo_json(config),
        )
        .into_bytes()
    }

    /// Runs one generated history and asserts the ledger invariant at the
    /// end of it.
    fn ledger_property_case(steps: &[(TopologyAction, Fault)]) {
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
        for (action, fault) in steps {
            world.github.lock().unwrap().update_comment_broken = *fault == Fault::RefusedWrites;
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
                TopologyAction::DeleteLedger { pr, webhook } => {
                    ledgers_on(&world, pr).first().and_then(|(id, _)| {
                        world.github.lock().unwrap().comments.remove(id);
                        webhook.then(|| {
                            comment_webhook(
                                &world.config,
                                pr,
                                "",
                                id.0,
                                "deleted",
                                TEST_BOT_ID,
                                AUTHOR,
                            )
                        })
                    })
                }
                TopologyAction::EditLedger { pr } => {
                    ledgers_on(&world, pr).first().map(|(id, _)| {
                        let forged =
                            crate::status::format_stack_ledger(&crate::status::StackLedger {
                                pr: PrNumber(pr),
                                declared: Some(crate::status::Declaration {
                                    predecessor: PrNumber(9),
                                    owner: crate::types::CommentId(1),
                                }),
                                seq: u64::MAX,
                                settled_through: None,
                            });
                        world
                            .github
                            .lock()
                            .unwrap()
                            .comments
                            .get_mut(id)
                            .unwrap()
                            .body = forged;
                        comment_webhook(
                            &world.config,
                            pr,
                            "doctored",
                            id.0,
                            "edited",
                            TEST_BOT_ID,
                            AUTHOR,
                        )
                    })
                }
            };
            if let Some(body) = delivery {
                world.enqueue(&mut processor, "issue_comment", body);
                match fault {
                    Fault::CrashBeforeAck => {
                        processor = drain_crashing_before_ack(&world, processor);
                    }
                    Fault::DeleteBeforeAck => {
                        drain_interfering_before_ack(
                            &mut world,
                            &mut processor,
                            Interference::Delete,
                        );
                    }
                    Fault::None | Fault::RefusedWrites => drain(&mut processor),
                }
            }
        }

        // The worker restarts — obligations are durable, so a ledger owed
        // when the process died is still owed when it comes back — GitHub
        // answers again, and it gets its retries: whatever the failures
        // were, the ledgers must converge on what the store holds.
        drop(processor);
        let mut processor = world.processor();
        world.github.lock().unwrap().update_comment_broken = false;
        next_comment += 10;
        let remark = topology_comment(&world.config, 2, "a remark", next_comment, "created");
        world.enqueue(&mut processor, "issue_comment", remark);
        drain(&mut processor);
        for _ in 0..4 {
            if processor
                .store_mut()
                .owed_stack_ledgers()
                .unwrap()
                .is_empty()
            {
                break;
            }
            tick(&world, &mut processor);
        }

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
            cases: 24,
            ..ProptestConfig::default()
        })]

        /// However the declarations are made, retracted, edited away and
        /// remade, however the ledger comments are deleted or doctored, and
        /// however many writes GitHub refuses or the process fails to
        /// acknowledge while it happens — once GitHub answers again every
        /// PR's ledger comment states exactly the declaration the store
        /// holds, and there is exactly one of them per PR.
        ///
        /// This is the property a lost-DB crawl depends on: it READS these
        /// comments instead of re-deriving the topology from the users'.
        #[test]
        fn ledgers_converge_on_the_declarations_the_store_holds(
            steps in proptest::collection::vec(arb_step(), 1..7),
        ) {
            ledger_property_case(&steps);
        }
    }

    /// The shrunk cases worth keeping as examples: a declaration remade after
    /// a retraction; a write that fails before the one that lands; a post
    /// whose acknowledgement the crash ate (the orphan is adopted); a
    /// deletion whose replacement post is likewise unacknowledged; a
    /// deletion whose webhook never comes (the next write's 404 heals it);
    /// and a doctored ledger behind refused writes.
    #[test]
    fn ledger_property_fixed_cases() {
        ledger_property_case(&[
            (TopologyAction::Declare { pr: 2, target: 1 }, Fault::None),
            (TopologyAction::Retract { pr: 2 }, Fault::None),
            (TopologyAction::Declare { pr: 2, target: 1 }, Fault::None),
        ]);
        ledger_property_case(&[
            (TopologyAction::Declare { pr: 2, target: 1 }, Fault::None),
            (TopologyAction::Retract { pr: 2 }, Fault::RefusedWrites),
            (
                TopologyAction::Declare { pr: 3, target: 2 },
                Fault::RefusedWrites,
            ),
        ]);
        ledger_property_case(&[
            (TopologyAction::Declare { pr: 3, target: 2 }, Fault::None),
            (TopologyAction::EditAway { pr: 3 }, Fault::None),
        ]);
        ledger_property_case(&[
            (
                TopologyAction::Declare { pr: 2, target: 1 },
                Fault::CrashBeforeAck,
            ),
            (TopologyAction::Retract { pr: 2 }, Fault::CrashBeforeAck),
        ]);
        ledger_property_case(&[
            (TopologyAction::Declare { pr: 2, target: 1 }, Fault::None),
            (
                TopologyAction::DeleteLedger {
                    pr: 2,
                    webhook: true,
                },
                Fault::CrashBeforeAck,
            ),
            (TopologyAction::Declare { pr: 3, target: 2 }, Fault::None),
        ]);
        ledger_property_case(&[
            (TopologyAction::Declare { pr: 2, target: 1 }, Fault::None),
            (
                TopologyAction::DeleteLedger {
                    pr: 2,
                    webhook: false,
                },
                Fault::None,
            ),
            (TopologyAction::Declare { pr: 2, target: 1 }, Fault::None),
        ]);
        ledger_property_case(&[
            (TopologyAction::Declare { pr: 2, target: 1 }, Fault::None),
            (TopologyAction::EditLedger { pr: 2 }, Fault::RefusedWrites),
        ]);
        // Two identical edits of one comment are two edits, not a
        // redelivery: the harness gives them distinct timestamps, as
        // GitHub does.
        ledger_property_case(&[
            (TopologyAction::Declare { pr: 2, target: 1 }, Fault::None),
            (TopologyAction::EditLedger { pr: 2 }, Fault::CrashBeforeAck),
            (TopologyAction::EditLedger { pr: 2 }, Fault::None),
        ]);
        // The history CI drew (nix-build, 2026-09-13): a deletion before
        // the acknowledgement on one PR, then on another.
        ledger_property_case(&[
            (TopologyAction::Declare { pr: 2, target: 1 }, Fault::None),
            (
                TopologyAction::Declare { pr: 3, target: 2 },
                Fault::DeleteBeforeAck,
            ),
            (TopologyAction::Retract { pr: 2 }, Fault::DeleteBeforeAck),
        ]);
    }

    /// One adversarial move against the ledger machinery: tampering,
    /// GitHub misbehaving, or a process death.
    #[derive(Debug, Clone, Copy)]
    enum AdversarialAction {
        /// A user (re)declares `pr`'s predecessor in a fresh comment. With
        /// `crash`, the process dies after the resulting write lands and
        /// before it is acknowledged.
        Declare { pr: u64, crash: bool },
        /// A maintainer edits `pr`'s highest-ranked ledger comment into a
        /// forgery with an unbeatable sequence number. When `stale`, the
        /// listing cache keeps serving the pre-edit body.
        TamperLedger { pr: u64, stale: bool },
        /// A maintainer edits the marker out of `pr`'s ledger comment,
        /// leaving plain text. When `stale`, the listing cache keeps
        /// serving the pre-edit body.
        EditAwayLedger { pr: u64, stale: bool },
        /// A maintainer edits some other bot reply on `pr` into a forged
        /// ledger. When `stale`, the listing cache keeps serving the
        /// reply's harmless pre-edit body.
        ForgeSibling { pr: u64, stale: bool },
        /// A maintainer deletes `pr`'s ledger comment; the webhook may
        /// arrive only after everything else, and the listing cache may
        /// keep serving the dead comment as a ghost.
        DeleteLedger {
            pr: u64,
            delayed_webhook: bool,
            ghost: bool,
        },
        /// GitHub's listings transiently omit every comment currently on
        /// `pr` (they still exist; writes by id still reach them).
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
        /// Listings fail outright (writes still land)...
        BreakListings,
        /// ...and work again.
        HealListings,
        /// The worker dies at quiescence; a fresh one takes over the store.
        Restart,
        /// Time passes mid-adversity: the stall timer fires and the
        /// machinery retries WHILE the world is still lying to it.
        Tick,
        /// The stall timer fires, and the process dies after the first
        /// comment write of the retry lands, before acknowledging it.
        CrashBeforeAck,
        /// A user (re)declares `pr`'s predecessor, and a maintainer
        /// deletes or doctors the ledger the resulting write lands —
        /// webhook processed — before the write is acknowledged.
        DeclareThenInterfere { pr: u64, interference: Interference },
    }

    fn arb_adversarial_action() -> impl Strategy<Value = AdversarialAction> {
        prop_oneof![
            3 => ((2u64..=3), proptest::bool::ANY)
                .prop_map(|(pr, crash)| AdversarialAction::Declare { pr, crash }),
            2 => ((2u64..=3), proptest::bool::ANY)
                .prop_map(|(pr, stale)| AdversarialAction::TamperLedger { pr, stale }),
            2 => ((2u64..=3), proptest::bool::ANY)
                .prop_map(|(pr, stale)| AdversarialAction::EditAwayLedger { pr, stale }),
            2 => ((2u64..=3), proptest::bool::ANY)
                .prop_map(|(pr, stale)| AdversarialAction::ForgeSibling { pr, stale }),
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
            1 => Just(AdversarialAction::BreakListings),
            1 => Just(AdversarialAction::HealListings),
            1 => Just(AdversarialAction::Restart),
            2 => Just(AdversarialAction::Tick),
            2 => Just(AdversarialAction::CrashBeforeAck),
            2 => ((2u64..=3), prop_oneof![Just(Interference::Delete), Just(Interference::Tamper)])
                .prop_map(|(pr, interference)| AdversarialAction::DeclareThenInterfere {
                    pr,
                    interference,
                }),
        ]
    }

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
        let mut delayed: Vec<Vec<u8>> = Vec::new();
        for action in actions {
            match *action {
                AdversarialAction::Declare { pr, crash } => {
                    next_comment += 10;
                    let body = topology_comment(
                        &world.config,
                        pr,
                        &format!("@merge-train predecessor #{}", pr - 1),
                        next_comment,
                        "created",
                    );
                    world.enqueue(&mut processor, "issue_comment", body);
                    if crash {
                        processor = drain_crashing_before_ack(&world, processor);
                    }
                }
                AdversarialAction::TamperLedger { pr, stale } => {
                    let Some((id, _)) = ledgers_on(&world, pr).into_iter().next() else {
                        continue;
                    };
                    let forged = forged_ledger_body(pr);
                    {
                        let mut github = world.github.lock().unwrap();
                        let comment = github.comments.get_mut(&id).unwrap();
                        let pre_edit = std::mem::replace(&mut comment.body, forged.clone());
                        if stale {
                            github.stale_listing_bodies.insert(id, pre_edit);
                        }
                    }
                    let hook = bot_comment_webhook(&world.config, pr, id.0, "edited", &forged);
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
                    let hook = bot_comment_webhook(&world.config, pr, id.0, "edited", "vandalized");
                    world.enqueue(&mut processor, "issue_comment", hook);
                }
                AdversarialAction::ForgeSibling { pr, stale } => {
                    let forged = forged_ledger_body(pr);
                    let id = plant_bot_comment(&world, pr, &forged);
                    if stale {
                        world
                            .github
                            .lock()
                            .unwrap()
                            .stale_listing_bodies
                            .insert(id, "an old reply of the bot's".to_owned());
                    }
                    let hook = bot_comment_webhook(&world.config, pr, id.0, "edited", &forged);
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
                    let hook = bot_comment_webhook(&world.config, pr, id.0, "deleted", "");
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
                AdversarialAction::BreakListings => {
                    world.github.lock().unwrap().list_comments_broken = true;
                }
                AdversarialAction::HealListings => {
                    world.github.lock().unwrap().list_comments_broken = false;
                }
                AdversarialAction::Restart => {
                    drop(processor);
                    processor = world.processor();
                }
                AdversarialAction::Tick => {
                    tick(&world, &mut processor);
                }
                AdversarialAction::CrashBeforeAck => {
                    world.advance_past_cooldown();
                    processor.requeue_marked_recoveries().unwrap();
                    processor = drain_crashing_before_ack(&world, processor);
                }
                AdversarialAction::DeclareThenInterfere { pr, interference } => {
                    next_comment += 10;
                    let body = topology_comment(
                        &world.config,
                        pr,
                        &format!("@merge-train predecessor #{}", pr - 1),
                        next_comment,
                        "created",
                    );
                    world.enqueue(&mut processor, "issue_comment", body);
                    drain_interfering_before_ack(&mut world, &mut processor, interference);
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
            github.list_comments_broken = false;
            github.post_comment_response_lost = false;
        }
        for hook in delayed {
            world.enqueue(&mut processor, "issue_comment", hook);
        }
        drain(&mut processor);
        // ...and each PR sees one more change. The guarantee is bounded
        // on purpose: a forgery presumed gone after spaced absences, or a
        // comment deleted with its webhook lost, is found again at the
        // NEXT sync on its PR rather than by watching for ever — so the
        // oracle grants that sync, and then requires everything of it.
        for pr in 2..=3u64 {
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
        drain(&mut processor);

        // The machinery gets its retries, spaced past the absence
        // cooldown, until nothing is pending.
        for _ in 0..8 {
            if processor
                .store_mut()
                .ledger_pending_prs()
                .unwrap()
                .is_empty()
            {
                break;
            }
            tick(&world, &mut processor);
        }

        assert_eq!(
            processor.store_mut().ledger_pending_prs().unwrap(),
            Vec::<PrNumber>::new(),
            "everything the ledger machinery owed drains once the adversary goes home"
        );
        assert_ledgers_match_store(&world, &processor);
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 32,
            ..ProptestConfig::default()
        })]

        /// However the declarations, tamperings, forgeries, deletions,
        /// stale listings, failed and unacknowledged writes, delayed
        /// webhooks, restarts and mid-write crashes interleave: once
        /// GitHub heals and every webhook has arrived, the system
        /// converges — nothing owed, and every PR carrying exactly the
        /// one ledger a lost-DB crawl should read.
        #[test]
        fn ledgers_converge_under_adversity(
            actions in proptest::collection::vec(arb_adversarial_action(), 1..10),
        ) {
            adversarial_case(&actions);
        }
    }

    /// The review-round shapes, pinned: tampering behind a stale listing
    /// (rounds 19 and 21); a forged sibling behind a broken then healed
    /// token (rounds 20 and 21); a deletion whose webhook outlives both a
    /// lost post response and the process (rounds 19 and 20); an
    /// auth-glitch 404 on a forged sibling behind a short listing (round
    /// 24); and a crash between a rewrite landing and its acknowledgement
    /// with the listing cache serving the old body (round 24).
    #[test]
    fn adversarial_fixed_cases() {
        adversarial_case(&[
            AdversarialAction::Declare {
                pr: 2,
                crash: false,
            },
            AdversarialAction::TamperLedger { pr: 2, stale: true },
        ]);
        adversarial_case(&[
            AdversarialAction::Declare {
                pr: 2,
                crash: false,
            },
            AdversarialAction::BreakUpdates,
            AdversarialAction::ForgeSibling {
                pr: 2,
                stale: false,
            },
            AdversarialAction::HealUpdates,
        ]);
        adversarial_case(&[
            AdversarialAction::Declare {
                pr: 2,
                crash: false,
            },
            AdversarialAction::LosePostResponses,
            AdversarialAction::DeleteLedger {
                pr: 2,
                delayed_webhook: true,
                ghost: false,
            },
            AdversarialAction::Restart,
        ]);
        adversarial_case(&[
            AdversarialAction::Declare {
                pr: 2,
                crash: false,
            },
            AdversarialAction::AuthGlitch404,
            AdversarialAction::HideListings { pr: 2 },
            AdversarialAction::ForgeSibling { pr: 2, stale: true },
            AdversarialAction::Tick,
            AdversarialAction::HealAuth,
        ]);
        adversarial_case(&[
            AdversarialAction::Declare {
                pr: 2,
                crash: false,
            },
            AdversarialAction::TamperLedger { pr: 2, stale: true },
            AdversarialAction::CrashBeforeAck,
            AdversarialAction::Declare { pr: 2, crash: true },
        ]);
        // A watched forgery presumed gone after two spaced absences, then
        // shown again once the listings recover: found at the PR's next
        // sync (Codex ledger review of the hardening).
        adversarial_case(&[
            AdversarialAction::AuthGlitch404,
            AdversarialAction::ForgeSibling {
                pr: 3,
                stale: false,
            },
            AdversarialAction::HideListings { pr: 3 },
            AdversarialAction::ForgeSibling {
                pr: 2,
                stale: false,
            },
            AdversarialAction::Tick,
        ]);
    }
}

/// A first-contact crawl checks whether the delivery that woke it is still
/// current by reading GitHub. If the process dies before that delivery is
/// closed, the retry finds a bootstrapped store, skips the check, and
/// would act on a payload whose comment may have been edited or deleted
/// meanwhile — and an edited-away `start` has no retraction path at all.
/// The crawl's mark commits with its events, and a marked delivery is
/// closed unhandled (Codex crawl review round 14, P1).
#[test]
fn a_delivery_whose_crawl_outlived_its_close_is_not_acted_on() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 1, &heads);
    drain(&mut processor);

    start_command(&mut world, &mut processor, 1);
    let delivery_id = format!("delivery-{}", world.next_delivery);
    // What the crawl's own transaction would have left behind.
    processor
        .store_mut()
        .append_batch_marking(&[], chrono::Utc::now(), Some(&delivery_id))
        .unwrap();
    drain(&mut processor);

    assert!(
        processor.state().active_trains.is_empty(),
        "a delivery whose freshness check did not survive must not start a train"
    );
    assert!(
        processor.store_mut().pending_commands().unwrap().is_empty(),
        "and it leaves no durable command behind"
    );
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

    // The author re-declares in a fresh comment, then deletes it: their
    // own retraction proceeds. Ownership only moves FORWARD in comment id,
    // so the restatement sits above the setup's declaration (id 20).
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

/// A retraction the AUTHOR makes is applied, and the PR's stack ledger is
/// rewritten to say the PR declares nothing — the ledger is state, not a
/// log, so the retraction leaves no separate trace to interpret. A
/// stranger's is denied and the ledger does not move.
#[test]
fn an_authorized_retraction_rewrites_the_ledger_to_not_stacked() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    let declared_ledger = ledger_on(&world, 2).expect("PR 2's ledger");
    assert_eq!(
        declared_ledger.declared.map(|d| d.predecessor),
        Some(PrNumber(1)),
        "precondition: the ledger records the edge"
    );

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

    // A stranger deletes the author's declaring comment (id 20, from the
    // stack setup): denied, and nothing moves.
    world.enqueue(
        &mut processor,
        "issue_comment",
        delete_body(20, STRANGER, "stranger"),
    );
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1)),
        "a denied retraction changes nothing"
    );
    assert_eq!(
        ledger_on(&world, 2)
            .and_then(|l| l.declared)
            .map(|d| d.predecessor),
        Some(PrNumber(1)),
        "and the ledger still records the edge"
    );

    // The author re-declares in a fresh comment and deletes that one.
    // Ownership only ever moves FORWARD in comment id, so the restatement
    // must sit above the setup's declaration (id 20) to take it.
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
        ledger_on(&world, 2).map(|l| l.declared),
        Some(None),
        "and the ledger now says the PR declares no predecessor"
    );
    assert_ledgers_match_store(&world, &processor);
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
/// timer does: a probe that concluded nothing gets another look. Time
/// passes first, past the absence cooldown: two listings closer together
/// than that count as one.
fn nudge(world: &mut World, processor: &mut Processor, comment_id: u64) {
    world.advance_past_cooldown();
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

    // The bot "edits" the author's declaring comment (id 20) away:
    // author stays the user, sender is the bot.
    let body = format!(
        r#"{{
            "action": "edited",
            "comment": {{
                "id": 20,
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
            .any(|(pr, text)| *pr == PrNumber(99) && text.contains("refusing the command")),
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

// ─── First-contact bootstrap: the crawl ───

/// Announces `n` open PRs (a linear stack by base branch) WITHOUT declaring
/// predecessors — for tests that place the declaration comments on GitHub
/// themselves.
fn enqueue_pr_opens(world: &mut World, processor: &mut Processor, n: usize, heads: &[Sha]) {
    let config = world.config.clone();
    for i in 1..=n {
        let base = if i == 1 {
            "main".to_owned()
        } else {
            format!("pr-{}", i - 1)
        };
        let body = pr_opened_body(&config, i as u64, &heads[i - 1], &format!("pr-{i}"), &base);
        world.enqueue(processor, "pull_request", body);
    }
}

/// The disaster: the state DB (and its WAL) is gone; the clone survives.
fn destroy_state_db(world: &World) {
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

/// An issue_comment payload with an exact comment id (no `% 10`) and a
/// separate sender: `text` is `None` for a deletion.
#[allow(clippy::too_many_arguments)]
fn raw_comment_json(
    config: &GitConfig,
    pr: u64,
    text: Option<&str>,
    author_id: u64,
    author_login: &str,
    sender_id: u64,
    sender_login: &str,
    id: u64,
    action: &str,
) -> Vec<u8> {
    let body = match text {
        Some(t) => format!("\"{t}\""),
        None => "null".to_owned(),
    };
    format!(
        r#"{{
            "action": "{action}",
            "comment": {{
                "id": {id},
                "body": {body},
                "user": {{ "id": {author_id}, "login": "{author_login}" }},
                "updated_at": "2026-07-01T13:00:00Z"
            }},
            "issue": {{
                "number": {pr},
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {sender_id}, "login": "{sender_login}" }}
        }}"#,
        repo = repo_json(config),
    )
    .into_bytes()
}

/// An `edited` payload for comment `id` on `pr`: `from` before, `to` after
/// (GitHub's `changes.body.from`).
#[allow(clippy::too_many_arguments)]
fn raw_edited_comment_json(
    config: &GitConfig,
    pr: u64,
    from: &str,
    to: &str,
    author_id: u64,
    author_login: &str,
    sender_id: u64,
    sender_login: &str,
    id: u64,
) -> Vec<u8> {
    let raw = raw_comment_json(
        config,
        pr,
        Some(to),
        author_id,
        author_login,
        sender_id,
        sender_login,
        id,
        "edited",
    );
    let mut json: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    json["changes"] = serde_json::json!({ "body": { "from": from } });
    serde_json::to_vec(&json).unwrap()
}

/// After a DB loss, a redelivered `created` webhook for a comment that has
/// SINCE BEEN DELETED must not be handled: the crawl cannot see the comment,
/// so nothing would stop the handler recreating a retracted declaration
/// (monolith review, P1). But absence is believed only on the SECOND look:
/// GitHub is not read-after-write consistent, so the first attempt
/// releases and re-crawls, and only a comment still missing then is
/// treated as gone (Codex crawl review round 6, P1). The crawl itself
/// lands on the first attempt.
#[test]
fn a_redelivered_created_webhook_for_a_deleted_comment_is_not_handled() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let body = raw_comment_json(
        &world.config,
        2,
        Some("@merge-train predecessor #1"),
        AUTHOR,
        "author",
        AUTHOR,
        "author",
        777,
        "created",
    );
    world.enqueue(&mut processor, "issue_comment", body);
    // The comment was deleted after this delivery was first made (its
    // deletion was processed before the loss); the redelivery describes a
    // comment GitHub no longer has.
    world
        .github
        .lock()
        .unwrap()
        .comments
        .remove(&CommentId(777));

    // First attempt: the crawl lands, and the delivery is RELEASED — the
    // listing may simply not have caught up yet.
    let delivery = processor.claim().unwrap().expect("the redelivery");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released,
        "an absent trigger is retried once, not believed"
    );
    // The retry, once GitHub has had time to catch up, finds it absent
    // again: now it is stale, so the crawl lands and the delivery closes
    // unhandled.
    world.advance_past_cooldown();
    drain(&mut processor);
    assert_eq!(processor.state().default_branch, "main", "the crawl landed");

    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "no declaration survives on GitHub; the stale redelivery must not recreate one"
    );
}

/// A redelivered `created` webhook for a comment that has SINCE BEEN EDITED
/// to ANOTHER text is stale: its body is no longer the comment's, and
/// handling it would record a declaration nothing on GitHub says. One
/// edited to the SAME text is handled: the creation is the author's own
/// utterance, and the listing still shows it, whoever touched the comment
/// since (the recovery model found the stricter rule losing an edge live
/// keeps).
#[test]
fn a_redelivered_created_webhook_for_an_edited_comment_is_handled_only_if_its_text_stands() {
    for (final_text, edge_expected) in [
        ("@merge-train predecessor #1", true),
        ("(edited away)", false),
    ] {
        let action = "created";
        let (mut world, heads) = World::linear_stack(2);
        let mut processor = world.processor();
        enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
        drain(&mut processor);
        destroy_state_db(&world);

        let mut processor = world.processor();
        let body = raw_comment_json(
            &world.config,
            2,
            Some("@merge-train predecessor #1"),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            777,
            action,
        );
        world.enqueue(&mut processor, "issue_comment", body);
        // The comment was edited after the `created` delivery was first
        // made — GitHub records the edit even to the same text.
        {
            let mut github = world.github.lock().unwrap();
            let comment = github.comments.get_mut(&CommentId(777)).unwrap();
            comment.body = final_text.to_owned();
            comment.edited = Edited::By {
                editor: Some(AUTHOR),
            };
        }
        drain_with_cooldowns(&world, &mut processor);

        assert_eq!(
            processor.state().prs[&PrNumber(2)].predecessor.is_some(),
            edge_expected,
            "action {action}, final text {final_text:?}"
        );
    }
}

/// An `edited` redelivery whose body the comment has since moved past is
/// as stale as a deleted one: the crawl saw the current body, and handling
/// the old payload would record what the comment no longer says (Codex
/// crawl review round 2, P1). A matching `edited` payload is current.
#[test]
fn a_superseded_edited_redelivery_is_not_handled() {
    for (current_body, edge_expected) in [
        ("(edited away)", false),
        ("@merge-train predecessor #1", true),
    ] {
        let (mut world, heads) = World::linear_stack(2);
        let mut processor = world.processor();
        enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
        drain(&mut processor);
        destroy_state_db(&world);

        let mut processor = world.processor();
        let body = raw_comment_json(
            &world.config,
            2,
            Some("@merge-train predecessor #1"),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            777,
            "edited",
        );
        world.enqueue(&mut processor, "issue_comment", body);
        {
            let mut github = world.github.lock().unwrap();
            let comment = github.comments.get_mut(&CommentId(777)).unwrap();
            comment.body = current_body.to_owned();
            comment.edited = Edited::By {
                editor: Some(AUTHOR),
            };
        }
        if !edge_expected {
            // The listed body differs from the payload's: doubted, and
            // stale only once the doubt has stood for the stall cadence.
            let delivery = processor.claim().unwrap().expect("the redelivery");
            assert_eq!(
                processor.process_claimed(delivery).unwrap(),
                PipelineOutcome::Released
            );
            world.advance_past_cooldown();
        }
        drain(&mut processor);
        assert_eq!(
            processor.state().prs[&PrNumber(2)].predecessor.is_some(),
            edge_expected,
            "current body {current_body:?}"
        );
    }
}

/// A stale PR webhook redelivered after a DB loss — here an unmerged
/// `closed` for a PR the crawl just found OPEN (it was reopened) — must
/// not be handled: it would close the PR in the store, against the present
/// the crawl just fetched (Codex crawl review round 2, P1).
#[test]
fn a_stale_pr_close_redelivered_after_a_db_loss_is_not_handled() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    assert!(processor.state().prs[&PrNumber(2)].predecessor.is_some());
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let (head, branch, base) = {
        let github = world.github.lock().unwrap();
        let fake = &github.prs[&PrNumber(2)];
        (
            github.branch_head(&fake.branch),
            fake.branch.clone(),
            fake.base_ref.clone(),
        )
    };
    // The PR is open on GitHub; this `closed` is an old redelivery. It
    // disagrees with the crawled snapshot: doubted, and stale only once
    // the doubt has stood for the stall cadence.
    let body = pr_closed_body(&world.config, 2, &head, &branch, &base);
    world.enqueue(&mut processor, "pull_request", body);
    let delivery = processor.claim().unwrap().expect("the close");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released
    );
    world.advance_past_cooldown();
    drain(&mut processor);
    assert!(
        processor.state().prs[&PrNumber(2)].state.is_open(),
        "the stale close must not have closed #2: {:?}",
        processor.state().prs[&PrNumber(2)].state
    );
}

/// GitHub down at first contact: the delivery releases (nothing can be
/// processed without the bootstrap) and succeeds when retried.
#[test]
fn bootstrap_outage_releases_and_retries() {
    let (mut world, heads) = World::linear_stack(1);
    world.github.lock().unwrap().unavailable = true;
    let mut processor = world.processor();
    let body = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
    world.enqueue(&mut processor, "pull_request", body);
    let delivery = processor.claim().unwrap().expect("queued");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released
    );
    assert!(processor.state().default_branch.is_empty());

    world.github.lock().unwrap().unavailable = false;
    let delivery = processor
        .claim()
        .unwrap()
        .expect("released back to pending");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    assert_eq!(processor.state().default_branch, "main");
}

/// A first-contact command whose crawl LANDS and whose handling is then
/// released for an unrelated transient failure (the role lookup) is
/// retried normally in the same process: its freshness check ran against
/// the present the crawl fetched. Only a crawl mark left by a process that
/// died closes the delivery unhandled.
#[test]
fn a_crawled_delivery_released_for_a_transient_failure_is_still_handled() {
    let (mut world, heads) = World::linear_stack(1);
    {
        let mut github = world.github.lock().unwrap();
        github.roles.insert(
            "maintainer".to_owned(),
            crate::effects::github::CollaboratorRole::Maintain,
        );
        github.permission_lookup_transient = true;
    }
    let mut processor = world.processor();
    let body = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
    world.enqueue(&mut processor, "pull_request", body);
    drain(&mut processor);
    // Not first contact any more? It is: the store's default branch is
    // set by the crawl, which the PR-opened delivery above triggered. So
    // reset to a fresh store to make the STOP the first contact.
    drop(processor);
    let mut world2 = world;
    world2.state_dir = TempDir::new().unwrap();
    let mut processor = world2.processor();
    let stop = comment_body(&world2.config, 1, "@merge-train stop", 777, "maintainer", 9);
    world2.enqueue(&mut processor, "issue_comment", stop);
    let delivery = processor.claim().unwrap().expect("queued");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released,
        "the crawl landed; the role lookup's outage released the delivery"
    );
    assert_eq!(processor.state().default_branch, "main", "the crawl landed");
    world2.github.lock().unwrap().permission_lookup_transient = false;
    let delivery = processor
        .claim()
        .unwrap()
        .expect("released back to pending");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    // A delivery closed unheard records no dedupe key; a handled one does.
    assert!(
        processor
            .store_mut()
            .is_duplicate(&crate::webhooks::dedupe::DedupeKey::issue_comment_created(
                PrNumber(1),
                crate::types::CommentId(9),
            ))
            .unwrap(),
        "the stop was handled, not closed unheard"
    );
}

/// A crawled comment delivery whose comment survived the gap UNCHANGED is
/// handled after a restart — a maintainer's `stop` that recovered a train
/// must still stop it — while one whose comment is gone is closed unheard.
#[test]
fn a_crawled_comment_delivery_is_re_checked_against_github_after_a_restart() {
    for comment_survives in [true, false] {
        let (mut world, heads) = World::linear_stack(1);
        let mut processor = world.processor();
        let body = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
        world.enqueue(&mut processor, "pull_request", body);
        drain(&mut processor);
        // A command: handled, it answers (no train to stop); closed
        // unheard, it leaves no trace.
        let stop = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 9);
        world.enqueue(&mut processor, "issue_comment", stop);
        let delivery = processor.claim().unwrap().expect("queued");
        // The crawl mark lands; the process dies before handling.
        processor
            .store_mut()
            .append_batch_marking(&[], Utc::now(), Some(&delivery.delivery_id))
            .unwrap();
        drop(processor);
        if !comment_survives {
            world
                .github
                .lock()
                .unwrap()
                .comments
                .remove(&crate::types::CommentId(9));
        }
        let mut processor = world.processor();
        if !comment_survives {
            // Absent from the listing: doubted first, believed gone once
            // the doubt has stood for the stall cadence.
            let delivery = processor.claim().unwrap().expect("pending");
            assert_eq!(
                processor.process_claimed(delivery).unwrap(),
                PipelineOutcome::Released
            );
            world.advance_past_cooldown();
        }
        drain(&mut processor);
        let handled = !world.github.lock().unwrap().posted_comments.is_empty();
        assert_eq!(
            handled, comment_survives,
            "handled exactly when the comment is still there unchanged (survives={comment_survives})"
        );
    }
}

/// A crawled comment delivery released in the SAME process (a transient
/// role-lookup failure) is re-checked against GitHub on its retry too:
/// the comment edited away or deleted in between is a withdrawn command,
/// closed unheard rather than acted on.
#[test]
fn a_crawled_delivery_withdrawn_before_its_retry_is_not_acted_on() {
    let (mut world, heads) = World::linear_stack(1);
    {
        let mut github = world.github.lock().unwrap();
        github.roles.insert(
            "maintainer".to_owned(),
            crate::effects::github::CollaboratorRole::Maintain,
        );
        github.permission_lookup_transient = true;
    }
    let mut processor = world.processor();
    let body = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
    world.enqueue(&mut processor, "pull_request", body);
    drain(&mut processor);
    drop(processor);
    let mut world2 = world;
    world2.state_dir = TempDir::new().unwrap();
    let mut processor = world2.processor();
    let stop = comment_body(&world2.config, 1, "@merge-train stop", 777, "maintainer", 9);
    world2.enqueue(&mut processor, "issue_comment", stop);
    let delivery = processor.claim().unwrap().expect("queued");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released
    );
    // Withdrawn before the retry: the comment is deleted.
    {
        let mut github = world2.github.lock().unwrap();
        github.permission_lookup_transient = false;
        github.comments.remove(&crate::types::CommentId(9));
    }
    // Absent from the listing: doubted, and believed gone only once the
    // doubt has stood for the stall cadence.
    let delivery = processor
        .claim()
        .unwrap()
        .expect("released back to pending");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released
    );
    world2.advance_past_cooldown();
    let delivery = processor
        .claim()
        .unwrap()
        .expect("released back to pending");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    assert!(
        world2.github.lock().unwrap().posted_comments.is_empty(),
        "closed unheard: the withdrawn command was not answered"
    );
}

/// Nothing touches a ledger before the crawl has landed. A delayed edit
/// webhook for a ledger the bot has already restored arrives at a fresh
/// store first; the crawl it triggers fails; the repair the webhook would
/// queue must not then run against the empty cache and neutralize the
/// genuine ledger (Codex topology review, P1) — the record the topology
/// crawl will read back.
#[test]
fn no_ledger_is_touched_before_the_crawl_has_landed() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    tick(&world, &mut processor);
    let (ledger_id, _) = ledgers_on(&world, 2)
        .into_iter()
        .next()
        .expect("#2's ledger is written");
    let ledger_body = world.github.lock().unwrap().comments[&ledger_id]
        .body
        .clone();
    drop(processor);
    destroy_state_db(&world);

    // The crawl cannot list comments; comment WRITES still work.
    world.github.lock().unwrap().list_comments_broken = true;
    let updates_before = world.github.lock().unwrap().comment_updates;
    let mut processor = world.processor();
    let hook = bot_comment_webhook(&world.config, 2, ledger_id.0, "edited", &ledger_body);
    world.enqueue(&mut processor, "issue_comment", hook);
    // The delayed-webhook shape: the bot restored the comment before the
    // webhook for the edit was delivered.
    restored_by_bot(&world, ledger_id, &ledger_body);
    let delivery = processor.claim().unwrap().expect("the edit");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released,
        "the crawl could not land"
    );
    run_sagas(&mut processor);
    assert_eq!(
        world.github.lock().unwrap().comments[&ledger_id].body,
        ledger_body,
        "the genuine ledger is untouched while the store is unbootstrapped"
    );
    assert_eq!(world.github.lock().unwrap().comment_updates, updates_before);

    world.github.lock().unwrap().list_comments_broken = false;
    world.advance_past_cooldown();
    // The crawl lands; the edit's payload is the maintainer's while the
    // comment's current bytes are the bot's (its restoration superseded
    // the edit): doubted, then stale — closed unheard, and rightly so.
    let delivery = processor.claim().unwrap().expect("the edit");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released
    );
    world.advance_past_cooldown();
    drain(&mut processor);
    assert_eq!(processor.state().default_branch, "main", "the crawl landed");
    assert_eq!(
        world.github.lock().unwrap().comments[&ledger_id].body,
        ledger_body,
        "the superseded edit touched nothing"
    );
    // (What becomes of the ledger once the store IS bootstrapped is the
    // topology crawl's concern: it adopts the ledger the store did not
    // write, and only then may the sync touch it.)
}

/// The deletion of a restatement the store never saw own the edge is a
/// retraction, and retractions are the PR author's alone: a stranger
/// deleting the author's restatement is refused with an answer, and the
/// edge stands (Codex first-contact review, P1).
#[test]
fn a_strangers_deletion_of_the_restatement_is_refused() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let config = world.config.clone();
    let declare = |id: u64| {
        raw_comment_json(
            &config,
            2,
            Some("@merge-train predecessor #1"),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            id,
            "created",
        )
    };
    world.enqueue(&mut processor, "issue_comment", declare(700));
    world.enqueue(&mut processor, "issue_comment", declare(701));
    let delete_b = raw_comment_json(
        &world.config,
        2,
        Some("@merge-train predecessor #1"),
        AUTHOR,
        "author",
        STRANGER,
        "stranger",
        701,
        "deleted",
    );
    world.enqueue(&mut processor, "issue_comment", delete_b);
    world
        .github
        .lock()
        .unwrap()
        .comments
        .remove(&CommentId(701));
    let delivery = processor.claim().unwrap().expect("A");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    let delivery = processor.claim().unwrap().expect("B created");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released
    );
    world.advance_past_cooldown();
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1)),
        "a stranger's deletion retracts nothing"
    );
    assert!(
        world
            .github
            .lock()
            .unwrap()
            .posted_comments
            .iter()
            .any(|(pr, body)| *pr == PrNumber(2) && body.contains("Only the PR author")),
        "and is answered"
    );
}

/// A `pull_request` `edited` payload retargeting PR `number` from `from`
/// onto `base`.
fn pr_retargeted_body(
    config: &GitConfig,
    number: u64,
    head: &Sha,
    branch: &str,
    from: &str,
    base: &str,
) -> Vec<u8> {
    let mut json: serde_json::Value =
        serde_json::from_slice(&pr_opened_body(config, number, head, branch, base)).unwrap();
    json["action"] = serde_json::json!("edited");
    json["changes"] = serde_json::json!({ "base": { "ref": { "from": from } } });
    serde_json::to_vec(&json).unwrap()
}

/// Live — no database loss — a higher comment id establishes nothing: A
/// declares #1, the author's newer B declares #3 and is rejected, the
/// author retargets the PR and edits A to declare #3. B now names the
/// predecessor, is newer than the owner and is the author's, yet never
/// owned anything: its deletion retracts nothing. What a comment said
/// before is consulted only for a delivery that straddled the crawl
/// (Codex trains review, P2).
#[test]
fn deleting_a_rejected_declaration_live_retracts_nothing() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    // PR 3, on main: a valid predecessor for PR 2 once PR 2 targets it.
    let config = world.config.clone();
    let three = create_branch_with_file(&config, "pr-3", "pr-3.txt", "content 3", "main");
    create_pr_ref(&config, 3, &three);
    world.github.lock().unwrap().prs.insert(
        PrNumber(3),
        FakePr {
            branch: "pr-3".to_owned(),
            base_ref: "main".to_owned(),
            state: FakePrState::Open,
            author_id: AUTHOR,
        },
    );
    world.enqueue(
        &mut processor,
        "pull_request",
        pr_opened_body(&config, 3, &three, "pr-3", "main"),
    );
    drain(&mut processor);
    let comment = |id: u64, body: &str, action: &str| {
        raw_comment_json(
            &config,
            2,
            Some(body),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            id,
            action,
        )
    };
    world.enqueue(
        &mut processor,
        "issue_comment",
        comment(700, "@merge-train predecessor #1", "created"),
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        comment(701, "@merge-train predecessor #3", "created"),
    );
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1))
    );
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor_comment_id,
        Some(CommentId(700)),
        "B was rejected"
    );

    world
        .github
        .lock()
        .unwrap()
        .prs
        .get_mut(&PrNumber(2))
        .unwrap()
        .base_ref = "pr-3".to_owned();
    let retarget = pr_retargeted_body(&config, 2, &heads[1], "pr-2", "pr-1", "pr-3");
    world.enqueue(&mut processor, "pull_request", retarget);
    let edit_a = raw_edited_comment_json(
        &config,
        2,
        "@merge-train predecessor #1",
        "@merge-train predecessor #3",
        AUTHOR,
        "author",
        AUTHOR,
        "author",
        700,
    );
    world.enqueue(&mut processor, "issue_comment", edit_a);
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(3))
    );
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor_comment_id,
        Some(CommentId(700))
    );

    world.enqueue(
        &mut processor,
        "issue_comment",
        comment(701, "@merge-train predecessor #3", "deleted"),
    );
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(3)),
        "deleting the rejected B retracts nothing"
    );
}

/// The crawl suppresses B's creation as stale — B is gone, or edited —
/// and B's retraction arrives only AFTER the crawl landed, unmarked. The
/// ownership B's creation would have taken is transferred to B when the
/// creation is suppressed, durably, so the later retraction is the
/// owner's own whenever it arrives (Codex first-contact review, P1).
#[test]
fn a_restatement_the_crawl_suppressed_is_retracted_after_the_crawl() {
    for by_edit in [false, true] {
        let (mut world, heads) = World::linear_stack(2);
        let mut processor = world.processor();
        enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
        drain(&mut processor);
        drop(processor);
        destroy_state_db(&world);

        let mut processor = world.processor();
        let config = world.config.clone();
        let declare = |id: u64| {
            raw_comment_json(
                &config,
                2,
                Some("@merge-train predecessor #1"),
                AUTHOR,
                "author",
                AUTHOR,
                "author",
                id,
                "created",
            )
        };
        world.enqueue(&mut processor, "issue_comment", declare(700));
        world.enqueue(&mut processor, "issue_comment", declare(701));
        {
            let mut github = world.github.lock().unwrap();
            if by_edit {
                let b = github.comments.get_mut(&CommentId(701)).unwrap();
                b.body = "never mind".to_owned();
                b.edited = Edited::By {
                    editor: Some(AUTHOR),
                };
            } else {
                github.comments.remove(&CommentId(701));
            }
        }
        // A: the crawl lands, A is fresh and handled.
        let delivery = processor.claim().unwrap().expect("A");
        assert_eq!(
            processor.process_claimed(delivery).unwrap(),
            PipelineOutcome::Processed
        );
        assert_eq!(
            processor.state().prs[&PrNumber(2)].predecessor,
            Some(PrNumber(1))
        );
        // B's creation: doubted — the comment is gone, or reads otherwise —
        // and stale once the doubt has stood.
        let delivery = processor.claim().unwrap().expect("B created");
        assert_eq!(
            processor.process_claimed(delivery).unwrap(),
            PipelineOutcome::Released
        );
        world.advance_past_cooldown();
        drain(&mut processor);
        assert_eq!(
            processor.state().prs[&PrNumber(2)].predecessor_comment_id,
            Some(CommentId(701)),
            "B's creation was suppressed, and B took ownership of the edge"
        );

        // B's retraction, received after the crawl landed.
        let retraction = if by_edit {
            raw_edited_comment_json(
                &config,
                2,
                "@merge-train predecessor #1",
                "never mind",
                AUTHOR,
                "author",
                AUTHOR,
                "author",
                701,
            )
        } else {
            raw_comment_json(
                &config,
                2,
                Some("@merge-train predecessor #1"),
                AUTHOR,
                "author",
                AUTHOR,
                "author",
                701,
                "deleted",
            )
        };
        world.enqueue(&mut processor, "issue_comment", retraction);
        drain(&mut processor);
        assert_eq!(
            processor.state().prs[&PrNumber(2)].predecessor,
            None,
            "the suppressed restatement's retraction (by_edit = {by_edit}) retracted the edge"
        );
    }
}

/// A crawled command whose PR the crawl DID cache, and which then became
/// unreadable before the delivery was processed: the freshness check
/// fails permanently, and the precache — which skips cached PRs — will
/// not refuse it. The delivery is refused explicitly, with an answer, and
/// not acted on (Codex first-contact review, P1).
#[test]
fn a_crawled_command_whose_pr_became_unreadable_is_refused() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    // A trace of the bot on the repository: a crawl that could rebuild the
    // topology from the users' comments (an ONBOARDING) would grant the
    // edge itself, and the refusal of the delivery would be unobservable.
    plant_bot_comment(&world, 1, "Hello from the bot.");
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let wake = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
    world.enqueue(&mut processor, "pull_request", wake);
    let declare = raw_comment_json(
        &world.config,
        2,
        Some("@merge-train predecessor #1"),
        AUTHOR,
        "author",
        AUTHOR,
        "author",
        700,
        "created",
    );
    world.enqueue(&mut processor, "issue_comment", declare);
    let delivery = processor.claim().unwrap().expect("the wake-up");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    assert!(
        processor.state().prs.contains_key(&PrNumber(2)),
        "the crawl cached PR 2"
    );
    world.github.lock().unwrap().prs.remove(&PrNumber(2));

    let delivery = processor.claim().unwrap().expect("the declaration");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed,
        "refused, not retried for ever"
    );
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "an unverifiable command is not acted on"
    );
    let posted = world.github.lock().unwrap().posted_comments.clone();
    assert!(
        posted
            .iter()
            .any(|(pr, body)| *pr == PrNumber(2) && body.contains("refusing the command")),
        "and it is answered: {posted:?}"
    );
}

/// Processes everything queued, letting a RELEASED delivery (a doubted
/// trigger) retry past the cooldown.
fn drain_with_cooldowns(world: &World, processor: &mut Processor) {
    let mut rounds = 0;
    while let Some(delivery) = processor.claim().unwrap() {
        rounds += 1;
        assert!(rounds < 50, "drain did not settle");
        if processor.process_claimed(delivery).unwrap() == PipelineOutcome::Released {
            world.advance_past_cooldown();
        }
    }
    run_sagas(processor);
}

/// A STRANGER's restatement, suppressed, transfers nothing: only the PR
/// author's own declaration can own an edge, live or in recovery. The
/// author then deleting the stranger's comment retracts nothing either.
#[test]
fn a_strangers_suppressed_restatement_transfers_nothing() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let config = world.config.clone();
    // A creation's sender is its author; a deletion's is whoever deleted.
    let comment = |id: u64, author: u64, login: &str, sender: u64, slogin: &str, action: &str| {
        raw_comment_json(
            &config,
            2,
            Some("@merge-train predecessor #1"),
            author,
            login,
            sender,
            slogin,
            id,
            action,
        )
    };
    world.enqueue(
        &mut processor,
        "issue_comment",
        comment(700, AUTHOR, "author", AUTHOR, "author", "created"),
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        comment(701, STRANGER, "stranger", STRANGER, "stranger", "created"),
    );
    world
        .github
        .lock()
        .unwrap()
        .comments
        .remove(&CommentId(701));
    drain_with_cooldowns(&world, &mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor_comment_id,
        Some(CommentId(700)),
        "the stranger's suppressed restatement took nothing"
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        comment(701, STRANGER, "stranger", AUTHOR, "author", "deleted"),
    );
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1)),
        "and its deletion retracts nothing"
    );
}

/// The author restates by EDITING a stranger's comment into the
/// declaration — live, that edit takes ownership — then edits it away.
/// Both edits are in the backlog, and the first is suppressed: the listing
/// shows the final prose. The ownership that suppressed edit would have
/// transferred is recorded, as a suppressed creation's is, so the final
/// edit is the owner's retraction; without it the PR stayed stacked after
/// the author unstacked it (the recovery model's finding).
#[test]
fn an_authors_restatement_by_edit_suppressed_still_takes_ownership() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let config = world.config.clone();
    let declare = "@merge-train predecessor #1";
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_comment_json(
            &config,
            2,
            Some(declare),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            700,
            "created",
        ),
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_comment_json(
            &config,
            2,
            Some(declare),
            STRANGER,
            "stranger",
            STRANGER,
            "stranger",
            701,
            "created",
        ),
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_edited_comment_json(
            &config, 2, declare, declare, STRANGER, "stranger", AUTHOR, "author", 701,
        ),
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_edited_comment_json(
            &config, 2, declare, "hello", STRANGER, "stranger", AUTHOR, "author", 701,
        ),
    );
    drain_with_cooldowns(&world, &mut processor);
    assert_eq!(processor.state().default_branch, "main", "the crawl landed");
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "the author unstacked the PR by editing their restatement away"
    );
}

/// A suppressed restatement by editing one of the BOT's own comments
/// transfers nothing: live, the handler ignores a bot-authored comment
/// whatever anyone edits it into, so the author's edit of it into the
/// declaration takes no ownership, and the original declaration keeps
/// the edge — and its deletion retracts it. Recovery must not hand the
/// edge to a comment every later event on which is ignored (Codex
/// first-contact review, P2).
#[test]
fn a_suppressed_restatement_in_a_bot_comment_transfers_nothing() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let config = world.config.clone();
    let declare = "@merge-train predecessor #1";
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_comment_json(
            &config,
            2,
            Some(declare),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            700,
            "created",
        ),
    );
    // The bot's own comment, edited by the PR author into the declaration
    // and then away again; the listing shows the final prose.
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_edited_comment_json(
            &config,
            2,
            "status",
            declare,
            TEST_BOT_ID,
            "merge-train",
            AUTHOR,
            "author",
            701,
        ),
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_edited_comment_json(
            &config,
            2,
            declare,
            "hello",
            TEST_BOT_ID,
            "merge-train",
            AUTHOR,
            "author",
            701,
        ),
    );
    drain_with_cooldowns(&world, &mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor_comment_id,
        Some(CommentId(700)),
        "a bot comment owns nothing, suppressed or not"
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_comment_json(
            &config, 2, None, AUTHOR, "author", AUTHOR, "author", 700, "deleted",
        ),
    );
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "and deleting the declaration retracts the edge"
    );
}

/// A suppressed transfer recorded on a cooldown retry owes the ledger a
/// rewrite — the owner changed — and the retry is the last thing in the
/// queue, with no timer outstanding: the rewrite must be queued with the
/// close, or GitHub's ledger names the old owner until unrelated traffic
/// or a restart happens by (Codex first-contact review, P2).
#[test]
fn a_suppressed_transfer_on_a_retry_queues_the_ledger_rewrite() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let config = world.config.clone();
    let comment = |id: u64| {
        raw_comment_json(
            &config,
            2,
            Some("@merge-train predecessor #1"),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            id,
            "created",
        )
    };
    world.enqueue(&mut processor, "issue_comment", comment(700));
    world.enqueue(&mut processor, "issue_comment", comment(701));
    // The restatement was deleted meanwhile; its deletion webhook is
    // still on its way.
    world
        .github
        .lock()
        .unwrap()
        .comments
        .remove(&CommentId(701));
    let delivery = processor.claim().unwrap().expect("the declaration");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    run_sagas(&mut processor);
    assert_eq!(
        ledger_on(&world, 2)
            .and_then(|l| l.declared)
            .map(|d| d.owner),
        Some(CommentId(700)),
        "the ledger names the declaration"
    );
    let delivery = processor.claim().unwrap().expect("the restatement");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released,
        "absent: doubted"
    );
    world.advance_past_cooldown();
    let delivery = processor
        .claim()
        .unwrap()
        .expect("the restatement, retried");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed,
        "still absent: closed, the ownership it would have taken recorded"
    );
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor_comment_id,
        Some(CommentId(701))
    );
    assert!(
        processor.claim().unwrap().is_none(),
        "nothing else is queued"
    );
    run_sagas(&mut processor);
    assert_eq!(
        ledger_on(&world, 2)
            .and_then(|l| l.declared)
            .map(|d| d.owner),
        Some(CommentId(701)),
        "the rewrite the transfer owed was queued with the close"
    );
}

/// The backlog holds A, the author's restatement B, and TWO edits of B:
/// to `never mind`, then to other prose. B's creation is suppressed (B is
/// edited), B's first edit is stale (the listing shows the final prose),
/// and only the final edit is handled — whose previous text is prose. The
/// retraction the author made must still land: ownership passed to B
/// when its creation was suppressed, durably, so the final edit is an
/// owner's edit away from a declaration (Codex first-contact review, P1).
#[test]
fn a_restatement_edited_twice_in_the_backlog_still_retracts() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let config = world.config.clone();
    let declare = |id: u64| {
        raw_comment_json(
            &config,
            2,
            Some("@merge-train predecessor #1"),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            id,
            "created",
        )
    };
    let edit_b = |from: &str, to: &str| {
        raw_edited_comment_json(
            &config, 2, from, to, AUTHOR, "author", AUTHOR, "author", 701,
        )
    };
    world.enqueue(&mut processor, "issue_comment", declare(700));
    world.enqueue(&mut processor, "issue_comment", declare(701));
    world.enqueue(
        &mut processor,
        "issue_comment",
        edit_b("@merge-train predecessor #1", "never mind"),
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        edit_b("never mind", "on second thought, this stands alone"),
    );
    drain_with_cooldowns(&world, &mut processor);
    assert_eq!(processor.state().default_branch, "main", "the crawl landed");
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "the author's retraction, edited over, still retracted the edge"
    );
}

/// A suppressed creation is not ownership: B was created as PROSE and
/// edited into a declaration of #3 before the crawl — its creation is
/// suppressed, its edit handled and REJECTED. The author then retargets
/// the PR and edits A to declare #3. Deleting B, whose final text names
/// today's predecessor, must retract nothing: B never owned the edge
/// (Codex first-contact review, P2).
#[test]
fn deleting_a_rejected_declaration_whose_creation_was_suppressed_retracts_nothing() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    let config = world.config.clone();
    let three = create_branch_with_file(&config, "pr-3", "pr-3.txt", "content 3", "main");
    create_pr_ref(&config, 3, &three);
    world.github.lock().unwrap().prs.insert(
        PrNumber(3),
        FakePr {
            branch: "pr-3".to_owned(),
            base_ref: "main".to_owned(),
            state: FakePrState::Open,
            author_id: AUTHOR,
        },
    );
    world.enqueue(
        &mut processor,
        "pull_request",
        pr_opened_body(&config, 3, &three, "pr-3", "main"),
    );
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let comment = |id: u64, body: &str, action: &str| {
        raw_comment_json(
            &config,
            2,
            Some(body),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            id,
            action,
        )
    };
    world.enqueue(
        &mut processor,
        "issue_comment",
        comment(700, "@merge-train predecessor #1", "created"),
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        comment(701, "hello", "created"),
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_edited_comment_json(
            &config,
            2,
            "hello",
            "@merge-train predecessor #3",
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            701,
        ),
    );
    drain_with_cooldowns(&world, &mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1))
    );
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor_comment_id,
        Some(CommentId(700)),
        "B's declaration of #3 was rejected"
    );

    world
        .github
        .lock()
        .unwrap()
        .prs
        .get_mut(&PrNumber(2))
        .unwrap()
        .base_ref = "pr-3".to_owned();
    let retarget = pr_retargeted_body(&config, 2, &heads[1], "pr-2", "pr-1", "pr-3");
    world.enqueue(&mut processor, "pull_request", retarget);
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_edited_comment_json(
            &config,
            2,
            "@merge-train predecessor #1",
            "@merge-train predecessor #3",
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            700,
        ),
    );
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(3))
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        comment(701, "@merge-train predecessor #3", "deleted"),
    );
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(3)),
        "deleting B, which never owned the edge, retracts nothing"
    );
}

/// The edit-side twin: a stranger editing the author's restatement away
/// is refused with an answer, and the edge stands (Codex first-contact
/// review, P1).
#[test]
fn a_strangers_edit_of_the_restatement_is_refused() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let config = world.config.clone();
    let declare = |id: u64| {
        raw_comment_json(
            &config,
            2,
            Some("@merge-train predecessor #1"),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            id,
            "created",
        )
    };
    world.enqueue(&mut processor, "issue_comment", declare(700));
    world.enqueue(&mut processor, "issue_comment", declare(701));
    let edit_b = raw_edited_comment_json(
        &world.config,
        2,
        "@merge-train predecessor #1",
        "never mind",
        AUTHOR,
        "author",
        STRANGER,
        "stranger",
        701,
    );
    world.enqueue(&mut processor, "issue_comment", edit_b);
    let delivery = processor.claim().unwrap().expect("A");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    drain_with_cooldowns(&world, &mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1)),
        "a stranger's edit retracts nothing"
    );
    assert!(
        world
            .github
            .lock()
            .unwrap()
            .posted_comments
            .iter()
            .any(|(pr, body)| *pr == PrNumber(2) && body.contains("Only the PR author")),
        "and is answered"
    );
}

/// The backlog holds the author's declaration A, a newer restatement B,
/// and the author's edit of B to `never mind`. B's creation is stale — the
/// comment has been edited — so the crawl leaves A owning the edge; but
/// live, B would have taken ownership and its edit retracted. The edit
/// carries what B said before, and the retraction happens (Codex
/// first-contact review, P1).
#[test]
fn an_edited_away_restatement_in_the_backlog_still_retracts() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let config = world.config.clone();
    let declare = |id: u64| {
        raw_comment_json(
            &config,
            2,
            Some("@merge-train predecessor #1"),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            id,
            "created",
        )
    };
    world.enqueue(&mut processor, "issue_comment", declare(700));
    world.enqueue(&mut processor, "issue_comment", declare(701));
    let edit_b = raw_edited_comment_json(
        &world.config,
        2,
        "@merge-train predecessor #1",
        "never mind",
        AUTHOR,
        "author",
        AUTHOR,
        "author",
        701,
    );
    world.enqueue(&mut processor, "issue_comment", edit_b);
    let delivery = processor.claim().unwrap().expect("A");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1))
    );
    drain_with_cooldowns(&world, &mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "the author's edit of the restatement retracted the edge"
    );
}

/// A newly created comment can be missing from a listing taken moments
/// later; a second absence is evidence it was deleted only once GitHub
/// has had time to catch up. Another webhook can wake the worker and
/// retry a released delivery at once, so the retry is judged by the
/// clock, not by its count (Codex topology review, P2).
#[test]
fn an_absent_trigger_is_not_believed_gone_within_the_cooldown() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let body = raw_comment_json(
        &world.config,
        2,
        Some("@merge-train predecessor #1"),
        AUTHOR,
        "author",
        AUTHOR,
        "author",
        777,
        "created",
    );
    world.enqueue(&mut processor, "issue_comment", body);
    world
        .github
        .lock()
        .unwrap()
        .comments
        .remove(&CommentId(777));
    for attempt in 0..2 {
        let delivery = processor.claim().unwrap().expect("pending");
        assert_eq!(
            processor.process_claimed(delivery).unwrap(),
            PipelineOutcome::Released,
            "attempt {attempt}: absence within the cooldown is not yet evidence"
        );
    }
    world.advance_past_cooldown();
    let delivery = processor.claim().unwrap().expect("pending");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed,
        "absent again after the cooldown: closed unheard"
    );
    assert_eq!(processor.state().prs[&PrNumber(2)].predecessor, None);
}

/// An author's RETRACTION edit can trigger the crawl while the listing
/// still serves the pre-edit declaration: the crawl restores that edge,
/// and the webhook's body differs from the listed one. A mismatch is
/// not proof the webhook is stale — the listing may be the older of the
/// two — so the delivery is doubted and retried after the stall cadence,
/// as an absent trigger is; closed on the first read, the withdrawn edge
/// would stand and a later train would drive the PR (Codex topology
/// review, P1).
#[test]
fn a_retraction_edit_the_listing_has_not_caught_up_with_is_retried() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let body = raw_comment_json(
        &world.config,
        2,
        Some("never mind"),
        AUTHOR,
        "author",
        AUTHOR,
        "author",
        777,
        "edited",
    );
    world.enqueue(&mut processor, "issue_comment", body);
    // GitHub has the edit; the listing lags, still serving the declaration.
    world
        .github
        .lock()
        .unwrap()
        .stale_listing_bodies
        .insert(CommentId(777), "@merge-train predecessor #1".to_owned());
    let delivery = processor.claim().unwrap().expect("the edit");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released,
        "a body the listing disagrees with is doubted, not disbelieved"
    );
    // The listing catches up; the retry after the cadence handles the edit.
    world.github.lock().unwrap().stale_listing_bodies.clear();
    world.advance_past_cooldown();
    drain(&mut processor);
    assert_eq!(processor.state().default_branch, "main", "the crawl landed");
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "the retraction was handled, not discarded as stale"
    );
}

/// Every delivery queued before the crawl landed is judged against the
/// present the crawl fetched, not only the one that triggered it: an old
/// `closed` for a PR the crawl just cached as open, queued behind an
/// unrelated wake-up, would otherwise close it in the store — and the
/// next freeze would omit an open descendant (Codex topology review,
/// P1).
#[test]
fn every_delivery_queued_before_the_crawl_landed_is_judged_against_its_present() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    tick(&world, &mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    // The wake-up, then the stale close queued behind it.
    let wake = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
    world.enqueue(&mut processor, "pull_request", wake);
    let (head, branch, base) = {
        let github = world.github.lock().unwrap();
        let fake = &github.prs[&PrNumber(2)];
        (
            github.branch_head(&fake.branch),
            fake.branch.clone(),
            fake.base_ref.clone(),
        )
    };
    let stale_close = pr_closed_body(&world.config, 2, &head, &branch, &base);
    world.enqueue(&mut processor, "pull_request", stale_close);
    let delivery = processor.claim().unwrap().expect("the wake-up");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    assert_eq!(processor.state().default_branch, "main", "the crawl landed");
    // Marked by the crawl, and disagreeing with its snapshot: doubted,
    // and stale only once the doubt has stood for the stall cadence.
    let delivery = processor.claim().unwrap().expect("the close");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released
    );
    world.advance_past_cooldown();
    drain(&mut processor);
    assert!(
        processor.state().prs[&PrNumber(2)].state.is_open(),
        "the stale close queued behind the trigger must not close #2: {:?}",
        processor.state().prs[&PrNumber(2)].state
    );
}

/// When a doubted pull-request delivery is judged by the PR fetched
/// afresh, that snapshot is the present — and the cache is RECONCILED to
/// it whatever the verdict. A reopen the crawl saw as closed (the listing
/// lagged) and whose payload the fresh PR has since moved past (a retarget
/// followed it) is stale, but the PR is open: closing the delivery
/// unheard and leaving the cache closed would keep the PR out of every
/// later train (Codex first-contact review, P1).
#[test]
fn a_fresh_snapshot_reconciles_the_cache_even_when_the_delivery_is_stale() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    // GitHub's listing lags: #2 reads closed there while the backlog holds
    // its close and then its reopen.
    world
        .github
        .lock()
        .unwrap()
        .prs
        .get_mut(&PrNumber(2))
        .unwrap()
        .state = crate::github::test_support::FakePrState::Closed;
    let mut processor = world.processor();
    let (head, branch) = {
        let github = world.github.lock().unwrap();
        let fake = &github.prs[&PrNumber(2)];
        (github.branch_head(&fake.branch), fake.branch.clone())
    };
    let close = pr_closed_body(&world.config, 2, &head, &branch, "pr-1");
    world.enqueue(&mut processor, "pull_request", close);
    let reopen = pr_reopened_body(&world.config, 2, &head, &branch, "pr-1");
    world.enqueue(&mut processor, "pull_request", reopen);
    // The close triggers the crawl, which seeds #2 and caches it CLOSED.
    let delivery = processor.claim().unwrap().expect("the close");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    assert!(!processor.state().prs[&PrNumber(2)].state.is_open());
    // The reopen, marked by the crawl, disagrees with that snapshot.
    let delivery = processor.claim().unwrap().expect("the reopen");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released
    );
    // GitHub catches up — and #2 was retargeted onto main meanwhile, so the
    // reopen's payload is stale against the fresh snapshot.
    {
        let mut github = world.github.lock().unwrap();
        let fake = github.prs.get_mut(&PrNumber(2)).unwrap();
        fake.state = crate::github::test_support::FakePrState::Open;
        fake.base_ref = "main".to_owned();
    }
    world.advance_past_cooldown();
    drain(&mut processor);
    let cached = &processor.state().prs[&PrNumber(2)];
    assert!(
        cached.state.is_open(),
        "the fresh snapshot reconciled the cache: {:?}",
        cached.state
    );
    assert_eq!(cached.base_ref, "main", "base too");
}

/// A delivery RECEIVED while the crawl was fetching reaches the store only
/// after the crawl landed — the worker that runs the crawl is the worker
/// that services intake — so it is absent when the crawl marks its
/// backlog. It describes a change the crawled present may or may not
/// hold, and is judged against that present like the backlog is: the mark
/// follows the time the webhook was received, not the time it was stored
/// (Codex first-contact review, P1).
#[test]
fn a_delivery_received_during_the_crawl_is_judged_against_its_present() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let wake = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
    world.enqueue(&mut processor, "pull_request", wake);
    let delivery = processor.claim().unwrap().expect("the wake-up");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed,
        "the crawl landed"
    );
    // A stale close for #2, received a minute before the crawl landed but
    // stored only now.
    let (head, branch, base) = {
        let github = world.github.lock().unwrap();
        let fake = &github.prs[&PrNumber(2)];
        (
            github.branch_head(&fake.branch),
            fake.branch.clone(),
            fake.base_ref.clone(),
        )
    };
    let stale_close = pr_closed_body(&world.config, 2, &head, &branch, &base);
    world.enqueue_received_at(
        &mut processor,
        "pull_request",
        stale_close,
        chrono::Utc::now() - chrono::Duration::minutes(1),
    );
    let delivery = processor.claim().unwrap().expect("the close");
    assert!(delivery.crawled, "received before the crawl landed: marked");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released,
        "and doubted against the crawled present, not handled"
    );
    assert!(processor.state().prs[&PrNumber(2)].state.is_open());
}

/// A first-contact command on a PR the crawl cannot fetch at all (a
/// permanent failure on the seed) is not stale — its comments were never
/// listed because the PR could not be reached, not because the cap bit —
/// and reaches the handler, which refuses it with an answer, as it would
/// after bootstrap (Codex first-contact review, P2).
#[test]
fn a_first_contact_command_on_an_unfetchable_pr_is_refused_with_an_answer() {
    let (mut world, _heads) = World::linear_stack(1);
    let mut processor = world.processor();
    let stop = comment_body(&world.config, 9, "@merge-train stop", AUTHOR, "author", 90);
    world.enqueue(&mut processor, "issue_comment", stop);
    let delivery = processor.claim().unwrap().expect("the command");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    assert_eq!(processor.state().default_branch, "main", "the crawl landed");
    run_sagas(&mut processor);
    let posted = world.github.lock().unwrap().posted_comments.clone();
    assert!(
        posted
            .iter()
            .any(|(pr, body)| *pr == PrNumber(9) && body.contains("cannot fetch PR #9")),
        "the command was answered, not closed unheard: {posted:?}"
    );
}

/// An `edited` payload is current only if the listed comment's current
/// bytes were written by the payload's SENDER: equal bodies alone prove
/// nothing about who wrote them. The author declares, retracts, and a
/// non-author with comment-edit rights restores the declaration text, all
/// queued behind the crawl. Judged by bodies, the author's first edit
/// would be accepted (its text matches the restored text), the retraction
/// discarded as stale, and the non-author's edit refused — leaving the
/// withdrawn edge standing (Codex first-contact review, P1).
#[test]
fn an_edited_payload_is_current_only_if_its_sender_wrote_the_listed_bytes() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let edit = |sender: u64, login: &str, text: &str| {
        raw_comment_json(
            &world.config,
            2,
            Some(text),
            AUTHOR,
            "author",
            sender,
            login,
            777,
            "edited",
        )
    };
    let declare = edit(AUTHOR, "author", "@merge-train predecessor #1");
    let retract = edit(AUTHOR, "author", "never mind");
    let restore = edit(STRANGER, "stranger", "@merge-train predecessor #1");
    world.enqueue(&mut processor, "issue_comment", declare);
    world.enqueue(&mut processor, "issue_comment", retract);
    world.enqueue(&mut processor, "issue_comment", restore);
    // The listing shows the restored text, in the stranger's bytes.
    for expected in [
        PipelineOutcome::Released, // the author's declaration: bytes match, writer does not
        PipelineOutcome::Released, // the retraction: bytes differ
    ] {
        let delivery = processor.claim().unwrap().expect("queued");
        assert_eq!(processor.process_claimed(delivery).unwrap(), expected);
        world.advance_past_cooldown();
        let delivery = processor.claim().unwrap().expect("doubted, retried");
        assert_eq!(
            processor.process_claimed(delivery).unwrap(),
            PipelineOutcome::Processed,
            "still disagreeing after the cadence: closed unheard"
        );
    }
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "the stranger's restoration is refused, and nothing else was believed"
    );
}

/// The same for a CREATED payload: the author creates the declaration,
/// retracts it by edit, and a non-author restores its text, all queued
/// behind the crawl. The listing shows the declaration in the stranger's
/// bytes. Judged by bodies, the creation would be believed (its text
/// matches), the retraction discarded as stale, and the stranger's edit
/// refused — the withdrawn edge revived. A creation is current only while
/// the comment's bytes are still its creator's (Codex first-contact
/// review, P1, second finding).
#[test]
fn a_created_payload_is_current_only_if_its_creator_still_wrote_the_listed_bytes() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let config = world.config.clone();
    let declare = "@merge-train predecessor #1";
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_comment_json(
            &config,
            2,
            Some(declare),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            777,
            "created",
        ),
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_edited_comment_json(
            &config,
            2,
            declare,
            "never mind",
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            777,
        ),
    );
    world.enqueue(
        &mut processor,
        "issue_comment",
        raw_edited_comment_json(
            &config,
            2,
            "never mind",
            declare,
            AUTHOR,
            "author",
            STRANGER,
            "stranger",
            777,
        ),
    );
    drain_with_cooldowns(&world, &mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "the retraction stood live, and recovery must not revive the edge"
    );
}

/// The receipt time a delivery carries into the mailbox is the HTTP
/// intake's, not the time the worker got round to storing it: a webhook
/// received during a crawl's reads is stored only after the crawl landed,
/// and must still be marked as received before it — stored with the
/// worker's clock it would not be (Codex first-contact review, P1).
#[test]
fn a_delivery_carries_its_receipt_time_into_the_store() {
    let (world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    // A crawl has landed.
    processor
        .store_mut()
        .append_batch_marking(
            &[StateEventPayload::DefaultBranchSet {
                branch: "main".to_owned(),
            }],
            chrono::Utc::now(),
            Some("the-trigger"),
        )
        .unwrap();
    // Received an hour ago, stored now, through the mailbox.
    let body = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
    let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
    let permit = std::sync::Arc::new(tokio::sync::Semaphore::new(1))
        .try_acquire_owned()
        .unwrap();
    let mut parked = None;
    super::handle_msg(
        &mut processor,
        WorkerMsg::Enqueue {
            delivery: IntakeDelivery {
                delivery_id: "late".to_owned(),
                event_type: "pull_request".to_owned(),
                headers: "{}".to_owned(),
                body,
                received_at: chrono::Utc::now() - chrono::Duration::hours(1),
            },
            ack: ack_tx,
            permit,
        },
        &mut parked,
    )
    .unwrap();
    assert_eq!(
        ack_rx.blocking_recv().unwrap().unwrap(),
        super::EnqueueOutcome::Enqueued
    );
    let stored = processor.claim().unwrap().expect("stored");
    assert_eq!(stored.delivery_id, "late");
    assert!(stored.crawled, "received before the crawl landed: marked");
}

/// A crawled delivery closed as stale records its dedupe key, exactly as
/// a stale trigger's close does: GitHub redelivers with fresh ids, and a
/// redelivery received after the crawl landed carries no mark and would
/// otherwise run the obsolete payload — here a declaration whose comment
/// was deleted during the gap (Codex first-contact review, P1).
#[test]
fn a_crawled_delivery_closed_as_stale_dedupes_its_redelivery() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    let body = raw_comment_json(
        &world.config,
        2,
        Some("@merge-train predecessor #1"),
        AUTHOR,
        "author",
        AUTHOR,
        "author",
        777,
        "created",
    );
    world.enqueue(&mut processor, "issue_comment", body.clone());
    // The crawl landed for it; the process died before it was handled,
    // and the comment was deleted meanwhile.
    let delivery = processor.claim().unwrap().expect("queued");
    processor
        .store_mut()
        .append_batch_marking(&[], Utc::now(), Some(&delivery.delivery_id))
        .unwrap();
    drop(processor);
    world
        .github
        .lock()
        .unwrap()
        .comments
        .remove(&CommentId(777));
    let mut processor = world.processor();
    let delivery = processor.claim().unwrap().expect("pending");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released,
        "absent: doubted"
    );
    world.advance_past_cooldown();
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "closed unheard"
    );
    // GitHub redelivers the same webhook under a fresh id, received now.
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "the redelivery is a duplicate of a closed delivery, not a fresh declaration"
    );
}

/// Two copies of one stale webhook, queued under different delivery ids
/// when the crawl lands: the first is doubted, then closed as stale,
/// recording its dedupe key — and the second is a duplicate of that close,
/// discarded at once. Judged for freshness first, it would be doubted and
/// released for another whole stall cadence, and every unrelated delivery
/// behind it would wait it out (Codex first-contact review, P2).
#[test]
fn a_duplicate_crawled_delivery_is_deduped_before_its_freshness_is_doubted() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    let body = raw_comment_json(
        &world.config,
        2,
        Some("@merge-train predecessor #1"),
        AUTHOR,
        "author",
        AUTHOR,
        "author",
        777,
        "created",
    );
    world.enqueue(&mut processor, "issue_comment", body.clone());
    world.enqueue(&mut processor, "issue_comment", body);
    // The crawl landed for both; the process died before either was
    // handled, and the comment was deleted meanwhile.
    let first = processor.claim().unwrap().expect("the first copy");
    let second = processor.claim().unwrap().expect("the second copy");
    assert_ne!(first.delivery_id, second.delivery_id);
    for id in [&first.delivery_id, &second.delivery_id] {
        processor
            .store_mut()
            .append_batch_marking(&[], Utc::now(), Some(id))
            .unwrap();
    }
    drop(processor);
    world
        .github
        .lock()
        .unwrap()
        .comments
        .remove(&CommentId(777));
    let mut processor = world.processor();
    let delivery = processor.claim().unwrap().expect("the first copy");
    assert_eq!(delivery.delivery_id, first.delivery_id);
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released,
        "absent: doubted"
    );
    world.advance_past_cooldown();
    let delivery = processor.claim().unwrap().expect("the first copy, retried");
    assert_eq!(delivery.delivery_id, first.delivery_id);
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed,
        "still absent after the cadence: closed"
    );
    let delivery = processor.claim().unwrap().expect("the second copy");
    assert_eq!(delivery.delivery_id, second.delivery_id);
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed,
        "a duplicate of a closed delivery: discarded at once, not doubted"
    );
    assert!(processor.claim().unwrap().is_none(), "nothing left");
    assert_eq!(processor.state().prs[&PrNumber(2)].predecessor, None);
}

/// The pre-crawl backlog holds a declaration A, a restatement B of the
/// same predecessor, and the author's deletion of B; the listing shows A
/// alone. Live, B's creation moved the edge's ownership to B and B's
/// deletion retracted it. Here B's creation is stale (B is gone) — but its
/// deletion still carries B's body: an author's deletion of a declaration
/// of the very predecessor the PR holds, made after the current owner, is
/// the retraction it would have been (Codex first-contact review, P1).
#[test]
fn a_deleted_restatement_still_retracts_the_edge_it_would_have_owned() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
    drain(&mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let config = world.config.clone();
    let declare = |id: u64| {
        raw_comment_json(
            &config,
            2,
            Some("@merge-train predecessor #1"),
            AUTHOR,
            "author",
            AUTHOR,
            "author",
            id,
            "created",
        )
    };
    world.enqueue(&mut processor, "issue_comment", declare(700));
    world.enqueue(&mut processor, "issue_comment", declare(701));
    let delete_b = raw_comment_json(
        &world.config,
        2,
        Some("@merge-train predecessor #1"),
        AUTHOR,
        "author",
        AUTHOR,
        "author",
        701,
        "deleted",
    );
    world.enqueue(&mut processor, "issue_comment", delete_b);
    // A (700) is listed; B (701) is gone.
    world
        .github
        .lock()
        .unwrap()
        .comments
        .remove(&CommentId(701));
    // A: fresh, handled. B's creation: absent — doubted, then stale.
    let delivery = processor.claim().unwrap().expect("A");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        Some(PrNumber(1))
    );
    let delivery = processor.claim().unwrap().expect("B created");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released
    );
    world.advance_past_cooldown();
    drain(&mut processor);
    assert_eq!(
        processor.state().prs[&PrNumber(2)].predecessor,
        None,
        "the author's deletion of the restatement retracted the edge"
    );
}

/// A command queued behind the first-contact trigger, on a PR that now
/// answers permanently "not found", cannot have its comments listed — and
/// must not be released for ever at the head of the queue. It reaches the
/// handler, whose precache refuses a command on an unfetchable PR with an
/// answer, as a fresh trigger's would (Codex first-contact review, P2).
#[test]
fn a_crawled_command_on_an_unfetchable_pr_is_refused_not_retried_for_ever() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    let wake = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
    world.enqueue(&mut processor, "pull_request", wake);
    let stop = comment_body(&world.config, 9, "@merge-train stop", AUTHOR, "author", 90);
    world.enqueue(&mut processor, "issue_comment", stop);
    drain(&mut processor);
    let posted = world.github.lock().unwrap().posted_comments.clone();
    assert!(
        posted
            .iter()
            .any(|(pr, body)| *pr == PrNumber(9) && body.contains("refusing the command")),
        "the command was answered, not retried: {posted:?}"
    );
}

/// A pull-request delivery that disagrees with the crawled snapshot is
/// DOUBTED like a comment that disagrees with the listing: GitHub's PR
/// listing can lag a `closed` it has already delivered, and closing the
/// delivery on the first read would leave the PR open in the store for
/// good. After the cooldown the PR is fetched afresh — the present, not
/// the cache the crawl took — and the delivery handled if it agrees,
/// closed unheard if not (Codex topology review, P2).
#[test]
fn a_close_the_crawl_listing_lagged_behind_is_handled_after_the_cooldown() {
    for github_catches_up in [true, false] {
        let (mut world, heads) = World::linear_stack(2);
        let mut processor = world.processor();
        world.enqueue_stack_setup(&mut processor, 2, &heads);
        drain(&mut processor);
        tick(&world, &mut processor);
        drop(processor);
        destroy_state_db(&world);

        let mut processor = world.processor();
        let wake = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
        world.enqueue(&mut processor, "pull_request", wake);
        let (head, branch, base) = {
            let github = world.github.lock().unwrap();
            let fake = &github.prs[&PrNumber(2)];
            (
                github.branch_head(&fake.branch),
                fake.branch.clone(),
                fake.base_ref.clone(),
            )
        };
        let close = pr_closed_body(&world.config, 2, &head, &branch, &base);
        world.enqueue(&mut processor, "pull_request", close);
        // The crawl lands for the wake-up, with #2 listed OPEN.
        let delivery = processor.claim().unwrap().expect("the wake-up");
        assert_eq!(
            processor.process_claimed(delivery).unwrap(),
            PipelineOutcome::Processed
        );
        assert!(processor.state().prs[&PrNumber(2)].state.is_open());
        // The close disagrees with that snapshot: doubted, not disbelieved.
        let delivery = processor.claim().unwrap().expect("the close");
        assert_eq!(
            processor.process_claimed(delivery).unwrap(),
            PipelineOutcome::Released
        );
        if github_catches_up {
            world
                .github
                .lock()
                .unwrap()
                .prs
                .get_mut(&PrNumber(2))
                .unwrap()
                .state = crate::github::test_support::FakePrState::Closed;
        }
        world.advance_past_cooldown();
        drain(&mut processor);
        assert_eq!(
            processor.state().prs[&PrNumber(2)].state.is_open(),
            !github_catches_up,
            "handled exactly when the fresh fetch agrees (catches_up={github_catches_up}): {:?}",
            processor.state().prs[&PrNumber(2)].state
        );
    }
}

/// A doubted pull-request delivery whose PR then cannot be fetched at all
/// — a permanent 404, the PR or the bot's access gone — is closed as
/// unverifiable, exactly as a comment delivery whose PR cannot be listed
/// is. Released, it would sit at the head of the repository's queue for
/// ever, and every unrelated delivery behind it with it (Codex
/// first-contact review, P2). The cache keeps the crawl's snapshot: the
/// delivery was never believed.
#[test]
fn a_crawled_pull_request_delivery_whose_pr_vanished_is_closed_not_retried_for_ever() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    drain(&mut processor);
    tick(&world, &mut processor);
    drop(processor);
    destroy_state_db(&world);

    let mut processor = world.processor();
    let wake = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
    world.enqueue(&mut processor, "pull_request", wake);
    let (head, branch, base) = {
        let github = world.github.lock().unwrap();
        let fake = &github.prs[&PrNumber(2)];
        (
            github.branch_head(&fake.branch),
            fake.branch.clone(),
            fake.base_ref.clone(),
        )
    };
    let close = pr_closed_body(&world.config, 2, &head, &branch, &base);
    world.enqueue(&mut processor, "pull_request", close);
    // Behind it, an unrelated delivery: the wake-up again, which agrees
    // with the cache and is handled.
    let behind = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
    world.enqueue(&mut processor, "pull_request", behind);
    // The crawl lands for the wake-up, with #2 listed OPEN.
    let delivery = processor.claim().unwrap().expect("the wake-up");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed
    );
    // The close disagrees with that snapshot: doubted.
    let delivery = processor.claim().unwrap().expect("the close");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Released
    );
    // Then the PR is gone from GitHub: fetching it 404s, permanently.
    world.github.lock().unwrap().prs.remove(&PrNumber(2));
    world.advance_past_cooldown();
    let delivery = processor.claim().unwrap().expect("the close, retried");
    assert_eq!(delivery.event_type, "pull_request");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed,
        "unverifiable for good: closed, not released for ever"
    );
    assert!(
        processor.state().prs[&PrNumber(2)].state.is_open(),
        "closed unheard: the cache keeps the crawl's snapshot"
    );
    let delivery = processor.claim().unwrap().expect("the delivery behind it");
    assert_eq!(
        processor.process_claimed(delivery).unwrap(),
        PipelineOutcome::Processed,
        "the queue moved on"
    );
    assert!(processor.claim().unwrap().is_none(), "nothing left");
}

/// A non-comment delivery whose crawl landed before the process died is
/// handled on restart: a PR event that agrees with the crawled present
/// carries no staleness risk, and a review or check event carries a
/// consequence the crawl cannot reconstruct. Only comment deliveries are
/// closed unheard.
#[test]
fn a_crawled_pull_request_delivery_that_agrees_with_the_cache_is_handled_after_a_restart() {
    let (mut world, heads) = World::linear_stack(1);
    let mut processor = world.processor();
    let body = pr_opened_body(&world.config, 1, &heads[0], "pr-1", "main");
    world.enqueue(&mut processor, "pull_request", body);
    let delivery = processor.claim().unwrap().expect("queued");
    // Land the crawl by hand, as the pipeline does before handling, and
    // die before handling.
    processor
        .store_mut()
        .append_batch_marking(
            &[StateEventPayload::DefaultBranchSet {
                branch: "main".to_owned(),
            }],
            Utc::now(),
            Some(&delivery.delivery_id),
        )
        .unwrap();
    drop(processor);
    let mut processor = world.processor();
    drain(&mut processor);
    assert!(
        processor.state().prs.contains_key(&PrNumber(1)),
        "the PR-opened delivery was handled: the PR is cached"
    );
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
                    received_at: chrono::Utc::now(),
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

// ─── The lost-DB crawl conformance harness ───

// ─── The recovery oracle: first contact against live processing ───

/// Recovery must reach what live processing reached. For any history of
/// comment events on a stack, processing each delivery LIVE — GitHub in
/// step at every event — and recovering from a LOST database — the whole
/// history unacked in the backlog, judged by the first-contact crawl
/// against GitHub's final state — must leave the same predecessor edge
/// and the same owning comment, with ONE permitted deviation: when the
/// comment that owns the edge live no longer READS as that declaration IN
/// THE AUTHOR'S OWN BYTES — a stranger changed its text, re-wrote the
/// same text, or deleted it (live refuses all three and keeps what the
/// author declared), or the author edited it to a declaration live
/// rejected (and kept the old one) — recovery may drop the edge, or
/// attribute it to another comment that does read as the author's own
/// declaration. The crawl believes only what GitHub shows and attributes;
/// the backlog alone cannot prove the author never retracted before the
/// loss, and treating a stranger's bytes as the author's would let a
/// stranger restoring the author's withdrawn text pass as the author's
/// own edit. Dropping fails safe (a PR off the default branch with no
/// predecessor is refused a train). Nothing else may differ: no edge
/// invented, moved, or owned by a comment that does not say so. The
/// histories are small:
/// creations, edits and deletions of PR 2's comments by the author or a
/// stranger, saying a declaration of #1, a declaration of a PR that does
/// not exist, or prose.
mod recovery_model {
    use proptest::prelude::*;

    use super::*;

    const DECLARE_1: &str = "@merge-train predecessor #1";
    const DECLARE_9: &str = "@merge-train predecessor #9";
    const PROSE: &str = "hello";

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Actor {
        Author,
        Stranger,
    }

    impl Actor {
        fn id(self) -> u64 {
            match self {
                Actor::Author => AUTHOR,
                Actor::Stranger => STRANGER,
            }
        }
        fn login(self) -> &'static str {
            match self {
                Actor::Author => "author",
                Actor::Stranger => "stranger",
            }
        }
    }

    /// One event of a history, already valid against the comments that
    /// exist at that point. Comments are named by INDEX: their GitHub ids
    /// are allocated by the fake as they are created, in step with the
    /// bot's own replies and ledgers (GitHub's ids are globally monotonic,
    /// and a fixed id would land on top of a reply the bot posted in
    /// between).
    #[derive(Clone, Debug)]
    enum Event {
        Create {
            k: usize,
            by: Actor,
            body: &'static str,
        },
        Edit {
            k: usize,
            by: Actor,
            from: &'static str,
            to: &'static str,
        },
        Delete {
            k: usize,
            by: Actor,
        },
    }

    /// A raw choice per step; interpreted against the live comment set so
    /// that every generated vector is a valid history and shrinks cleanly.
    #[derive(Clone, Copy, Debug)]
    struct Choice {
        kind: u8,
        which: u8,
        body: u8,
        by_author: bool,
    }

    fn arb_choice() -> impl Strategy<Value = Choice> {
        (0u8..3, 0u8..4, 0u8..3, any::<bool>()).prop_map(|(kind, which, body, by_author)| Choice {
            kind,
            which,
            body,
            by_author,
        })
    }

    fn body(n: u8) -> &'static str {
        match n {
            0 => DECLARE_1,
            1 => DECLARE_9,
            _ => PROSE,
        }
    }

    /// Interprets the choices: a creation gets the next id; an edit or a
    /// deletion picks among the comments alive at that point, and with
    /// none alive becomes a creation. Each comment's author is whoever
    /// created it; an edit or deletion may be by anyone.
    fn history(choices: &[Choice]) -> Vec<Event> {
        let mut events = Vec::new();
        // (index, author, current body) of the comments alive.
        let mut alive: Vec<(usize, Actor, &'static str)> = Vec::new();
        let mut next = 0;
        for c in choices {
            let by = if c.by_author {
                Actor::Author
            } else {
                Actor::Stranger
            };
            let kind = if alive.is_empty() { 0 } else { c.kind };
            match kind {
                0 => {
                    let k = next;
                    next += 1;
                    let text = body(c.body);
                    alive.push((k, by, text));
                    events.push(Event::Create { k, by, body: text });
                }
                1 => {
                    let i = usize::from(c.which) % alive.len();
                    let (k, _, from) = alive[i];
                    let to = body(c.body);
                    alive[i].2 = to;
                    events.push(Event::Edit { k, by, from, to });
                }
                _ => {
                    let i = usize::from(c.which) % alive.len();
                    let (k, _, _) = alive.remove(i);
                    events.push(Event::Delete { k, by });
                }
            }
        }
        events
    }

    /// The next comment id GitHub would hand out: above every comment the
    /// fake holds, the bot's own replies included.
    fn allocate_id(world: &World) -> u64 {
        let mut github = world.github.lock().unwrap();
        let floor = github
            .comments
            .keys()
            .next_back()
            .map_or(0, |max| max.0 + 1);
        let id = github.next_comment.max(floor);
        github.next_comment = id + 1;
        id
    }

    /// The comments a run has created so far: index to (id, author).
    type Created = HashMap<usize, (u64, Actor)>;

    /// The webhook payload of an event, allocating a creation's id from
    /// the fake. A creation's author is the actor; an edit or deletion
    /// keeps the comment's author and carries the actor as the sender.
    fn payload(world: &World, created: &mut Created, event: &Event) -> Vec<u8> {
        let config = &world.config;
        match event {
            Event::Create { k, by, body } => {
                let id = allocate_id(world);
                created.insert(*k, (id, *by));
                raw_comment_json(
                    config,
                    2,
                    Some(body),
                    by.id(),
                    by.login(),
                    by.id(),
                    by.login(),
                    id,
                    "created",
                )
            }
            Event::Edit { k, by, from, to } => {
                let (id, author) = created[k];
                raw_edited_comment_json(
                    config,
                    2,
                    from,
                    to,
                    author.id(),
                    author.login(),
                    by.id(),
                    by.login(),
                    id,
                )
            }
            Event::Delete { k, by } => {
                let (id, author) = created[k];
                raw_comment_json(
                    config,
                    2,
                    Some(PROSE),
                    author.id(),
                    author.login(),
                    by.id(),
                    by.login(),
                    id,
                    "deleted",
                )
            }
        }
    }

    /// Whether comment `k` reads, at the end of the history, as the
    /// author's own declaration of #1 — as the crawl attributes text, by
    /// who wrote the comment's current bytes: the declaration, written
    /// last by the author (its creator, or its last editor). A stranger's
    /// edit to the very same text makes the bytes the stranger's — an
    /// edge live keeps and recovery may drop, never the reverse: a
    /// creation whose text matches but whose bytes are no longer the
    /// author's may hide an author's retraction in between (Codex
    /// first-contact review, P1). A deleted comment reads as nothing.
    fn authors_declaration(events: &[Event], k: usize) -> bool {
        let mut state: Option<(&'static str, Actor)> = None;
        for e in events {
            match e {
                Event::Create { k: c, by, body } if *c == k => state = Some((*body, *by)),
                Event::Edit { k: c, by, to, .. } if *c == k => state = Some((*to, *by)),
                Event::Delete { k: c, .. } if *c == k => state = None,
                _ => {}
            }
        }
        state == Some((DECLARE_1, Actor::Author))
    }

    /// PR 2's edge as the store holds it: the predecessor and the INDEX of
    /// the comment that owns the declaration.
    fn edge(processor: &Processor, created: &Created) -> (Option<PrNumber>, Option<usize>) {
        let pr = &processor.state().prs[&PrNumber(2)];
        let owner = pr.predecessor_comment_id.map(|id| {
            *created
                .iter()
                .find(|(_, (created_id, _))| *created_id == id.0)
                .map(|(k, _)| k)
                .expect("the owner is a comment the history created")
        });
        (pr.predecessor, owner)
    }

    /// Every event processed as it happens.
    fn live(events: &[Event]) -> (Option<PrNumber>, Option<usize>) {
        let (mut world, heads) = World::linear_stack(2);
        let mut processor = world.processor();
        enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
        drain(&mut processor);
        let mut created = Created::new();
        for event in events {
            let body = payload(&world, &mut created, event);
            world.enqueue(&mut processor, "issue_comment", body);
            drain(&mut processor);
        }
        edge(&processor, &created)
    }

    /// The whole history unacked when the database is lost: GitHub holds
    /// its final state, and the crawl judges the backlog against it.
    fn recovered(events: &[Event]) -> (Option<PrNumber>, Option<usize>) {
        let (mut world, heads) = World::linear_stack(2);
        let mut processor = world.processor();
        enqueue_pr_opens(&mut world, &mut processor, 2, &heads);
        drain(&mut processor);
        drop(processor);
        destroy_state_db(&world);
        let mut processor = world.processor();
        let mut created = Created::new();
        for event in events {
            let body = payload(&world, &mut created, event);
            world.enqueue(&mut processor, "issue_comment", body);
        }
        drain_with_cooldowns(&world, &mut processor);
        assert_eq!(processor.state().default_branch, "main", "the crawl landed");
        edge(&processor, &created)
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 48,
            .. ProptestConfig::default()
        })]

        #[test]
        fn recovery_reaches_what_live_processing_reached(
            choices in proptest::collection::vec(arb_choice(), 1..=8),
        ) {
            let events = history(&choices);
            let expected = live(&events);
            let actual = recovered(&events);
            let live_owner_unattributable =
                expected.1.is_some_and(|owner| !authors_declaration(&events, owner));
            let deviation_permitted = live_owner_unattributable
                && (actual == (None, None)
                    || (actual.0 == expected.0
                        && actual.1.is_some_and(|owner| authors_declaration(&events, owner))));
            prop_assert!(
                actual == expected || deviation_permitted,
                "recovered {actual:?}, live {expected:?}; history: {events:#?}"
            );
        }
    }
}
