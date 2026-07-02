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
use crate::github::test_support::{FakeGitHub, FakePr, FakePrState};
use crate::state::RepoState;
use crate::store::Store;
use crate::types::{PrNumber, Sha};

use super::executor::{GitHubExec, SagaBatch, execute_batch};
use super::pipeline::{PipelineOutcome, Processor, WorkerDeps};
use super::test_support::TEST_BOT_ID;
use super::{GitSettings, IntakeDelivery, WorkerMsg};

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
                "updated_at": "2026-07-01T10:00:0{comment_id}Z"
            }},
            "issue": {{
                "number": {pr},
                "pull_request": {{ "url": "..." }},
                "user": {{ "id": {AUTHOR}, "login": "author" }}
            }},
            "repository": {repo},
            "sender": {{ "id": {commenter_id}, "login": "{commenter_login}" }}
        }}"#,
        comment_id = comment_id % 10,
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
        let github = Arc::new(Mutex::new(FakeGitHub::new(config.clone(), fake_prs)));
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
fn execute(processor: &Processor, batch: &SagaBatch) -> Vec<EffectOutcome> {
    let interpreter = WorktreeGitInterpreter::new(processor.git_config(), batch.root);
    execute_batch(&interpreter, processor.github(), batch)
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
        delete_body(0, STRANGER, "stranger"),
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
        5,
    );
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);
    world.enqueue(
        &mut processor,
        "issue_comment",
        delete_body(5, AUTHOR, "author"),
    );
    drain(&mut processor);
    assert_eq!(
        declared(&processor),
        None,
        "the author's own deletion must retract the declaration"
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
    let batch = processor.pump().unwrap().expect("start plans a saga");
    let outcomes = execute(&processor, &batch);

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
        let outcomes = execute(&processor, &batch);
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
        let outcomes = execute(&processor, &batch);
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
        let outcomes = execute(&processor, &batch);
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
        let outcomes = execute(&processor, &batch);
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
        let outcomes = execute(&processor, &batch);
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
    let preflight = processor.pump().unwrap().expect("start plans preflight");
    let outcomes = execute(&processor, &preflight);
    let batch = processor
        .on_outcomes(preflight.root, outcomes, preflight.feedback)
        .unwrap()
        .expect("preflight completion continues the saga");
    assert!(
        processor.state().active_trains.contains_key(&PrNumber(1)),
        "the train must exist before the stop for this test to bite"
    );
    let outcomes = execute(&processor, &batch);
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
        let outcomes = execute(&processor, &batch);
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
    let preflight = processor.pump().unwrap().expect("start plans preflight");
    let outcomes = execute(&processor, &preflight);
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
    let batch = processor.pump().unwrap().expect("start 1 plans preflight");
    let outcomes = execute(&processor, &batch);

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
        let outcomes = execute(&processor, &batch);
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
    let batch = processor.pump().unwrap().expect("start 1 plans preflight");
    let outcomes = execute(&processor, &batch);

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
        let outcomes = execute(&processor, &batch);
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
    let batch = processor.pump().unwrap().expect("start plans preflight");
    let outcomes = execute(&processor, &batch);

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

// ─── Inherited mid-flight trains are refused until M6 ───

#[test]
fn inherited_mid_flight_train_refuses_evaluation_but_stops_cleanly() {
    let (mut world, heads) = World::linear_stack(2);
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);

    // Run the saga a few steps in (train active, mid-cascade), then "crash".
    while let Some(delivery) = processor.claim().unwrap() {
        processor.process_claimed(delivery).unwrap();
    }
    let mut batch = processor.pump().unwrap().expect("start plans a saga");
    for _ in 0..4 {
        let outcomes = execute(&processor, &batch);
        match processor
            .on_outcomes(batch.root, outcomes, batch.feedback)
            .unwrap()
        {
            Some(next) => batch = next,
            None => break,
        }
    }
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .any(|t| t.state.is_active()),
        "precondition: the train is mid-flight"
    );
    drop(processor);

    // A fresh process inherits the mid-flight train.
    let mut processor = world.processor();

    // CI webhooks would normally re-drive it; the worker must refuse.
    let head = {
        let github = world.github.lock().unwrap();
        github.branch_head("pr-1")
    };
    let body = check_suite_green_body(&world.config, &head, &[1], 800);
    world.enqueue(&mut processor, "check_suite", body);
    drain(&mut processor);
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .any(|t| t.state.is_active()),
        "an inherited mid-flight train must not advance before M6"
    );
    assert_eq!(
        world
            .github
            .lock()
            .unwrap()
            .squash_count
            .values()
            .sum::<u32>(),
        0
    );

    // But a stop works: terminal event + cleanup.
    assert!(!processor.inherited_markers().is_empty());
    let body = comment_body(&world.config, 1, "@merge-train stop", AUTHOR, "author", 44);
    world.enqueue(&mut processor, "issue_comment", body);
    drain(&mut processor);
    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active())
    );
    // Stopping the inherited train must clear its refuse-evaluation marker,
    // or a restarted train that parks (e.g. waiting on CI in production,
    // where mergeability starts Unknown) is refused every evaluation until
    // the process restarts (Codex M5 review).
    assert!(
        processor.inherited_markers().is_empty(),
        "the stop must clear the inherited marker"
    );

    // The documented recovery path continues: re-issuing `start` must yield
    // a train that actually advances.
    // (A fresh comment id: re-using the first start's would dedupe.)
    let body = comment_body(&world.config, 1, "@merge-train start", AUTHOR, "author", 99);
    world.enqueue(&mut processor, "issue_comment", body);
    drive_to_completion(&mut world, &mut processor);
    for i in 1..=2u64 {
        assert!(
            processor.state().prs[&PrNumber(i)].state.is_merged(),
            "PR #{i} did not merge after the stop-and-restart recovery"
        );
    }
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
