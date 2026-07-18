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
use crate::types::{CommentId, PrNumber, Sha};

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
    let batch = processor.pump().unwrap().expect("start plans preflight");

    // GitHub goes down for the preflight execution, then recovers.
    world.github.lock().unwrap().unavailable = true;
    let outcomes = execute(&processor, &batch);
    assert!(outcomes.iter().any(|o| o.result.is_err()));
    world.github.lock().unwrap().unavailable = false;

    let mut next = processor
        .on_outcomes(batch.root, outcomes, batch.feedback)
        .unwrap();
    while let Some(batch) = next {
        let outcomes = execute(&processor, &batch);
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
    let batch = processor.pump().unwrap().expect("start 1 plans preflight");
    let _outcomes = execute(&processor, &batch);
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
                    let outcomes = execute(&processor, &batch);
                    executed += 1;
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
    processor.requeue_marked_recoveries();
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
    processor.requeue_marked_recoveries();

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
    let b1 = processor.pump().unwrap().expect("preflight");
    let o1 = execute(&processor, &b1);
    let b2 = processor
        .on_outcomes(b1.root, o1, b1.feedback)
        .unwrap()
        .expect("status post");
    let o2 = execute(&processor, &b2);
    let b3 = processor
        .on_outcomes(b2.root, o2, b2.feedback)
        .unwrap()
        .expect("refetch");
    let o3 = execute(&processor, &b3);
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

/// Onboarding: a stack that predates the bot — its predecessor declaration
/// exists only as a comment on GitHub, never delivered as a webhook — is
/// learned by the first-contact crawl, and a single `start` runs it to
/// completion.
#[test]
fn onboarding_crawl_learns_an_existing_stack() {
    let (mut world, _heads) = World::linear_stack(2);
    world.github.lock().unwrap().comments.insert(
        CommentId(1000),
        FakeComment {
            pr: PrNumber(2),
            author_id: AUTHOR,
            body: "@merge-train predecessor #1".to_owned(),
            edited: false,
        },
    );
    let mut processor = world.processor();
    // The first thing the bot ever hears about this repo is the start.
    start_command(&mut world, &mut processor, 1);
    drive_to_completion(&mut world, &mut processor);
    for i in 1..=2u64 {
        assert!(
            processor.state().prs[&PrNumber(i)].state.is_merged(),
            "PR #{i} must merge off crawl-learned topology"
        );
    }
}

/// The disaster the crawl exists for: the state DB is DESTROYED while a
/// train is mid-cascade. The next webhook triggers the crawl, which
/// rebuilds the cache and topology and adopts the train from the bot's
/// status comment; M6 recovery then resumes it to exactly-once completion.
#[test]
fn a_lost_state_db_is_rebuilt_by_the_crawl_and_the_train_resumes() {
    let (mut world, heads) = World::linear_stack(2);
    // The declaration comment exists on (fake) GitHub, as it would in
    // reality — the crawl must rebuild topology from it.
    world.github.lock().unwrap().comments.insert(
        CommentId(1000),
        FakeComment {
            pr: PrNumber(2),
            author_id: AUTHOR,
            body: "@merge-train predecessor #1".to_owned(),
            edited: false,
        },
    );
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    // Mid-`Preparing`, with the status comment live on the fake.
    run_batches_then_crash(&mut world, processor, 4);

    // The disaster: the state DB (and its WAL) is gone; the clone survives.
    let db = world.db_path();
    for path in [
        db.clone(),
        db.with_extension("db-wal"),
        db.with_extension("db-shm"),
        db.with_extension("lock"),
    ] {
        let _ = std::fs::remove_file(path);
    }

    let mut processor = world.processor();
    assert!(
        processor.state().default_branch.is_empty(),
        "precondition: the store really is fresh"
    );
    // Any webhook wakes the repo; the crawl rebuilds everything first.
    let head = {
        let github = world.github.lock().unwrap();
        github.branch_head("pr-1")
    };
    let body = check_suite_green_body(&world.config, &head, &[1], world.next_delivery + 900);
    world.enqueue(&mut processor, "check_suite", body);
    drive_to_completion(&mut world, &mut processor);
    assert_recovered_exactly_once(&world, &mut processor, "lost-db");
}

/// A frozen descendant CLOSED (unmerged) during the DB-loss gap is neither
/// open nor recently merged — the crawl fetches it individually so the
/// resumed train sees the topology and aborts CLEANLY instead of erroring
/// on a PR it cannot see (Codex crawl review, P2).
#[test]
fn a_member_closed_during_the_db_loss_gap_aborts_the_train_cleanly() {
    let (mut world, heads) = World::linear_stack(2);
    world.github.lock().unwrap().comments.insert(
        CommentId(1000),
        FakeComment {
            pr: PrNumber(2),
            author_id: AUTHOR,
            body: "@merge-train predecessor #1".to_owned(),
            edited: false,
        },
    );
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    run_batches_then_crash(&mut world, processor, 4);

    // The gap: the DB dies AND the frozen descendant is closed unmerged.
    let db = world.db_path();
    for path in [
        db.clone(),
        db.with_extension("db-wal"),
        db.with_extension("db-shm"),
        db.with_extension("lock"),
    ] {
        let _ = std::fs::remove_file(path);
    }
    world
        .github
        .lock()
        .unwrap()
        .prs
        .get_mut(&PrNumber(2))
        .unwrap()
        .state = FakePrState::Closed;

    // The wake-up IS the close webhook for the descendant.
    let mut processor = world.processor();
    let head = {
        let github = world.github.lock().unwrap();
        github.branch_head("pr-2")
    };
    let body = pr_closed_body(&world.config, 2, &head, "pr-2", "pr-1");
    world.enqueue(&mut processor, "pull_request", body);
    drain(&mut processor);
    // Give recovery its evaluation rounds (CI nudges are irrelevant here).
    for _ in 0..5 {
        drain(&mut processor);
    }

    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active()),
        "the train must end cleanly (aborted on the closed member), not sit \
         stuck active: {:?}",
        processor.state().active_trains
    );
    assert!(
        processor.state().prs.contains_key(&PrNumber(2)),
        "the closed member was fetched into the cache"
    );
}

/// A stack EXTENDED during a DB-loss gap must abort on recovery, matching
/// the live topology-change abort: a new PR declaring a stack member as its
/// predecessor appears during the outage, the crawl records it as baseline,
/// and the adopted train aborts instead of silently resuming over changed
/// topology (Codex crawl review round 4, P1).
#[test]
fn a_stack_extended_during_a_db_loss_gap_aborts_on_recovery() {
    let (mut world, heads) = World::linear_stack(2);
    world.github.lock().unwrap().comments.insert(
        CommentId(1000),
        FakeComment {
            pr: PrNumber(2),
            author_id: AUTHOR,
            body: "@merge-train predecessor #1".to_owned(),
            edited: false,
        },
    );
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    run_batches_then_crash(&mut world, processor, 4);

    // The DB dies.
    let db = world.db_path();
    for path in [
        db.clone(),
        db.with_extension("db-wal"),
        db.with_extension("db-shm"),
        db.with_extension("lock"),
    ] {
        let _ = std::fs::remove_file(path);
    }
    // During the gap a NEW PR #3 is opened declaring #2 — the stack grew
    // under the interrupted train.
    let head3 = create_branch_with_file(&world.config, "pr-3", "pr-3.txt", "content 3", "pr-2");
    create_pr_ref(&world.config, 3, &head3);
    {
        let mut github = world.github.lock().unwrap();
        github.prs.insert(
            PrNumber(3),
            FakePr {
                author_id: AUTHOR,
                branch: "pr-3".to_owned(),
                base_ref: "pr-2".to_owned(),
                state: FakePrState::Open,
            },
        );
        github.comments.insert(
            CommentId(1001),
            FakeComment {
                pr: PrNumber(3),
                author_id: AUTHOR,
                body: "@merge-train predecessor #2".to_owned(),
                edited: false,
            },
        );
    }

    // The wake-up webhook triggers the crawl, which must abort the train.
    let mut processor = world.processor();
    let head = {
        let github = world.github.lock().unwrap();
        github.branch_head("pr-1")
    };
    let body = check_suite_green_body(&world.config, &head, &[1], world.next_delivery + 900);
    world.enqueue(&mut processor, "check_suite", body);
    drain(&mut processor);

    assert!(
        processor
            .state()
            .active_trains
            .values()
            .all(|t| !t.state.is_active()),
        "the extended-stack train must not resume active: {:?}",
        processor.state().active_trains
    );
    assert!(
        matches!(
            processor
                .state()
                .active_trains
                .get(&PrNumber(1))
                .map(|t| &t.state),
            Some(crate::types::TrainState::Aborted { .. })
        ),
        "train #1 must be aborted (topology changed under it)"
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
        "an aborted train must not squash anything"
    );
}

/// A train's root closed UNMERGED during a DB-loss gap is invisible to
/// both crawl list endpoints, so its status comment would never be seen —
/// leaving the train orphaned with no abort, cleanup, or final status. The
/// wake-up webhook that names it (its own `pull_request.closed`) seeds the
/// crawl, so the train is adopted and then aborted by the close (Codex
/// crawl review round 6, P2).
#[test]
fn a_root_closed_unmerged_during_the_gap_is_seeded_and_aborted() {
    let (mut world, heads) = World::linear_stack(2);
    world.github.lock().unwrap().comments.insert(
        CommentId(1000),
        FakeComment {
            pr: PrNumber(2),
            author_id: AUTHOR,
            body: "@merge-train predecessor #1".to_owned(),
            edited: false,
        },
    );
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    run_batches_then_crash(&mut world, processor, 4);

    // The DB dies AND the root #1 is closed unmerged.
    let db = world.db_path();
    for path in [
        db.clone(),
        db.with_extension("db-wal"),
        db.with_extension("db-shm"),
        db.with_extension("lock"),
    ] {
        let _ = std::fs::remove_file(path);
    }
    let head1 = {
        let mut github = world.github.lock().unwrap();
        github.prs.get_mut(&PrNumber(1)).unwrap().state = FakePrState::Closed;
        github.branch_head("pr-1")
    };

    // The wake-up IS the close webhook for the root: it seeds the crawl
    // with #1, whose status comment is then found.
    let mut processor = world.processor();
    let body = pr_closed_body(&world.config, 1, &head1, "pr-1", "main");
    world.enqueue(&mut processor, "pull_request", body);
    drain(&mut processor);

    assert!(
        matches!(
            processor
                .state()
                .active_trains
                .get(&PrNumber(1))
                .map(|t| &t.state),
            Some(crate::types::TrainState::Aborted { .. })
        ),
        "the train whose root closed must be adopted and aborted, not orphaned: {:?}",
        processor.state().active_trains
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
        "a train aborted on a closed root must not squash"
    );
}

/// The general closed-root case: the root is closed unmerged and the
/// wake-up webhook does NOT name it — only its open descendant's
/// predecessor declaration does. The crawl must follow that declaration to
/// the closed root, find its status comment, and adopt+abort the train
/// (Codex crawl review round 7, P2 — the fixpoint expansion).
#[test]
fn a_closed_root_reached_only_via_a_descendant_declaration_is_recovered() {
    let (mut world, heads) = World::linear_stack(2);
    world.github.lock().unwrap().comments.insert(
        CommentId(1000),
        FakeComment {
            pr: PrNumber(2),
            author_id: AUTHOR,
            body: "@merge-train predecessor #1".to_owned(),
            edited: false,
        },
    );
    let mut processor = world.processor();
    world.enqueue_stack_setup(&mut processor, 2, &heads);
    start_command(&mut world, &mut processor, 1);
    run_batches_then_crash(&mut world, processor, 4);

    let db = world.db_path();
    for path in [
        db.clone(),
        db.with_extension("db-wal"),
        db.with_extension("db-shm"),
        db.with_extension("lock"),
    ] {
        let _ = std::fs::remove_file(path);
    }
    world
        .github
        .lock()
        .unwrap()
        .prs
        .get_mut(&PrNumber(1))
        .unwrap()
        .state = FakePrState::Closed;

    // The wake-up is a check_suite on the OPEN descendant #2 — it names #2,
    // not the closed root #1. Only #2's declaration reaches #1.
    let mut processor = world.processor();
    let head2 = {
        let github = world.github.lock().unwrap();
        github.branch_head("pr-2")
    };
    let body = check_suite_green_body(&world.config, &head2, &[2], world.next_delivery + 900);
    world.enqueue(&mut processor, "check_suite", body);
    drain(&mut processor);

    assert!(
        matches!(
            processor
                .state()
                .active_trains
                .get(&PrNumber(1))
                .map(|t| &t.state),
            Some(crate::types::TrainState::Aborted { .. })
        ),
        "the crawl must follow the descendant's declaration to the closed root \
         and abort its train: {:?}",
        processor.state().active_trains
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
            let outcomes = execute(self.processor.as_ref().unwrap(), &batch);
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

/// Codex crawl review rounds 1–18 were each one hand-explored corner of a
/// single question: after the state DB is destroyed, does first-contact
/// recovery (`worker/bootstrap::crawl_events` + the M6 resume path) leave
/// the system in a state the live path could defend? This module mechanizes
/// the reviewer, exactly as `interleaving` (above) mechanized the
/// saga-ordering review rounds.
///
/// - [`a_db_loss_with_a_quiet_gap_is_unobservable`] — the differential
///   property. One generated history (stack shape; valid, junk, stranger,
///   restated, and retracted declarations; starts and stops) runs in two
///   worlds: L never crashes; C crashes at a generated saga depth, loses
///   the whole DB, and recovers from the crawl. When nothing touched GitHub
///   during the outage, recovery owes EQUIVALENCE: same cache (only
///   unreferenced closed-unmerged PRs may be forgotten — neither list
///   endpoint returns them), same predecessor edges under the same owning
///   comments, same train outcomes, same merges, plus each world's own
///   absolutes (≤1 squash, exact store↔GitHub agreement, matched intent
///   ledgers, empty command backlog).
///
/// - [`gap_mutations_keep_recovery_inside_the_envelope`] — the envelope
///   property. The gap mutates GitHub while the DB is gone (closes, manual
///   merges, new stacked PRs, comment edits and deletions, a deleted status
///   comment), so equivalence is unattainable BY DESIGN; what recovery owes
///   is the documented envelope: every at-loss train whose status comment
///   survives is adopted (never orphaned); a train whose comment is gone is
///   not resurrected; an extended stack is never driven (owner ruling:
///   recovery aborts on ANY extension); every recovered predecessor edge is
///   backed by a surviving, unedited, author-authored declaration; the
///   store never claims a merge reality did not perform; and the system
///   reaches quiescence.
///
/// Comment EDITS are gap-only moves: a pre-loss edit is honored live by
/// authorizing the *editor* (`sender_id`), which the crawl cannot
/// reconstruct from `ListComments` (rounds 2/15) — that divergence is
/// documented, not accidental, so the differential property excludes edits
/// and the envelope property owns them. Likewise stranger comment
/// *deletions*: GitHub loses the comment either way, but live keeps the
/// unauthorized retraction's edge while the crawl cannot see it.
///
/// One residual the differential property EXEMPTS rather than excludes:
/// a command acknowledged but not yet answered when the DB dies is gone —
/// GitHub never redelivers an acked webhook — so its effects never happen.
/// The loss is bounded (the addressed stack) and visible (an ack reaction
/// with no follow-up); the user re-issues. See [`command_loss_exemptions`].
///
/// Every case does real git work; case counts are deliberately small. Raise
/// `PROPTEST_CASES` when touching the crawl or recovery.
mod lost_db {
    use std::collections::HashSet;

    use proptest::prelude::*;
    use proptest::sample::Index;

    use super::*;
    use crate::commands::{Command, parse_command};
    use crate::git::test_support::squash_merge_to_main;
    use crate::persistence::event::StateEventPayload;
    use crate::state::descendants::collect_all_descendants;
    use crate::status::parse::parse_status_comment;
    use crate::store::DurableCommand;
    use crate::types::{PrState, TrainRecord, TrainState};

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
                edited: false,
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
                // The author deletes their latest declaring comment.
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

        for (i, (kind, a, b)) in cmds.iter().enumerate() {
            let id = 2000 + i as u64;
            let modulus = if allow_late_decls { 4 } else { 3 };
            match kind % modulus {
                0 | 1 => {
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
                }
                // A declaration racing the running train: live records it
                // (a new-PR extension does not abort a live train); the
                // crawl must answer with the conservative abort.
                3 => {
                    let pr = a.index(n) as u64 + 1;
                    let target = b.index(n) as u64 + 1;
                    declare(world, processor, &mut h, pr, target, id, AUTHOR, "author");
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
    fn deliver_unseen_merges(world: &mut World, processor: &mut Processor) -> usize {
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

    /// Drains, plays reality's parts (CI green for whatever waits,
    /// merged-close webhooks for unheard squashes), and repeats until no
    /// train is active and nothing new was delivered. Panics if the system
    /// will not go quiet — the stuck-train detector.
    fn settle(world: &mut World, processor: &mut Processor) {
        for _round in 0..40 {
            drain(processor);
            let delivered = deliver_unseen_merges(world, processor);
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
    ) -> (RepoState, Vec<DurableCommand>) {
        let mut executed = 0;
        'outer: while executed < depth {
            while let Some(delivery) = processor.claim().unwrap() {
                processor.process_claimed(delivery).unwrap();
            }
            match processor.pump().unwrap() {
                Some(first) => {
                    let mut batch = first;
                    loop {
                        let outcomes = execute(&processor, &batch);
                        executed += 1;
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

    /// What an acknowledged-but-unanswered command's loss is allowed to
    /// change: the addressed PR, everything below it, and any at-loss train
    /// whose stack contains it. GitHub never redelivers an acked webhook,
    /// so when the DB dies holding such a command, its effects simply never
    /// happen — a bounded, visible residual (the user has an ack reaction
    /// and no follow-up, and re-issues). The differential oracle exempts
    /// exactly this blast radius; everything else still owes equivalence.
    fn command_loss_exemptions(
        at_loss: &RepoState,
        pending: &[DurableCommand],
    ) -> (HashSet<PrNumber>, HashSet<u64>) {
        let mut prs: HashSet<PrNumber> = HashSet::new();
        let mut roots: HashSet<u64> = HashSet::new();
        for command in pending {
            let pr = match command {
                DurableCommand::Start { pr } | DurableCommand::Stop { pr, .. } => *pr,
            };
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
        settle(&mut world, &mut processor);
        (world, processor)
    }

    fn run_lost(
        bases: &[usize],
        decls: &[(u8, Index, Index)],
        cmds: &[(u8, Index, Index)],
        depth: usize,
    ) -> (World, Processor, HashSet<PrNumber>, HashSet<u64>) {
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
        let (at_loss, pending) = run_batches_then_snapshot(&mut world, processor, depth);
        let (exempt_prs, exempt_roots) = command_loss_exemptions(&at_loss, &pending);
        crash_db(&world);

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
        settle(&mut world, &mut processor);
        (world, processor, exempt_prs, exempt_roots)
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
    /// agreement, matched intent ledgers for completed trains, and an empty
    /// command backlog.
    fn assert_consistent(world: &World, processor: &mut Processor, ctx: &str) {
        {
            let github = world.github.lock().unwrap();
            for (pr, count) in &github.squash_count {
                assert!(*count <= 1, "{ctx}: PR #{pr} squashed {count} times");
            }
            for (pr, fake) in &github.prs {
                let store_merged = processor
                    .state()
                    .prs
                    .get(pr)
                    .is_some_and(|c| c.state.is_merged());
                let real_merged = matches!(fake.state, FakePrState::Merged { .. });
                assert_eq!(
                    store_merged, real_merged,
                    "{ctx}: store and reality disagree about PR #{pr}"
                );
            }
        }
        let events = processor.store_mut().events().unwrap();
        let completed: Vec<PrNumber> = events
            .iter()
            .filter_map(|e| match e.payload {
                StateEventPayload::TrainCompleted { root_pr } => Some(root_pr),
                _ => None,
            })
            .collect();
        for root in completed {
            let facts = ReplayFacts::for_train(&events, root);
            assert_eq!(
                facts.unmatched().count(),
                0,
                "{ctx}: completed train #{root} has unmatched intents"
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
                continue;
            };
            if !exempt_prs.contains(pr) {
                assert_eq!(
                    pr_kind(&l.state),
                    pr_kind(&c.state),
                    "PR #{pr}: cached state diverged"
                );
            }
            assert_eq!(
                l.predecessor, c.predecessor,
                "PR #{pr}: predecessor edge diverged"
            );
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
        assert_eq!(live_trains, lost_trains, "train outcomes diverged");
    }

    // ── The envelope property's gap moves ──

    /// What the gap did to GitHub while the DB was gone, for the oracle.
    #[derive(Default, Debug)]
    struct Gap {
        closed: Vec<u64>,
        /// New stacked PRs and their declaration targets: (source, target).
        extensions: Vec<(u64, u64)>,
        /// Comments edited INTO declarations: (source pr, target).
        edited_decls: Vec<(u64, u64)>,
        /// Every comment id touched by an edit (excluded from redelivery).
        edited_ids: HashSet<u64>,
        /// Roots whose bot status comment was deleted.
        deleted_status_roots: Vec<u64>,
    }

    fn open_prs(world: &World) -> Vec<u64> {
        let github = world.github.lock().unwrap();
        let mut open: Vec<u64> = github
            .prs
            .iter()
            .filter(|(_, p)| matches!(p.state, FakePrState::Open))
            .map(|(n, _)| n.0)
            .collect();
        open.sort_unstable();
        open
    }

    /// Applies fake-only mutations — reality moving while the bot is dead.
    /// No webhooks: those died with the DB.
    fn apply_gap(world: &mut World, specs: &[(u8, Index, Index)], history: &History) -> Gap {
        let mut gap = Gap::default();
        for (j, (kind, a, b)) in specs.iter().enumerate() {
            match kind % 7 {
                // A PR is closed unmerged.
                0 => {
                    let open = open_prs(world);
                    if open.is_empty() {
                        continue;
                    }
                    let pr = open[a.index(open.len())];
                    world
                        .github
                        .lock()
                        .unwrap()
                        .prs
                        .get_mut(&PrNumber(pr))
                        .unwrap()
                        .state = FakePrState::Closed;
                    gap.closed.push(pr);
                }
                // Someone merges a PR by hand (the button, not the bot).
                1 => {
                    let open = open_prs(world);
                    if open.is_empty() {
                        continue;
                    }
                    let pr = open[a.index(open.len())];
                    let (head, config) = {
                        let github = world.github.lock().unwrap();
                        (
                            github.branch_head(&github.prs[&PrNumber(pr)].branch),
                            world.config.clone(),
                        )
                    };
                    let squash = squash_merge_to_main(&config, &head);
                    world
                        .github
                        .lock()
                        .unwrap()
                        .prs
                        .get_mut(&PrNumber(pr))
                        .unwrap()
                        .state = FakePrState::Merged {
                        squash_sha: squash.squash_sha,
                    };
                }
                // A new PR appears, stacked on an existing one, with an
                // author declaration — the round-4 extension.
                2 => {
                    let (max, target, target_branch) = {
                        let github = world.github.lock().unwrap();
                        let max = github.prs.keys().map(|p| p.0).max().unwrap();
                        let existing: Vec<u64> = {
                            let mut v: Vec<u64> = github.prs.keys().map(|p| p.0).collect();
                            v.sort_unstable();
                            v
                        };
                        let target = existing[a.index(existing.len())];
                        (max, target, github.prs[&PrNumber(target)].branch.clone())
                    };
                    let new = max + 1;
                    let branch = format!("pr-{new}");
                    let head = create_branch_with_file(
                        &world.config,
                        &branch,
                        &format!("pr-{new}.txt"),
                        &format!("content {new}"),
                        &target_branch,
                    );
                    create_pr_ref(&world.config, new, &head);
                    let mut github = world.github.lock().unwrap();
                    github.prs.insert(
                        PrNumber(new),
                        FakePr {
                            branch,
                            base_ref: target_branch,
                            state: FakePrState::Open,
                            author_id: AUTHOR,
                        },
                    );
                    github.comments.insert(
                        CommentId(3000 + j as u64),
                        FakeComment {
                            pr: PrNumber(new),
                            author_id: AUTHOR,
                            body: format!("@merge-train predecessor #{target}"),
                            edited: false,
                        },
                    );
                    gap.extensions.push((new, target));
                }
                // An existing user comment is edited INTO a declaration —
                // the round-14/15/17/18 move. The crawl cannot attribute
                // the editor, so the edge is untrusted but the possible
                // extension must still be honored.
                3 => {
                    let mut github = world.github.lock().unwrap();
                    let candidates: Vec<u64> = history
                        .user_comments
                        .iter()
                        .copied()
                        .filter(|id| github.comments.contains_key(&CommentId(*id)))
                        .collect();
                    if candidates.is_empty() {
                        continue;
                    }
                    let id = candidates[a.index(candidates.len())];
                    let max = github.prs.keys().map(|p| p.0).max().unwrap();
                    let target = b.index(max as usize) as u64 + 1;
                    let comment = github.comments.get_mut(&CommentId(id)).unwrap();
                    comment.body = format!("@merge-train predecessor #{target}");
                    comment.edited = true;
                    let source = comment.pr.0;
                    gap.edited_decls.push((source, target));
                    gap.edited_ids.insert(id);
                }
                // An existing user comment is edited AWAY (no longer a
                // command). Any edge it owned must not be reconstructed.
                4 => {
                    let mut github = world.github.lock().unwrap();
                    let candidates: Vec<u64> = history
                        .user_comments
                        .iter()
                        .copied()
                        .filter(|id| github.comments.contains_key(&CommentId(*id)))
                        .collect();
                    if candidates.is_empty() {
                        continue;
                    }
                    let id = candidates[a.index(candidates.len())];
                    let comment = github.comments.get_mut(&CommentId(id)).unwrap();
                    comment.body = "(edited away)".to_owned();
                    comment.edited = true;
                    gap.edited_ids.insert(id);
                }
                // A user comment is deleted (by anyone — no webhook, so the
                // deleter's identity is unknowable to the crawl).
                5 => {
                    let mut github = world.github.lock().unwrap();
                    let candidates: Vec<u64> = history
                        .user_comments
                        .iter()
                        .copied()
                        .filter(|id| github.comments.contains_key(&CommentId(*id)))
                        .collect();
                    if candidates.is_empty() {
                        continue;
                    }
                    let id = candidates[a.index(candidates.len())];
                    github.comments.remove(&CommentId(id));
                }
                // The bot's status comment for some root is deleted: the
                // off-disk backup is gone, and with the DB also gone the
                // train must NOT be resurrected (the documented envelope).
                6 => {
                    let mut github = world.github.lock().unwrap();
                    let mut records: Vec<(u64, u64)> = github
                        .comments
                        .iter()
                        .filter(|(_, c)| c.author_id == TEST_BOT_ID)
                        .filter_map(|(id, c)| {
                            let record = parse_status_comment(&c.body).ok()?;
                            (record.original_root_pr == c.pr).then_some((c.pr.0, id.0))
                        })
                        .collect();
                    records.sort_unstable();
                    if records.is_empty() {
                        continue;
                    }
                    let (root, id) = records[a.index(records.len())];
                    github.comments.remove(&CommentId(id));
                    gap.deleted_status_roots.push(root);
                }
                _ => unreachable!(),
            }
        }
        gap
    }

    /// The generated wake-up webhook: whatever reality happens to send
    /// first after the outage. Falls back to a check-suite on PR 1 when the
    /// chosen kind has no subject.
    fn enqueue_wakeup(
        world: &mut World,
        processor: &mut Processor,
        wake: &(u8, Index),
        history: &History,
        gap: &Gap,
    ) {
        let (kind, pick) = wake;
        match kind % 4 {
            // The close webhook for a gap-closed PR.
            1 if !gap.closed.is_empty() => {
                let pr = gap.closed[pick.index(gap.closed.len())];
                let (head, branch, base) = {
                    let github = world.github.lock().unwrap();
                    let fake = &github.prs[&PrNumber(pr)];
                    (
                        github.branch_head(&fake.branch),
                        fake.branch.clone(),
                        fake.base_ref.clone(),
                    )
                };
                let body = pr_closed_body(&world.config, pr, &head, &branch, &base);
                world.enqueue(processor, "pull_request", body);
            }
            // GitHub redelivers an old declaration comment — the round
            // 10/11 trigger. (Commands are excluded: re-running an old
            // start/stop is real live behavior but out of envelope scope,
            // and gap-edited comments would misrepresent the wire payload.)
            2 => {
                let candidates: Vec<(u64, u64, String)> = {
                    let github = world.github.lock().unwrap();
                    history
                        .user_comments
                        .iter()
                        .filter(|id| !gap.edited_ids.contains(id))
                        .filter_map(|id| {
                            let c = github.comments.get(&CommentId(*id))?;
                            matches!(
                                parse_command(&c.body, "merge-train"),
                                Some(Command::Predecessor(_))
                            )
                            .then(|| (*id, c.pr.0, c.body.clone()))
                        })
                        .collect()
                };
                if candidates.is_empty() {
                    return fallback_wakeup(world, processor);
                }
                let (id, pr, text) = candidates[pick.index(candidates.len())].clone();
                let body = user_comment_json(
                    &world.config,
                    pr,
                    Some(&text),
                    AUTHOR,
                    "author",
                    AUTHOR,
                    "author",
                    id,
                    "created",
                );
                world.enqueue(processor, "issue_comment", body);
            }
            // The opened webhook for a gap-born extension PR.
            3 if !gap.extensions.is_empty() => {
                let (pr, _) = gap.extensions[pick.index(gap.extensions.len())];
                let (head, branch, base) = {
                    let github = world.github.lock().unwrap();
                    let fake = &github.prs[&PrNumber(pr)];
                    (
                        github.branch_head(&fake.branch),
                        fake.branch.clone(),
                        fake.base_ref.clone(),
                    )
                };
                let body = pr_opened_body(&world.config, pr, &head, &branch, &base);
                world.enqueue(processor, "pull_request", body);
            }
            _ => fallback_wakeup(world, processor),
        }
    }

    fn fallback_wakeup(world: &mut World, processor: &mut Processor) {
        let head = world.github.lock().unwrap().branch_head("pr-1");
        let suite = world.next_delivery + 900;
        let body = check_suite_green_body(&world.config, &head, &[1], suite);
        world.enqueue(processor, "check_suite", body);
    }

    // ── The envelope oracle ──

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

    #[allow(clippy::too_many_arguments)]
    fn assert_envelope(
        world: &World,
        processor: &mut Processor,
        at_loss: &RepoState,
        status_roots_at_loss: &HashSet<u64>,
        gap: &Gap,
        squash_before: &HashMap<PrNumber, u32>,
    ) {
        // The absolutes: never ahead of reality, never a double squash.
        {
            let github = world.github.lock().unwrap();
            for (pr, count) in &github.squash_count {
                assert!(*count <= 1, "PR #{pr} squashed {count} times");
            }
            for (pr, cached) in &processor.state().prs {
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
        }

        let events = processor.store_mut().events().unwrap();
        let adopted: HashSet<u64> = events
            .iter()
            .filter_map(|e| match &e.payload {
                StateEventPayload::TrainRecordAdopted { root_pr, .. } => Some(root_pr.0),
                _ => None,
            })
            .collect();
        let adopted_completed: HashSet<u64> = events
            .iter()
            .filter_map(|e| match &e.payload {
                StateEventPayload::TrainRecordAdopted { root_pr, record }
                    if matches!(record.state, TrainState::Completed { .. }) =>
                {
                    Some(root_pr.0)
                }
                _ => None,
            })
            .collect();
        let aborted: HashSet<u64> = events
            .iter()
            .filter_map(|e| match &e.payload {
                StateEventPayload::TrainAborted { root_pr, .. } => Some(root_pr.0),
                _ => None,
            })
            .collect();

        let squash_delta = |members: &HashSet<PrNumber>| -> u32 {
            let github = world.github.lock().unwrap();
            members
                .iter()
                .map(|pr| {
                    github.squash_count.get(pr).copied().unwrap_or(0)
                        - squash_before.get(pr).copied().unwrap_or(0)
                })
                .sum()
        };

        for (root, record) in at_loss
            .active_trains
            .iter()
            .filter(|(_, t)| t.state.is_active())
        {
            let members = train_members(record);
            let comment_survives = status_roots_at_loss.contains(&root.0)
                && !gap.deleted_status_roots.contains(&root.0);

            if comment_survives {
                assert!(
                    adopted.contains(&root.0),
                    "train #{root} had a surviving status comment but was never \
                     adopted — orphaned by the crawl"
                );
            } else {
                // No sound record to resurrect from: the stack must behave
                // as if no train was running.
                assert!(
                    processor
                        .state()
                        .active_trains
                        .get(root)
                        .is_none_or(|t| !t.state.is_active()),
                    "train #{root} resurrected without a status comment"
                );
                assert_eq!(
                    squash_delta(&members),
                    0,
                    "train #{root} has no status comment yet its members were \
                     squashed after recovery"
                );
            }

            // The owner ruling: recovery never drives an extended stack.
            // (Idle-phase trains have no frozen set to extend yet.)
            if record.cascade_phase.progress().is_some() {
                let stack = train_stack(at_loss, record);
                let extended =
                    gap.extensions
                        .iter()
                        .chain(gap.edited_decls.iter())
                        .any(|(source, target)| {
                            stack.contains(&PrNumber(*target))
                                && !stack.contains(&PrNumber(*source))
                        });
                if extended {
                    assert_eq!(
                        squash_delta(&members),
                        0,
                        "train #{root}'s stack was extended during the gap, yet \
                         recovery squashed its members (must abort instead)"
                    );
                    if comment_survives {
                        assert!(
                            aborted.contains(&root.0) || adopted_completed.contains(&root.0),
                            "train #{root}'s stack was extended during the gap; \
                             recovery must abort it loudly (or adopt it as already \
                             complete), not leave it limbo: aborted={aborted:?}, \
                             completed={adopted_completed:?}"
                        );
                    }
                }
            }
        }

        // No phantom edges: every recovered predecessor edge is backed by a
        // surviving, unedited declaration by the PR's author saying exactly
        // that. (All pre-loss declarations were `created` comments, so an
        // edge whose comment the gap edited or deleted must be gone.)
        {
            let github = world.github.lock().unwrap();
            for (pr, cached) in &processor.state().prs {
                let (Some(target), Some(comment_id)) =
                    (cached.predecessor, cached.predecessor_comment_id)
                else {
                    continue;
                };
                let comment = github.comments.get(&comment_id);
                let backed = comment.is_some_and(|c| {
                    c.pr == *pr
                        && !c.edited
                        && github
                            .prs
                            .get(pr)
                            .is_some_and(|p| p.author_id == c.author_id)
                        && matches!(
                            parse_command(&c.body, "merge-train"),
                            Some(Command::Predecessor(t)) if t == target
                        )
                });
                assert!(
                    backed,
                    "PR #{pr}'s recovered predecessor edge to #{target} (comment \
                     {comment_id}) is not backed by a surviving unedited author \
                     declaration: {comment:?}"
                );
            }
        }

        assert!(
            processor.store_mut().pending_commands().unwrap().is_empty(),
            "acknowledged commands left unanswered at quiescence"
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

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 6,
            ..ProptestConfig::default()
        })]

        /// With a QUIET gap — nothing touched GitHub while the DB was gone
        /// — losing the database at any saga depth must be unobservable
        /// once the crawl and M6 recovery finish.
        #[test]
        fn a_db_loss_with_a_quiet_gap_is_unobservable(
            bases in arb_bases(),
            decls in proptest::collection::vec(any::<(u8, Index, Index)>(), 1..8),
            cmds in proptest::collection::vec(any::<(u8, Index, Index)>(), 0..4),
            depth in 1usize..=10,
        ) {
            let (live_world, mut live) = run_live(&bases, &decls, &cmds);
            let (lost_world, mut lost, exempt_prs, exempt_roots) =
                run_lost(&bases, &decls, &cmds, depth);
            assert_equivalent(
                &live_world,
                &mut live,
                &lost_world,
                &mut lost,
                &exempt_prs,
                &exempt_roots,
            );
        }

        /// With a NOISY gap — reality moved while the DB was gone —
        /// recovery owes the documented envelope, not equivalence.
        #[test]
        fn gap_mutations_keep_recovery_inside_the_envelope(
            bases in arb_bases(),
            decls in proptest::collection::vec(any::<(u8, Index, Index)>(), 1..8),
            cmds in proptest::collection::vec(any::<(u8, Index, Index)>(), 0..4),
            depth in 1usize..=10,
            gap_specs in proptest::collection::vec(any::<(u8, Index, Index)>(), 1..5),
            wake in any::<(u8, Index)>(),
        ) {
            let (mut world, heads) = build_world(&bases);
            let mut processor = world.processor();
            let history =
                enqueue_history(&mut world, &mut processor, &bases, &heads, &decls, &cmds, true);
            let (at_loss, _pending) = run_batches_then_snapshot(&mut world, processor, depth);
            crash_db(&world);

            // Which roots still had their off-disk backup at the moment of
            // death (before the gap has a chance to delete it).
            let status_roots_at_loss: HashSet<u64> = {
                let github = world.github.lock().unwrap();
                github
                    .comments
                    .values()
                    .filter(|c| c.author_id == TEST_BOT_ID)
                    .filter_map(|c| {
                        let record = parse_status_comment(&c.body).ok()?;
                        (record.original_root_pr == c.pr).then_some(c.pr.0)
                    })
                    .collect()
            };
            let gap = apply_gap(&mut world, &gap_specs, &history);
            let squash_before = world.github.lock().unwrap().squash_count.clone();

            let mut processor = world.processor();
            enqueue_wakeup(&mut world, &mut processor, &wake, &history, &gap);
            settle(&mut world, &mut processor);
            assert_envelope(
                &world,
                &mut processor,
                &at_loss,
                &status_roots_at_loss,
                &gap,
                &squash_before,
            );
        }
    }
}
