//! Edge-case and regression tests for the worker module.
//!
//! These tests cover specific behaviors, edge cases, and regression scenarios.
//! Core spec tests remain in worker.rs alongside the implementation.

use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

use crate::persistence::snapshot::{PersistedRepoSnapshot, save_snapshot_atomic};
use crate::spool::delivery::{SpooledDelivery, WebhookEnvelope, spool_webhook};
use crate::types::{DeliveryId, PrNumber, RepoId};
use crate::webhooks::priority::EventPriority;

use super::worker::{
    RepoWorker, WorkerConfig, apply_event_to_state, find_latest_snapshot, parse_snapshot_generation,
};

// ─── Test Helpers ───

fn make_pr_opened_envelope(pr_number: u64) -> WebhookEnvelope {
    let body = serde_json::json!({
        "action": "opened",
        "number": pr_number,
        "pull_request": {
            "number": pr_number,
            "head": {
                "sha": "a".repeat(40),
                "ref": "feature-branch"
            },
            "base": {
                "sha": "b".repeat(40),
                "ref": "main"
            },
            "draft": false,
            "merged": false,
            "user": {
                "id": 12345,
                "login": "testuser"
            }
        },
        "repository": {
            "name": "repo",
            "owner": {
                "login": "owner"
            }
        }
    });

    WebhookEnvelope {
        event_type: "pull_request".to_string(),
        headers: HashMap::new(),
        body,
    }
}

fn make_comment_envelope(pr_number: u64, body_text: &str) -> WebhookEnvelope {
    let body = serde_json::json!({
        "action": "created",
        "issue": {
            "number": pr_number,
            "pull_request": {}
        },
        "comment": {
            "id": 98765,
            "body": body_text,
            "user": {
                "id": 12345,
                "login": "testuser"
            }
        },
        "repository": {
            "name": "repo",
            "owner": {
                "login": "owner"
            }
        }
    });

    WebhookEnvelope {
        event_type: "issue_comment".to_string(),
        headers: HashMap::new(),
        body,
    }
}

// ─── Snapshot generation parsing tests ───

#[test]
fn parse_snapshot_generation_valid() {
    let path = PathBuf::from("snapshot.0.json");
    assert_eq!(parse_snapshot_generation(&path), Some(0));

    let path = PathBuf::from("snapshot.42.json");
    assert_eq!(parse_snapshot_generation(&path), Some(42));

    let path = PathBuf::from("snapshot.12345.json");
    assert_eq!(parse_snapshot_generation(&path), Some(12345));
}

#[test]
fn parse_snapshot_generation_invalid() {
    assert_eq!(
        parse_snapshot_generation(&PathBuf::from("events.0.log")),
        None
    );
    assert_eq!(
        parse_snapshot_generation(&PathBuf::from("snapshot.json")),
        None
    );
    assert_eq!(
        parse_snapshot_generation(&PathBuf::from("snapshot.abc.json")),
        None
    );
    assert_eq!(
        parse_snapshot_generation(&PathBuf::from("other.0.json")),
        None
    );
}

#[test]
fn find_latest_snapshot_returns_highest_generation() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path();

    // Create multiple snapshots
    let snapshot = PersistedRepoSnapshot::new("main");
    save_snapshot_atomic(&state_dir.join("snapshot.0.json"), &snapshot).unwrap();
    save_snapshot_atomic(&state_dir.join("snapshot.5.json"), &snapshot).unwrap();
    save_snapshot_atomic(&state_dir.join("snapshot.2.json"), &snapshot).unwrap();

    let latest = find_latest_snapshot(state_dir).unwrap().unwrap();
    assert!(latest.ends_with("snapshot.5.json"));
}

#[test]
fn find_latest_snapshot_nonexistent_dir() {
    let dir = tempdir().unwrap();
    let state_dir = dir.path().join("nonexistent");

    let result = find_latest_snapshot(&state_dir).unwrap();
    assert!(result.is_none());
}

// ─── Batch and durability tests ───

#[test]
fn done_marker_only_created_after_flush() {
    // Test oracle: .done marker is only created AFTER state is durably persisted.
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    // Spool an event
    let envelope = make_pr_opened_envelope(42);
    let delivery_id = DeliveryId::new("test-delivery");
    spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Record the initial log position
    let initial_position = worker.event_log.position().unwrap();

    // Process the event
    let result = worker.process_next().unwrap();
    assert!(result.processed, "Event should be processed");

    // After processing: log should have advanced (events written)
    let post_process_position = worker.event_log.position().unwrap();
    assert!(
        post_process_position > initial_position,
        "Event log should have events written after processing"
    );

    // Before flush: .done marker should NOT exist
    let delivery = SpooledDelivery::new(&spool_dir, delivery_id.clone());
    assert!(
        !delivery.is_done(),
        "Done marker should not exist before flush (events written but not synced)"
    );

    // After flush: event_log.sync() is called, THEN .done marker is created
    worker.flush_pending_batch().unwrap();
    assert!(
        delivery.is_done(),
        "Done marker should exist after flush (log synced)"
    );

    // Verify the event log file actually exists and has content
    let log_path = state_dir.join(format!("events.{}.log", worker.state.log_generation));
    assert!(log_path.exists(), "Event log file should exist");
    let log_content = std::fs::read_to_string(&log_path).unwrap();
    assert!(
        !log_content.is_empty(),
        "Event log should contain the persisted event"
    );
    assert!(
        log_content.contains("pr_opened"),
        "Event log should contain the PrOpened event"
    );
}

// ─── Deduplication tests ───

#[test]
fn enqueue_skips_already_done_deliveries() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    // Spool and process an event
    let envelope = make_pr_opened_envelope(42);
    let delivery_id = DeliveryId::new("already-processed");
    spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Process and flush
    let (count, _effects) = worker.process_all().unwrap();
    assert_eq!(count, 1);
    assert!(worker.queue_is_empty());

    // Manually construct a delivery for the same ID
    let delivery = SpooledDelivery::new(&spool_dir, delivery_id.clone());
    assert!(delivery.is_done(), "Delivery should be marked done");

    // Try to enqueue it again - should be skipped
    worker.enqueue(delivery).unwrap();
    assert!(
        worker.queue_is_empty(),
        "Already-done delivery should not be enqueued"
    );
}

#[test]
fn enqueue_skips_duplicate_deliveries_in_queue() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    // Spool an event
    let envelope = make_pr_opened_envelope(42);
    let delivery_id = DeliveryId::new("test-delivery");
    spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Queue should have 1 item from startup drain
    assert_eq!(worker.queue_len(), 1);

    // Try to enqueue the same delivery again
    let delivery = SpooledDelivery::new(&spool_dir, delivery_id);
    worker.enqueue(delivery).unwrap();

    // Queue should still have only 1 item (duplicate was skipped)
    assert_eq!(
        worker.queue_len(),
        1,
        "Duplicate delivery should be skipped"
    );
}

#[test]
fn enqueue_marks_malformed_delivery_done() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Malformed pull_request payload (missing required pull_request + repository fields).
    let malformed = WebhookEnvelope {
        event_type: "pull_request".to_string(),
        headers: HashMap::new(),
        body: serde_json::json!({
            "action": "opened"
        }),
    };
    let delivery_id = DeliveryId::new("malformed-runtime-delivery");
    spool_webhook(&spool_dir, &delivery_id, &malformed).unwrap();

    let delivery = SpooledDelivery::new(&spool_dir, delivery_id.clone());
    let enqueued = worker.enqueue(delivery).unwrap();

    assert!(
        !enqueued,
        "Malformed delivery should be skipped, not enqueued"
    );
    assert!(
        worker.queue_is_empty(),
        "Malformed delivery should not remain in queue"
    );
    let delivery = SpooledDelivery::new(&spool_dir, delivery_id);
    assert!(
        delivery.is_done(),
        "Malformed delivery should be marked done to avoid retry loops"
    );
}

#[tokio::test]
async fn drain_spool_periodic_does_not_count_already_queued_delivery() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Spool one valid delivery and enqueue it manually.
    let envelope = make_pr_opened_envelope(42);
    let delivery_id = DeliveryId::new("already-queued");
    spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

    let delivery = SpooledDelivery::new(&spool_dir, delivery_id);
    let enqueued = worker.enqueue(delivery).unwrap();
    assert!(enqueued);
    assert_eq!(worker.queue_len(), 1);

    // Periodic drain sees the same spool file but should not count it as newly enqueued.
    let shutdown = CancellationToken::new();
    let drained = worker.drain_spool_periodic(&shutdown).await.unwrap();
    assert_eq!(
        drained, 0,
        "Already-queued delivery should not be counted as newly drained"
    );
    assert_eq!(
        worker.queue_len(),
        1,
        "Periodic drain should not mutate queue for already-queued deliveries"
    );
}

// ─── Queue ordering tests ───

#[test]
fn multiple_stop_commands_maintain_fifo_order() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    // Spool multiple stop commands
    for i in 0..3 {
        let envelope = make_comment_envelope(42, "@merge-train stop");
        let delivery_id = DeliveryId::new(format!("stop-{}", i));
        spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();
    }

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    assert_eq!(worker.queue_len(), 3);

    // All should be high priority and in FIFO order
    let first = worker.queue.pop().unwrap();
    assert_eq!(first.priority, EventPriority::High);
    assert_eq!(first.delivery_id.as_str(), "stop-0");

    let second = worker.queue.pop().unwrap();
    assert_eq!(second.priority, EventPriority::High);
    assert_eq!(second.delivery_id.as_str(), "stop-1");

    let third = worker.queue.pop().unwrap();
    assert_eq!(third.priority, EventPriority::High);
    assert_eq!(third.delivery_id.as_str(), "stop-2");
}

#[test]
fn worker_processes_backlog_serially() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    // Spool multiple events
    for i in 0..5 {
        let envelope = make_pr_opened_envelope(i);
        let delivery_id = DeliveryId::new(format!("delivery-{}", i));
        spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();
    }

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    assert_eq!(worker.queue_len(), 5);

    // Process one at a time and verify queue decreases
    for remaining in (0..5).rev() {
        let result = worker.process_next().unwrap();
        assert!(result.processed);
        assert_eq!(worker.queue_len(), remaining);
    }

    // Queue should be empty
    assert!(worker.queue_is_empty());

    // All deliveries should be marked done after flush
    worker.flush_pending_batch().unwrap();

    for i in 0..5 {
        let delivery = SpooledDelivery::new(&spool_dir, DeliveryId::new(format!("delivery-{}", i)));
        assert!(delivery.is_done(), "Delivery {} should be marked done", i);
    }
}

#[test]
fn process_all_handles_empty_queue() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Queue should be empty
    assert!(worker.queue_is_empty());

    // process_all should return (0, empty effects) without error
    let (count, effects) = worker.process_all().unwrap();
    assert_eq!(count, 0);
    assert!(effects.is_empty());
}

#[test]
fn queue_push_returns_false_for_duplicates() {
    use crate::types::Sha;
    use crate::webhooks::events::{GitHubEvent, PrAction, PullRequestEvent};

    let mut queue = super::queue::EventQueue::new();

    let event = GitHubEvent::PullRequest(PullRequestEvent {
        repo: RepoId::new("owner", "repo"),
        action: PrAction::Opened,
        pr_number: PrNumber(42),
        merged: false,
        merge_commit_sha: None,
        head_sha: Sha::parse("a".repeat(40)).unwrap(),
        base_branch: "main".to_string(),
        head_branch: "feature".to_string(),
        is_draft: false,
        author_id: 1,
    });

    // First push should succeed
    let first = queue.push(
        event.clone(),
        DeliveryId::new("test-id"),
        EventPriority::Normal,
    );
    assert!(first, "First push should succeed");

    // Second push with same ID should fail
    let second = queue.push(event, DeliveryId::new("test-id"), EventPriority::Normal);
    assert!(!second, "Second push with same ID should fail");

    // Queue should only have 1 item
    assert_eq!(queue.len(), 1);
}

#[test]
fn queue_contains_tracks_queued_ids() {
    use crate::types::Sha;
    use crate::webhooks::events::{GitHubEvent, PrAction, PullRequestEvent};

    let mut queue = super::queue::EventQueue::new();

    let event = GitHubEvent::PullRequest(PullRequestEvent {
        repo: RepoId::new("owner", "repo"),
        action: PrAction::Opened,
        pr_number: PrNumber(42),
        merged: false,
        merge_commit_sha: None,
        head_sha: Sha::parse("a".repeat(40)).unwrap(),
        base_branch: "main".to_string(),
        head_branch: "feature".to_string(),
        is_draft: false,
        author_id: 1,
    });

    let delivery_id = DeliveryId::new("test-id");

    // Before push: should not contain
    assert!(!queue.contains(&delivery_id));

    // After push: should contain
    queue.push(event, delivery_id.clone(), EventPriority::Normal);
    assert!(queue.contains(&delivery_id));

    // After pop: should not contain
    let _ = queue.pop();
    assert!(!queue.contains(&delivery_id));
}

// ─── Polling and timer tests ───

#[test]
fn poll_config_has_sane_defaults() {
    let config = super::poll::PollConfig::new();

    // 10 minute poll interval
    assert_eq!(config.poll_interval.as_secs(), 600);
    // 5 minute wait timeout
    assert_eq!(config.wait_timeout.as_secs(), 300);
    // 5 second recheck interval
    assert_eq!(config.recheck_interval.as_secs(), 5);
    // 20% jitter
    assert_eq!(config.jitter_percent, 20);
}

#[test]
fn time_until_next_poll_returns_none_when_no_trains() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let worker = RepoWorker::new(config).unwrap();

    // No active trains means no polling needed
    assert!(worker.state.active_trains.is_empty());

    // time_until_next_poll should return None when there are no active trains
    assert!(
        worker.time_until_next_poll().is_none(),
        "time_until_next_poll should return None when no trains are active"
    );
}

#[test]
fn time_until_next_poll_returns_some_when_trains_active() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Add an active train
    let train = crate::types::TrainRecord::new(PrNumber(10));
    worker.state.active_trains.insert(PrNumber(10), train);

    // time_until_next_poll should return Some duration when trains are active
    assert!(
        worker.time_until_next_poll().is_some(),
        "time_until_next_poll should return Some when trains are active"
    );
}

#[test]
fn find_train_root_finds_root_by_current_pr() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Add an active train
    let train = crate::types::TrainRecord::new(PrNumber(10));
    worker.state.active_trains.insert(PrNumber(10), train);

    // Train root should be found by root PR
    assert_eq!(worker.find_train_root(PrNumber(10)), Some(PrNumber(10)));

    // Non-existent PR should not be found
    assert_eq!(worker.find_train_root(PrNumber(99)), None);
}

// ─── Spool drain tests ───

#[tokio::test]
async fn drain_spool_periodic_picks_up_missed_deliveries() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Process any startup deliveries
    let (initial_count, _) = worker.process_all().unwrap();
    assert_eq!(initial_count, 0, "Should start empty");

    // Now spool a new delivery (simulating one that was missed)
    let envelope = make_pr_opened_envelope(99);
    let delivery_id = DeliveryId::new("missed-delivery");
    spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

    // Run periodic drain
    let shutdown = CancellationToken::new();
    let drained = worker.drain_spool_periodic(&shutdown).await.unwrap();
    assert_eq!(drained, 1, "Should have drained the missed delivery");

    // Delivery should be processed and marked done after flush
    worker.flush_pending_batch().unwrap();
    let delivery = SpooledDelivery::new(&spool_dir, delivery_id);
    assert!(delivery.is_done(), "Drained delivery should be marked done");
}

#[tokio::test]
async fn drain_spool_periodic_skips_already_done() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    // Spool and process a delivery first
    let envelope = make_pr_opened_envelope(42);
    let delivery_id = DeliveryId::new("already-done");
    spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Process it
    let (count, _) = worker.process_all().unwrap();
    assert_eq!(count, 1);

    // Now run periodic drain - should not pick it up again
    let shutdown = CancellationToken::new();
    let drained = worker.drain_spool_periodic(&shutdown).await.unwrap();
    assert_eq!(drained, 0, "Already-done delivery should not be re-drained");
}

#[tokio::test]
async fn startup_backlog_re_evaluates_cascades() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    // Spool a delivery that will be in the startup backlog
    let envelope = make_pr_opened_envelope(42);
    let delivery_id = DeliveryId::new("startup-backlog");
    spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Add an active train to verify cascade re-evaluation considers it.
    let mut train = crate::types::TrainRecord::new(PrNumber(10));
    train.state = crate::types::TrainState::Running;
    worker.state.active_trains.insert(PrNumber(10), train);

    // Create the stack token that would normally be created on train start
    worker.get_or_create_stack_token(PrNumber(10));

    // Process the startup backlog
    let shutdown = CancellationToken::new();
    let (count, effects) = worker.process_all().unwrap();
    assert_eq!(count, 1, "Should process one delivery from startup backlog");

    // Execute effects (which may update state)
    if !effects.is_empty() {
        worker.execute_effects(effects, &shutdown).await.unwrap();
    }

    // Re-evaluate cascades (the key behavior we're testing)
    let result = worker
        .re_evaluate_cascades_with_persistence(&shutdown, "test")
        .await;
    assert!(result.is_ok(), "Cascade re-evaluation should not error");

    // The train should still exist
    assert!(
        worker.state.active_trains.contains_key(&PrNumber(10)),
        "Active train should still exist after re-evaluation"
    );
}

#[tokio::test]
async fn periodic_spool_drain_re_evaluates_cascades() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Process any existing deliveries
    let (initial_count, _) = worker.process_all().unwrap();
    assert_eq!(initial_count, 0, "Should start empty");

    // Add an active train
    let mut train = crate::types::TrainRecord::new(PrNumber(10));
    train.state = crate::types::TrainState::Running;
    worker.state.active_trains.insert(PrNumber(10), train);
    worker.get_or_create_stack_token(PrNumber(10));

    // Now spool a new delivery (simulating one that was missed)
    let envelope = make_pr_opened_envelope(99);
    let delivery_id = DeliveryId::new("missed-delivery-cascade-test");
    spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

    // Run handle_periodic_spool_drain
    let shutdown = CancellationToken::new();
    let result = worker.handle_periodic_spool_drain(&shutdown).await;
    assert!(
        result.is_ok(),
        "Periodic spool drain with cascade re-evaluation should not error"
    );

    // Verify the delivery was processed
    worker.flush_pending_batch().unwrap();
    let delivery = SpooledDelivery::new(&spool_dir, delivery_id);
    assert!(
        delivery.is_done(),
        "Drained delivery should be marked done after cascade re-evaluation"
    );

    // The active train should still exist
    assert!(
        worker.state.active_trains.contains_key(&PrNumber(10)),
        "Active train should still exist after periodic drain"
    );
}

// ─── Cancellation token tests ───

#[test]
fn existing_trains_get_tokens_at_startup() {
    use crate::types::TrainRecord;

    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    // Create a snapshot with an active train
    std::fs::create_dir_all(&state_dir).unwrap();
    let mut snapshot = PersistedRepoSnapshot::new("main");
    snapshot
        .active_trains
        .insert(PrNumber(42), TrainRecord::new(PrNumber(42)));
    snapshot
        .active_trains
        .insert(PrNumber(99), TrainRecord::new(PrNumber(99)));
    save_snapshot_atomic(&state_dir.join("snapshot.0.json"), &snapshot).unwrap();

    // Create worker - it should load the snapshot and create tokens
    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let worker = RepoWorker::new(config).unwrap();

    // Verify tokens were created for existing trains
    assert!(
        worker.stack_tokens.contains_key(&PrNumber(42)),
        "Token should exist for train 42 loaded from snapshot"
    );
    assert!(
        worker.stack_tokens.contains_key(&PrNumber(99)),
        "Token should exist for train 99 loaded from snapshot"
    );

    // Tokens should not be cancelled initially
    assert!(
        !worker
            .stack_tokens
            .get(&PrNumber(42))
            .unwrap()
            .is_cancelled(),
        "Token for train 42 should not be cancelled initially"
    );
    assert!(
        !worker
            .stack_tokens
            .get(&PrNumber(99))
            .unwrap()
            .is_cancelled(),
        "Token for train 99 should not be cancelled initially"
    );
}

// ─── Critical event tests ───

#[test]
fn critical_event_syncs_log_before_done_marker_even_when_no_pending_batch() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    // Create a start command envelope that will trigger TrainStarted (critical)
    let envelope = make_comment_envelope(42, "@merge-train start");
    let delivery_id = DeliveryId::new("critical-test");
    spool_webhook(&spool_dir, &delivery_id, &envelope).unwrap();

    // We also need a PR in state for the start command to work
    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir)
        .with_bot_name("merge-train");
    let mut worker = RepoWorker::new(config).unwrap();

    // Add a PR to state so the start command can work
    let cached_pr = crate::types::CachedPr::new(
        PrNumber(42),
        crate::types::Sha::parse("a".repeat(40)).unwrap(),
        "feature".to_string(),
        "main".to_string(), // targets main, so it's a root
        None,               // no predecessor
        crate::types::PrState::Open,
        crate::types::MergeStateStatus::Clean,
        false,
    );
    worker.state.prs.insert(PrNumber(42), cached_pr);

    // Ensure pending_batch is empty before we start
    assert!(
        worker.pending_batch.is_empty(),
        "pending_batch should be empty initially"
    );

    // Record the initial log position
    let initial_position = worker.event_log.position().unwrap();

    // Process the event
    let result = worker.process_next().unwrap();
    assert!(result.processed, "Event should be processed");

    // After processing: log should have advanced
    let post_process_position = worker.event_log.position().unwrap();
    assert!(
        post_process_position > initial_position,
        "Event log should have events written after processing"
    );

    // For critical events, done marker should be created immediately
    let delivery = SpooledDelivery::new(&spool_dir, delivery_id.clone());
    assert!(
        delivery.is_done(),
        "Done marker should exist immediately after critical event is processed"
    );

    // Verify the train was actually started
    assert!(
        worker.state.active_trains.contains_key(&PrNumber(42)),
        "Train should be active after start command"
    );

    // Verify the event log contains the TrainStarted event
    let log_path = state_dir.join(format!("events.{}.log", worker.state.log_generation));
    assert!(log_path.exists(), "Event log file should exist");
    let log_content = std::fs::read_to_string(&log_path).unwrap();
    assert!(
        log_content.contains("train_started"),
        "Event log should contain the TrainStarted event: {}",
        log_content
    );
}

// ─── Poll response tests ───

#[test]
fn poll_response_updates_cached_pr_state() {
    use crate::effects::PrData;

    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Add a PR to state with initial values
    let initial_sha = crate::types::Sha::parse("a".repeat(40)).unwrap();
    let cached_pr = crate::types::CachedPr::new(
        PrNumber(42),
        initial_sha.clone(),
        "feature".to_string(),
        "main".to_string(),
        None,
        crate::types::PrState::Open,
        crate::types::MergeStateStatus::Unknown,
        false,
    );
    worker.state.prs.insert(PrNumber(42), cached_pr);

    // Simulate a PrRefetched response with updated data
    let new_sha = crate::types::Sha::parse("b".repeat(40)).unwrap();
    let response = crate::effects::GitHubResponse::PrRefetched {
        pr: PrData {
            number: PrNumber(42),
            head_sha: new_sha.clone(),
            head_ref: "feature-updated".to_string(),
            base_ref: "develop".to_string(),
            state: crate::types::PrState::Open,
            is_draft: true,
        },
        merge_state: crate::types::MergeStateStatus::Clean,
    };

    // Process the response
    worker.process_github_response(response);

    // Verify the cached PR was updated
    let updated_pr = worker.state.prs.get(&PrNumber(42)).unwrap();
    assert_eq!(updated_pr.head_sha, new_sha, "head_sha should be updated");
    assert_eq!(
        updated_pr.head_ref, "feature-updated",
        "head_ref should be updated"
    );
    assert_eq!(updated_pr.base_ref, "develop", "base_ref should be updated");
    assert!(updated_pr.is_draft, "is_draft should be updated");
    assert_eq!(
        updated_pr.merge_state_status,
        crate::types::MergeStateStatus::Clean,
        "merge_state_status should be updated"
    );
}

#[test]
fn poll_response_creates_new_pr_if_not_cached() {
    use crate::effects::PrData;

    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Verify PR 42 doesn't exist initially
    assert!(!worker.state.prs.contains_key(&PrNumber(42)));

    // Simulate a PrRefetched response for a new PR
    let sha = crate::types::Sha::parse("c".repeat(40)).unwrap();
    let response = crate::effects::GitHubResponse::PrRefetched {
        pr: PrData {
            number: PrNumber(42),
            head_sha: sha.clone(),
            head_ref: "new-feature".to_string(),
            base_ref: "main".to_string(),
            state: crate::types::PrState::Open,
            is_draft: false,
        },
        merge_state: crate::types::MergeStateStatus::Clean,
    };

    // Process the response
    worker.process_github_response(response);

    // Verify the PR was created
    assert!(
        worker.state.prs.contains_key(&PrNumber(42)),
        "PR should be created"
    );
    let created_pr = worker.state.prs.get(&PrNumber(42)).unwrap();
    assert_eq!(created_pr.head_sha, sha);
    assert_eq!(created_pr.head_ref, "new-feature");
    assert_eq!(created_pr.base_ref, "main");
    assert!(!created_pr.is_draft);
    assert_eq!(
        created_pr.merge_state_status,
        crate::types::MergeStateStatus::Clean
    );
    // Predecessor is not discovered via API, so it should be None
    assert!(created_pr.predecessor.is_none());
}

// ─── Cascade scan tests ───

#[test]
fn late_addition_scan_detects_orphaned_prs() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Add predecessor PR (merged)
    let merge_sha = crate::types::Sha::parse("a".repeat(40)).unwrap();
    let pred_pr = crate::types::CachedPr::new(
        PrNumber(1),
        crate::types::Sha::parse("b".repeat(40)).unwrap(),
        "feature-1".to_string(),
        "main".to_string(),
        None,
        crate::types::PrState::Merged {
            merge_commit_sha: merge_sha,
        },
        crate::types::MergeStateStatus::Unknown,
        false,
    );
    worker.state.prs.insert(PrNumber(1), pred_pr);

    // Add descendant PR (still targets feature-1, not main)
    let mut desc_pr = crate::types::CachedPr::new(
        PrNumber(2),
        crate::types::Sha::parse("c".repeat(40)).unwrap(),
        "feature-2".to_string(),
        "feature-1".to_string(), // NOT retargeted to main yet
        Some(PrNumber(1)),       // predecessor declared
        crate::types::PrState::Open,
        crate::types::MergeStateStatus::Unknown,
        false,
    );
    desc_pr.predecessor = Some(PrNumber(1));
    worker.state.prs.insert(PrNumber(2), desc_pr);

    // Use the actual helper function
    let late_additions = worker.find_late_additions();

    assert_eq!(late_additions.len(), 1, "Should detect one late addition");
    assert_eq!(
        late_additions[0].0,
        PrNumber(2),
        "PR #2 should be the late addition"
    );
    assert_eq!(
        late_additions[0].1,
        PrNumber(1),
        "Predecessor should be PR #1"
    );
}

#[test]
fn manually_retargeted_scan_detects_dangerous_prs() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Add predecessor PR (merged)
    let merge_sha = crate::types::Sha::parse("a".repeat(40)).unwrap();
    let pred_pr = crate::types::CachedPr::new(
        PrNumber(1),
        crate::types::Sha::parse("b".repeat(40)).unwrap(),
        "feature-1".to_string(),
        "main".to_string(),
        None,
        crate::types::PrState::Merged {
            merge_commit_sha: merge_sha,
        },
        crate::types::MergeStateStatus::Unknown,
        false,
    );
    worker.state.prs.insert(PrNumber(1), pred_pr);

    // Add descendant PR (manually retargeted to main, NOT reconciled)
    let mut desc_pr = crate::types::CachedPr::new(
        PrNumber(2),
        crate::types::Sha::parse("c".repeat(40)).unwrap(),
        "feature-2".to_string(),
        "main".to_string(), // Already retargeted to main
        Some(PrNumber(1)),  // predecessor declared
        crate::types::PrState::Open,
        crate::types::MergeStateStatus::Clean,
        false,
    );
    desc_pr.predecessor = Some(PrNumber(1));
    desc_pr.predecessor_squash_reconciled = None; // NOT reconciled!
    worker.state.prs.insert(PrNumber(2), desc_pr);

    // Use the actual helper function
    let dangerous = worker.find_manually_retargeted();

    assert_eq!(dangerous.len(), 1, "Should detect one dangerous PR");
    assert_eq!(
        dangerous[0].0,
        PrNumber(2),
        "PR #2 should be the dangerous one"
    );
    assert_eq!(dangerous[0].1, PrNumber(1), "Predecessor should be PR #1");
}

#[test]
fn properly_reconciled_pr_is_not_flagged() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Add predecessor PR (merged)
    let merge_sha = crate::types::Sha::parse("a".repeat(40)).unwrap();
    let pred_pr = crate::types::CachedPr::new(
        PrNumber(1),
        crate::types::Sha::parse("b".repeat(40)).unwrap(),
        "feature-1".to_string(),
        "main".to_string(),
        None,
        crate::types::PrState::Merged {
            merge_commit_sha: merge_sha.clone(),
        },
        crate::types::MergeStateStatus::Unknown,
        false,
    );
    worker.state.prs.insert(PrNumber(1), pred_pr);

    // Add descendant PR (retargeted and properly reconciled)
    let mut desc_pr = crate::types::CachedPr::new(
        PrNumber(2),
        crate::types::Sha::parse("c".repeat(40)).unwrap(),
        "feature-2".to_string(),
        "main".to_string(),
        Some(PrNumber(1)),
        crate::types::PrState::Open,
        crate::types::MergeStateStatus::Clean,
        false,
    );
    desc_pr.predecessor = Some(PrNumber(1));
    desc_pr.predecessor_squash_reconciled = Some(merge_sha); // Properly reconciled
    worker.state.prs.insert(PrNumber(2), desc_pr);

    // This PR should NOT be flagged
    let dangerous = worker.find_manually_retargeted();

    assert!(dangerous.is_empty(), "Reconciled PR should not be flagged");
}

// ─── Phase transition tests ───

#[test]
fn phase_transition_persistence_updates_train_in_state() {
    let dir = tempdir().unwrap();
    let spool_dir = dir.path().join("spool");
    let state_dir = dir.path().join("state");

    let config = WorkerConfig::new(RepoId::new("owner", "repo"), &spool_dir, &state_dir);
    let mut worker = RepoWorker::new(config).unwrap();

    // Add an active train
    let train = crate::types::TrainRecord::new(PrNumber(10));
    worker.state.active_trains.insert(PrNumber(10), train);

    // Create a PhaseTransition event
    let squash_sha = crate::types::Sha::parse("d".repeat(40)).unwrap();
    let new_phase = crate::types::CascadePhase::Reconciling {
        progress: crate::types::DescendantProgress::new(vec![]),
        squash_sha: squash_sha.clone(),
    };

    let event = worker
        .event_log
        .append(
            crate::persistence::event::StateEventPayload::PhaseTransition {
                train_root: PrNumber(10),
                current_pr: PrNumber(11),
                predecessor_pr: Some(PrNumber(10)),
                last_squash_sha: Some(squash_sha.clone()),
                phase: new_phase.clone(),
                state: None,
                wait_condition: None,
            },
        )
        .unwrap();

    // Apply the event to state
    apply_event_to_state(&mut worker.state, &event);

    // Verify the train was updated
    let updated_train = worker.state.active_trains.get(&PrNumber(10)).unwrap();
    assert_eq!(
        updated_train.current_pr,
        PrNumber(11),
        "current_pr should be updated"
    );
    assert_eq!(
        updated_train.last_squash_sha,
        Some(squash_sha),
        "last_squash_sha should be updated"
    );
    assert_eq!(
        updated_train.cascade_phase.name(),
        "reconciling",
        "cascade_phase should be updated to Reconciling"
    );
}

// ─── Config tests ───

#[test]
fn wait_timeout_is_configurable() {
    let default_config = super::poll::PollConfig::new();
    let custom_config = super::poll::PollConfig::new().with_wait_timeout(
        std::time::Duration::from_secs(600), // 10 minutes
    );

    // Default should be 5 minutes
    assert_eq!(default_config.wait_timeout.as_secs(), 300);

    // Custom should be 10 minutes
    assert_eq!(custom_config.wait_timeout.as_secs(), 600);
}
