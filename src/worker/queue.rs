//! Priority queue for webhook event processing.
//!
//! Events are ordered by priority (stop commands first), then by sequence number
//! (FIFO within the same priority level). This ensures stop commands can halt
//! cascades promptly while maintaining ordering guarantees.
//!
//! # Priority Levels
//!
//! - `High`: Stop commands (`@merge-train stop`, `@merge-train stop --force`)
//! - `Normal`: All other events
//!
//! From DESIGN.md:
//! > "priority: stop > others"

use std::collections::{BinaryHeap, HashSet};

use crate::types::{DeliveryId, MergeStateStatus, PrNumber, Sha};
use crate::webhooks::events::GitHubEvent;
use crate::webhooks::priority::EventPriority;

/// Condition being waited for during non-blocking polling.
///
/// When the bot needs to wait for GitHub state to propagate, it records
/// what condition it's waiting for and schedules timer-based re-checks.
///
/// Each variant includes a retry count for exponential backoff.
///
/// This type is serializable so it can be persisted in the train record
/// for recovery after restart.
///
/// # Implemented vs Planned
///
/// Currently only `CheckSuiteCompleted` is scheduled by the worker when entering
/// the `WaitingCi` state. The other variants are defined for future use:
///
/// - `HeadRefOid`: Will be scheduled after push operations to wait for GitHub
///   to update the PR's headRefOid. This handles eventual consistency where
///   GitHub's API may return stale data immediately after a push.
///
/// - `MergeStateStatus`: Will be scheduled when merge state is `Unknown` to
///   wait for GitHub to compute the mergeable status. This handles the delay
///   between PR updates and GitHub's merge conflict computation.
///
/// These wait conditions are not yet scheduled because:
/// 1. Stage 17 focuses on correctness of the core cascade logic
/// 2. The poll fallback (10-minute interval) catches these cases eventually
/// 3. Full implementation requires integration with Stage 18's git operations
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WaitCondition {
    /// Waiting for headRefOid to match after a push.
    ///
    /// **Status**: Defined but not yet scheduled. The infrastructure exists for
    /// persistence and timer handling, but no code path currently creates this
    /// condition. Rely on poll fallback until Stage 18 git integration.
    HeadRefOid {
        pr: PrNumber,
        expected: Sha,
        retry_count: u32,
    },

    /// Waiting for mergeStateStatus to change from a specific value.
    ///
    /// **Status**: Defined but not yet scheduled. Would be useful after detecting
    /// `Unknown` merge state to expedite re-evaluation, but currently relies on
    /// poll fallback and webhook-triggered re-evaluation.
    MergeStateStatus {
        pr: PrNumber,
        not: MergeStateStatus,
        retry_count: u32,
    },

    /// Waiting for check suite to complete for a specific SHA.
    ///
    /// **Status**: Actively used. Scheduled when entering `WaitingCi` state after
    /// a cascade step. Enables 5-second re-checks for CI completion detection.
    CheckSuiteCompleted { sha: Sha, retry_count: u32 },
}

impl WaitCondition {
    /// Creates a new HeadRefOid wait condition with retry count 0.
    pub fn head_ref_oid(pr: PrNumber, expected: Sha) -> Self {
        WaitCondition::HeadRefOid {
            pr,
            expected,
            retry_count: 0,
        }
    }

    /// Creates a new MergeStateStatus wait condition with retry count 0.
    pub fn merge_state_status(pr: PrNumber, not: MergeStateStatus) -> Self {
        WaitCondition::MergeStateStatus {
            pr,
            not,
            retry_count: 0,
        }
    }

    /// Creates a new CheckSuiteCompleted wait condition with retry count 0.
    pub fn check_suite_completed(sha: Sha) -> Self {
        WaitCondition::CheckSuiteCompleted {
            sha,
            retry_count: 0,
        }
    }

    /// Returns the current retry count.
    pub fn retry_count(&self) -> u32 {
        match self {
            WaitCondition::HeadRefOid { retry_count, .. } => *retry_count,
            WaitCondition::MergeStateStatus { retry_count, .. } => *retry_count,
            WaitCondition::CheckSuiteCompleted { retry_count, .. } => *retry_count,
        }
    }

    /// Returns a new condition with the retry count incremented.
    pub fn with_incremented_retry(&self) -> Self {
        match self {
            WaitCondition::HeadRefOid {
                pr,
                expected,
                retry_count,
            } => WaitCondition::HeadRefOid {
                pr: *pr,
                expected: expected.clone(),
                retry_count: retry_count + 1,
            },
            WaitCondition::MergeStateStatus {
                pr,
                not,
                retry_count,
            } => WaitCondition::MergeStateStatus {
                pr: *pr,
                not: *not,
                retry_count: retry_count + 1,
            },
            WaitCondition::CheckSuiteCompleted { sha, retry_count } => {
                WaitCondition::CheckSuiteCompleted {
                    sha: sha.clone(),
                    retry_count: retry_count + 1,
                }
            }
        }
    }
}

/// An entry in the priority queue.
///
/// Events are ordered by:
/// 1. Priority (high priority first)
/// 2. Sequence number (lower sequence numbers first, FIFO)
#[derive(Debug, Clone)]
pub struct QueuedEvent {
    /// The parsed GitHub event.
    pub event: GitHubEvent,

    /// The delivery ID from GitHub (for marking done after processing).
    pub delivery_id: DeliveryId,

    /// Event priority (high for stop commands, normal for others).
    pub priority: EventPriority,

    /// Sequence number for FIFO ordering within the same priority level.
    /// Lower numbers are processed first.
    sequence: u64,
}

impl QueuedEvent {
    /// Creates a new queued event.
    pub fn new(
        event: GitHubEvent,
        delivery_id: DeliveryId,
        priority: EventPriority,
        sequence: u64,
    ) -> Self {
        QueuedEvent {
            event,
            delivery_id,
            priority,
            sequence,
        }
    }
}

// Custom ordering for the priority queue.
// BinaryHeap is a max-heap, so we need to reverse the ordering for FIFO.
impl PartialEq for QueuedEvent {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Eq for QueuedEvent {}

impl PartialOrd for QueuedEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueuedEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority first, then lower sequence number first (FIFO)
        match self.priority.cmp(&other.priority) {
            std::cmp::Ordering::Equal => {
                // For FIFO: lower sequence number should come first.
                // BinaryHeap is a max-heap, so we need to reverse this.
                other.sequence.cmp(&self.sequence)
            }
            other_ordering => other_ordering,
        }
    }
}

/// A priority queue for webhook events.
///
/// Uses a binary heap (max-heap) to efficiently process high-priority events first.
/// Within the same priority level, events are processed in FIFO order.
///
/// Tracks delivery IDs to prevent duplicate enqueueing.
#[derive(Debug, Default)]
pub struct EventQueue {
    /// The underlying binary heap.
    heap: BinaryHeap<QueuedEvent>,

    /// Counter for generating sequence numbers.
    next_sequence: u64,

    /// Set of delivery IDs currently in the queue.
    /// Used to prevent duplicate enqueueing.
    queued_ids: HashSet<DeliveryId>,
}

impl EventQueue {
    /// Creates a new empty event queue.
    pub fn new() -> Self {
        EventQueue {
            heap: BinaryHeap::new(),
            next_sequence: 0,
            queued_ids: HashSet::new(),
        }
    }

    /// Returns the number of events in the queue.
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Returns true if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Returns true if a delivery with this ID is already in the queue.
    pub fn contains(&self, delivery_id: &DeliveryId) -> bool {
        self.queued_ids.contains(delivery_id)
    }

    /// Pushes an event onto the queue.
    ///
    /// Events are assigned a sequence number to maintain FIFO ordering
    /// within the same priority level.
    ///
    /// Returns `true` if the event was enqueued, `false` if it was already
    /// in the queue (duplicate delivery ID).
    pub fn push(
        &mut self,
        event: GitHubEvent,
        delivery_id: DeliveryId,
        priority: EventPriority,
    ) -> bool {
        // Prevent duplicate enqueueing
        if self.queued_ids.contains(&delivery_id) {
            return false;
        }

        let sequence = self.next_sequence;
        self.next_sequence += 1;

        self.queued_ids.insert(delivery_id.clone());
        self.heap
            .push(QueuedEvent::new(event, delivery_id, priority, sequence));
        true
    }

    /// Pops the highest-priority event from the queue.
    ///
    /// Returns `None` if the queue is empty.
    pub fn pop(&mut self) -> Option<QueuedEvent> {
        let event = self.heap.pop()?;
        self.queued_ids.remove(&event.delivery_id);
        Some(event)
    }

    /// Peeks at the highest-priority event without removing it.
    ///
    /// Returns `None` if the queue is empty.
    pub fn peek(&self) -> Option<&QueuedEvent> {
        self.heap.peek()
    }

    /// Drains all events from the queue, returning them in priority order.
    ///
    /// This is primarily useful for testing.
    pub fn drain(&mut self) -> Vec<QueuedEvent> {
        let mut events = Vec::with_capacity(self.heap.len());
        while let Some(event) = self.pop() {
            events.push(event);
        }
        // queued_ids is already cleared by pop() calls
        events
    }

    /// Clears the queue.
    pub fn clear(&mut self) {
        self.heap.clear();
        self.queued_ids.clear();
        // Don't reset next_sequence to maintain FIFO guarantees
    }
}

/// Internal event type for the worker queue.
///
/// This allows the worker to receive both webhook events and internal signals.
#[derive(Debug, Clone)]
pub enum QueuedEventPayload {
    /// A webhook event from GitHub.
    GitHub(GitHubEvent),

    /// Internal: trigger periodic re-sync of repository state.
    PeriodicSync,

    /// Internal: poll active trains for missed webhook recovery.
    PollActiveTrains,

    /// Internal: timer-based re-evaluation for non-blocking waits.
    TimerReEval {
        train_root: PrNumber,
        condition: WaitCondition,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Sha;
    use crate::types::{CommentId, PrNumber, RepoId};
    use crate::webhooks::events::{CommentAction, IssueCommentEvent, PrAction, PullRequestEvent};
    use proptest::prelude::*;

    fn make_stop_comment() -> GitHubEvent {
        GitHubEvent::IssueComment(IssueCommentEvent {
            repo: RepoId::new("owner", "repo"),
            action: CommentAction::Created,
            pr_number: Some(PrNumber(42)),
            comment_id: CommentId(123),
            body: "@merge-train stop".to_string(),
            author_id: 1,
            author_login: "user".to_string(),
        })
    }

    fn make_pr_event(pr_number: u64) -> GitHubEvent {
        GitHubEvent::PullRequest(PullRequestEvent {
            repo: RepoId::new("owner", "repo"),
            action: PrAction::Opened,
            pr_number: PrNumber(pr_number),
            merged: false,
            merge_commit_sha: None,
            head_sha: Sha::parse("a".repeat(40)).unwrap(),
            base_branch: "main".to_string(),
            head_branch: "feature".to_string(),
            is_draft: false,
            author_id: 1,
        })
    }

    // ─── Basic queue operations ───

    #[test]
    fn new_queue_is_empty() {
        let queue = EventQueue::new();
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn push_increases_length() {
        let mut queue = EventQueue::new();
        queue.push(
            make_pr_event(1),
            DeliveryId::new("d1"),
            EventPriority::Normal,
        );
        assert_eq!(queue.len(), 1);
        assert!(!queue.is_empty());
    }

    #[test]
    fn pop_decreases_length() {
        let mut queue = EventQueue::new();
        queue.push(
            make_pr_event(1),
            DeliveryId::new("d1"),
            EventPriority::Normal,
        );
        queue.pop();
        assert!(queue.is_empty());
    }

    #[test]
    fn pop_empty_returns_none() {
        let mut queue = EventQueue::new();
        assert!(queue.pop().is_none());
    }

    #[test]
    fn peek_returns_highest_priority() {
        let mut queue = EventQueue::new();
        queue.push(
            make_pr_event(1),
            DeliveryId::new("d1"),
            EventPriority::Normal,
        );
        queue.push(
            make_stop_comment(),
            DeliveryId::new("d2"),
            EventPriority::High,
        );

        let peeked = queue.peek().unwrap();
        assert_eq!(peeked.priority, EventPriority::High);
        assert_eq!(peeked.delivery_id.as_str(), "d2");

        // Peek doesn't remove
        assert_eq!(queue.len(), 2);
    }

    // ─── Priority ordering ───

    #[test]
    fn high_priority_processed_before_normal() {
        let mut queue = EventQueue::new();

        // Push normal priority first
        queue.push(
            make_pr_event(1),
            DeliveryId::new("normal"),
            EventPriority::Normal,
        );
        // Push high priority second
        queue.push(
            make_stop_comment(),
            DeliveryId::new("high"),
            EventPriority::High,
        );

        // High priority should be popped first
        let first = queue.pop().unwrap();
        assert_eq!(first.priority, EventPriority::High);
        assert_eq!(first.delivery_id.as_str(), "high");

        let second = queue.pop().unwrap();
        assert_eq!(second.priority, EventPriority::Normal);
        assert_eq!(second.delivery_id.as_str(), "normal");
    }

    #[test]
    fn multiple_high_priority_fifo() {
        let mut queue = EventQueue::new();

        // Push multiple high priority events
        queue.push(
            make_stop_comment(),
            DeliveryId::new("stop1"),
            EventPriority::High,
        );
        queue.push(
            make_stop_comment(),
            DeliveryId::new("stop2"),
            EventPriority::High,
        );
        queue.push(
            make_stop_comment(),
            DeliveryId::new("stop3"),
            EventPriority::High,
        );

        // Should be processed in FIFO order
        assert_eq!(queue.pop().unwrap().delivery_id.as_str(), "stop1");
        assert_eq!(queue.pop().unwrap().delivery_id.as_str(), "stop2");
        assert_eq!(queue.pop().unwrap().delivery_id.as_str(), "stop3");
    }

    #[test]
    fn fifo_within_same_priority() {
        let mut queue = EventQueue::new();

        queue.push(
            make_pr_event(1),
            DeliveryId::new("pr1"),
            EventPriority::Normal,
        );
        queue.push(
            make_pr_event(2),
            DeliveryId::new("pr2"),
            EventPriority::Normal,
        );
        queue.push(
            make_pr_event(3),
            DeliveryId::new("pr3"),
            EventPriority::Normal,
        );

        // FIFO order
        assert_eq!(queue.pop().unwrap().delivery_id.as_str(), "pr1");
        assert_eq!(queue.pop().unwrap().delivery_id.as_str(), "pr2");
        assert_eq!(queue.pop().unwrap().delivery_id.as_str(), "pr3");
    }

    #[test]
    fn interleaved_priorities() {
        let mut queue = EventQueue::new();

        // Interleave normal and high priority events
        queue.push(
            make_pr_event(1),
            DeliveryId::new("n1"),
            EventPriority::Normal,
        );
        queue.push(
            make_stop_comment(),
            DeliveryId::new("h1"),
            EventPriority::High,
        );
        queue.push(
            make_pr_event(2),
            DeliveryId::new("n2"),
            EventPriority::Normal,
        );
        queue.push(
            make_stop_comment(),
            DeliveryId::new("h2"),
            EventPriority::High,
        );
        queue.push(
            make_pr_event(3),
            DeliveryId::new("n3"),
            EventPriority::Normal,
        );

        // All high priority first (in FIFO order), then normal (in FIFO order)
        assert_eq!(queue.pop().unwrap().delivery_id.as_str(), "h1");
        assert_eq!(queue.pop().unwrap().delivery_id.as_str(), "h2");
        assert_eq!(queue.pop().unwrap().delivery_id.as_str(), "n1");
        assert_eq!(queue.pop().unwrap().delivery_id.as_str(), "n2");
        assert_eq!(queue.pop().unwrap().delivery_id.as_str(), "n3");
    }

    // ─── Drain ───

    #[test]
    fn drain_returns_all_in_order() {
        let mut queue = EventQueue::new();

        queue.push(
            make_pr_event(1),
            DeliveryId::new("n1"),
            EventPriority::Normal,
        );
        queue.push(
            make_stop_comment(),
            DeliveryId::new("h1"),
            EventPriority::High,
        );
        queue.push(
            make_pr_event(2),
            DeliveryId::new("n2"),
            EventPriority::Normal,
        );

        let events = queue.drain();

        assert_eq!(events.len(), 3);
        assert_eq!(events[0].delivery_id.as_str(), "h1");
        assert_eq!(events[1].delivery_id.as_str(), "n1");
        assert_eq!(events[2].delivery_id.as_str(), "n2");

        assert!(queue.is_empty());
    }

    // ─── Property tests ───

    fn arb_priority() -> impl Strategy<Value = EventPriority> {
        prop_oneof![Just(EventPriority::Normal), Just(EventPriority::High),]
    }

    proptest! {
        /// High priority events always come before normal priority events.
        #[test]
        fn prop_high_priority_before_normal(
            high_count in 1usize..5,
            normal_count in 1usize..5,
        ) {
            let mut queue = EventQueue::new();

            // Push normal first
            for i in 0..normal_count {
                queue.push(
                    make_pr_event(i as u64),
                    DeliveryId::new(format!("n{}", i)),
                    EventPriority::Normal,
                );
            }

            // Push high second
            for i in 0..high_count {
                queue.push(
                    make_stop_comment(),
                    DeliveryId::new(format!("h{}", i)),
                    EventPriority::High,
                );
            }

            // Pop all and verify ordering
            let events = queue.drain();

            // First `high_count` events should all be high priority
            for event in &events[..high_count] {
                prop_assert_eq!(event.priority, EventPriority::High);
            }

            // Remaining events should be normal priority
            for event in &events[high_count..] {
                prop_assert_eq!(event.priority, EventPriority::Normal);
            }
        }

        /// Events with same priority maintain FIFO order.
        #[test]
        fn prop_fifo_within_priority(
            count in 2usize..10,
            priority in arb_priority(),
        ) {
            let mut queue = EventQueue::new();

            // Push events in order
            for i in 0..count {
                queue.push(
                    make_pr_event(i as u64),
                    DeliveryId::new(format!("d{}", i)),
                    priority,
                );
            }

            // Pop and verify FIFO order
            for i in 0..count {
                let event = queue.pop().unwrap();
                prop_assert_eq!(event.delivery_id.as_str(), format!("d{}", i));
            }
        }

        /// Queue length is always accurate.
        #[test]
        fn prop_length_accurate(
            push_count in 0usize..20,
            pop_count in 0usize..20,
        ) {
            let mut queue = EventQueue::new();

            // Push events
            for i in 0..push_count {
                queue.push(
                    make_pr_event(i as u64),
                    DeliveryId::new(format!("d{}", i)),
                    EventPriority::Normal,
                );
            }

            // Pop events (up to what we have)
            let actual_pops = pop_count.min(push_count);
            for _ in 0..actual_pops {
                queue.pop();
            }

            prop_assert_eq!(queue.len(), push_count - actual_pops);
        }
    }
}
