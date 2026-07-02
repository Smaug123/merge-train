//! Webhook handling for GitHub events.
//!
//! This module provides:
//! - Signature verification for webhook payloads (HMAC-SHA256), used at
//!   intake by the server before a delivery is accepted
//! - Event parsing from raw JSON payloads (`parser`), run by the per-repo
//!   worker at drain time
//! - Logical-event deduplication keys (`dedupe`), checked against the Store's
//!   `dedupe_keys` table so redeliveries under new delivery IDs are skipped
//! - Pure event handlers (`handlers`), which turn a parsed event plus current
//!   state into state events, effects, and engine triggers
//!
//! The `priority` module (classification for a priority queue) is currently
//! unwired: the worker processes deliveries strictly in arrival order, and
//! stop commands reach the engine at observation boundaries instead of by
//! jumping the queue. It is kept for the deferred priority-scheduling work.

pub mod dedupe;
pub mod events;
pub mod handlers;
pub mod parser;
pub mod priority;
pub mod signature;

// Re-export commonly used types
pub use events::{
    CheckSuiteAction, CheckSuiteConclusion, CheckSuiteEvent, CommentAction, GitHubEvent,
    IssueCommentEvent, MergeStatus, PrAction, PullRequestEvent, PullRequestReviewEvent,
    ReviewAction, ReviewState, StatusEvent, StatusState,
};
pub use parser::{ParseError, parse_webhook};
pub use priority::{EventPriority, classify_priority, classify_priority_with_bot_name};
pub use signature::{
    compute_signature, format_signature_header, parse_signature_header, verify_signature,
};
