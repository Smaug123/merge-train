//! Webhook handling for GitHub events.
//!
//! This module provides:
//! - Signature verification for webhook payloads (HMAC-SHA256)
//! - Event parsing from raw JSON payloads
//! - Priority classification for event processing
//!
//! # Processing Pipeline
//!
//! 1. **Signature verification** (`signature` module): Validate HMAC-SHA256
//! 2. **Event parsing** (`parser` module): Parse JSON into typed events
//! 3. **Priority classification** (`priority` module): Determine processing order
//!
//! # Example
//!
//! ```ignore
//! use merge_train::webhooks::{verify_signature, parse_webhook, classify_priority};
//!
//! // 1. Verify signature
//! if !verify_signature(payload, signature_header, secret) {
//!     return Err("invalid signature");
//! }
//!
//! // 2. Parse event
//! let event = parse_webhook(event_type, payload)?;
//!
//! // 3. Classify priority
//! if let Some(event) = event {
//!     let priority = classify_priority(&event);
//!     // ... enqueue with priority
//! }
//! ```

pub mod events;
pub mod parser;
pub mod priority;
pub mod signature;

// Re-export commonly used types
pub use events::{
    CheckSuiteAction, CheckSuiteEvent, CommentAction, GitHubEvent, IssueCommentEvent, PrAction,
    PullRequestEvent, PullRequestReviewEvent, ReviewAction, ReviewState, StatusEvent, StatusState,
};
pub use parser::{ParseError, parse_webhook};
pub use priority::{EventPriority, classify_priority, classify_priority_with_bot_name};
pub use signature::{
    compute_signature, format_signature_header, parse_signature_header, verify_signature,
};
