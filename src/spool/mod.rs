//! Webhook delivery spool for crash-safe event processing.
//!
//! This module provides a durable queue for webhook deliveries using the filesystem.
//! Each delivery progresses through states using marker files:
//!
//! ```text
//! <delivery-id>.json       - pending (contains payload)
//! <delivery-id>.json.proc  - processing (empty marker: worker claimed it)
//! <delivery-id>.json.done  - processed (empty marker: state effects persisted)
//! ```
//!
//! # Crash Safety
//!
//! - Payload files are written atomically (temp file + rename + fsync + dir fsync)
//! - Marker files are empty, making creation atomic
//! - On recovery, deliveries with `.proc` but no `.done` are reprocessed
//!
//! # Deduplication
//!
//! GitHub may redeliver webhooks with new delivery IDs for the same logical event.
//! The dedupe module provides keys that identify logical events for deduplication.

pub mod dedupe;
pub mod delivery;
pub mod drain;

pub use dedupe::{
    DEFAULT_DEDUPE_TTL_HOURS, DedupeKey, extract_dedupe_key, is_duplicate, mark_seen,
    prune_expired_keys, prune_expired_keys_default,
};
pub use delivery::{SpoolError, SpooledDelivery, mark_done, mark_processing, spool_delivery};
pub use drain::{cleanup_interrupted_processing, drain_pending};
