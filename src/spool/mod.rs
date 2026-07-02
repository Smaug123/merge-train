//! Deduplication keys for webhook deliveries.
//!
//! GitHub may redeliver the same logical event under new delivery IDs. A
//! [`DedupeKey`] identifies the *logical* event (e.g. a comment id, a PR head
//! sha) so a redelivery can be recognized and skipped.
//!
//! The durable queue itself now lives in the SQLite [`crate::store::Store`]
//! (the `deliveries` and `dedupe_keys` tables); the per-repo worker
//! ([`crate::worker`]) drains it. The filesystem marker-spool that used to live
//! here has been deleted.

pub mod dedupe;

pub use dedupe::DedupeKey;
