//! Persistence layer for the merge train bot.
//!
//! This module provides crash-safe event logging with replay capability.
//! The event log is the primary persistence mechanism for train state.
//!
//! # Design
//!
//! - **JSON Lines format**: One JSON object per line, terminated by `\n`
//! - **Crash-safe**: Partial writes are detected and truncated on replay
//! - **fsync strategy**: Critical events sync immediately; non-critical events batch
//!
//! # Recovery
//!
//! On startup, the event log is replayed from a snapshot's `log_position`.
//! If the final line is incomplete (crash mid-write), it's truncated.
//! This ensures the log always contains a valid prefix of events.

pub mod event;
pub mod fsync;
pub mod log;

pub use event::{StateEvent, StateEventPayload};
pub use fsync::{fsync_dir, fsync_file};
pub use log::EventLog;
