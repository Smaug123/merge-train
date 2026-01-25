//! Per-repo worker system for webhook event processing.
//!
//! This module provides the worker infrastructure for processing GitHub webhook
//! events. Each repository gets a dedicated worker that processes events serially,
//! ensuring consistency within each repo while allowing concurrent processing
//! across different repositories.
//!
//! # Architecture
//!
//! From DESIGN.md:
//! - Per-repo serial processing with cross-repo concurrency
//! - Priority queue: stop commands processed first
//! - Crash-safe delivery with `.done` marker only after state is persisted
//! - Non-blocking polling for wait conditions
//! - Polling fallback for active trains (missed webhook recovery)
//!
//! # Module Structure
//!
//! - [`queue`]: Priority queue for event ordering
//! - [`worker`]: Per-repo event loop and state management
//! - [`dispatch`]: Routes events to appropriate workers
//! - [`message`]: Worker message types for async communication
//! - [`poll`]: Polling configuration and jitter
//! - [`effects`]: Effect execution with cancellation support

mod dispatch;
mod effects;
mod message;
mod poll;
mod queue;
#[allow(clippy::module_inception)]
mod worker;

#[cfg(test)]
mod tests;

pub use dispatch::{Dispatcher, DispatcherConfig};
pub use effects::{EffectError, EffectExecutor, EffectResult};
pub use message::WorkerMessage;
pub use poll::PollConfig;
pub use queue::{EventQueue, QueuedEvent, QueuedEventPayload, WaitCondition};
pub use worker::{RepoWorker, WorkerConfig, WorkerError};
