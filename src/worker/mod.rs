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
//!
//! # Module Structure
//!
//! - [`queue`]: Priority queue for event ordering
//! - [`worker`]: Per-repo event loop and state management
//! - [`dispatch`]: Routes events to appropriate workers

mod dispatch;
mod queue;
#[allow(clippy::module_inception)]
mod worker;

pub use dispatch::{Dispatcher, DispatcherConfig};
pub use queue::{EventQueue, QueuedEvent, QueuedEventPayload};
pub use worker::{RepoWorker, WorkerConfig, WorkerError};
