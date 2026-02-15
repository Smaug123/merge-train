//! Worker message types for async communication.
//!
//! This module defines the messages that can be sent to a per-repo worker.
//! Workers receive these messages via an async channel and process them
//! in their event loop.

use crate::spool::delivery::SpooledDelivery;
use crate::types::PrNumber;

use super::queue::WaitCondition;

/// Messages that can be sent to a per-repo worker.
///
/// Workers receive these messages via `tokio::sync::mpsc` and process them
/// serially to maintain per-repo event ordering guarantees.
#[derive(Debug)]
pub enum WorkerMessage {
    /// A new webhook delivery to process.
    ///
    /// The delivery has already been spooled to disk; this message
    /// notifies the worker to process it.
    Delivery(SpooledDelivery),

    /// Cancel operations for a specific stack.
    ///
    /// This is sent when a stop command is received. The worker should
    /// cancel the `CancellationToken` for the specified stack's root PR,
    /// which will interrupt any in-flight git or GitHub operations.
    CancelStack(PrNumber),

    /// Poll all active trains for missed webhook recovery.
    ///
    /// This is sent periodically (default: every 10 minutes) to ensure
    /// trains make progress even if webhooks are missed or delayed.
    PollActiveTrains,

    /// Timer fired for re-evaluating a wait condition.
    ///
    /// This is used for non-blocking polling when waiting for GitHub
    /// state to propagate (e.g., headRefOid to match after a push).
    TimerFired {
        /// The root PR of the train being waited on.
        train_root: PrNumber,
        /// The condition being waited for.
        condition: WaitCondition,
    },

    /// Request a graceful shutdown.
    ///
    /// The worker should finish processing the current event, flush
    /// any pending batches, and exit its event loop.
    Shutdown,
}
