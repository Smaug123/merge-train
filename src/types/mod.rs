//! Core domain types for the merge train bot.
//!
//! This module contains all the fundamental types used throughout the application,
//! designed to encode invariants via the type system.

pub mod ids;
pub mod pr;
pub mod stack;
pub mod train;

// Re-export commonly used types at the module level
pub use ids::{CommentId, DeliveryId, InvalidSha, PrNumber, RepoId, Sha};
pub use pr::{CachedPr, MergeStateStatus, PrState};
pub use stack::{AbortReason, BlockReason, CascadeStepOutcome};
pub use train::{CascadePhase, DescendantProgress, TrainError, TrainRecord, TrainState};
