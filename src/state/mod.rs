//! Pure state logic for the merge train bot.
//!
//! This module contains the functional core: pure functions for computing
//! stack topology, managing descendants, and validating state transitions.
//! All I/O and effects are handled elsewhere.

pub mod descendants;
pub mod topology;
pub mod transitions;
pub mod validation;

// Re-export commonly used types and functions
pub use descendants::{build_descendants_index, remaining_descendants};
pub use topology::{MergeStack, compute_stacks, detect_cycle, is_root};
pub use transitions::{PhaseOutcome, next_phase};
pub use validation::{PredecessorValidationError, validate_predecessor_declaration};
