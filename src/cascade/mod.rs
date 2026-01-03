//! Cascade engine for orchestrating merge train operations.
//!
//! This module implements the core cascade state machine that drives the
//! merge train workflow. It orchestrates:
//!
//! - **Start/stop** operations for trains
//! - **Phase transitions** through the cascade: Preparing → SquashPending →
//!   Reconciling → CatchingUp → Retargeting → Idle
//! - **Recovery** from partial state after crashes
//!
//! # Architecture
//!
//! The cascade engine follows the effects-as-data pattern:
//! - Pure functions compute state transitions and return `Effect` values
//! - Effects are executed by separate interpreters (GitHub API, git commands)
//! - This enables thorough testing without I/O
//!
//! # Key Invariants
//!
//! 1. **Frozen descendants**: The set of descendants is captured when entering
//!    `Preparing` and carried through all subsequent phases. Recovery uses this
//!    frozen set, not the current descendants index.
//!
//! 2. **Phase ordering**: Phases proceed in strict order. `next_phase()` enforces
//!    valid transitions.
//!
//! 3. **SHA guards**: Squash-merge uses `expected_sha` to prevent race conditions
//!    where unreviewed commits land between readiness check and merge.

pub mod engine;
pub mod phases;
pub mod recovery;
pub mod step;

// Re-export commonly used types
pub use engine::{CascadeEngine, CascadeError, StartTrainResult, StopTrainResult};
pub use phases::{PhaseAction, PhaseExecutionResult};
pub use recovery::RecoveryPlan;
pub use step::{StepContext, StepResult};
