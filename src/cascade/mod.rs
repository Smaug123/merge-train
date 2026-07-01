//! The cascade engine (stage M2): a pure planner for merge-train cascades.
//!
//! Given the materialized [`crate::state::RepoState`] and an observation,
//! the engine plans one step — the state events to append, the effects to
//! run, and how the worker loop should proceed. It performs no I/O and never
//! reads the clock; the worker (M5) owns execution.
//!
//! Entry points: [`start_train`], [`stop_train`], [`advance`], and
//! [`recover_train`] (which is `advance` with [`Observation::Evaluate`] —
//! recovery and resumption are one code path). [`observe`] maps a plan's
//! executed effect outcomes to the next observation so the executor stays
//! free of domain knowledge.

mod engine;
mod observe;
mod plan;

#[cfg(test)]
mod model_tests;

pub use engine::{MAX_TRAIN_SIZE, advance, recover_train, start_train, stop_train};
pub use observe::{ObserveError, observe};
pub use plan::{
    CascadeError, Control, EffectError, EffectOutcome, EffectResponse, IntentFact, Observation,
    ReplayFacts, StepPlan,
};
