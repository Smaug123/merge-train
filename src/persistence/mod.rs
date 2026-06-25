//! Persistence vocabulary.
//!
//! Durability lives entirely in the SQLite [`crate::store::Store`] (event log +
//! materialized state, crash-safe via WAL). What remains here is the
//! substrate-independent vocabulary the Store and the rest of the system share:
//!
//! - [`event`] — the [`StateEventPayload`] vocabulary and `StateEvent` envelope.
//! - [`snapshot`] — [`PersistedRepoSnapshot`], the serialized `RepoState` the
//!   Store keeps in its cached-state row (and that the status-comment backup
//!   embeds).
//!
//! The hand-rolled filesystem durability stack (generation-based compaction,
//! the append-only `EventLog` format, the recovery/settlement machinery, the
//! snapshot file-IO, the generation file layout, and the fsync primitives) has
//! been deleted: SQLite owns crash-safety now.

pub mod event;
pub mod snapshot;

pub use event::{StateEvent, StateEventPayload};
pub use snapshot::{PersistedRepoSnapshot, SCHEMA_VERSION};
