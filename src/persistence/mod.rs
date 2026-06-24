//! Persistence vocabulary and filesystem primitives.
//!
//! Durability now lives in the SQLite [`crate::store::Store`] (event log +
//! materialized state, crash-safe via WAL). What remains here is the
//! substrate-independent vocabulary the Store and the rest of the system share:
//!
//! - [`event`] — the [`StateEventPayload`] vocabulary and `StateEvent` envelope.
//! - [`snapshot`] — [`PersistedRepoSnapshot`], the serialized `RepoState` the
//!   Store stores in its cached-state row (and the status-comment backup).
//! - [`generation`] — the legacy `<state_dir>/<owner>/<repo>/snapshot.N.json`
//!   file-layout helpers, still read by the `/state` endpoint until it reads
//!   the Store.
//! - [`fsync`] — atomic write/rename/fsync primitives used by the webhook
//!   intake spool.
//!
//! The hand-rolled durability stack (generation-based compaction, the
//! append-only `EventLog` file format, and the recovery/settlement machinery)
//! has been deleted: SQLite owns crash-safety now.

pub mod event;
pub mod fsync;
pub mod generation;
pub mod snapshot;

pub use event::{StateEvent, StateEventPayload};
pub use fsync::{fsync_dir, fsync_file};
pub use generation::{
    GenerationFileKind, WriteGenerationError, delete_old_generation, events_path,
    find_current_snapshot, read_generation, snapshot_path, write_generation,
};
pub use snapshot::{
    PersistedRepoSnapshot, SCHEMA_VERSION, load_snapshot, save_snapshot_atomic, try_load_snapshot,
};
