//! Persistence layer for the merge train bot.
//!
//! This module provides crash-safe event logging with replay capability,
//! snapshot persistence, and generation-based compaction.
//!
//! # Architecture
//!
//! The persistence layer uses a combination of:
//! - **Event log**: Append-only JSON Lines format for incremental updates
//! - **Snapshots**: Periodic full-state captures for fast recovery
//! - **Generations**: Crash-safe compaction using generation-based file naming
//!
//! # File Layout
//!
//! ```text
//! <state_dir>/<owner>/<repo>/
//!   generation        # current generation number (single integer)
//!   snapshot.N.json   # snapshot for generation N
//!   events.N.log      # event log for generation N (JSON Lines)
//! ```
//!
//! # Recovery
//!
//! On startup, call [`recovery::recover`]: the single entry point that locks
//! the state directory, settles generations (validating the surviving
//! snapshot before deleting anything), loads the snapshot, replays events,
//! and returns a [`recovery::PersistenceHandle`] whose event log carries the
//! replay-derived next sequence number. The handle keeps the log inseparable
//! from the directory lock, and compaction goes through
//! [`recovery::PersistenceHandle::compact`], which switches the handle to the
//! new generation's log.
//!
//! # Crash Safety
//!
//! - Event log: Torn tail writes detected and truncated on replay; any other
//!   inconsistency is uncharacterizable and fails loudly
//! - Snapshots: Written atomically using write-to-temp-then-rename
//! - Compaction: Generation-based scheme ensures either old or new generation is complete
//! - All critical operations use fsync on both files and directories
//! - Settlement: the live process never deletes a snapshot whose generation
//!   might be committed; an ambiguous compaction commit is fatal for the
//!   handle and settled — losing nothing — by the next startup's recovery

pub mod compaction;
pub mod event;
pub mod fsync;
pub mod generation;
pub mod log;
pub mod pruning;
pub mod recovery;
pub mod snapshot;

pub use compaction::{CompactionError, CompactionOutcome, should_compact};
pub use event::{StateEvent, StateEventPayload};
pub use fsync::{fsync_dir, fsync_file};
pub use generation::{
    GenerationFileKind, WriteGenerationError, delete_old_generation, events_path,
    find_current_snapshot, read_generation, snapshot_path, write_generation,
};
pub use log::EventLog;
pub use pruning::{PruneConfig, prune_snapshot};
pub use recovery::{PersistenceHandle, RecoverError, RecoveredState, recover};
pub use snapshot::{
    PersistedRepoSnapshot, SCHEMA_VERSION, load_snapshot, save_snapshot_atomic, try_load_snapshot,
};
