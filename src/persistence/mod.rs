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
//! On startup:
//! 1. Read the `generation` file to find current generation N
//! 2. Load `snapshot.<N>.json` (fallback to `snapshot.<N-1>.json` if crash during compaction)
//! 3. Replay `events.<N>.log` from the snapshot's `log_position`
//! 4. Clean up stale files from older generations
//!
//! # Crash Safety
//!
//! - Event log: Partial writes detected and truncated on replay
//! - Snapshots: Written atomically using write-to-temp-then-rename
//! - Compaction: Generation-based scheme ensures either old or new generation is complete
//! - All critical operations use fsync on both files and directories

pub mod compaction;
pub mod event;
pub mod fsync;
pub mod generation;
pub mod log;
pub mod pruning;
pub mod snapshot;

pub use compaction::{cleanup_stale_generations, compact, should_compact};
pub use event::{StateEvent, StateEventPayload};
pub use fsync::{fsync_dir, fsync_file};
pub use generation::{
    delete_old_generation, events_path, increment_generation, read_generation, snapshot_path,
    write_generation,
};
pub use log::EventLog;
pub use pruning::{PruneConfig, prune_snapshot};
pub use snapshot::{
    PersistedRepoSnapshot, SCHEMA_VERSION, load_snapshot, save_snapshot_atomic, try_load_snapshot,
};
