//! SQLite-backed event store (Stage S1 of the SQLite migration).
//!
//! Event-sourcing on SQLite — the substrate that replaces the hand-rolled
//! filesystem event log + snapshots + spool (see `SQLITE_MIGRATION_PLAN.md`).
//! This stage establishes the foundation:
//!
//! - `events` — the append-only log (`seq`, `ts`, JSON `payload`);
//! - `repo_state` — a single row holding the materialized [`RepoState`]. It is
//!   **required**, not derivable: bootstrap-from-crawl and status-comment
//!   recovery (later stages) produce state the event vocabulary doesn't carry,
//!   so a replay-only `load` would lose it.
//!
//! The single mutation rule survives the substrate change: [`Store::append`]
//! appends an event, applies it via [`RepoState::apply_event`], and upserts the
//! cache **in one transaction** — so a crash before commit leaves neither.
//!
//! One DB file per repo, WAL mode, and an exclusive per-repo process lock held
//! for the store's lifetime: SQLite/WAL serializes individual writes but does
//! not stop a second process from opening the same DB and driving the same
//! repo's git/GitHub effects, so the lock (inheriting `StateDirLock`'s role) is
//! what actually enforces one writer per repo.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension};
use thiserror::Error;

use crate::persistence::event::{StateEvent, StateEventPayload};
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::state::RepoState;

/// Schema version for the SQLite store. Bump on a breaking schema change; a DB
/// at a different version is rejected loudly rather than mis-read.
const STORE_SCHEMA_VERSION: i64 = 1;

/// Errors from the store.
#[derive(Debug, Error)]
pub enum StoreError {
    /// An error from SQLite itself.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Failed to (de)serialize the cached state or an event payload.
    #[error("state (de)serialization error: {0}")]
    Json(#[from] serde_json::Error),
    /// A filesystem error (lock file, parent directory).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Another process holds the per-repo lock.
    #[error("another process holds the lock for {0}")]
    Locked(PathBuf),
    /// The DB was written by an incompatible schema version.
    #[error("schema version mismatch: store is v{found}, this build expects v{expected}")]
    SchemaMismatch {
        /// The version this build understands.
        expected: i64,
        /// The version found in the DB.
        found: i64,
    },
}

/// A per-repo SQLite event store. Owns the single write connection and an
/// exclusive process lock, and holds the materialized [`RepoState`] in memory.
pub struct Store {
    conn: Connection,
    /// Exclusive per-repo lock, released when the store is dropped.
    _lock: File,
    state: RepoState,
    /// The next sequence number to assign.
    next_seq: u64,
}

impl Store {
    /// Opens (or creates) the per-repo store at `db_path`.
    ///
    /// Acquires the per-repo lock, applies the connection PRAGMAs, reads
    /// `user_version` (creating the schema on a fresh DB, failing loud on a
    /// mismatch), then loads the cached state.
    pub fn open(db_path: &Path) -> Result<Store, StoreError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // (1) Exclusive per-repo process lock, held for the store's lifetime.
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(db_path.with_extension("lock"))?;
        match lock.try_lock_exclusive() {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == fs2::lock_contended_error().raw_os_error() => {
                return Err(StoreError::Locked(db_path.to_path_buf()));
            }
            Err(e) => return Err(StoreError::Io(e)),
        }

        // (2) Open the DB and apply connection PRAGMAs.
        let conn = Connection::open(db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;

        // (3) Read user_version *before* setting it: fresh ⇒ create schema,
        // match ⇒ proceed, mismatch ⇒ fail loud (never clobber it).
        let found: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if found == 0 {
            init_schema(&conn)?;
        } else if found != STORE_SCHEMA_VERSION {
            return Err(StoreError::SchemaMismatch {
                expected: STORE_SCHEMA_VERSION,
                found,
            });
        }

        // (4) Load the cached materialized state (empty on a fresh DB).
        let (state, next_seq) = load_cached(&conn)?;

        Ok(Store {
            conn,
            _lock: lock,
            state,
            next_seq,
        })
    }

    /// The materialized state.
    pub fn state(&self) -> &RepoState {
        &self.state
    }

    /// The next sequence number that [`append`](Self::append) will assign.
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// THE single mutation entry point: append `payload` (stamped with `ts` by
    /// the caller — timestamps enter at the shell), apply it to the in-memory
    /// state, and upsert the cache, all in one transaction. A crash before the
    /// commit leaves the log, the cache, and the in-memory state all unchanged.
    pub fn append(
        &mut self,
        payload: StateEventPayload,
        ts: DateTime<Utc>,
    ) -> Result<StateEvent, StoreError> {
        let event = StateEvent {
            seq: self.next_seq,
            ts,
            payload,
        };

        // Apply to a scratch copy and serialize everything *before* opening the
        // transaction, so a serialization error can't leave the in-memory state
        // ahead of the durable log.
        let payload_json = serde_json::to_string(&event.payload)?;
        let mut next_state = self.state.clone();
        next_state.apply_event(&event);
        let cache_json =
            serde_json::to_string(&next_state.to_snapshot(0, 0, self.next_seq + 1, ts))?;

        let tx = self.conn.transaction()?;
        tx.execute(
            "INSERT INTO events (seq, ts, payload) VALUES (?1, ?2, ?3)",
            rusqlite::params![event.seq as i64, ts.to_rfc3339(), payload_json],
        )?;
        tx.execute(
            "INSERT INTO repo_state (id, snapshot) VALUES (0, ?1)
             ON CONFLICT(id) DO UPDATE SET snapshot = excluded.snapshot",
            rusqlite::params![cache_json],
        )?;
        tx.commit()?;

        self.state = next_state;
        self.next_seq += 1;
        Ok(event)
    }

    /// Replays the entire `events` log from an empty state — the equivalence
    /// oracle for the cache. While all state is event-derived (i.e. before a
    /// later stage's bootstrap introduces non-event state), `replay() ==
    /// state()`.
    pub fn replay(&self) -> Result<RepoState, StoreError> {
        let mut state =
            RepoState::from_snapshot(PersistedRepoSnapshot::new(self.state.default_branch.clone()));
        let mut stmt = self
            .conn
            .prepare("SELECT seq, ts, payload FROM events ORDER BY seq")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        for row in rows {
            let (seq, ts, payload) = row?;
            let payload: StateEventPayload = serde_json::from_str(&payload)?;
            let ts = parse_ts(&ts)?;
            state.apply_event(&StateEvent {
                seq: seq as u64,
                ts,
                payload,
            });
        }
        Ok(state)
    }
}

/// Creates the schema and stamps the version, atomically.
fn init_schema(conn: &Connection) -> Result<(), StoreError> {
    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(
        "CREATE TABLE events (
            seq     INTEGER PRIMARY KEY,
            ts      TEXT NOT NULL,
            payload TEXT NOT NULL
        );
        CREATE TABLE repo_state (
            id       INTEGER PRIMARY KEY CHECK (id = 0),
            snapshot TEXT NOT NULL
        );",
    )?;
    // `user_version` is a transactional header write, so the DDL above and this
    // bump commit together — a crash can't leave a partial schema at version 0.
    tx.pragma_update(None, "user_version", STORE_SCHEMA_VERSION)?;
    tx.commit()?;
    Ok(())
}

/// Loads the cached state and `next_seq`; an empty state on a fresh DB.
fn load_cached(conn: &Connection) -> Result<(RepoState, u64), StoreError> {
    let cached: Option<String> = conn
        .query_row("SELECT snapshot FROM repo_state WHERE id = 0", [], |r| {
            r.get(0)
        })
        .optional()?;
    match cached {
        Some(json) => {
            let snapshot: PersistedRepoSnapshot = serde_json::from_str(&json)?;
            let next_seq = snapshot.next_seq;
            Ok((RepoState::from_snapshot(snapshot), next_seq))
        }
        // Fresh store: empty state. `default_branch` is a placeholder until a
        // later stage's bootstrap records the real value.
        None => Ok((
            RepoState::from_snapshot(PersistedRepoSnapshot::new(String::new())),
            0,
        )),
    }
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>, StoreError> {
    DateTime::parse_from_rfc3339(s)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| StoreError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{arb_state_event_payload, test_timestamp};
    use proptest::prelude::*;
    use tempfile::tempdir;

    fn open_temp(dir: &tempfile::TempDir) -> Store {
        Store::open(&dir.path().join("state.db")).unwrap()
    }

    proptest! {
        /// The materialized cache equals replaying the log from empty: every
        /// `append` applies incrementally exactly as a fresh replay would.
        #[test]
        fn cache_equals_replay(
            payloads in prop::collection::vec(arb_state_event_payload(), 0..30),
        ) {
            let dir = tempdir().unwrap();
            let mut store = open_temp(&dir);
            for (i, payload) in payloads.into_iter().enumerate() {
                let ts = test_timestamp() + chrono::Duration::seconds(i as i64);
                store.append(payload, ts).unwrap();
            }
            prop_assert_eq!(store.state(), &store.replay().unwrap());
        }

        /// Closing and reopening the store recovers the same state and the same
        /// `next_seq` from the durable cache.
        #[test]
        fn reopen_recovers_state(
            payloads in prop::collection::vec(arb_state_event_payload(), 0..30),
        ) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("state.db");

            let (state_before, seq_before) = {
                let mut store = Store::open(&path).unwrap();
                for (i, payload) in payloads.into_iter().enumerate() {
                    let ts = test_timestamp() + chrono::Duration::seconds(i as i64);
                    store.append(payload, ts).unwrap();
                }
                (store.state().clone(), store.next_seq())
            };

            let reopened = Store::open(&path).unwrap();
            prop_assert_eq!(reopened.state(), &state_before);
            prop_assert_eq!(reopened.next_seq(), seq_before);
        }
    }

    #[test]
    fn second_open_is_locked() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");
        let _first = Store::open(&path).unwrap();
        match Store::open(&path) {
            Err(StoreError::Locked(_)) => {}
            Ok(_) => panic!("expected Locked, got a second open"),
            Err(e) => panic!("expected Locked, got {e:?}"),
        }
    }

    #[test]
    fn reopen_after_drop_succeeds() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");
        drop(Store::open(&path).unwrap());
        // Lock released on drop ⇒ a fresh open succeeds.
        let _again = Store::open(&path).unwrap();
    }

    #[test]
    fn schema_version_mismatch_fails_loud() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");
        drop(Store::open(&path).unwrap());

        // Bump the on-disk version past what this build understands.
        {
            let conn = Connection::open(&path).unwrap();
            conn.pragma_update(None, "user_version", STORE_SCHEMA_VERSION + 1)
                .unwrap();
        }

        match Store::open(&path) {
            Err(StoreError::SchemaMismatch { expected, found }) => {
                assert_eq!(expected, STORE_SCHEMA_VERSION);
                assert_eq!(found, STORE_SCHEMA_VERSION + 1);
            }
            Ok(_) => panic!("expected SchemaMismatch, got a store"),
            Err(e) => panic!("expected SchemaMismatch, got {e:?}"),
        }
    }
}
