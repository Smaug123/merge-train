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
use crate::persistence::snapshot::{PersistedRepoSnapshot, SCHEMA_VERSION};
use crate::state::RepoState;
use crate::types::PrNumber;
use crate::webhooks::dedupe::DedupeKey;

/// Schema version for the SQLite store. Bump on a breaking schema change; a DB
/// at a different version is rejected loudly rather than mis-read.
///
/// v2 added the `deliveries` and `dedupe_keys` tables (the webhook queue).
const STORE_SCHEMA_VERSION: i64 = 4;

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
    /// The DB was written by an incompatible store schema version.
    #[error("schema version mismatch: store is v{found}, this build expects v{expected}")]
    SchemaMismatch {
        /// The version this build understands.
        expected: i64,
        /// The version found in the DB.
        found: i64,
    },
    /// The cached `repo_state` row was written by an incompatible snapshot
    /// schema version (separate from the SQLite store schema above).
    #[error("cached state schema mismatch: snapshot is v{found}, this build expects v{expected}")]
    CachedStateSchemaMismatch {
        /// The snapshot schema version this build understands.
        expected: u32,
        /// The version found in the cached row.
        found: u32,
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

/// A claimed webhook delivery, as returned by [`Store::claim_next_delivery`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    /// Monotonic arrival order (the drain key).
    pub arrival: i64,
    /// The `X-GitHub-Delivery` id (unique; the intake idempotency key).
    pub delivery_id: String,
    /// The `X-GitHub-Event` header.
    pub event_type: String,
    /// Captured headers, as JSON.
    pub headers: String,
    /// The raw webhook payload bytes.
    pub body: Vec<u8>,
    /// When the delivery was received.
    pub received_at: DateTime<Utc>,
}

/// A user command persisted in `pending_commands`: authorized at intake,
/// awaiting the saga slot, durable until answered (Codex M5 rounds 2/19 —
/// a command that lived only in RAM between its delivery's close and its
/// application was lost by a crash, and redelivery is deduped).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableCommand {
    /// `@bot start` on `pr`.
    Start {
        /// The PR the start was issued on.
        pr: PrNumber,
    },
    /// `@bot stop[ --force]` on `pr`.
    Stop {
        /// The PR the stop was issued on.
        pr: PrNumber,
        /// Whether `--force` was given.
        force: bool,
    },
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

        // (4) Requeue any delivery left `processing` by a dead worker. With the
        // per-repo lock held, a `processing` row at open time is abandoned, so
        // resetting it to `pending` re-drains it (without this, the drain — which
        // only takes `pending` — would strand it forever).
        conn.execute(
            "UPDATE deliveries SET status = 'pending' WHERE status = 'processing'",
            [],
        )?;

        // (5) Load the cached materialized state (empty on a fresh DB).
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

    /// Reads the materialized [`PersistedRepoSnapshot`] from the repo DB at
    /// `db_path`, **without** opening the full `Store` or taking the per-repo
    /// lock — for read-only observers like the `/state` endpoint. Returns `None`
    /// if the DB or its cached-state row is absent.
    ///
    /// WAL lets this read run concurrently with the owning worker's writer
    /// without blocking it. The connection is opened read-write but marked
    /// `query_only`: a pure read-only open can fail to initialize WAL
    /// shared-memory when no writer is currently attached, while `query_only`
    /// still forbids writes.
    pub fn read_snapshot(db_path: &Path) -> Result<Option<PersistedRepoSnapshot>, StoreError> {
        if !db_path.exists() {
            return Ok(None);
        }
        let conn = Connection::open(db_path)?;
        conn.pragma_update(None, "query_only", true)?;

        // Mirror `Store::open`'s schema checks (Codex review #54): fail loud on
        // an incompatible store or snapshot version rather than serving state a
        // normal load would reject and hiding an upgrade/corruption issue.
        let found: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if found != STORE_SCHEMA_VERSION {
            return Err(StoreError::SchemaMismatch {
                expected: STORE_SCHEMA_VERSION,
                found,
            });
        }

        let json: Option<String> = conn
            .query_row("SELECT snapshot FROM repo_state WHERE id = 0", [], |r| {
                r.get(0)
            })
            .optional()?;
        let Some(json) = json else {
            return Ok(None);
        };
        let snapshot: PersistedRepoSnapshot = serde_json::from_str(&json)?;
        if snapshot.schema_version != SCHEMA_VERSION {
            return Err(StoreError::CachedStateSchemaMismatch {
                expected: SCHEMA_VERSION,
                found: snapshot.schema_version,
            });
        }
        Ok(Some(snapshot))
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
        let seq = self.next_seq;
        let event = StateEvent { seq, ts, payload };

        let mut next_state = self.state.clone();
        let tx = self.conn.transaction()?;
        insert_and_apply(&tx, &mut next_state, &event)?;
        upsert_cache(&tx, &next_state, seq + 1, ts)?;
        tx.commit()?;

        self.state = next_state;
        self.next_seq += 1;
        Ok(event)
    }

    /// Appends `payloads` in order as ONE transaction — the whole batch commits
    /// or none of it does. This is the durability shape a cascade `StepPlan`
    /// requires: the relative order of a plan's events (e.g. `PrSynchronized`
    /// strictly before `ReconciliationRecorded`, a terminal event before
    /// nothing else) must be atomic, never observable half-applied.
    pub fn append_batch(
        &mut self,
        payloads: &[StateEventPayload],
        ts: DateTime<Utc>,
    ) -> Result<Vec<StateEvent>, StoreError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let mut next_state = self.state.clone();
        let mut seq = self.next_seq;
        let mut events = Vec::with_capacity(payloads.len());

        let tx = self.conn.transaction()?;
        for payload in payloads {
            let event = StateEvent {
                seq,
                ts,
                payload: payload.clone(),
            };
            insert_and_apply(&tx, &mut next_state, &event)?;
            events.push(event);
            seq += 1;
        }
        upsert_cache(&tx, &next_state, seq, ts)?;
        tx.commit()?;

        self.state = next_state;
        self.next_seq = seq;
        Ok(events)
    }

    /// Reads the full event log in append order.
    ///
    /// The worker derives [`crate::cascade::ReplayFacts`] from this on every
    /// train evaluation. Reading the whole log is O(events-since-the-last-
    /// checkpoint): [`Store::compact`] (run by the worker's idle
    /// maintenance) keeps the log bounded to roughly the events written
    /// since the last train-free idle moment.
    pub fn events(&self) -> Result<Vec<StateEvent>, StoreError> {
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
        let mut events = Vec::new();
        for row in rows {
            let (seq, ts, payload) = row?;
            events.push(StateEvent {
                seq: seq as u64,
                ts: parse_ts(&ts)?,
                payload: serde_json::from_str(&payload)?,
            });
        }
        Ok(events)
    }

    /// Enqueues a pending webhook delivery. Returns `false` if a delivery with
    /// the same id is already present (idempotent intake — GitHub redelivers).
    pub fn enqueue(
        &mut self,
        delivery_id: &str,
        event_type: &str,
        headers: &str,
        body: &[u8],
        received_at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let n = self.conn.execute(
            "INSERT OR IGNORE INTO deliveries
                 (delivery_id, event_type, headers, body, status, received_at)
             VALUES (?1, ?2, ?3, ?4, 'pending', ?5)",
            rusqlite::params![
                delivery_id,
                event_type,
                headers,
                body,
                received_at.to_rfc3339()
            ],
        )?;
        Ok(n > 0)
    }

    /// Claims the lowest-`arrival` pending delivery, marking it `processing`.
    pub fn claim_next_delivery(&mut self) -> Result<Option<Delivery>, StoreError> {
        let tx = self.conn.transaction()?;
        let row = tx
            .query_row(
                "SELECT arrival, delivery_id, event_type, headers, body, received_at
                 FROM deliveries WHERE status = 'pending' ORDER BY arrival LIMIT 1",
                [],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, Vec<u8>>(4)?,
                        r.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()?;
        let delivery = match row {
            Some((arrival, delivery_id, event_type, headers, body, received_at)) => {
                tx.execute(
                    "UPDATE deliveries SET status = 'processing' WHERE arrival = ?1",
                    rusqlite::params![arrival],
                )?;
                Some(Delivery {
                    arrival,
                    delivery_id,
                    event_type,
                    headers,
                    body,
                    received_at: parse_ts(&received_at)?,
                })
            }
            None => None,
        };
        tx.commit()?;
        Ok(delivery)
    }

    /// Closes a delivery: appends its final state events, records the dedupe
    /// key (if any), persists any authorized `commands` the delivery
    /// carries, and marks it `done` — all in one transaction, so the result
    /// and the close commit together (no window where state advanced but the
    /// delivery is still open). See `SQLITE_MIGRATION_PLAN.md`.
    ///
    /// Returns the `pending_commands` row ids for `commands`, in order — the
    /// caller deletes each row (`delete_pending_command`) once the command
    /// is answered.
    pub fn commit_delivery(
        &mut self,
        delivery_id: &str,
        events: &[StateEventPayload],
        dedupe: Option<&DedupeKey>,
        commands: &[DurableCommand],
        ts: DateTime<Utc>,
    ) -> Result<Vec<i64>, StoreError> {
        let mut next_state = self.state.clone();
        let mut seq = self.next_seq;

        let tx = self.conn.transaction()?;
        for payload in events {
            let event = StateEvent {
                seq,
                ts,
                payload: payload.clone(),
            };
            insert_and_apply(&tx, &mut next_state, &event)?;
            seq += 1;
        }
        upsert_cache(&tx, &next_state, seq, ts)?;
        if let Some(key) = dedupe {
            tx.execute(
                "INSERT OR IGNORE INTO dedupe_keys (key, seen_at) VALUES (?1, ?2)",
                rusqlite::params![key.as_str(), ts.to_rfc3339()],
            )?;
        }
        let mut command_ids = Vec::with_capacity(commands.len());
        for command in commands {
            let (kind, pr, force) = match command {
                DurableCommand::Start { pr } => ("start", *pr, false),
                DurableCommand::Stop { pr, force } => ("stop", *pr, *force),
            };
            tx.execute(
                "INSERT INTO pending_commands (kind, pr, force_stop) VALUES (?1, ?2, ?3)",
                rusqlite::params![kind, pr.0 as i64, force],
            )?;
            command_ids.push(tx.last_insert_rowid());
        }
        tx.execute(
            "UPDATE deliveries SET status = 'done' WHERE delivery_id = ?1",
            rusqlite::params![delivery_id],
        )?;
        tx.commit()?;

        self.state = next_state;
        self.next_seq = seq;
        Ok(command_ids)
    }

    /// The persisted user commands not yet answered, in arrival (`id`) order
    /// — the user's command order, which reload must preserve (a reloaded
    /// `start → stop` must not become `stop → start`).
    pub fn pending_commands(&self) -> Result<Vec<(i64, DurableCommand)>, StoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, kind, pr, force_stop FROM pending_commands ORDER BY id")?;
        let rows = stmt.query_map([], |r| {
            let id: i64 = r.get(0)?;
            let kind: String = r.get(1)?;
            let pr = PrNumber(r.get::<_, i64>(2)? as u64);
            let force: bool = r.get(3)?;
            Ok((id, kind, pr, force))
        })?;
        let mut commands = Vec::new();
        for row in rows {
            let (id, kind, pr, force) = row?;
            let command = match kind.as_str() {
                "start" => DurableCommand::Start { pr },
                "stop" => DurableCommand::Stop { pr, force },
                other => {
                    return Err(StoreError::Io(std::io::Error::other(format!(
                        "unknown pending command kind {other:?} (row {id})"
                    ))));
                }
            };
            commands.push((id, command));
        }
        Ok(commands)
    }

    /// Atomically replaces a pending stop with stops for `prs` — the fan-out
    /// expansion: a stop for a root whose train is about to fan out becomes
    /// stops for every spawned root, and the replacement must be durable
    /// BEFORE the fan-out integrates or a crash in between loses the
    /// acknowledged stop (Codex M5 round 15: the old-root row resolves to no
    /// train after the fan-out). Returns the new row ids, in `prs` order.
    pub fn replace_pending_stop(
        &mut self,
        old_id: i64,
        prs: &[(PrNumber, bool)],
    ) -> Result<Vec<i64>, StoreError> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM pending_commands WHERE id = ?1",
            rusqlite::params![old_id],
        )?;
        let mut ids = Vec::with_capacity(prs.len());
        for (pr, force) in prs {
            tx.execute(
                "INSERT INTO pending_commands (kind, pr, force_stop) VALUES ('stop', ?1, ?2)",
                rusqlite::params![pr.0 as i64, *force],
            )?;
            ids.push(tx.last_insert_rowid());
        }
        tx.commit()?;
        Ok(ids)
    }

    /// Removes an answered command. Deleting after (not atomically with) the
    /// command's durable answer means a crash in between replays it, which
    /// is harmless: stopping a stopped train answers "no active train", and
    /// starting an already-started one is rejected as already running.
    pub fn delete_pending_command(&mut self, id: i64) -> Result<(), StoreError> {
        self.conn.execute(
            "DELETE FROM pending_commands WHERE id = ?1",
            rusqlite::params![id],
        )?;
        Ok(())
    }

    /// Releases a claimed delivery back to `pending` — used when processing
    /// cannot proceed for an *external* reason (GitHub unreachable) and must
    /// be retried later without losing the delivery or killing the worker.
    /// Only a `processing` row is touched; releasing an unclaimed or closed
    /// delivery is a no-op.
    pub fn release_delivery(&mut self, delivery_id: &str) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE deliveries SET status = 'pending'
             WHERE delivery_id = ?1 AND status = 'processing'",
            rusqlite::params![delivery_id],
        )?;
        Ok(())
    }

    /// Whether `key` has already been seen (a duplicate to skip).
    pub fn is_duplicate(&self, key: &DedupeKey) -> Result<bool, StoreError> {
        let exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM dedupe_keys WHERE key = ?1)",
            rusqlite::params![key.as_str()],
            |r| r.get(0),
        )?;
        Ok(exists)
    }

    /// Prunes dedupe keys first seen before `cutoff`. Returns the count removed.
    pub fn prune_dedupe(&mut self, cutoff: DateTime<Utc>) -> Result<usize, StoreError> {
        Ok(self.conn.execute(
            "DELETE FROM dedupe_keys WHERE seen_at < ?1",
            rusqlite::params![cutoff.to_rfc3339()],
        )?)
    }

    /// Prunes `done` deliveries received before `cutoff` (frees the body BLOBs
    /// once past the idempotency grace period). Returns the count removed.
    pub fn prune_deliveries(&mut self, cutoff: DateTime<Utc>) -> Result<usize, StoreError> {
        Ok(self.conn.execute(
            "DELETE FROM deliveries WHERE status = 'done' AND received_at < ?1",
            rusqlite::params![cutoff.to_rfc3339()],
        )?)
    }

    /// Compacts the event log: replaces every event with one `Checkpoint`
    /// event (at `next_seq - 1`) carrying the current state, when it is
    /// safe and worthwhile:
    ///
    /// - **safe** — no train is ACTIVE (enforced here, not by the caller:
    ///   `ReplayFacts::for_train` reads active trains' intent history from
    ///   the log, and summarizing it away would break recovery); retained
    ///   stopped/aborted records need no history.
    /// - **worthwhile** — the log holds more than `threshold` events
    ///   (compacting a fresh checkpoint would just churn the WAL).
    ///
    /// The delete and the checkpoint insert commit in one transaction, and
    /// `next_seq` is unchanged, so a crash leaves either the old log or
    /// the compacted one. The from-empty replay oracle survives verbatim:
    /// a checkpoint replays by replacing the state wholesale.
    ///
    /// Returns the number of events the checkpoint replaced, or `None` if
    /// compaction was refused or not worthwhile.
    pub fn compact(
        &mut self,
        threshold: u64,
        now: DateTime<Utc>,
    ) -> Result<Option<u64>, StoreError> {
        if self
            .state
            .active_trains
            .values()
            .any(|t| t.state.is_active())
        {
            return Ok(None);
        }
        let count: u64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))?;
        // Below the threshold the churn isn't worth a snapshot-sized WAL
        // write; and a log that is already exactly one checkpoint gains
        // nothing from another pass, whatever the threshold.
        if count == 0 || count <= threshold {
            return Ok(None);
        }
        if count == 1
            && let Some(only) = self.events()?.first()
            && matches!(only.payload, StateEventPayload::Checkpoint { .. })
        {
            return Ok(None);
        }

        let checkpoint = StateEvent {
            seq: self.next_seq - 1,
            ts: now,
            payload: StateEventPayload::Checkpoint {
                snapshot: self.state.to_snapshot(self.next_seq, now),
            },
        };
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM events", [])?;
        tx.execute(
            "INSERT INTO events (seq, ts, payload) VALUES (?1, ?2, ?3)",
            rusqlite::params![
                checkpoint.seq as i64,
                checkpoint.ts.to_rfc3339(),
                serde_json::to_string(&checkpoint.payload)?
            ],
        )?;
        tx.commit()?;
        Ok(Some(count))
    }

    /// Replays the entire `events` log from an empty state — the equivalence
    /// oracle for the cache. While all state is event-derived (i.e. before a
    /// later stage's bootstrap introduces non-event state), `replay() ==
    /// state()`.
    pub fn replay(&self) -> Result<RepoState, StoreError> {
        let mut state = RepoState::from_snapshot(PersistedRepoSnapshot::new(
            self.state.default_branch.clone(),
        ));
        for event in self.events()? {
            state.apply_event(&event);
        }
        Ok(state)
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Release the advisory lock explicitly rather than relying on the file
        // descriptor close alone: under load the implicit release can leave a
        // brief window where an immediate reopen in the same process still
        // observes the lock as held (Codex review #50 [P1]).
        let _ = fs2::FileExt::unlock(&self._lock);
    }
}

/// Inserts `event` into the log and applies it to `state`, within `tx`.
fn insert_and_apply(
    tx: &rusqlite::Transaction,
    state: &mut RepoState,
    event: &StateEvent,
) -> Result<(), StoreError> {
    state.apply_event(event);
    tx.execute(
        "INSERT INTO events (seq, ts, payload) VALUES (?1, ?2, ?3)",
        rusqlite::params![
            event.seq as i64,
            event.ts.to_rfc3339(),
            serde_json::to_string(&event.payload)?
        ],
    )?;
    Ok(())
}

/// Upserts the single-row materialized cache from `state`, within `tx`.
fn upsert_cache(
    tx: &rusqlite::Transaction,
    state: &RepoState,
    next_seq: u64,
    ts: DateTime<Utc>,
) -> Result<(), StoreError> {
    let cache_json = serde_json::to_string(&state.to_snapshot(next_seq, ts))?;
    tx.execute(
        "INSERT INTO repo_state (id, snapshot) VALUES (0, ?1)
         ON CONFLICT(id) DO UPDATE SET snapshot = excluded.snapshot",
        rusqlite::params![cache_json],
    )?;
    Ok(())
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
        );
        -- The webhook queue. `arrival` (AUTOINCREMENT) is the monotonic
        -- drain order; `delivery_id` (X-GitHub-Delivery) is unique for
        -- idempotent intake.
        CREATE TABLE deliveries (
            arrival     INTEGER PRIMARY KEY AUTOINCREMENT,
            delivery_id TEXT NOT NULL UNIQUE,
            event_type  TEXT NOT NULL,
            headers     TEXT NOT NULL,
            body        BLOB NOT NULL,
            status      TEXT NOT NULL,
            received_at TEXT NOT NULL
        );
        CREATE INDEX deliveries_drain ON deliveries (status, arrival);
        -- Seen dedupe keys with the time first seen, for TTL pruning.
        CREATE TABLE dedupe_keys (
            key     TEXT PRIMARY KEY,
            seen_at TEXT NOT NULL
        );
        -- Authorized user commands (start/stop) awaiting the saga slot.
        -- Inserted in the same transaction as the delivery's close, so an
        -- acknowledged command survives a crash while it waits out an
        -- in-flight saga (Codex M5 rounds 2 and 19). `id` order is the
        -- user's command order. Deleted when the command is answered.
        CREATE TABLE pending_commands (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            kind       TEXT NOT NULL,
            pr         INTEGER NOT NULL,
            force_stop INTEGER NOT NULL
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
            // Validate the snapshot format version too — a row written by an
            // incompatible build that still happens to deserialize must fail
            // loud, not be silently materialized (Codex review #50).
            if snapshot.schema_version != SCHEMA_VERSION {
                return Err(StoreError::CachedStateSchemaMismatch {
                    expected: SCHEMA_VERSION,
                    found: snapshot.schema_version,
                });
            }
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

        /// One `append_batch` is observationally equal to appending the same
        /// payloads one at a time: same state, same `next_seq`, same log.
        #[test]
        fn append_batch_equals_sequential_appends(
            payloads in prop::collection::vec(arb_state_event_payload(), 0..20),
        ) {
            let dir_batch = tempdir().unwrap();
            let dir_seq = tempdir().unwrap();
            let mut batched = open_temp(&dir_batch);
            let mut sequential = open_temp(&dir_seq);
            let ts = test_timestamp();

            let events = batched.append_batch(&payloads, ts).unwrap();
            for payload in &payloads {
                sequential.append(payload.clone(), ts).unwrap();
            }

            prop_assert_eq!(batched.state(), sequential.state());
            prop_assert_eq!(batched.next_seq(), sequential.next_seq());
            prop_assert_eq!(&events, &batched.events().unwrap());
            prop_assert_eq!(&events, &sequential.events().unwrap());
        }

        /// `events` reads back exactly what was written, in append order,
        /// across a mix of single appends, batches, and delivery commits.
        #[test]
        fn events_reads_back_the_log_in_order(
            singles in prop::collection::vec(arb_state_event_payload(), 0..8),
            batch in prop::collection::vec(arb_state_event_payload(), 0..8),
            committed in prop::collection::vec(arb_state_event_payload(), 0..8),
        ) {
            let dir = tempdir().unwrap();
            let mut store = open_temp(&dir);
            let ts = test_timestamp();

            let mut expected = Vec::new();
            for payload in &singles {
                expected.push(store.append(payload.clone(), ts).unwrap());
            }
            expected.extend(store.append_batch(&batch, ts).unwrap());
            store.enqueue("d1", "pull_request", "{}", b"{}", ts).unwrap();
            store.claim_next_delivery().unwrap().unwrap();
            store.commit_delivery("d1", &committed, None, &[], ts).unwrap();
            expected.extend(committed.iter().enumerate().map(|(i, p)| StateEvent {
                seq: (singles.len() + batch.len() + i) as u64,
                ts,
                payload: p.clone(),
            }));

            prop_assert_eq!(store.events().unwrap(), expected);
        }

        /// Compaction preserves every observable: the cache, the from-empty
        /// replay oracle, `next_seq`, and the state a reopen recovers — and
        /// the log afterwards is exactly one checkpoint plus whatever was
        /// appended after it.
        #[test]
        fn compaction_preserves_state_and_replay(
            before in prop::collection::vec(arb_state_event_payload(), 1..20),
            after in prop::collection::vec(arb_state_event_payload(), 0..8),
        ) {
            let dir = tempdir().unwrap();
            let path = dir.path().join("state.db");
            let mut store = Store::open(&path).unwrap();
            let ts = test_timestamp();
            for payload in &before {
                store.append(payload.clone(), ts).unwrap();
            }
            let state_before = store.state().clone();
            let seq_before = store.next_seq();
            let active = state_before.active_trains.values().any(|t| t.state.is_active());

            let compacted = store.compact(0, ts).unwrap();
            if active {
                // Safety: an active train's history is never summarized.
                prop_assert_eq!(compacted, None);
                prop_assert_eq!(store.events().unwrap().len(), before.len());
                return Ok(());
            }
            if before.len() == 1
                && matches!(before[0], StateEventPayload::Checkpoint { .. })
            {
                // A log that is already exactly one checkpoint stays put.
                prop_assert_eq!(compacted, None);
                return Ok(());
            }
            prop_assert_eq!(compacted, Some(before.len() as u64));
            prop_assert_eq!(store.state(), &state_before);
            prop_assert_eq!(store.next_seq(), seq_before);
            prop_assert_eq!(&store.replay().unwrap(), &state_before);
            let log = store.events().unwrap();
            prop_assert_eq!(log.len(), 1);
            let is_checkpoint = matches!(log[0].payload, StateEventPayload::Checkpoint { .. });
            prop_assert!(is_checkpoint);
            prop_assert_eq!(log[0].seq, seq_before - 1);

            // Appends continue seamlessly, and every oracle still holds.
            for payload in &after {
                store.append(payload.clone(), ts).unwrap();
            }
            prop_assert_eq!(store.state(), &store.replay().unwrap());
            prop_assert_eq!(store.events().unwrap().len(), 1 + after.len());

            // A reopen sees the compacted log's state.
            let final_state = store.state().clone();
            let final_seq = store.next_seq();
            drop(store);
            let reopened = Store::open(&path).unwrap();
            prop_assert_eq!(reopened.state(), &final_state);
            prop_assert_eq!(reopened.next_seq(), final_seq);
            prop_assert_eq!(&reopened.replay().unwrap(), reopened.state());
        }
    }

    /// The threshold gate: a log at or below it is left alone (compacting
    /// a fresh checkpoint would churn forever at every idle boundary).
    #[test]
    fn compaction_respects_the_threshold() {
        let dir = tempdir().unwrap();
        let mut store = open_temp(&dir);
        let ts = test_timestamp();
        for i in 0..5u64 {
            store
                .append(
                    StateEventPayload::DefaultBranchSet {
                        branch: format!("b{i}"),
                    },
                    ts,
                )
                .unwrap();
        }
        assert_eq!(
            store.compact(5, ts).unwrap(),
            None,
            "5 events ≤ threshold 5"
        );
        assert_eq!(
            store.compact(4, ts).unwrap(),
            Some(5),
            "5 events > threshold 4"
        );
        // Already just a checkpoint: never worthwhile again.
        assert_eq!(
            store.compact(0, ts).unwrap(),
            None,
            "a lone checkpoint stays"
        );
    }

    /// An ACTIVE train's history is never compacted away — the refusal is
    /// the store's own invariant, not caller discipline.
    #[test]
    fn compaction_refuses_while_a_train_is_active() {
        let dir = tempdir().unwrap();
        let mut store = open_temp(&dir);
        let ts = test_timestamp();
        store
            .append(
                StateEventPayload::TrainStarted {
                    root_pr: crate::types::PrNumber(1),
                    current_pr: crate::types::PrNumber(1),
                },
                ts,
            )
            .unwrap();
        assert_eq!(store.compact(0, ts).unwrap(), None);
        assert_eq!(store.events().unwrap().len(), 1, "log untouched");

        // Retired (stopped) trains need no history: compaction proceeds.
        store
            .append(
                StateEventPayload::TrainStopped {
                    root_pr: crate::types::PrNumber(1),
                },
                ts,
            )
            .unwrap();
        assert_eq!(store.compact(0, ts).unwrap(), Some(2));
        assert_eq!(store.state(), &store.replay().unwrap());
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

    #[test]
    fn cached_state_schema_mismatch_fails_loud() {
        use crate::types::PrNumber;

        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let mut store = Store::open(&path).unwrap();
            store
                .append(
                    StateEventPayload::TrainStarted {
                        root_pr: PrNumber(1),
                        current_pr: PrNumber(1),
                    },
                    test_timestamp(),
                )
                .unwrap();
        }

        // Rewrite the cached row with an incompatible snapshot schema version
        // that still deserializes.
        {
            let conn = Connection::open(&path).unwrap();
            let json: String = conn
                .query_row("SELECT snapshot FROM repo_state WHERE id = 0", [], |r| {
                    r.get(0)
                })
                .unwrap();
            let mut snap: PersistedRepoSnapshot = serde_json::from_str(&json).unwrap();
            snap.schema_version = SCHEMA_VERSION + 1;
            let bad = serde_json::to_string(&snap).unwrap();
            conn.execute("UPDATE repo_state SET snapshot = ?1 WHERE id = 0", [bad])
                .unwrap();
        }

        match Store::open(&path) {
            Err(StoreError::CachedStateSchemaMismatch { expected, found }) => {
                assert_eq!(expected, SCHEMA_VERSION);
                assert_eq!(found, SCHEMA_VERSION + 1);
            }
            Ok(_) => panic!("expected CachedStateSchemaMismatch, got a store"),
            Err(e) => panic!("expected CachedStateSchemaMismatch, got {e:?}"),
        }
    }

    #[test]
    fn released_delivery_is_reclaimable() {
        let dir = tempdir().unwrap();
        let mut store = open_temp(&dir);
        let ts = test_timestamp();
        store
            .enqueue("d1", "pull_request", "{}", b"{}", ts)
            .unwrap();

        let claimed = store.claim_next_delivery().unwrap().unwrap();
        assert!(store.claim_next_delivery().unwrap().is_none());

        store.release_delivery(&claimed.delivery_id).unwrap();
        let reclaimed = store.claim_next_delivery().unwrap().unwrap();
        assert_eq!(reclaimed.delivery_id, "d1");

        // Releasing a closed delivery is a no-op — it must not reopen.
        store.commit_delivery("d1", &[], None, &[], ts).unwrap();
        store.release_delivery("d1").unwrap();
        assert!(store.claim_next_delivery().unwrap().is_none());
    }

    #[test]
    fn enqueue_claim_commit_flow() {
        use crate::types::{CommentId, PrNumber};
        use crate::webhooks::dedupe::DedupeKey;

        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("state.db")).unwrap();
        let ts = test_timestamp();

        assert!(
            store
                .enqueue("d1", "issue_comment", "{}", b"body", ts)
                .unwrap()
        );
        // Idempotent: the same delivery id again is a no-op.
        assert!(
            !store
                .enqueue("d1", "issue_comment", "{}", b"body", ts)
                .unwrap()
        );

        let claimed = store.claim_next_delivery().unwrap().expect("a delivery");
        assert_eq!(claimed.delivery_id, "d1");
        assert_eq!(claimed.event_type, "issue_comment");
        assert_eq!(claimed.body, b"body");

        let key = DedupeKey::issue_comment_created(PrNumber(7), CommentId(1));
        assert!(!store.is_duplicate(&key).unwrap());
        store
            .commit_delivery(
                "d1",
                &[StateEventPayload::TrainStarted {
                    root_pr: PrNumber(7),
                    current_pr: PrNumber(7),
                }],
                Some(&key),
                &[],
                ts,
            )
            .unwrap();

        // State advanced, dedupe recorded, delivery done (no longer claimable).
        assert!(store.state().active_trains.contains_key(&PrNumber(7)));
        assert!(store.is_duplicate(&key).unwrap());
        assert!(store.claim_next_delivery().unwrap().is_none());
    }

    #[test]
    fn claim_orders_by_arrival() {
        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("state.db")).unwrap();
        let ts = test_timestamp();
        for id in ["a", "b", "c"] {
            store.enqueue(id, "status", "{}", b"x", ts).unwrap();
        }
        let mut order = vec![];
        while let Some(d) = store.claim_next_delivery().unwrap() {
            order.push(d.delivery_id);
        }
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn processing_is_requeued_on_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");
        let ts = test_timestamp();
        {
            let mut store = Store::open(&path).unwrap();
            store.enqueue("d1", "status", "{}", b"x", ts).unwrap();
            // Claim (→ processing), then "crash" by dropping before commit.
            assert!(store.claim_next_delivery().unwrap().is_some());
        }
        // Reopen requeues the abandoned `processing` delivery to `pending`.
        let mut reopened = Store::open(&path).unwrap();
        assert_eq!(
            reopened
                .claim_next_delivery()
                .unwrap()
                .map(|d| d.delivery_id),
            Some("d1".to_string())
        );
    }

    #[test]
    fn prune_drops_old_dedupe_and_done_deliveries() {
        use crate::types::{CommentId, PrNumber};
        use crate::webhooks::dedupe::DedupeKey;

        let dir = tempdir().unwrap();
        let mut store = Store::open(&dir.path().join("state.db")).unwrap();
        let old = test_timestamp();
        let cutoff = old + chrono::Duration::hours(1);

        store.enqueue("d1", "status", "{}", b"x", old).unwrap();
        store.claim_next_delivery().unwrap();
        let key = DedupeKey::issue_comment_created(PrNumber(1), CommentId(1));
        store
            .commit_delivery("d1", &[], Some(&key), &[], old)
            .unwrap();

        assert!(store.is_duplicate(&key).unwrap());
        assert_eq!(store.prune_dedupe(cutoff).unwrap(), 1);
        assert!(!store.is_duplicate(&key).unwrap());
        assert_eq!(store.prune_deliveries(cutoff).unwrap(), 1);
        assert!(store.claim_next_delivery().unwrap().is_none());
    }

    #[test]
    fn read_snapshot_returns_the_cached_state() {
        use crate::types::PrNumber;
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let mut store = Store::open(&path).unwrap();
            store
                .append(
                    StateEventPayload::TrainStarted {
                        root_pr: PrNumber(7),
                        current_pr: PrNumber(7),
                    },
                    test_timestamp(),
                )
                .unwrap();
        }
        let snapshot = Store::read_snapshot(&path).unwrap().expect("a snapshot");
        assert!(snapshot.active_trains.contains_key(&PrNumber(7)));
    }

    #[test]
    fn read_snapshot_is_none_for_missing_db() {
        let dir = tempdir().unwrap();
        assert!(
            Store::read_snapshot(&dir.path().join("absent.db"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn read_snapshot_rejects_store_schema_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");
        drop(Store::open(&path).unwrap());
        {
            let conn = Connection::open(&path).unwrap();
            conn.pragma_update(None, "user_version", STORE_SCHEMA_VERSION + 1)
                .unwrap();
        }
        match Store::read_snapshot(&path) {
            Err(StoreError::SchemaMismatch { expected, found }) => {
                assert_eq!(expected, STORE_SCHEMA_VERSION);
                assert_eq!(found, STORE_SCHEMA_VERSION + 1);
            }
            other => panic!("expected SchemaMismatch, got {other:?}"),
        }
    }

    #[test]
    fn read_snapshot_rejects_cached_state_schema_mismatch() {
        use crate::types::PrNumber;

        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");
        {
            let mut store = Store::open(&path).unwrap();
            store
                .append(
                    StateEventPayload::TrainStarted {
                        root_pr: PrNumber(1),
                        current_pr: PrNumber(1),
                    },
                    test_timestamp(),
                )
                .unwrap();
        }
        {
            let conn = Connection::open(&path).unwrap();
            let json: String = conn
                .query_row("SELECT snapshot FROM repo_state WHERE id = 0", [], |r| {
                    r.get(0)
                })
                .unwrap();
            let mut snap: PersistedRepoSnapshot = serde_json::from_str(&json).unwrap();
            snap.schema_version = SCHEMA_VERSION + 1;
            let bad = serde_json::to_string(&snap).unwrap();
            conn.execute("UPDATE repo_state SET snapshot = ?1 WHERE id = 0", [bad])
                .unwrap();
        }
        match Store::read_snapshot(&path) {
            Err(StoreError::CachedStateSchemaMismatch { expected, found }) => {
                assert_eq!(expected, SCHEMA_VERSION);
                assert_eq!(found, SCHEMA_VERSION + 1);
            }
            other => panic!("expected CachedStateSchemaMismatch, got {other:?}"),
        }
    }

    #[test]
    fn read_snapshot_reads_concurrently_with_the_writer() {
        use crate::types::PrNumber;
        // WAL: the read-only read succeeds while the owning Store still holds the
        // DB open (and its lock).
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.db");
        let mut store = Store::open(&path).unwrap();
        store
            .append(
                StateEventPayload::TrainStarted {
                    root_pr: PrNumber(3),
                    current_pr: PrNumber(3),
                },
                test_timestamp(),
            )
            .unwrap();
        let snapshot = Store::read_snapshot(&path).unwrap().expect("a snapshot");
        assert!(snapshot.active_trains.contains_key(&PrNumber(3)));
        drop(store);
    }
}
