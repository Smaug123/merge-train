# SQLite Migration Plan

Replace the hand-rolled filesystem durability layer (`persistence/` + `spool/`,
~12k LOC) with **event-sourcing on SQLite**. Keep the functional core, the
`StateEventPayload` vocabulary, `RepoState::apply_event`, replay determinism,
and the status-comment backup. SQLite becomes the storage substrate; the
event-sourced model is unchanged.

## Owner decisions (resolved)

1. **Driver:** `rusqlite` (sync, `bundled` SQLite — no system dep), one
   single-writer worker thread per repo. Not `sqlx`/async.
2. **Granularity:** one DB file per repo, `<state_dir>/<owner>/<repo>/state.db`.
3. **Status-comment backup:** kept (the one durability property a single local
   SQLite file does not give — state recoverable from GitHub if the disk dies).
4. **Sequencing:** land this migration **before** the cascade engine (M2–M6),
   so the engine is built on the Store and the three `CASCADE_ENGINE_PLAN.md`
   findings are folded into the rewrite rather than fixed on a doomed substrate.
5. **Backward compatibility:** fully broken. There is no live state to migrate;
   deployments wipe the state dir. No data-migration code.

## Why

~7–8k of the ~12k LOC in `persistence/`+`spool/` exists only to hand-roll ACID
+ a durable queue on the filesystem: `compaction.rs` (2854), `recovery.rs`
(1442), `generation.rs` (462), `fsync.rs` (294) + its fault-injection seam,
`log.rs`'s file format (1732), most of `snapshot.rs`, and the marker-file spool
in `delivery.rs` (1624) + `drain.rs` (823). Every crash-point bug the
`rem/03-persistence` review rounds found (orphan snapshots, the settlement
invariant, post-rename fsync failures) lived in exactly this layer — the class
SQLite makes unrepresentable. The consumer surface is tiny (`server/{mod,state,
webhook}.rs` + tests), and the big future consumer — the M5 worker — does not
exist yet, so the timing is ideal.

## Architecture

### Keep (substrate-independent)
`state/` (RepoState, `apply_event`, topology, transitions, validation),
`types/`, the `StateEventPayload` event vocabulary (`persistence/event.rs`),
`DedupeKey` construction (`for_event` etc.), webhook parsing, the
`effects/`+`git/`+`github/` interpreters, the engine/saga design (M2–M6), and
the status-comment backup module.

### Delete (~7–8k LOC retired)
`compaction.rs`, `generation.rs`, `fsync.rs` + the fault-injection apparatus,
most of `recovery.rs` (generation/orphan/settlement machinery), most of
`snapshot.rs`, the `EventLog` file format in `log.rs`, and the marker-file spool
in `delivery.rs`/`drain.rs`. Plus the crash-point test sweeps — SQLite owns
crash-safety; we stop testing storage durability and test only our logic.

### Build — `src/store/` (the Store; per repo; WAL mode)

```sql
PRAGMA journal_mode = WAL;        -- crash-safe; concurrent read for status/health
PRAGMA synchronous = FULL;        -- durability over speed (correctness > availability)
PRAGMA user_version = 1;          -- schema version; bump on breaking change

CREATE TABLE events (
  seq     INTEGER PRIMARY KEY,    -- global monotonic sequence
  ts      TEXT    NOT NULL,       -- RFC3339 (event.ts, stamped at the shell)
  payload TEXT    NOT NULL        -- StateEventPayload via the existing serde
);

CREATE TABLE deliveries (
  delivery_id TEXT    PRIMARY KEY,-- X-GitHub-Delivery (intake idempotency)
  arrival     INTEGER NOT NULL,   -- monotonic arrival marker (drain order)
  event_type  TEXT    NOT NULL,   -- X-GitHub-Event header
  headers     TEXT    NOT NULL,   -- captured headers (incl. signature) as JSON
  body        BLOB    NOT NULL,   -- raw webhook payload bytes
  status      TEXT    NOT NULL,   -- 'pending' | 'processing' | 'done'
  received_at TEXT    NOT NULL
);
CREATE INDEX deliveries_drain ON deliveries(status, arrival);

CREATE TABLE dedupe_keys (
  key     TEXT PRIMARY KEY,       -- DedupeKey::as_str()
  seen_at TEXT NOT NULL           -- for TTL pruning
);

CREATE TABLE meta (               -- next_seq, default_branch cache, etc.
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
```

Active trains / cached PRs are **not** stored as tables in M1: `RepoState` is
reconstructed by replaying `events`. (An optional single-row cached-state blob
can be added later if cold-start replay is ever slow; pure replay is the simple
default and keeps one source of truth.)

Store API (sync; called from the per-repo worker thread):
- `Store::open(path) -> Store` — open/create, apply PRAGMAs, run schema DDL if
  fresh, check `user_version` (fail loud on mismatch — the explicit break).
- `load(&self) -> RepoState` — replay all `events` through `apply_event`.
- `claim_next_delivery(&mut self) -> Option<Delivery>` — txn: pick lowest
  `arrival` with `status='pending'`, set `'processing'`, return it.
- `commit_delivery(&mut self, id, events: &[StateEventPayload], dedupe: Option<&DedupeKey>)`
  — **one transaction**: INSERT the state events (assigning `seq` from `meta`),
  INSERT-OR-IGNORE the dedupe key, set the delivery `'done'`. (See below.)
- `is_duplicate(&self, key) -> bool`, `prune_dedupe(&mut self, ttl)`.
- `enqueue(&mut self, envelope)` — INSERT a `pending` delivery (intake).

### Concurrency
Per-repo single writer = the worker thread owns its `Store` connection. The
async server (axum) hands raw deliveries to the repo's worker (enqueue) and
**never** writes state. Read-only endpoints (`/health`, `/state`) use a
separate read-only connection; WAL lets them read without blocking the writer.

## The per-delivery transaction (replaces the marker dance)

```
-- intake (server): INSERT delivery (status='pending')   [own txn]
-- worker loop:
claim_next_delivery()                                    -- txn: pending -> processing
parse_webhook(body)
if is_duplicate(key) { mark done; continue }             -- dedupe is a fast-path only
let events = handle_event(...)                            -- pure
run external effects via the outbox (see below)
commit_delivery(id, &events, Some(&key))                 -- ONE txn:
    INSERT events...; UPDATE meta.next_seq;
    INSERT OR IGNORE dedupe_keys(key, now);
    UPDATE deliveries SET status='done' WHERE id=?;
COMMIT
```

`mark_seen` lives **inside** the commit with the state events and the done
marker. A crash anywhere before COMMIT leaves all three un-applied, so the
delivery re-drains and reprocesses (idempotent). This makes
`CASCADE_ENGINE_PLAN.md` finding #1 (dedupe marked before processing ⇒ dropped
event) structurally impossible.

## External effects — the irreducible part (findings #2/#3)

A `git push` / GitHub merge cannot join a SQLite transaction, so the **outbox**
pattern stays — just expressed cleanly:
- Intent rows (the existing `IntentPush*`/`Done*` events) are written in the
  same txn as the state change; a relay performs the effect and records the
  result. At-least-once + idempotent effects (the existing pre-action checks).
- **Finding #2:** persist the fencing token (`expected_sha`) in the same txn
  *before* the external squash — now a trivial column/event, no in-memory gap.
- **Finding #3:** run the external cleanup (worktree removal, final status
  comment) *before* writing the terminal `TrainStopped`/`TrainAborted` row —
  saga discipline, unchanged by the substrate.

## Testing

- **Drop:** the `fsync` fault plan, the compact-op crash sweeps, generation /
  orphan / settlement tests.
- **Keep / add:** `apply_event`/replay properties (`replay(events) == state`,
  the compaction-equivalence property restated as replay-vs-replay), event-
  vocab serde round-trips, `DedupeKey` properties, and one integration test:
  `kill -9` mid-transaction ⇒ on restart SQLite recovers, the worker re-drains,
  and final `RepoState` equals the no-crash run. (We test *our* redrain, not
  SQLite's durability.)

## Stages — each a green, reviewable PR

- **S1 — Store foundation.** Add `rusqlite` (`bundled`) and **confirm it builds
  in the nix flake** (gating risk). `src/store/`: schema DDL, `open`,
  `append`/`load` (events ↔ RepoState replay), `meta`/`next_seq`. Property
  tests. Parallel to the existing code; nothing rewired yet.
- **S2 — queue + dedupe in the Store.** `deliveries` + `dedupe_keys` tables and
  methods (`enqueue`, `claim_next_delivery`, `commit_delivery`, dedupe, prune).
  Rewire `server/webhook.rs` intake to `enqueue`; drain reads from the Store.
- **S3 — recovery + single-mutation path.** Replace `recover()` with
  `Store::open` + `load`; route the per-delivery transaction; wire the
  status-comment backup read/write alongside the Store.
- **S4 — delete the old substrate.** Remove `compaction`/`generation`/`fsync`/
  the `EventLog` file format / the marker-file spool + their tests; collapse
  `persistence/` to `{event vocab, Store}` and `spool/` to `{DedupeKey, Store}`.

After S4, the cascade engine (M2–M6) builds on the Store, and
`CASCADE_ENGINE_PLAN.md`'s persistence seams (critical-fsync events, the marker
protocol, intent/done) are rewritten against it — folding in the three findings.

## Risks / open

- **Driver build in nix:** `rusqlite`'s `bundled` feature vendors SQLite (no
  system lib); S1 step 0 confirms it compiles in the flake before anything else.
- **sync/async boundary:** mitigated by the per-repo worker thread owning the
  writer connection; the async server only enqueues and reads.
- **Cold-start replay cost:** pure replay is the M1 default; add a cached-state
  row only if a real repo's log makes startup slow (measure first).
- **Status-comment backup:** its format/module is unchanged; it reads/writes
  alongside the Store (belt-and-suspenders), not through it.
