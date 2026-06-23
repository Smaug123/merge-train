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
  fresh, check `user_version` (fail loud on mismatch — the explicit break), and
  **reset any `status='processing'` delivery back to `'pending'`**. With one
  writer per repo, a `processing` row at open time is abandoned by a dead
  worker — this is the SQLite equivalent of the old
  `cleanup_interrupted_processing`. Without it, a delivery interrupted between
  claim and the closing commit is stuck forever, since the drain only takes
  `pending` (Codex plan-review P1-A).
- `load(&self) -> RepoState` — replay all `events` through `apply_event`.
- `claim_next_delivery(&mut self) -> Option<Delivery>` — txn: pick lowest
  `arrival` with `status='pending'`, set `'processing'`, return it.
- `append(&mut self, events: &[StateEventPayload])` — append state events in
  their own txn (assigning `seq` from `meta`); used for the outbox
  `Intent*`/`Done*`/observation commits during a saga.
- `commit_delivery(&mut self, id, events, dedupe)` — the **closing
  transaction**: append any remaining state events, INSERT-OR-IGNORE the dedupe
  key, set the delivery `'done'`. For an effect-free delivery this is the whole
  processing step; effectful deliveries commit intent/result txns first (see the
  flow below).
- `is_duplicate(&self, key) -> bool`, `prune_dedupe(&mut self, ttl)`.
- `enqueue(&mut self, envelope)` — INSERT a `pending` delivery (intake).

### Concurrency
Per-repo single writer = the worker thread owns its `Store` connection. The
async server (axum) hands raw deliveries to the repo's worker (enqueue) and
**never** writes state. Read-only endpoints (`/health`, `/state`) use a
separate read-only connection; WAL lets them read without blocking the writer.

## The per-delivery flow (replaces the marker dance)

Intake and processing are separate; the spool is the `deliveries` table.

- **Intake (server):** INSERT a `pending` delivery in its own txn, then notify
  the repo's worker.
- **Startup recovery:** `Store::open` resets `processing` → `pending` (above),
  so a delivery interrupted mid-processing is re-drained, never stuck.

The worker loop, for one delivery:

```
claim_next_delivery()                 -- txn: pending -> processing
parse_webhook(body); key = DedupeKey::for_event(event)
if is_duplicate(key) { commit_delivery(id, [], None); continue }  -- txn: mark 'done'
events, plan = handle_event(...)      -- pure: state events + effect intents (triggers)
```

**Effect-free delivery** (a pure state/cache change — most events): a single
closing txn — `commit_delivery` — `{ append events; INSERT OR IGNORE dedupe
key; set delivery 'done' }`. The dedupe mark lives *with* the events and the
done marker, so a crash before COMMIT leaves all three un-applied and the
delivery re-drains. This makes `CASCADE_ENGINE_PLAN.md` finding #1 (dedupe
marked before processing ⇒ dropped event) structurally impossible.

**Effectful delivery** (the cascade saga): because a `git push` / GitHub merge
cannot join a SQLite txn, the **outbox** discipline — *intent committed before
the effect, result after, done last* (Codex plan-review P1-B):

1. txn **I** — append the `IntentPush*` event(s): the durable "about to do X",
   carrying the fencing token (`expected_sha`). **Committed before the effect.**
2. the relay performs the **idempotent** effect, guarded by the fencing token.
3. txn **R** — append the `Done*` / observation event(s) and advance state.
4. `commit_delivery` — closing txn: dedupe key seen + delivery `'done'`.

On a crash mid-saga, replay finds an `Intent*` without its `Done*` and re-runs
the idempotent effect (the existing pre-action checks — already merged? ref
already at the expected tree? — make the retry a safe no-op or redo). This is
the corrected ordering that also resolves `CASCADE_ENGINE_PLAN.md`:
- **Finding #2:** the fencing token (`expected_sha`) is in txn **I**, durable
  *before* the squash — no in-memory gap.
- **Finding #3:** the external cleanup (worktree removal, final status comment)
  runs as the effect in steps 1–2; the terminal `TrainStopped`/`TrainAborted`
  is the txn-**R** state event *after* cleanup — never before.

The key correction over the first draft: effects never run before their intent
is durable, and the delivery is marked `done` only after every effect's result
is committed — so no crash window repeats an irreversible effect or strands a
claimed delivery.

## Testing

- **Drop:** the `fsync` fault plan, the compact-op crash sweeps, generation /
  orphan / settlement tests.
- **Keep / add:** `apply_event`/replay properties (`replay(events) == state`,
  the compaction-equivalence property restated as replay-vs-replay), event-
  vocab serde round-trips, `DedupeKey` properties, and crash-recovery
  integration tests (we test *our* recovery, not SQLite's durability):
  - a `processing` row left by a dead worker is requeued to `pending` on
    `Store::open` and re-drained (P1-A);
  - a delivery interrupted after an effect but before its `Done*` txn replays as
    an `Intent*`-without-`Done*` and the idempotent re-run yields the same final
    `RepoState` as the no-crash run (P1-B);
  - dedupe + done commit atomically: a crash before the closing txn re-drains
    and reprocesses with no dropped event (finding #1).

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
