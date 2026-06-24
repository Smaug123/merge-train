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
-- user_version is SET only when creating a fresh DB (see Store::open); it is
-- READ first on every open so a mismatch fails loud instead of being clobbered.
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

A `repo_state` row/table holds the **materialized `RepoState`** (the snapshot
equivalent) and is **required**, not a perf tweak (Codex plan-review P1-G):
bootstrap-from-GitHub-crawl and status-comment recovery materialize state —
`default_branch`, `CachedPr.merge_state_status`, an active `TrainRecord` — that
the `StateEventPayload` vocabulary does **not** express, so a replay-only `load`
would lose open PRs / active trains on the next restart. `apply_event` updates
the in-memory state and upserts this row **in the same txn** as the event. This
is the existing snapshot+log model, minus generations/compaction/orphan-rollback
— in SQLite the "snapshot" is just an upserted row. Replay-from-`events` stays
as the recovery/equivalence oracle (`replay == cached state`), not the hot
`load` path.

Store API (sync; called from the per-repo worker thread):
- `Store::open(path) -> Store` — in order:
  1. **Acquire an exclusive per-repo process lock** (an `flock`/lockfile beside
     the DB — the role the current `StateDirLock` plays). SQLite/WAL only
     serializes individual *writes*; it does **not** stop two bot processes
     (rollout overlap, double supervisor start, a stray dev instance) from each
     opening the repo DB, claiming different deliveries, and running concurrent
     git/GitHub effects. This lock preserves single-writer-per-repo *across
     processes* and must outlive the worker; **do not delete `StateDirLock`'s
     role when deleting `recovery.rs`** (Codex plan-review P1-D).
  2. Apply connection PRAGMAs (`journal_mode`/`synchronous`).
  3. **Read `user_version`**: `0`/absent ⇒ fresh DB, run the schema DDL and
     *set* `user_version` **in one transaction** (atomic — a crash mid-DDL must
     not leave a partial schema still at version 0, Codex plan-review P2-K);
     equal ⇒ proceed; mismatch ⇒ **fail loud** (the explicit break — never
     overwrite it) (Codex plan-review P2-F).
  4. **Reset any `status='processing'` delivery back to `'pending'`** — with the
     per-repo lock held, a `processing` row at open is abandoned by a dead
     worker (the SQLite equivalent of `cleanup_interrupted_processing`); without
     it an interrupted delivery is stuck, since the drain only takes `pending`
     (Codex plan-review P1-A).
- `load(&self) -> RepoState` — replay all `events` through `apply_event`.
- `claim_next_delivery(&mut self) -> Option<Delivery>` — txn: pick lowest
  `arrival` with `status='pending'`, set `'processing'`, return it.
- `append(&mut self, events: &[StateEventPayload])` — append state events in
  their own txn (assigning `seq` from `meta`); used for the outbox
  `Intent*`/`Done*`/observation commits during a saga.
- `commit_delivery(&mut self, id, final_events, dedupe)` — the **closing
  transaction**, atomically: append the delivery's **final** state event(s)
  (for an effectful delivery, the last `Done*`/observation that advances state),
  INSERT-OR-IGNORE the dedupe key, set the delivery `'done'`. The final result
  and the close are the *same* txn, so there is no window where state has
  advanced but the delivery is still open and un-deduped — closing this window
  is Codex plan-review P1-C. For an effect-free delivery this is the whole
  processing step.
- `is_duplicate(&self, key) -> bool`, `prune_dedupe(&mut self, ttl)`.
- `prune_deliveries(&mut self, grace)` — delete `done` deliveries older than the
  grace period (frees the raw-body BLOBs; keeps delivery-id idempotency during
  the grace). Replaces `cleanup_done_deliveries` — without it the per-repo DB
  grows unbounded as `done` rows retain ≤25 MB payloads forever (Codex
  plan-review P2-I).
- `enqueue(&mut self, envelope)` — INSERT a `pending` delivery. Called **by the
  worker thread** (which owns the write connection and holds the lock), never by
  the server (Codex plan-review P1-H — see Intake).

### Concurrency
Two levels of mutual exclusion:
- **Across processes:** the exclusive per-repo lock taken in `Store::open`
  (above). SQLite/WAL serializes *writes* but would happily let two processes
  each claim different deliveries and run git/GitHub effects concurrently — the
  lock (inheriting `StateDirLock`'s role) is what actually enforces one driver
  per repo. Held for the worker's lifetime.
- **Within the process:** the worker thread owns its `Store` write connection
  and is the *only* writer. The async server **never opens the Store**; it hands
  raw deliveries to the repo's worker over an in-process channel and awaits the
  worker's durable-INSERT ack before replying 200 (a second `Store::open` from
  the handler would deadlock on the worker-held lock — Codex plan-review P1-H).
  Read-only endpoints (`/health`, `/state`) use a separate read-only connection;
  WAL lets them read without blocking the writer.

## The per-delivery flow (replaces the marker dance)

Intake and processing are separate; the spool is the `deliveries` table.

- **Intake:** the axum handler sends the raw delivery (headers + body) to the
  repo's worker over an in-process channel; the worker `enqueue`s the `pending`
  row (it holds the lock + connection) and acks; the handler returns 200 only
  after that durable INSERT. A crash before the INSERT means no 200, so GitHub
  redelivers — at-least-once intake (Codex plan-review P1-H).
- **Startup recovery:** `Store::open` resets `processing` → `pending` (above),
  so a delivery interrupted mid-processing is re-drained, never stuck.

The worker loop, for one delivery:

```
claim_next_delivery()                 -- txn: pending -> processing
event = match parse_webhook(type, body) {
    Ok(None)        => { commit_delivery(id, [], None); continue }  -- ignored event type
    Err(_malformed) => { commit_delivery(id, [], None); continue }  -- log + close; never re-loop
    Ok(Some(e))     => e,
}
key = DedupeKey::for_event(&event)    -- Option: a non-PR comment has no key
if key.as_ref().is_some_and(is_duplicate) { commit_delivery(id, [], None); continue }
events, plan = handle_event(&event, ...)   -- pure: state events + effect intents (triggers)
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

1. txn **I** — append the `Intent*` event(s): the durable "about to do X",
   carrying the fencing token (`expected_sha`). **Committed before the effect.**
2. the relay performs the **idempotent** effect, guarded by the fencing token.
3. txn **R** — append the `Done*` / observation event(s) and advance state. For
   an *intermediate* step this is a plain `append`; for the delivery's **last**
   step it is the `commit_delivery` closing txn, so the final result event, the
   dedupe key, and delivery `'done'` commit **together** — there is no window
   where state advanced but the delivery is still open (Codex plan-review P1-C).

On a crash mid-saga, replay finds an `Intent*` without its `Done*` and re-runs
the idempotent effect (the existing pre-action checks — already merged? ref
already at the expected tree? — make the retry a safe no-op or redo). The
saga-resume logic (re-running `handle_event`/the engine over a partly-logged
delivery without double-emitting) is the **M5 worker's** responsibility and is
specified there; this plan only fixes the *substrate* ordering. The corrected
ordering also resolves `CASCADE_ENGINE_PLAN.md`:
- **Finding #2:** the fencing token must be durable in txn **I** — which
  requires the one targeted vocabulary extension this migration makes:
  `IntentSquash` (today `{ train_root, pr }`) gains `expected_sha: Sha`, since
  there is otherwise nowhere to replay the token from after a crash (Codex
  plan-review P2-E). This is the sole exception to "keep the vocabulary."
- **Finding #3:** the external cleanup (worktree removal, final status comment)
  runs as the effect in steps 1–2; the terminal `TrainStopped`/`TrainAborted`
  is the txn-**R** state event *after* cleanup — never before.

The key correction over the first draft: effects never run before their intent
is durable, the delivery is closed atomically with its final result, and a
per-repo process lock keeps a second process from driving the same repo — so no
crash or overlap window repeats an irreversible effect or strands a delivery.

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
    and reprocesses with no dropped event (finding #1);
  - a crash after the final result txn does **not** re-enter `handle_event`,
    because that result and the close are one txn (P1-C);
  - a second `Store::open` on the same repo path fails/blocks while the first
    holds the lock (P1-D);
  - `Store::open` on a DB with a mismatched `user_version` fails loud and leaves
    the stored version untouched (P2-F).

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
