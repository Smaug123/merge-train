# Staged Implementation Plan: the cascade engine ("missing middle")

> **Status note (2026-06-25):** M1 is landed. The persistence substrate has
> since been migrated to SQLite (`SQLITE_MIGRATION_PLAN.md`), so this plan's
> **M5/M6 substrate details are superseded**: there is no `EventLog`/spool/
> `persistence::recover` — durability is the per-repo SQLite `Store` and its
> per-repo worker (`src/worker/`, the M5 *spine*). The **pure** stages (M2
> cascade engine, M3 handlers, M4 git interpreter) are substrate-independent and
> remain accurate; the worker's `process_one` seam is where M3→M2→M4 wire in.

Produced 2026-06-11 after the full-codebase remediation; consumes the hardened
module APIs as of that date. Covers IMPLEMENTATION_PLAN.md Stages 10, 15, 17,
18. Six stages: M1 (vocabulary + state materialization), M2 (cascade engine,
pure), M3 (event handlers, pure), M4 (git effect interpreter), M5 (per-repo
worker + wiring), M6 (bootstrap/recovery). M2/M3 and M4 are parallelizable
after M1. Implement each stage on its own branch, stacked as necessary, so a
reviewer can review each in isolation.

---

## Resolved seam decisions (binding for all stages)

**(a) GitResponse cannot represent conflict/rejection.** Extend `GitResponse`
in `src/effects/git.rs` with serde-able mirrors of `git::MergeResult` and
`git::PushResult`:

```rust
pub enum MergeOutcome { Success { commit_sha: Sha }, Conflict { conflicting_files: Vec<String> }, AlreadyUpToDate }
pub enum PushOutcome { Pushed, Rejected { reason: String } }   // Rejected = non-fast-forward; NOT an Err
pub enum GitResponse {
    Ok, Sha(Sha), Bool(bool),
    Merge(MergeOutcome),
    Push(PushOutcome),
    Prepared { merge: MergeOutcome, predecessor_head: Sha },   // response to PrepareDescendant
}
```

Conflicts and push rejections are *domain outcomes the engine branches on*
(abort vs retry per DESIGN.md §Pause Conditions), so they must be
`Ok(GitResponse::...)`, never interpreter `Err`. Also add compound `GitEffect`
variants mirroring the existing compound `MergeReconcile` precedent,
interpreted by the already-hardened `src/git/` functions:

```rust
GitEffect::PrepareDescendant { descendant_branch: String, predecessor_pr: PrNumber },        // → git::merge::prepare_descendant
GitEffect::CatchUpDescendant { descendant_branch: String, default_branch: String },          // → git::merge::catch_up_descendant
GitEffect::CheckPushCompleted { branch: String, expected_tree: Sha, pre_push_sha: Sha, second_parent: Option<Sha> },  // → git::push::is_push_completed (recovery)
```

`MergeReconcile` keeps its required `expected_squash_parent` +
`predecessor_pre_squash_head` fields — and must also gain
`predecessor_pr: PrNumber`: the interpreter (`reconcile_descendant`) now takes
a `ReconcileRequest` and resolves the actually-squashed head from
`refs/pull/<n>/head`, refusing unless it equals the prepared head (the
ancestry check alone cannot detect a force-push between preparation and
squash). It performs **merges only — no push**.
Its `target_branch` field is removed (see Open Question 1):
`intent_push_reconcile` requires `expected_tree`, which only exists *after*
the merge, so the push must be a separate effect bracketed by its own
intent/done events.

**(b) dedupe::WebhookEventData vs webhooks::events::GitHubEvent.** Unify by
making `GitHubEvent` carry the fields dedupe keys need, then **delete**
`WebhookEventData` and `extract_dedupe_key`. The parser currently drops fields
present in every payload: add `updated_at: DateTime<Utc>` to
`IssueCommentEvent` (comment.updated_at) and `PullRequestEvent`
(pull_request.updated_at), `suite_id: u64` + `updated_at` to
`CheckSuiteEvent`, `updated_at` to `StatusEvent`, `review_id: u64` to
`PullRequestReviewEvent`. Then implement
`DedupeKey::for_event(&GitHubEvent) -> DedupeKey` in `src/spool/dedupe.rs`
reusing the existing key constructors. Bridge property test proves equality
with the legacy constructors before deletion.

**(c) Who interprets `Effect::RecordReconciliation`.** The worker's effect
executor (M5), not the GitHub/Git interpreters. It appends a **new** event-log
variant (M1):

```rust
StateEventPayload::ReconciliationRecorded { pr: PrNumber, squash_sha: Sha }   // is_critical() == true
```

and `apply_event` sets
`prs[pr].predecessor_squash_reconciled = Some(squash_sha)`. Equality with the
predecessor's `merge_commit_sha` (required by `is_root` in
`src/state/topology.rs`) holds **by construction**:
`apply_event(SquashCommitted { pr, sha })` sets
`prs[pr].state = Merged { merge_commit_sha: sha }`, and the engine emits
`RecordReconciliation` with the *same* `squash_sha` carried in the
`Reconciling` phase — both flow from the single `GitHubResponse::Merged { sha }`.
A later `pull_request.closed` webhook for the predecessor carries the same SHA
from GitHub and is an idempotent re-apply. This is a stated M1 property test.

**(d) Timestamps and randomness enter at the shell only.** Already true for
`EventLog::append` (stamps `Utc::now()`) and `ArrivalMarker::now()`. The
engine (M2) and handlers (M3) take `now: DateTime<Utc>` parameters and never
call the clock; replay determinism comes from `apply_event` taking
`&StateEvent` and using `event.ts` for `started_at`/`ended_at`. Poll jitter
(deferred) and spool temp naming are shell concerns.

**(e) Capturing `expected_squash_parent` and threading prepare's pinned head.**
- *Prepare → squash*: `prepare_descendant` returns
  `PrepareResult::predecessor_head` per descendant. Engine rule: the **first**
  successful prep sets `train.predecessor_head_sha = Some(pinned)`; any later
  descendant whose pinned head differs ⇒ abort ("predecessor force-pushed
  mid-preparation"). The squash then uses
  `GitHubEffect::SquashMerge { pr: current_pr, expected_sha: pinned }`
  (falling back to the cached head when there are no descendants), so GitHub's
  409-on-mismatch closes the prep→squash race.
- *Squash → reconcile*: after `Squashed { sha }`, engine emits
  `SquashCommitted` (critical fsync), then `Git::Fetch(default_branch)` +
  `Git::RevParse("<sha>^")` to capture the parent **at observation time**,
  stores it in a new field `TrainRecord.last_squash_parent_sha: Option<Sha>`
  (M1, `#[serde(default)]`), and persists it durably via a new optional field
  `last_squash_parent: Option<Sha>` on `StateEventPayload::PhaseTransition`,
  written with the `Reconciling` transition. Recovery reads it from the latest
  `PhaseTransition`; if absent (crash in the gap before that transition — at
  which point no reconcile intent can exist yet), recovery re-observes by
  recomputing, which is sound because nothing has consumed the pin yet.
  `MergeReconcile` then receives both `expected_squash_parent` and
  `predecessor_pre_squash_head` from the train record, satisfying
  `reconcile_descendant`'s ancestry-proof refusal.

**(f) Worker concurrency model.** One tokio task per repo (DESIGN §Per-repo
serial event processing). A `Dispatcher`
(`Arc<Mutex<HashMap<RepoId, WorkerHandle>>>`) lazily spawns workers; the
webhook handler, after `spool_webhook` succeeds and before returning 202,
calls `dispatcher.notify(&repo_id)` — a `try_send` of
`WorkerMessage::SpoolNudge` on a **bounded** mpsc channel (capacity ~8; `Full`
is silently dropped because nudges coalesce — the spool itself is the durable
queue, drained exhaustively per wakeup). Within a worker, events are strictly
serial: drain spool → parse → dedupe → push into an in-memory
`BinaryHeap<QueuedEvent>` ordered by `(EventPriority desc, arrival,
delivery_id)` (this finally wires `classify_priority`) → process one at a
time. All git operations (compound `GitEffect`s backed by `std::process` in
`src/git/`) and sync spool/log I/O run inside `tokio::task::spawn_blocking`;
the worker awaits each, so serialism is preserved while the runtime isn't
blocked. GitHub effects use the async `OctocrabClient`. Stack-scoped
`CancellationToken`s for interrupting *in-flight* git ops are deferred — stop
commands are honored at every observation boundary via queue priority
(bounded staleness = one git operation).

---

## Stage M1 — Effect/event vocabulary + materialized RepoState (pure)

**Dependencies:** none. **Implements:** DESIGN.md §Per-repo state,
§Incremental updates, preconditions of §Durability and commit points.

**Files:**
- Create `src/state/repo.rs`
- Modify: `src/effects/git.rs` (seam a), `src/effects/mod.rs`,
  `src/persistence/event.rs` (seam c, e: `ReconciliationRecorded`;
  `PhaseTransition.last_squash_parent: Option<Sha>`; `DoneRetarget` gains
  `new_base: String`), `src/types/train.rs`
  (`last_squash_parent_sha: Option<Sha>`), `src/webhooks/events.rs` +
  `src/webhooks/parser.rs` + `src/spool/dedupe.rs` (seam b),
  `src/test_utils.rs` (generators).

**Key signatures:**

```rust
// src/state/repo.rs — the in-memory materialization DESIGN calls RepoState.
pub struct RepoState {
    pub default_branch: String,
    pub prs: HashMap<PrNumber, CachedPr>,
    pub active_trains: HashMap<PrNumber, TrainRecord>,
    pub descendants: HashMap<PrNumber, HashSet<PrNumber>>,   // derived, rebuilt on load
    pub seen_dedupe_keys: HashMap<String, DateTime<Utc>>,
}
impl RepoState {
    pub fn from_snapshot(s: PersistedRepoSnapshot) -> Self;   // rebuilds descendants via build_descendants_index
    pub fn to_snapshot(&self, log_generation: u64, log_position: u64, next_seq: u64, snapshot_at: DateTime<Utc>) -> PersistedRepoSnapshot;
    /// THE single mutation entry point. Every state change is an appended event then this.
    pub fn apply_event(&mut self, event: &StateEvent);        // total: unknown PRs/trains tolerated (log + skip)
}
```

`apply_event` semantics that matter: `TrainStarted` creates `TrainRecord`
(using `event.ts`); `PhaseTransition` updates
`cascade_phase`/`current_pr`/`predecessor_pr`/`last_squash_parent_sha`;
`SquashCommitted` marks the PR merged (seam c); `ReconciliationRecorded` sets
the marker; `DoneRetarget` sets `base_ref = new_base`; `PredecessorDeclared`
updates `predecessor` + descendants index; `TrainStopped/Aborted/Completed`
end trains.

**Correctness oracle (property tests):**
- *Index coherence + totality*: for arbitrary snapshots and arbitrary event
  sequences, `apply_event` never panics and
  `state.descendants == build_descendants_index(&state.prs)` after every step.
- *Compaction equivalence*: for any events `es` and cut `k`:
  `from_snapshot(applyₖ.to_snapshot(...))` then `apply(es[k..])` ==
  `apply_all(es)`. (This is the oracle M5/M6 recovery rests on.)
- *is_root by construction* (seam c): after `SquashCommitted{pr: P, sha: s}`,
  `ReconciliationRecorded{pr: D, squash_sha: s}`,
  `DoneRetarget{pr: D, new_base: default}` ⇒ `is_root(D)`; with any `s' != s`
  ⇒ `!is_root(D)`.
- *Dedupe bridge*: `DedupeKey::for_event` output equals the legacy
  constructor output for every event shape (then delete `WebhookEventData`).
- Serde roundtrips for all new/changed variants.

**Crash boundary:** none introduced — pure. The contract this stage creates
("append then apply, never mutate otherwise") is what makes every later crash
analysis tractable.

---

## Stage M2 — Cascade engine (pure planner)

> **Amendments (2026-07-01, applied during implementation).** This section was
> written before the SQLite migration; the Store's event-sourcing model
> invalidates some signature details below. The binding deviations:
>
> 1. **`StepPlan` has no `train` field.** `Store::append → apply_event` is the
>    single mutation path; a returned "updated `TrainRecord` copy" would be a
>    second source of truth that diverges from replay. Every train mutation the
>    engine wants rides in `StepPlan.events`. Consequently `apply_event`
>    derives `recovery_seq` deterministically (one bump per train-mutating
>    event), so replayed state agrees with the live record and with the status
>    comment M6 compares against.
> 2. **The prepare-pin is made durable.** `TrainRecord.predecessor_head_sha`
>    appeared in no event, so a crash in the prep→reconcile window lost the
>    force-push-race guard `reconcile_descendant` requires (the same class as
>    review finding #2 on `expected_sha`). `PhaseTransition` gains
>    `predecessor_head_sha` (carried from `SquashPending` entry onward) and
>    `IntentPushPrep` gains `predecessor_head` (durable from the first prep
>    push), both `#[serde(default)]`.
> 3. **`Effect::RecordReconciliation` is deleted** (seam c superseded). The
>    engine emits `StateEventPayload::ReconciliationRecorded` in the same
>    append batch as `DonePushReconcile` — atomic ordering instead of an
>    executor special case.
> 4. **Terminal-event-first, cleanup best-effort** (inverts model property 5).
>    Cleanup-before-terminal requires threading a continuation through the
>    executor between plans. Instead the terminal event lands first, worktree
>    cleanup/removal is a best-effort effect after it, and every cascade-step
>    entry defensively re-creates/cleans the worktree. A dirty worktree is
>    re-establishable state; the guarantee (clean worktree before the next
>    irreversible op) is preserved.
> 5. **`recover_train` = `advance(state, root, Evaluate { facts })`.** Recovery
>    and resume are one operation; `Observation::Evaluate` carries a
>    [`ReplayFacts`] intent/done ledger for the current phase (M5 derives it
>    from the events table). One planning path, verified once by property 4.
> 6. **Executor stays dumb.** `observe(&[(Effect, outcome)]) -> Observation`
>    is a pure M2 function mapping a plan's effect outcomes to the next
>    observation (incl. the preflight trio → one observation); `StepPlan`
>    separates observed `effects` from `best_effort` effects (status-comment
>    updates, cleanup) whose failures are logged, never fed back.
> 7. **New events** `train_parked`/`train_resumed` (WaitingCi durability),
>    `status_comment_posted` (comment id durability — replay would otherwise
>    post duplicate status comments), `pr_merge_state_changed` (mergeability
>    observations enter the cache through the log like everything else). New
>    compound `GitEffect`s beyond the list in seam (a): `UpdateRootForBehind`
>    (→ `git::merge::update_root_for_behind`, the DESIGN `BEHIND` flow),
>    `CleanupWorktree` (→ `git::recovery::cleanup_worktree_on_abort`). Push
>    points are captured *inside* the merge-effect responses (`Prepared` /
>    `MergedForPush` carry an optional `PushPoint` from
>    `git::push::capture_pre_push_state`) rather than by a separate effect:
>    the capture is only meaningful in the worktree-HEAD context of the merge,
>    and a stateless engine cannot thread the prepare-pin across an extra
>    observation round-trip.

**Dependencies:** M1. **Implements:** DESIGN.md §Operation sequence,
§Descendant set freezing, §Durability and commit points, §Expected state
tracking, §Pause Conditions, §Fan-out handling.

**Files:** create `src/cascade/mod.rs`, `src/cascade/engine.rs`,
`src/cascade/plan.rs`, `src/cascade/recovery.rs`, `src/cascade/model_tests.rs`
(test-only model world).

**Key signatures:**

```rust
pub enum Observation {
    Evaluate,                                                  // start/resume/webhook-triggered re-eval
    Prepared { descendant: PrNumber, merge: MergeOutcome, predecessor_head: Sha },
    Squashed { sha: Sha },
    SquashFailed { kind: SquashFailureKind },                  // transient vs permanent per DESIGN §Pause Conditions
    SquashParentResolved { parent: Sha },
    Reconciled { descendant: PrNumber, merge: MergeOutcome },
    CaughtUp { descendant: PrNumber, merge: MergeOutcome },
    PushPointCaptured { branch: String, pre_push_sha: Sha, expected_tree: Sha },
    Pushed { branch: String, outcome: PushOutcome },
    Retargeted { pr: PrNumber },
    PrRefreshed { data: PrData },
    EffectFailed { error: EffectError },                       // classified transient/permanent
}

pub enum Control { Continue, Park /* WaitingCi */, Done, FanOut { new_trains: Vec<TrainRecord> } }

pub struct StepPlan {
    pub events: Vec<StateEventPayload>,   // appended+applied (fsync per is_critical) BEFORE effects run
    pub train: TrainRecord,               // updated copy (recovery_seq bumped)
    pub effects: Vec<Effect>,
    pub control: Control,
}

pub fn start_train(state: &RepoState, root: PrNumber, now: DateTime<Utc>) -> Result<StepPlan, CascadeError>;
pub fn stop_train(state: &RepoState, pr: PrNumber, force: bool, now: DateTime<Utc>) -> Result<StepPlan, CascadeError>;
pub fn advance(state: &RepoState, train: &TrainRecord, obs: Observation, now: DateTime<Utc>) -> Result<StepPlan, CascadeError>;

// src/cascade/recovery.rs
pub struct ReplayFacts { pub unmatched_intents: Vec<StateEventPayload>, /* from RecoveredState.events scan */ }
pub fn recover_train(state: &RepoState, train: &TrainRecord, facts: &ReplayFacts, now: DateTime<Utc>) -> Result<StepPlan, CascadeError>;
```

**Composes:** `state::transitions::{next_phase, start_preparing}` (note:
trivially-complete phases auto-settle — the engine derives work from
`progress.remaining()` of whatever phase it lands in and never assumes
single-step transitions; the only un-jumpable gate is `SquashPending`),
`state::descendants::collect_direct_descendants` (open-only traversal for the
freeze), `state::validation`, `types::CascadePhase/PhaseOutcome`,
`status::format_status_comment` (a `StatusCommentTooLarge` error ⇒ abort the
train), `preflight::*` for `start_train` (emit
`GetRepoSettings`/`GetBranchProtection`/`GetRulesets` effects, consume via
`DismissStaleApprovalsCheckInput::from_responses`), `commands` types.

Per-phase sequence per descendant: `PrepareDescendant` → capture push point
(`RevParse origin/<br>`, `RevParse HEAD^{tree}`) → `IntentPushPrep` event →
`Push` → `DonePushPrep`; squash: `IntentSquash` →
`SquashMerge{expected_sha}` → `SquashCommitted` → parent capture →
`PhaseTransition(Reconciling, last_squash_parent)`; reconcile:
`MergeReconcile` (merges only) → push bracket → `DonePushReconcile` +
`Effect::RecordReconciliation`; catch-up and retarget likewise
(`IntentRetarget`/`DoneRetarget{new_base}`). Fan-out: when >1 frozen
descendant finishes `Retargeting`, emit `FanOutCompleted` + new
`TrainRecord`s via `Control::FanOut`.

**Correctness oracle (model-based property tests, the heart of the stage):**
a test-only `ModelWorld` (in-memory PR table + ancestry DAG honoring
interpreter semantics: SquashMerge succeeds iff `expected_sha` matches; push
rejects on foreign advance; MergeReconcile refuses without ancestry proof)
and a driver loop `advance ∘ execute_model`. Properties:
1. **Phase legality**: every consecutive phase pair passes
   `verify_transition_invariants` and `can_transition_to`; phases never
   regress.
2. **Intent/done bracketing**: every irreversible effect is preceded by its
   matching `Intent*` event, and its `Done*`/`SquashCommitted` lands before
   the next `PhaseTransition`.
3. **Frozen set invariant**: `frozen_descendants` is byte-identical across
   all phases of one step and equals the open descendants at `Preparing`
   entry; PRs added to `RepoState` mid-step receive no effects that step.
4. **Crash-resume equivalence** (the big one): record the full
   `(events, world)` trace of an uninterrupted run; for *every* prefix cut,
   rebuild `RepoState` by replaying `events[..k]`, run `recover_train` +
   driver against the world-at-cut; the final world equals the uninterrupted
   run, and the model observes ≤1 accepted `SquashMerge` per PR.
5. **Stop safety**: injecting `stop_train` at any observation point yields
   `TrainStopped` and no subsequent irreversible effects; worktree-cleanup
   effects are emitted before the stopped/aborted state event.
6. **Reconciliation marker**: each non-skipped descendant gets exactly one
   `RecordReconciliation` whose `squash_sha` equals the phase's `squash_sha`.
7. **Fan-out**: new train roots = frozen − skipped, each `Running`/`Idle`.

**Crash-boundary statement:** the engine *defines* the crash contract —
intent before irreversible op, done after; `SquashCommitted` durable before
any reconcile effect; crash between squash API success and `SquashCommitted`
fsync is recovered by `recover_train` seeing `IntentSquash` without
`SquashCommitted` and planning `GetPr` (if merged: adopt `merge_commit_sha`
after `ValidateSquashCommit`; if not: re-plan squash — safe because the
original attempt provably didn't land, `expected_sha` still guards). Push
crash windows recover via `CheckPushCompleted` (tree + parent-chain check per
DESIGN §Git push idempotency).

---

## Stage M3 — Pure event handlers

**Dependencies:** M1 (parallel with M2; references engine only as data via
`Trigger`). **Implements:** DESIGN.md §Event Handling table, §Incremental
updates, comment edit/delete rules.

**Files:** create `src/webhooks/handlers.rs` plus
`src/webhooks/handlers/{issue_comment,pull_request,check_suite,status,review}.rs`.

**Key signatures:**

```rust
pub struct HandlerCtx { pub bot_user_id: u64, pub bot_name: String, pub now: DateTime<Utc> }
pub enum Trigger {                       // worker feeds these to the engine (M2) — handlers never run cascades
    StartTrain { pr: PrNumber },
    StopTrain { pr: PrNumber, force: bool },
    EvaluateTrain { root: PrNumber },    // CI/review/merge events that may unblock a parked train
    LateAddition { pr: PrNumber, merged_predecessor: PrNumber },   // detection only
}
pub struct HandlerOutput { pub events: Vec<StateEventPayload>, pub effects: Vec<Effect>, pub triggers: Vec<Trigger> }
pub fn handle_event(event: &GitHubEvent, state: &RepoState, ctx: &HandlerCtx) -> Result<HandlerOutput, HandlerError>;
```

**Composes:** `commands::parse_command`,
`state::validation::{validate_predecessor_declaration,
validate_predecessor_update, validate_base_branch_matches_predecessor}`,
`state::topology::detect_cycle`; dedupe check is the worker's job (handlers
assume deduped input). Effects emitted: ack reactions (`AddReaction`),
rejection comments (`PostComment`), nothing irreversible to trains.
`pull_request_review.dismissed` on an active train ⇒ `TrainAborted` event;
CI-failure-shaped events on a parked train ⇒ `EvaluateTrain`.

**Correctness oracle:** the unit table from IMPLEMENTATION_PLAN Stage 15.
Properties: handlers are pure and deterministic; every emitted event applies
cleanly via M1 `apply_event`; stop on existing train *always* contains
`TrainStopped`; multiple-predecessor rule (first declaration authoritative).

**Crash boundary:** none — pure.

---

## Stage M4 — Git effect interpreter (shell bridge)

**Dependencies:** M1 (parallel with M2/M3). **Implements:** DESIGN.md §Local
git workflow, interpreter validation requirements documented on
`GitEffect::MergeReconcile`.

**Files:** create `src/git/interpreter.rs`; extend
`src/git/property_tests.rs`.

**Key signatures:**

```rust
pub struct WorktreeGitInterpreter { pub worktree: PathBuf, pub identity: CommitIdentity, pub default_branch: String }
impl WorktreeGitInterpreter {
    pub async fn interpret(&self, effect: GitEffect) -> Result<GitResponse, GitError>;  // every arm: tokio::task::spawn_blocking
}
```

Mapping: `PrepareDescendant` → `prepare_descendant` ⇒
`GitResponse::Prepared{..}`; `MergeReconcile` → `reconcile_descendant` ⇒
`GitResponse::Merge(..)`; `CatchUpDescendant` → `catch_up_descendant`;
`Push` → `push_head_to_branch` ⇒ `GitResponse::Push(..)`;
`CheckPushCompleted` → `is_push_completed` ⇒ `Bool`; `ValidateSquashCommit` →
`is_valid_squash_merge`; the small ops map to `src/git/mod.rs` helpers.
Conflicts/rejections are `Ok(domain outcome)`; only uncharacterizable git
failures are `Err` (seam a).

**Correctness oracle:** reuse the real-git property harness
(`src/git/property_tests.rs` generators): for generated stacks, driving the
*effect vocabulary* (Prepare → squash-simulated-on-bare-remote →
MergeReconcile → CatchUp → Push) preserves the content-preservation,
intervening-main-preservation, and $SQUASH_SHA^-ordering properties — i.e.,
the compound effects are faithful to the functions already proven. Plus
targeted tests: a real conflicting merge ⇒ `Ok(Merge(Conflict{files}))`;
non-fast-forward ⇒ `Ok(Push(Rejected))`; `CheckPushCompleted` true after
re-running an identical push (idempotency oracle); single-threaded tokio
runtime does not deadlock (spawn_blocking bridging).

**Crash boundary:** the interpreter itself is stateless; a crash mid-git-op
leaves a dirty worktree, which `git::recovery::cleanup_worktree_on_restart`
(already implemented) plus M2's recovery contract handle.

---

## Stage M5 — Per-repo serial worker + server wiring

**Dependencies:** M2, M3, M4. **Implements:** DESIGN.md §Per-repo serial
event processing worker loop, §Event processing flow, §Restart safety steps
1–4.

**Files:** create `src/worker/mod.rs`, `src/worker/dispatch.rs`,
`src/worker/queue.rs`, `src/worker/worker.rs`, `src/worker/executor.rs`;
modify `src/server/webhook.rs` (+`dispatcher.notify`), `src/server/mod.rs`
(`AppState` gains `Dispatcher` + GitHub/git config), `src/main.rs` (startup:
`cleanup_interrupted_processing` once per repo spool before serving, then
spawn dispatcher).

**Key signatures:**

```rust
pub enum WorkerMessage { SpoolNudge, Shutdown }
pub enum QueuedEventPayload { GitHub(GitHubEvent), /* reserved: PeriodicSync, PollActiveTrains */ }
pub struct QueuedEvent { pub priority: EventPriority, pub arrival: ArrivalMarker, pub delivery: SpooledDelivery, pub payload: QueuedEventPayload }

pub struct Dispatcher { /* Arc<Mutex<HashMap<RepoId, WorkerHandle>>> */ }
impl Dispatcher { pub fn notify(&self, repo: &RepoId); pub async fn shutdown(self); }

pub async fn run_worker(repo: RepoId, spool_dir: PathBuf, state_dir: PathBuf, deps: WorkerDeps, rx: mpsc::Receiver<WorkerMessage>) -> Result<(), WorkerFatal>;

// src/worker/executor.rs — turns plans into observations; the ONLY interpreter of Effect::RecordReconciliation (seam c)
pub async fn execute_plan(plan: StepPlan, persist: &mut Persist<'_>, git: &WorktreeGitInterpreter, github: &OctocrabClient) -> Result<Vec<Observation>, ExecError>;
pub struct Persist<'a> { pub state: &'a mut RepoState, pub log: &'a mut EventLog, pub pending_done: &'a mut Vec<SpooledDelivery> }
impl Persist<'_> { pub fn commit(&mut self, payload: StateEventPayload) -> Result<(), EventLogError>; }  // append → apply → fsync-if-critical
```

Per-delivery protocol (DESIGN steps 3–8): `mark_processing` (skip on
`AlreadyClaimed`) → `parse_webhook` → `DedupeKey::for_event`
check/`mark_seen` → `handle_event` → `commit` each event (batched fsync for
non-critical; flush batch before any irreversible effect) → run triggers
through the engine loop (`start_train`/`advance` ↔ `execute_plan` until
`Park`/`Done`) → `log.sync()` if batch pending → `mark_done` + spool dir
fsync. On `EventLogError::PoisonedLog`: worker enters `Failed` lifecycle —
stops claiming deliveries, leaves them spooled, surfaces via health endpoint;
fatal per repo. Unknown-PR events: emit single `GetPr` effect, insert into
cache (the miss-count re-bootstrap threshold is deferred).

**Correctness oracle:**
- Integration: spooled event picked up end-to-end; two same-repo deliveries
  processed in arrival order (assert via log `seq`); different repos progress
  concurrently; stop spooled after 10 normal events is processed first.
- **Crash-point property**: a test-only step-counter hook aborts the worker
  at each numbered point between claim → append → fsync → mark_done; restart
  (cleanup + re-drain) reprocesses pending deliveries; assert no delivery is
  lost, and duplicated *handling* is suppressed by dedupe keys / idempotent
  `apply_event` — final `RepoState` equals the no-crash run.
- Property: N concurrent `notify` calls ⇒ every delivery processed exactly
  once (claim atomicity of `mark_processing`).

**Crash boundaries:** (i) after claim, before any append — `.proc` without
`.done`, cleaned at startup, reprocessed; (ii) after critical append, before
`mark_done` — delivery replays; dedupe key (durable only via snapshot, so
possibly lost) plus idempotent `apply_event` and the engine's intent/done
recovery make the replay harmless; (iii) batched events — `.done` deferred
until batch fsync, so a crash replays the whole batch (same argument); (iv)
mid-effect — M2's intent/done contract (recovery happens in M6's startup
path; until M6 lands, restart-with-active-train is unsupported and the
worker refuses to resume non-Idle trains with a loud error, keeping this
stage shippable).

---

## Stage M6 — Bootstrap and recovery

**Dependencies:** M5 (+ M2 `recover_train`). **Implements:** DESIGN.md
§Bootstrap algorithm, §Handling cache misses, §Recovery precedence,
§Supplementary GitHub recovery, §Restart safety / Worktree cleanup on restart.

**Files:** create `src/bootstrap/mod.rs`, `src/bootstrap/disk.rs`,
`src/bootstrap/github.rs`, `src/bootstrap/recovery.rs`; modify `src/main.rs`
(GitHub auth config — `GITHUB_TOKEN`/app credentials — currently absent from
`Config`).

**Key signatures:**

```rust
pub async fn bootstrap_repo(repo: &RepoId, state_dir: &Path, github: &OctocrabClient, now: DateTime<Utc>)
    -> Result<(RepoState, EventLog, StateDirLock, ReplayFacts), BootstrapError>;
// disk fast path: persistence::recover() → RepoState::from_snapshot + apply_event over RecoveredState.events
//   → GetMergeState refresh per open PR (concurrency ~10) → collect ReplayFacts from unmatched intents
// fallback path (recover() hard error or stale snapshot): GitHub crawl via effects
//   (GetRepoSettings, ListOpenPrs, ListRecentlyMergedPrs, ListComments) + commands::parse_command for
//   predecessor declarations + status::parse_status_comment for TrainRecords (author_id == bot_user_id check)
//   → write fresh snapshot.

pub fn recovery_precedence(local: TrainRecord, remote: Option<TrainRecord>) -> TrainRecord;  // higher recovery_seq wins; tie → local
pub async fn recover_trains(state: &mut RepoState, facts: &ReplayFacts, deps: &WorkerDeps) -> Result<(), RecoveryError>;
// per active non-Idle train: cleanup_worktree_on_restart → supplementary GitHub recovery
// (fetch status comment, parse, recovery_precedence) → cascade::recover_train → engine loop
```

Worker integration: `run_worker` calls `bootstrap_repo` before its first
drain (lifecycle Unknown → Bootstrapping → Ready, with `Failed` + backoff on
error).

**Correctness oracle:**
- Property: `recovery_precedence` selects max `recovery_seq`, local on tie.
- Property (round-trip):
  `parse_status_comment(format_status_comment(t, msg)) == t` modulo
  `status_comment_id` — required for the GitHub fallback to be lossless.
- Integration: restart with valid snapshot ⇒ zero GitHub calls except
  merge-state refresh (counting mock); corrupted snapshot ⇒ `RecoverError` ⇒
  fallback crawl reconstructs `active_trains` from status comments; local
  `recovery_seq` behind comment ⇒ comment wins (the lost-fsync scenario).
- Integration (end-to-end): kill the process in `Preparing` /
  `SquashPending` / `Reconciling` against a real bare-remote fixture;
  restart; train completes; predecessor squashed exactly once.

**Crash boundaries:** bootstrap itself only writes a snapshot via
`save_snapshot_atomic` (already crash-safe); a crash mid-fallback-crawl
re-crawls (idempotent reads). The dangerous window — squash landed on GitHub
but `SquashCommitted` lost — is closed here by supplementary GitHub recovery
+ the `IntentSquash`-without-done path planning `GetPr`.

---

## Explicitly deferred (with ordering rationale)

- **Polling fallback (`PollActiveTrains`) and `PeriodicSync` timers +
  jitter**: pure liveness insurance for missed webhooks; correctness is
  carried by webhooks + restart recovery. `QueuedEventPayload` reserves the
  variants so this is additive after M6.
- **Late-addition reconciliation flow** (`handle_late_addition`): a distinct
  git flow with its own squash-vs-rebase validation. M3 *detects*
  (`Trigger::LateAddition`) and M5 responds with an explicit "not yet
  supported — rebase onto main or restart the train" comment, so the failure
  is loud, not silent. Implement after M6 as its own stage.
- **Stack-scoped `CancellationToken` / interrupting in-flight git ops**: stop
  is honored at every observation boundary via queue priority; staleness
  bound is one git operation. Token plumbing is additive inside M5's executor.
- **Auto-resume timer machinery / `waiting_for` condition records +
  timeouts**: M2 parks (`Control::Park`) and M3 re-triggers on
  `check_suite`/`status`/`review` events; the timer half rides in with
  polling.
- **`miss_count >= 5` re-bootstrap, state pruning wiring, compaction
  scheduling, status-export endpoint reading live worker state** (it
  currently serves the last on-disk snapshot — eventually consistent).
- **`stop --force` admin actions beyond stop + worktree cleanup** (Open
  Question 2). **Abort notifications on downstream PRs** (comment on the
  failing PR ships in M2; fanning comments to descendants is additive).

## Resolved design questions (owner-confirmed 2026-06-14)

1. **`GitEffect::MergeReconcile` contract change**: its `target_branch`
   ("branch to push the result to") field implies push-inside-the-effect, but
   `intent_push_reconcile` requires `expected_tree`, which exists only after
   the merge and must be fsynced *before* the push. The plan splits it
   (MergeReconcile = merges only; separate intent-bracketed `Push`), removing
   `target_branch`. Nothing persisted serializes effects today, so the
   wire-format change should be free — confirm.
   **RESOLVED: split confirmed.** `MergeReconcile` becomes merges-only, drops
   `target_branch`, gains `predecessor_pr: PrNumber`; the push is a separate
   intent-bracketed `Push` effect.
2. **`Command::StopForce` v1 scope**: which admin actions beyond stop +
   worktree cleanup — close/edit the status comment? comment on descendants?
   close PRs? Proposed v1: stop + final status-comment update only.
   **RESOLVED: v1 = stop + worktree cleanup + a single final status-comment
   update only.** No descendant comments, no PR closing.
3. **Bot identity sourcing**: `bot_user_id` (status-comment author
   verification during GitHub recovery) and `bot_name` (command parsing):
   fetch via `GET /user` at startup, or require env config? Proposed: fetch
   at startup, fail fast.
   **RESOLVED: fetch via `GET /user` at startup, fail fast.** No env/config
   sourcing in v1.
