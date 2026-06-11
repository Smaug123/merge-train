# Merge Train Bot - Incremental Implementation Plan

## Overview

Implement DESIGN.md in 19 incremental, reviewable stages. Each stage has an explicit correctness oracle (property-based tests, unit tests, or integration tests) that verifies completion. Recovery mechanisms are built alongside each feature.

**Key architectural decisions:**
- Effects-as-data pattern for GitHub API (pure core, imperative shell)
- Fine-grained stages for reviewability
- Recovery built alongside each feature, not retrofitted

---

## Stage 1: Core Domain Types (complete)

**Goal:** Foundation types that encode invariants via the type system.

**Files to create:**
- `src/lib.rs` - Library root
- `src/types/mod.rs` - Type module re-exports
- `src/types/ids.rs` - Newtype wrappers: `PrNumber`, `Sha`, `RepoId`, `DeliveryId`, `CommentId`
- `src/types/pr.rs` - `CachedPr`, `PrState`, `MergeStateStatus`
- `src/types/train.rs` - `TrainRecord`, `TrainState`, `CascadePhase`
- `src/types/stack.rs` - `CascadeStepOutcome`, `AbortReason`, `BlockReason`

**Key invariants to encode:**
- `CascadePhase` variants carry `frozen_descendants` through all phases (not re-queryable)
- `PrState::Merged` requires `merge_commit_sha: Sha` (not Option)
- `TrainRecord` distinguishes `original_root_pr` (stable) from `current_pr` (advances)
- Phase transitions are a state machine (enforced by types)

**Property tests:**
- Serde roundtrip for all types
- `CascadePhase` transition validity (e.g., Preparing -> SquashPending valid, Preparing -> Retargeting invalid)
- Newtype comparisons match underlying type

**Deliverable:** Pure types with comprehensive property tests. No I/O.

---

## Stage 2: Pure State Logic (Functional Core) (complete)

**Goal:** Stack topology and state transitions as pure functions.

**Files to create:**
- `src/state/mod.rs`
- `src/state/topology.rs` - `is_root()`, `compute_stacks()`, `detect_cycle()`
- `src/state/descendants.rs` - `build_descendants_index()`, `remaining_descendants()`
- `src/state/transitions.rs` - `next_phase()`, phase transition validation
- `src/state/validation.rs` - `validate_predecessor_declaration()`

**Pure functions:**
```rust
fn is_root(pr: &CachedPr, default_branch: &str) -> bool
fn compute_stacks(prs: &HashMap<PrNumber, CachedPr>, default_branch: &str) -> Vec<MergeStack>
fn detect_cycle(prs: &HashMap<PrNumber, CachedPr>) -> Option<Vec<PrNumber>>
fn build_descendants_index(prs: &HashMap<PrNumber, CachedPr>) -> HashMap<PrNumber, HashSet<PrNumber>>
fn remaining_descendants(phase: &CascadePhase) -> Vec<PrNumber>  // frozen - completed - skipped
fn next_phase(current: &CascadePhase, outcome: PhaseOutcome) -> CascadePhase
```

**Property tests:**
- `is_root` correct iff targets default branch AND (no predecessor OR predecessor merged)
- `compute_stacks` produces connected components with valid roots
- `detect_cycle` finds all cycles in generated graphs
- Descendant index is inverse of predecessor relationships
- `remaining_descendants` never includes late additions (frozen invariant)
- Phase transitions maintain invariants (frozen_descendants unchanged, skipped preserved, completed grows within each phase but resets at phase boundaries)

**Deliverable:** Pure state logic with property tests proving correctness.

---

## Stage 3: Command Parser (complete)

**Goal:** Parse `@merge-train` commands from issue comments.

**Files to create:**
- `src/commands/mod.rs`
- `src/commands/parser.rs` - `parse_command()`
- `src/commands/types.rs` - `Command` enum

**Types:**
```rust
enum Command {
    Predecessor(PrNumber),
    Start,
    Stop,
    StopForce,
}
```

**Property tests:**
- Valid command strings parse correctly
- Invalid strings return None (not panic)
- Whitespace/case variations handled
- Multiple commands: only first recognized
- Edge cases: `#` followed by non-number, `u64::MAX`

**Deliverable:** Robust command parser with property tests.

---

## Stage 4: Effects-as-Data for GitHub API (complete)

**Goal:** Define effect types that describe GitHub operations without executing them.

**Files to create:**
- `src/effects/mod.rs`
- `src/effects/github.rs` - `GitHubEffect` enum
- `src/effects/git.rs` - `GitEffect` enum
- `src/effects/interpreter.rs` - Effect interpreter traits

**Effect types:**
```rust
enum GitHubEffect {
    GetPr { repo: RepoId, pr: PrNumber },
    ListOpenPrs { repo: RepoId },
    SquashMerge { repo: RepoId, pr: PrNumber, expected_sha: Sha },
    RetargetPr { repo: RepoId, pr: PrNumber, new_base: String },
    PostComment { repo: RepoId, pr: PrNumber, body: String },
    UpdateComment { repo: RepoId, comment_id: CommentId, body: String },
    AddReaction { repo: RepoId, comment_id: CommentId, reaction: String },
    GetBranchProtection { repo: RepoId, branch: String },
    GetRepoSettings { repo: RepoId },
}

enum GitEffect {
    Fetch { remote: String },
    Merge { branch: String, strategy: MergeStrategy },
    Push { branch: String },
    IsAncestor { potential_ancestor: Sha, descendant: Sha },
    CreateWorktree { name: String },
    RemoveWorktree { name: String },
}
```

**Property tests:**
- Effect equality and hashing (for deduplication)
- Serde roundtrip

**Deliverable:** Effect type definitions. Interpreters come later.

---

## Stage 5: Persistence Layer (Event Log) (complete)

**Goal:** Crash-safe event logging with replay.

**Files to create:**
- `src/persistence/mod.rs`
- `src/persistence/event.rs` - `StateEvent`, `StateEventPayload`
- `src/persistence/log.rs` - `EventLog` (append + replay)
- `src/persistence/fsync.rs` - `fsync_file()`, `fsync_dir()`

**Key operations:**
```rust
fn append_event(log: &mut File, event: &StateEvent, critical: bool) -> io::Result<()>
fn replay_events(path: &Path, from_offset: u64) -> Result<Vec<StateEvent>>
```

**Property tests (with tempfile):**
- Write N events, replay yields N events
- Partial line at end of file is handled (truncated write)
- Critical events trigger fsync
- Non-critical events don't fsync (performance)
- Crash simulation: truncate at random byte, replay recovers valid prefix

**Deliverable:** Crash-safe event log.

---

## Stage 6: Persistence Layer (Snapshots + Compaction) (complete)

**Goal:** Snapshot persistence with generation-based compaction.

**Files to create:**
- `src/persistence/snapshot.rs` - `PersistedRepoSnapshot`, read/write
- `src/persistence/generation.rs` - Generation file management
- `src/persistence/compaction.rs` - `compact()`
- `src/persistence/pruning.rs` - State pruning logic

**Key operations:**
```rust
fn save_snapshot_atomic(path: &Path, snapshot: &PersistedRepoSnapshot) -> Result<()>
fn load_snapshot(path: &Path) -> Result<PersistedRepoSnapshot>
fn compact(state_dir: &Path, current_state: &RepoState) -> Result<()>
fn prune_snapshot(snapshot: &mut PersistedRepoSnapshot, retention_days: u32)
```

**Property tests:**
- Snapshot roundtrip preserves all data
- Compaction preserves active trains
- Compaction + crash at any point -> recovery valid
- Pruning respects retention policy
- Generation file is atomic

**Deliverable:** Full persistence layer with recovery.

---

## Stage 7: Webhook Spool (complete)

**Goal:** Durable webhook delivery spooling with deduplication.

**Files to create:**
- `src/spool/mod.rs`
- `src/spool/delivery.rs` - `SpooledDelivery`, marker files
- `src/spool/dedupe.rs` - Deduplication keys
- `src/spool/drain.rs` - `drain_pending()`

**Key operations:**
```rust
fn spool_delivery(dir: &Path, delivery_id: &str, payload: &[u8]) -> Result<()>
fn mark_processing(delivery: &SpooledDelivery) -> Result<()>
fn mark_done(delivery: &SpooledDelivery) -> Result<()>
fn drain_pending(dir: &Path) -> Result<Vec<SpooledDelivery>>
```

**Property tests:**
- Delivery survives crash at any point in write sequence
- Duplicate delivery IDs are rejected
- Drain returns all pending in deterministic order
- Processing/done markers are idempotent

**Deliverable:** Crash-safe webhook spooling.

---

## Stage 8: Git Operations (complete)

**Goal:** Local git operations for cascade (prepare, reconcile, catch-up).

**Files to create:**
- `src/git/mod.rs`
- `src/git/worktree.rs` - Worktree management
- `src/git/merge.rs` - `prepare_descendant()`, `reconcile_descendant()`, `catch_up_descendant()`
- `src/git/push.rs` - Push operations
- `src/git/recovery.rs` - Worktree cleanup, orphan detection

**Key operations:**
```rust
fn worktree_for_stack(base_dir: &Path, root_pr: PrNumber) -> Result<PathBuf>
fn remove_worktree(base_dir: &Path, root_pr: PrNumber) -> Result<()>
fn prepare_descendant(worktree: &Path, predecessor_head: &Sha) -> Result<MergeResult>
fn reconcile_descendant(worktree: &Path, squash_sha: &Sha) -> Result<MergeResult>
fn catch_up_descendant(worktree: &Path, default_branch: &str) -> Result<MergeResult>
fn is_ancestor(worktree: &Path, potential_ancestor: &Sha, descendant: &Sha) -> Result<bool>
```

**⚠️ Critical details from DESIGN.md:**
- **Detached HEAD mode**: All worktree operations must use `git checkout --detach` to avoid git's restriction that a branch can only be checked out in one worktree. Push with `HEAD:refs/heads/<branch>`.
- **Worktree cleanup on abort**: Must run `git merge --abort`, `git reset --hard`, `git clean -fd` before transitioning to aborted state, otherwise subsequent checkouts fail with "you need to resolve your current index first".
- **Worktree cleanup on restart**: Detect corrupted worktrees (failed reset), delete and recreate if necessary.
- **Stale worktree cleanup**: On startup, remove worktrees older than 24 hours that aren't associated with active trains.
- **Stop command**: Removes the stack's worktree immediately via `remove_worktree`.

**Property tests (with real git + tempfile):**
- Descendant content preserved after cascade (DESIGN.md Property 1)
- Intervening main commits preserved (Property 2)
- Squash parent ordering prevents lost commits (Property 3)
- Recovery uses frozen descendants, not current index (Property 9)
- Worktree cleanup on abort leaves no orphans

**Deliverable:** Git operations with property tests using real git.

---

## Stage 9: GitHub Effect Interpreter (complete)

**Goal:** Execute GitHub effects via octocrab.

**Files to create:**
- `src/github/mod.rs`
- `src/github/client.rs` - `OctocrabClient`
- `src/github/interpreter.rs` - `interpret_github_effect()`
- `src/github/retry.rs` - Exponential backoff

**Key operations:**
```rust
async fn interpret_github_effect(client: &Octocrab, effect: GitHubEffect) -> Result<GitHubResponse>
```

**⚠️ Critical details from DESIGN.md:**
- **SHA guard on squash-merge**: The `SquashMerge` effect MUST pass `expected_sha` to GitHub's merge endpoint. This prevents race conditions where someone pushes unreviewed commits between readiness check and merge. Handle HTTP 409 (SHA mismatch) by re-evaluating readiness.
- **Transient vs permanent failures**: Distinguish retriable errors (5xx, rate limits, "Required status check is expected") from permanent failures (4xx, "Pull request is not mergeable"). Retry transient up to 3 times with exponential backoff.
- **GraphQL for mergeStateStatus**: Use GraphQL API to query `mergeStateStatus` field, which encapsulates all branch protection logic.

**Tests:**
- Unit tests for retry logic
- Integration tests (if GitHub test repo available)
- Property: retry respects exponential backoff bounds

**Deliverable:** GitHub API client as effect interpreter.

---

## Stage 10: Cascade Engine (NOT IMPLEMENTED — previously mislabelled "complete")

> **Status correction (2026-06-10):** none of the files below exist. The phase
> transition state machine landed in `src/state/transitions.rs` (Stage 2), and
> the git-side merge mechanics landed in `src/git/` (Stage 8), but the engine
> that composes them — `start_train`/`stop_train`/`execute_cascade_step`/
> `recover_train` returning effects — was never written. Nothing in production
> code sets `predecessor_squash_reconciled` or interprets
> `Effect::RecordReconciliation`, so a cascade cannot currently proceed past
> its first PR.

**Goal:** The core cascade state machine.

**Files to create:**
- `src/cascade/mod.rs`
- `src/cascade/engine.rs` - `CascadeEngine`
- `src/cascade/step.rs` - `execute_cascade_step()`
- `src/cascade/phases.rs` - Phase-specific logic
- `src/cascade/recovery.rs` - Recovery from partial state

**Key operations:**
```rust
fn start_train(state: &mut RepoState, root_pr: PrNumber) -> Result<Vec<Effect>>
fn stop_train(state: &mut RepoState, root_pr: PrNumber) -> Result<Vec<Effect>>
fn execute_cascade_step(state: &mut RepoState, train: &mut TrainRecord) -> Result<(PhaseResult, Vec<Effect>)>
fn recover_train(state: &mut RepoState, train: &TrainRecord) -> Result<Vec<Effect>>
```

**Property tests:**
- Cascade step never skips a phase
- Recovery from any crash point produces valid state
- Frozen descendants invariant maintained
- Late additions don't corrupt ongoing cascades
- Fan-out creates independent trains correctly

**Deliverable:** Complete cascade engine with recovery.

---

## Stage 11: Status Comment Serialization (complete)

**Dependencies:** Stage 1 (Types), Stage 10 (Cascade Engine references TrainRecord)

**Implements:** DESIGN.md §"Status comments (required, non-authoritative for normal operation)"

Status comments are the bot's backup recovery mechanism when local state is lost. They embed machine-readable JSON in an HTML comment, alongside human-readable status. This stage implements the serialization layer only; posting/updating comments via GitHub API is handled by the effect interpreter (Stage 9).

**Files to create:**
- `src/status/mod.rs`
- `src/status/format.rs` - `format_status_comment()`, `StatusCommentBody`
- `src/status/parse.rs` - `parse_status_comment()`, extraction from comment body

**Key operations:**
```rust
fn format_status_comment(train: &TrainRecord, human_message: &str) -> String
fn parse_status_comment(body: &str) -> Result<TrainRecord>
fn truncate_for_size_limit(train: &TrainRecord) -> TrainRecord  // Truncate error.message, etc.
```

**Correctness oracle:**
- Property: `parse(format(train, msg)) == Ok(train)` for all valid TrainRecords
- Property: `format(train, msg).len() < 65536` for trains up to 50 PRs (the configured limit)
- Property: Truncation preserves all fields except error.message/error.stderr
- Unit test: Known example from DESIGN.md parses correctly

---

## Stage 12: Preflight Checks (complete)

**Dependencies:** Stage 9 (GitHub Interpreter for API calls)

**Implements:** DESIGN.md §"Dismiss stale approvals preflight check", §"Merge method preflight check"

Preflight checks run before starting a train. They query repository settings and reject incompatible configurations with clear error messages. Both checks are independent and can fail independently.

**Files to create:**
- `src/preflight/mod.rs`
- `src/preflight/merge_method.rs` - `check_squash_only()`
- `src/preflight/branch_protection.rs` - `check_dismiss_stale_approvals()`

**Key operations:**
```rust
async fn check_squash_only(github: &GitHubClient, repo: &RepoId) -> Result<(), PreflightError>
async fn check_dismiss_stale_approvals(github: &GitHubClient, repo: &RepoId) -> Result<(), PreflightWarning>
fn format_preflight_error(error: &PreflightError) -> String  // User-facing message
```

**Correctness oracle:**
- Unit test: `check_squash_only` returns error when `allow_merge_commit: true`
- Unit test: `check_squash_only` returns error when `allow_rebase_merge: true`
- Unit test: `check_squash_only` succeeds when only `allow_squash_merge: true`
- Unit test: `check_dismiss_stale_approvals` returns warning when setting enabled
- Unit test: `check_dismiss_stale_approvals` also checks rulesets
- Property: Error messages contain actionable guidance (mention settings path)

---

## Stage 13: Webhook Signature Verification (complete)

**Dependencies:** None (pure cryptographic function)

**Implements:** DESIGN.md §"Per-repo serial event processing" (signature validation step)

GitHub signs webhook payloads with HMAC-SHA256. This stage implements verification against a shared secret. Signature verification is the first step in webhook processing; invalid signatures are rejected before parsing.

**Files to create:**
- `src/webhooks/mod.rs`
- `src/webhooks/signature.rs` - `verify_signature()`

**Key operations:**
```rust
fn verify_signature(payload: &[u8], signature_header: &str, secret: &[u8]) -> bool
fn parse_signature_header(header: &str) -> Option<Vec<u8>>  // "sha256=..." -> bytes
```

**Correctness oracle:**
- Unit test: Known test vector from GitHub documentation
- Property: `verify(payload, sign(payload, secret), secret) == true`
- Property: `verify(payload, sign(payload, wrong_secret), secret) == false`
- Property: `verify(modified_payload, sign(original, secret), secret) == false`
- Unit test: Malformed signature header returns `false` (not panic)

---

## Stage 14: Webhook Payload Parsing (complete)

**Dependencies:** Stage 1 (Types), Stage 13 (Signature verification comes first in pipeline)

**Implements:** DESIGN.md §"Event Handling" (event type table)

Parse raw webhook JSON into typed events. Each GitHub event type has distinct payload structure. This stage also implements priority classification (stop commands get highest priority).

**Files to create:**
- `src/webhooks/parser.rs` - `parse_webhook()`
- `src/webhooks/events.rs` - `GitHubEvent` enum with all variants
- `src/webhooks/priority.rs` - `classify_priority()`

**Key types:**
```rust
enum GitHubEvent {
    IssueComment { pr: PrNumber, comment_id: CommentId, body: String, action: CommentAction },
    PullRequest { pr: PrNumber, action: PrAction, ... },
    CheckSuite { ... },
    PullRequestReview { ... },
    Status { ... },
}

enum EventPriority { High, Normal }  // High = stop commands
```

**Correctness oracle:**
- Property: All valid GitHub webhook payloads (generated) parse without error
- Property: `classify_priority(issue_comment with "@merge-train stop") == High`
- Property: `classify_priority(other events) == Normal`
- Unit test: Each event type from DESIGN.md table parses correctly
- Unit test: Unknown event types return `Ok(None)` (ignored, not error)

---

## Stage 15: Event Handlers (Pure)

**Dependencies:** Stage 2 (State logic), Stage 3 (Command parser), Stage 10 (Cascade engine), Stage 14 (Parsed events)

**Implements:** DESIGN.md §"Event Handling" (action column), §"Incremental updates"

Map parsed webhook events to state mutations and effect descriptions. Handlers are pure functions: they take current state and an event, and return state updates plus effects to execute. This enables testing without I/O.

**Files to create:**
- `src/webhooks/handlers.rs` - `handle_event()`
- `src/webhooks/handlers/issue_comment.rs` - predecessor, start, stop commands
- `src/webhooks/handlers/pull_request.rs` - opened, closed, edited, synchronize
- `src/webhooks/handlers/check_suite.rs` - CI completion
- `src/webhooks/handlers/review.rs` - approval, dismissal

**Key operations:**
```rust
fn handle_event(
    event: GitHubEvent,
    state: &RepoState,
) -> Result<(Vec<StateEvent>, Vec<Effect>)>
```

**Correctness oracle:**
- Unit test per event type from DESIGN.md §"Event Handling" table:
  - `issue_comment` + `predecessor #N` → `pr.predecessor = Some(N)`, effects include ack reaction
  - `issue_comment` + `start` → train created, cascade effects
  - `issue_comment` + `stop` → train stopped
  - `pull_request.closed` (merged) → `pr.state = Merged`
  - `pull_request_review.dismissed` → train aborted (not waiting_ci)
  - `check_suite.completed` → re-evaluate effects if SHA matches
- Property: Handlers never mutate state directly (return events only)
- Property: Stop commands always produce `train_stopped` event when train exists

---

## Stage 16: HTTP Server Skeleton (complete)

**Dependencies:** Stage 7 (Spool), Stage 13 (Signature verification)

**Implements:** DESIGN.md §"Per-repo serial event processing" (HTTP handler steps 1-6), §"State Export API"

The HTTP server accepts webhooks, validates signatures, spools them durably, and returns 202 Accepted. State inspection and health endpoints enable observability. This stage creates the server infrastructure; per-repo workers come next.

**Files to create:**
- `src/server/mod.rs`
- `src/server/routes.rs` - axum route definitions
- `src/server/webhook.rs` - webhook endpoint handler
- `src/server/state.rs` - state inspection endpoint
- `src/server/health.rs` - health check

**Endpoints:**
- `POST /webhook` - Validates signature, spools delivery, returns 202
- `GET /api/v1/repos/{owner}/{repo}/state` - Returns repo state JSON
- `GET /health` - Returns 200 if server is running

**Correctness oracle:**
- Integration test: Server starts and `/health` returns 200
- Integration test: Valid webhook → 202 Accepted, file appears in spool directory
- Integration test: Invalid signature → 401 Unauthorized, no file in spool
- Integration test: `/api/v1/repos/{owner}/{repo}/state` returns valid JSON
- Property: Webhook endpoint returns 202 before processing completes (async)

---

## Stage 17: Per-Repo Worker

**Dependencies:** Stage 5-6 (Persistence), Stage 7 (Spool), Stage 15 (Event handlers), Stage 16 (Server dispatches to workers)

**Implements:** DESIGN.md §"Per-repo serial event processing" (worker loop), §"Non-blocking polling", §"Polling fallback for active trains"

Each repository gets a dedicated worker that processes events serially. Workers drain the spool, apply handlers, persist state, and drive cascades. Multiple repos process concurrently; events within a repo are serialized.

**Files to create:**
- `src/worker/mod.rs`
- `src/worker/queue.rs` - Priority queue implementation
- `src/worker/worker.rs` - Per-repo event loop
- `src/worker/dispatch.rs` - Route events to workers, create workers on demand

**Key operations:**
```rust
async fn run_worker(repo: RepoId, spool: SpoolDir, state_dir: PathBuf) -> Result<()>
fn drain_spool(spool: &SpoolDir) -> Result<Vec<SpooledDelivery>>
async fn process_event(event: GitHubEvent, state: &mut RepoState) -> Result<()>
```

**Correctness oracle:**
- Integration test: Spooled event is picked up and processed by worker
- Integration test: Priority queue orders stop commands before other events
- Integration test: Two events for same repo process serially (not concurrently)
- Integration test: Events for different repos process concurrently
- Property: Worker marks delivery `.done` only after state is persisted

---

## Stage 18: Bootstrap and Recovery

**Dependencies:** Stage 5-6 (Persistence), Stage 9 (GitHub client), Stage 11 (Status comment parsing), Stage 17 (Workers)

**Implements:** DESIGN.md §"Bootstrap algorithm", §"Recovery precedence", §"Supplementary GitHub recovery", §"Worktree cleanup on restart"

On startup, the bot loads state from disk, replays the event log, and recovers any interrupted trains. If local state is missing, it falls back to GitHub-based recovery by scanning for status comments.

**Files to create:**
- `src/bootstrap/mod.rs`
- `src/bootstrap/disk.rs` - Load snapshot + replay event log
- `src/bootstrap/github.rs` - Scan PRs and status comments for recovery
- `src/bootstrap/recovery.rs` - Resume interrupted trains

**Key operations:**
```rust
async fn bootstrap_repo(repo: RepoId, state_dir: &Path, github: &GitHubClient) -> Result<RepoState>
async fn recover_trains(state: &mut RepoState, github: &GitHubClient) -> Result<()>
fn recovery_precedence(local: &TrainRecord, github: &TrainRecord) -> &TrainRecord  // Compare recovery_seq
```

**Correctness oracle:**
- Integration test: Restart with valid snapshot → state loaded without GitHub API calls
- Integration test: Restart with corrupted snapshot → falls back to GitHub recovery
- Integration test: `recovery_seq` comparison selects more advanced record
- Property: `recovery_precedence(a, b)` returns record with higher `recovery_seq`
- Integration test: Interrupted train (Reconciling phase) resumes correctly after restart

---

## Stage 19: End-to-End Integration Tests

**Dependencies:** All previous stages

**Implements:** Verification of complete system behavior as described throughout DESIGN.md

This stage adds comprehensive integration tests that exercise the full system. Tests use real git operations (as in Stage 8) and a test harness that simulates GitHub API responses.

**Files to create:**
- `tests/integration/mod.rs`
- `tests/integration/cascade.rs` - Full cascade flows
- `tests/integration/recovery.rs` - Crash recovery scenarios
- `tests/integration/fan_out.rs` - Fan-out handling
- `tests/integration/commands.rs` - Start/stop command flows

**Correctness oracle (integration tests):**
- Linear cascade: `main <- #1 <- #2 <- #3` merges all PRs in order
- Fan-out: `main <- #1 <- {#2, #3}` creates independent trains after #1 merges
- Late addition: PR declared after predecessor merged is reconciled correctly
- Stop during cascade: `@merge-train stop` halts cascade, cleans up worktree
- Crash recovery per phase:
  - Crash in `Preparing` → resumes preparation for remaining descendants
  - Crash in `SquashPending` → checks if PR already merged, proceeds or retries
  - Crash in `Reconciling` → uses `last_squash_sha` to complete reconciliation
  - Crash in `Retargeting` → idempotent retarget API calls
- Review dismissal → aborts (not waiting_ci)
- Preflight rejection → clear error message, no train created

---

## Dependency Graph

```
Stage 1 (Types)
    │
    ├──────────────────────────────────────────────┐
    v                                              v
Stage 2 (Pure State) ◄─── Stage 3 (Commands)   Stage 11 (Status Comments)
    │                                              │
    v                                              │
Stage 4 (Effects)                                  │
    │                                              │
    ├─────────────────────┬────────────────────────┤
    v                     v                        │
Stage 5 (Event Log)   Stage 9 (GitHub Client)      │
    │                     │                        │
    v                     v                        │
Stage 6 (Snapshots)   Stage 12 (Preflight)         │
    │                                              │
    v                                              │
Stage 7 (Spool) ◄────────────────────────────┐     │
    │                                        │     │
    v                                        │     │
Stage 8 (Git Ops)                            │     │
    │                                        │     │
    v                                        │     │
Stage 10 (Cascade Engine)                    │     │
    │                                        │     │
    v                                        │     │
Stage 13 (Signature) ───► Stage 14 (Parsing) │     │
                              │              │     │
                              v              │     │
                         Stage 15 (Handlers) │     │
                              │              │     │
                              v              │     │
                         Stage 16 (HTTP) ────┘     │
                              │                    │
                              v                    │
                         Stage 17 (Worker)         │
                              │                    │
                              v                    v
                         Stage 18 (Bootstrap) ◄────┘
                              │
                              v
                         Stage 19 (E2E Tests)
```

Note: Stages 11-14 can be implemented in parallel since they have no dependencies on each other. Stage 12 (Preflight) can also proceed in parallel with Stages 13-14.

---

## Critical Files Summary

| Stage | Key Files |
|-------|-----------|
| 1 | `src/types/train.rs` (TrainRecord, CascadePhase) |
| 2 | `src/state/topology.rs` (is_root, compute_stacks) |
| 4 | `src/effects/github.rs` (GitHubEffect enum) |
| 5-6 | `src/persistence/log.rs`, `src/persistence/snapshot.rs` |
| 8 | `src/git/merge.rs` (prepare, reconcile, catch_up) |
| 10 | `src/cascade/engine.rs` (state machine driver) |
| 11 | `src/status/format.rs` (recovery data in comments) |
| 15 | `src/webhooks/handlers.rs` (event → state mutations) |
| 17 | `src/worker/worker.rs` (per-repo event loop) |
| 18 | `src/bootstrap/recovery.rs` (train recovery logic) |
