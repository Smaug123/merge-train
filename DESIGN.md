# Merge Train Bot Design

## Overview

A GitHub bot that orchestrates sequential squash-merging of stacked PRs into the repository's default branch (usually `main`). The bot maintains in-memory state per repository and persists it to a local on-disk state store so it can recover cleanly after restarts. GitHub is used as the command surface (comments/webhooks) and as the system of record for PRs, branches, and checks — but not as a persistence layer for the bot's own operational/structural state. It uses local git for merge operations (required for ours-strategy merges) and the GitHub API for everything else.

## Goals

- Enable stacked PR workflows with squash-merge and linear history on `main`
- Automate the tedious merge cascade after each PR lands
- Fail fast and loudly on any unexpected condition
- Support multiple independent trains in the same repo
- Be structured for easy migration to automatic triggering later

## Non-goals

- Conflict resolution (abort and notify human)
- Replacing CI (the bot waits for checks, doesn't run them)
- Managing the GPT-5 review bot (separate system)
- Cross-fork PRs (the bot only operates when the PR's head and base are in the same repository; fork-based PRs are ignored because the bot cannot push to arbitrary forks)
- GitHub merge queue compatibility (the bot assumes users are not using GitHub's built-in merge queue feature; running both systems simultaneously on the same repository would cause conflicts)
- Repos requiring approval on latest commit (the bot pushes new commits to PR branches during cascade operations; if branch protection requires re-approval after each push, the cascade cannot proceed automatically)

---

## User Interface

### Declaring stack membership

On a PR whose base branch is *not* `main`, comment:

```
@merge-train predecessor #123
```

This declares that the current PR is stacked on top of PR #123. The bot will:

1. Validate that #123 either targets `main` or itself has a predecessor declaration
2. Acknowledge with a reaction (👍) or error comment

A PR with a valid predecessor declaration is automatically part of any train rooted at its ancestor.

### Triggering the merge train

When the root PR (the one targeting `main`) is ready to merge, comment:

```
@merge-train start
```

The bot will:

1. Persist a "train started" record for this stack in the local state store
2. Walk the linked list to find all descendants (including any added after this command)
3. Validate the root PR is mergeable (approved, required checks passing — see below)
4. Squash-merge the root PR
5. Begin the cascade (persisting phase transitions to disk and optionally posting/updating a human-readable status comment)

**Definition of "ready to merge"**: The bot uses GitHub's `mergeStateStatus` field (via GraphQL) to determine readiness. A PR is ready when `mergeStateStatus` is `CLEAN` or `UNSTABLE`:

| Status | Meaning | Bot action |
|--------|---------|------------|
| `CLEAN` | All requirements satisfied | ✅ Proceed with merge |
| `UNSTABLE` | Non-required checks failing | ✅ Proceed with merge |
| `BLOCKED` | Required checks not passing or missing approvals | ⏳ Wait |
| `BEHIND` | Head branch behind base (strict mode) | ⏳ Wait (bot will update) |
| `DIRTY` | Merge conflicts | ❌ Abort cascade |
| `UNKNOWN` | State not yet computed | ⏳ Wait and re-check |

This delegates all branch protection logic to GitHub, ensuring the bot respects required status checks, required reviewers, and any other protection rules without duplicating that logic.

**Note on "check failure"**: Throughout this document, "check failure" or "required check failure" refers to `mergeStateStatus` transitioning to `BLOCKED`. The bot does not query branch protection rules to distinguish required from non-required checks — it relies entirely on GitHub's `mergeStateStatus` computation. This means:
- If a non-required check fails → `UNSTABLE` → bot proceeds
- If a required check fails → `BLOCKED` → bot waits/aborts
- If approval is withdrawn → `BLOCKED` → bot waits/aborts (same signal as check failure)

Once started, the cascade proceeds automatically through all descendants. New PRs that declare themselves as descendants mid-cascade will be picked up when the cascade reaches their predecessor.

### Stopping

```
@merge-train stop
```

Requests a halt of the cascade for the stack containing this PR. Due to inherent race conditions, the stop takes effect at the next opportunity — any in-flight git operation or API call may complete before the halt is observed. The bot will:

1. Cancel any in-flight git operations
2. Hard-reset the local git repo to the default branch (cleans up any partial merge state)
3. Persist the stack as `"state": "stopped"` in the local state store
4. Optionally post/update a status comment describing the current state
5. Take no further action on this stack until `@merge-train start` is issued again

The stop command is scoped to a single stack — other independent stacks in the same repo are unaffected.

### Aborting

The bot automatically aborts a cascade when it encounters an error (merge conflict, required check failure, etc.). This is distinct from a manual stop:

- **Abort**: Caused by an error condition. The bot posts diagnostics and waits for the condition to resolve. Some conditions (like required check failure) auto-resume when fixed.
- **Stop**: Explicit human request. The cascade will not resume until `@merge-train start` is issued again.

---

## Data Model

The stack structure is declared via PR comments (predecessor declarations) and persisted in the local state file:

```
main ← PR #123 ← PR #124 ← PR #125      (linear)
        (root)

main ← PR #123 ←┬─ PR #124              (fan-out)
        (root)  └─ PR #125
```

Each non-root PR has exactly one `@merge-train predecessor #N` comment. The root PR targets the default branch directly and has no predecessor comment.

Multiple PRs may declare the same predecessor (fan-out). Each non-root PR has exactly one predecessor, but a PR may have multiple descendants.

### Stack membership

A PR is "in a train" if:

- It has a `@merge-train predecessor` comment pointing to another PR, and
- That predecessor chain eventually reaches a PR targeting `main`

Being in a train does not require the train to have started. Once the root receives `@merge-train start`, all current and future descendants will be processed.

### Local train state (authoritative)

When the bot takes ownership of a stack (via `@merge-train start`), it persists the train's operational state to disk. This local record is the authoritative source of truth for restart recovery.

**Train record fields:**

| Field | Type | Description |
|-------|------|-------------|
| `version` | `number` | Schema version (currently `1`) |
| `state` | `string` | One of: `running`, `stopped`, `waiting_ci`, `aborted` |
| `current_pr` | `number` | PR currently being processed (the train's current root PR) |
| `cascade_phase` | `string` | One of: `idle`, `preparing`, `squash_pending`, `reconciling` |
| `predecessor_pr` | `number?` | PR number of predecessor (for fetching via `refs/pull/<n>/head` during recovery) |
| `last_squash_sha` | `string?` | SHA of last squash commit (for reconciliation recovery) |
| `started_at` | `string` | ISO 8601 timestamp when started |
| `stopped_at` | `string?` | ISO 8601 timestamp if stopped |
| `error` | `object?` | Error details if aborted: `{ "type": "...", "message": "..." }` |

**Cascade phases:**

- `idle`: Not currently performing any operation; waiting for CI or next event
- `preparing`: Merging predecessor head and default branch into descendants (before squash)
- `squash_pending`: Preparation complete; about to squash-merge current PR
- `reconciling`: Squash complete; performing ours-strategy merges into descendants

**Recovery semantics:**

If the bot crashes mid-cascade, the `cascade_phase` indicates where to resume:

| Phase | Recovery action |
|-------|-----------------|
| `idle` | Re-evaluate current PR's readiness |
| `preparing` | Re-run preparation (idempotent if already pushed) |
| `squash_pending` | Check if squash already happened; if not, perform it |
| `reconciling` | Use `last_squash_sha` to complete reconciliation (see below for fallback) |

**Reconciling recovery with missing `last_squash_sha`:**

If recovery finds `cascade_phase = "reconciling"` but `last_squash_sha` is null (lost due to crash after squash but before durable write):

1. Check if `predecessor_pr` is recorded (should be set during preparation)
2. Fetch the predecessor PR from GitHub API
3. If `state == "merged"`, extract `merge_commit_sha` — this is the squash SHA
4. Continue reconciliation with the recovered SHA
5. If predecessor is not merged, the squash didn't actually happen — revert to `squash_pending`

This derives the squash SHA from GitHub rather than hard-failing, since the squash-merge is recorded in GitHub's PR state even if our local record was lost.

**GitHub-based recovery**: If the local state files are missing or corrupted, the bot can recover train state from GitHub during bootstrap by scanning for status comments on open root PRs. The status comment contains the full `TrainRecord` as machine-readable JSON, enabling precise recovery without inference. See "Status comments" below.

**Supplementary GitHub recovery**: Even when local state exists, the bot may consult GitHub status comments to fill gaps caused by crashes between event log writes and status comment updates. On restart with existing local state:

1. For each active train, fetch the status comment from GitHub
2. Compare `cascade_phase` and `last_squash_sha` between local and GitHub
3. If GitHub has a more advanced phase or SHA that local state lacks:
   - The local write was lost (crash before fsync or during write)
   - Use the GitHub values to update local state
4. Continue recovery from the merged state

This handles the scenario where: local state shows `squash_pending`, but GitHub shows `reconciling` with `last_squash_sha`. The squash succeeded and was recorded to GitHub, but the local write was lost. Without this, recovery would attempt to squash again (which would fail or duplicate).

**Recovery precedence:**
- If GitHub is ahead: Use GitHub's phase/SHA (operation succeeded, local lost the record)
- If local is ahead: Use local (status comment update was delayed/lost)
- If both agree: Normal recovery

### Status comments (required, non-authoritative for normal operation)

The bot MUST post and update a status comment on the current root PR at each phase transition. This comment includes:

- Full machine-readable train state in `<!-- merge-train-state {...} -->` JSON
- Human-readable status for user visibility

**Format:**

```markdown
<!-- merge-train-state
{
  "version": 1,
  "state": "running",
  "current_pr": 124,
  "cascade_phase": "reconciling",
  "predecessor_pr": 123,
  "last_squash_sha": "abc123def456",
  "started_at": "2024-01-15T09:00:00Z"
}
-->
**Merge Train Status**

Train running — reconciling PR #124 after squash
```

**JSON fields:** Same as the `TrainRecord` fields documented in "Local train state" above: `version`, `state`, `current_pr`, `cascade_phase`, `predecessor_pr`, `last_squash_sha`, `started_at`.

**Authoritative source of truth:** During normal operation, the local event log is authoritative. Status comments serve as:
- User-facing observability (what is the bot doing?)
- Fallback recovery source if local state is lost

**Security:** The bot verifies that the comment author's user ID matches its own before parsing, preventing injection of fake state by malicious users.

**Deletion handling:** If a user deletes the status comment, the bot recreates it on the next phase transition.

### Local state storage

The bot persists per-repo state using an append-only event log with periodic snapshots:

**Directory structure (default):**

```
/var/lib/merge-train/state/
  owner/
    repo/
      events.log      # append-only state event log (JSON Lines)
      snapshot.json   # periodic snapshot for fast bootstrap
      spool/          # webhook delivery spool (separate, transient)
        <delivery-id>.json
```

**Webhook spool lifecycle:**

The spool is a durable queue, not just a transient buffer. Deliveries progress through states:

```
<delivery-id>.json       → pending (just received)
<delivery-id>.json.proc  → processing (worker claimed it)
<delivery-id>.json.done  → processed (state effects durably persisted)
```

A delivery is only deleted (GC'd) after its `.done` marker exists AND a grace period (e.g., 1 hour) has passed. This allows debugging of recently processed events.

**Dedupe strategy:** Deliveries are deduped by multiple stable identifiers, not just `X-GitHub-Delivery`:
- For `issue_comment` events: `(PR number, comment ID)` — comment ID is globally unique
- For `pull_request` events: `(PR number, action, head SHA)` — same PR+action+SHA = same logical event
- For `check_suite` events: `(check suite ID)`

This handles GitHub's redelivery with new delivery IDs for the same logical event.

The event log is persistent (state changes are retained for recovery/debugging).

**Event log format (`events.log`):**

Newline-delimited JSON (JSON Lines), one event per line. Each event includes a monotonic sequence number:

```json
{"seq":1,"ts":"2024-01-15T10:00:00Z","type":"train_started","root_pr":123}
{"seq":2,"ts":"2024-01-15T10:01:00Z","type":"phase_transition","train_root":123,"phase":"preparing"}
{"seq":3,"ts":"2024-01-15T10:02:00Z","type":"pr_merged","pr":123,"sha":"abc123"}
{"seq":4,"ts":"2024-01-15T10:03:00Z","type":"train_completed","root_pr":123}
```

**Snapshot format (`snapshot.json`):**

```json
{
  "schema_version": 1,
  "snapshot_at": "2024-01-15T10:30:00Z",
  "log_position": 1234,
  "next_seq": 5,
  "default_branch": "main",
  "prs": {
    "123": {
      "head_sha": "abc123def456",
      "base_ref": "main",
      "predecessor": null,
      "state": "open",
      "closed_at": null
    },
    "124": {
      "head_sha": "def456ghi789",
      "base_ref": "feature-123",
      "predecessor": 123,
      "state": "open",
      "closed_at": null
    }
  },
  "active_trains": {
    "123": {
      "version": 1,
      "state": "running",
      "current_pr": 123,
      "cascade_phase": "idle",
      "predecessor_pr": null,
      "last_squash_sha": null,
      "started_at": "2024-01-15T09:00:00Z",
      "stopped_at": null,
      "error": null
    }
  }
}
```

**Bootstrap from local storage:**

1. Load `snapshot.json` if it exists
2. Seek to `log_position` in `events.log` and replay from there
3. Skip any trailing partial line (incomplete JSON due to crash mid-append)
4. Rebuild in-memory state

**Partial line handling:** On replay, if the final line doesn't parse as valid JSON, truncate the log file at the start of that line and continue. This handles crashes mid-append without failing boot.

**Compaction:**

- Periodically (e.g., every hour) or when log exceeds size threshold
- Write current in-memory state to new `snapshot.json` with `log_position` = current file size
- Truncate `events.log` to zero (events are now captured in snapshot)
- Events for completed/stopped trains can be omitted from the snapshot

**Staleness detection**: The `snapshot_at` field indicates when state was last persisted. If this is older than a threshold (default: 1 hour) and the event log is empty/missing, the bot performs a full re-sync from the GitHub API to catch drift and missed deliveries.

---

## State Management

### Design decision: local file state persistence

The bot persists per-repo state using an append-only event log with periodic snapshots (see "Local state storage" in Data Model). This enables fast bootstrap, robust restart recovery, and GitHub-based disaster recovery.

**Why local files instead of GitHub storage or a database:**

- **Restart safety**: State and in-progress cascades survive process restarts.
- **Append-only durability**: Event log can survive partial writes; snapshots use atomic rename.
- **Debuggable**: Can inspect event history to understand state transitions.
- **No GitHub state coupling for normal operation**: No reliance on special refs or Contents/Git Data APIs.
- **GitHub recovery fallback**: Status comments on PRs enable full recovery if local state is lost.
- **Easy ops**: Files are easy to back up, inspect, and rotate.

**Event log append strategy**:

- Append JSON line to `events.log`
- `fsync` on **recovery-critical events** (see below) to ensure durability before proceeding
- Partial writes leave valid JSON Lines prefix (crash-safe)

**Recovery-critical events** (require fsync before continuing):
- `train_started`, `train_stopped`, `train_completed`, `train_aborted`
- `phase_transition` (any phase change: `preparing`, `squash_pending`, `reconciling`, `idle`)
- `squash_committed` (records the squash SHA needed for reconciliation recovery)

Non-critical events (fsync batched for performance):
- `pr_merged`, `pr_state_changed`, `predecessor_declared`

**Snapshot update strategy**:

- Serialize state to a temp file in the same directory as `snapshot.json`
- `fsync` the temp file
- Rename over `snapshot.json` (atomic on POSIX filesystems)
- Truncate `events.log`
- `fsync` the directory to persist the rename (best-effort)

**Concurrency**: This design assumes a single active bot process per `state_dir`. Multi-instance deployments require external coordination (leader election, sticky routing, or a shared database) and are out of scope for this document.

The bot maintains in-memory state per repository for fast event processing. On startup it loads existing snapshots and replays event logs from disk; for unknown repos it bootstraps on the first relevant webhook (or via an operator-initiated full sync), then persists state.

### Repository lifecycle

Each repo follows this state machine:

```
               startup / first webhook
    ┌─────────┐    ────────────►   ┌───────────────┐
    │ Unknown │                    │ Bootstrapping │
    └─────────┘                    └───────┬───────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    │                      │                      │
                    ▼                      ▼                      ▼
             ┌───────────┐          ┌───────────┐          ┌──────────┐
             │   Ready   │◄────────►│  Re-sync  │          │  Failed  │
             └───────────┘  error/  └───────────┘          └────┬─────┘
                            periodic                            │
                                                     retry      │
                                            ┌───────────────────┘
                                            ▼
                                     ┌───────────────┐
                                     │ Bootstrapping │
                                     └───────────────┘
```

- **Unknown**: No state for this repo yet
- **Bootstrapping**: Loading from disk and/or performing a full GitHub API crawl; incoming events are queued
- **Ready**: Processing events incrementally
- **Failed**: Bootstrap failed; retry with backoff

### Per-repo state

```rust
pub struct RepoState {
    /// The repository's default branch (e.g., "main", "master")
    default_branch: String,

    /// All known PRs (open + recently merged)
    prs: HashMap<PrNumber, CachedPr>,

    /// Reverse index: predecessor → descendants
    descendants: HashMap<PrNumber, HashSet<PrNumber>>,

    /// Active trains, keyed by root PR number
    active_trains: HashMap<PrNumber, TrainRecord>,

    /// Filesystem path for this repo's state file
    state_path: PathBuf,

    /// When bootstrap completed
    bootstrapped_at: Instant,

    /// For periodic re-sync
    last_sync: Instant,

    /// When state was last persisted to disk
    last_persisted: Option<Instant>,

    /// Whether in-memory state has changed since last persistence
    dirty: bool,

    /// Cache miss counter for drift detection
    miss_count: u32,
}
```

### Bootstrap algorithm

On startup (and on first webhook for an unknown repo), the bot follows a two-phase approach: load from local disk first, then fall back to a full GitHub API crawl if necessary.

**Phase 1: Disk-based bootstrap (fast path)**

1. Transition to `Bootstrapping` state (queue incoming events)
2. Attempt to load repo state from disk:
   a. Load `<state_dir>/<owner>/<repo>/snapshot.json` if it exists
   b. Replay events from `events.log` that have timestamps after `snapshot_at`
   c. Rebuild in-memory state
3. If snapshot found and not stale (`snapshot_at` within threshold):
   a. Rebuild derived indexes (e.g., `descendants`) from the persisted graph
   b. For each open PR, fetch current `mergeStateStatus` via GraphQL (merge readiness may have changed)
   c. Build `RepoState` from disk state + refreshed merge states
   d. Skip to step 13 (recovery check)
4. If snapshot missing, stale, or corrupted: proceed to Phase 2

**Phase 2: Full API crawl (fallback path)**

5. Fetch repository metadata: `GET /repos/{o}/{r}` → extract `default_branch`
6. Fetch all open PRs: `GET /repos/{o}/{r}/pulls?state=open`
7. Fetch recently merged PRs: `GET /repos/{o}/{r}/pulls?state=closed&sort=updated`
8. For each PR, fetch comments to find:
   - `@merge-train predecessor` declarations (from humans)
   - `<!-- merge-train-state {...} -->` status comments (from bot — for train recovery)
9. **Train state recovery**: For each open PR targeting the default branch:
   a. If a bot-owned status comment with `<!-- merge-train-state {...} -->` is found:
   b. Verify comment author matches bot's user ID (security check)
   c. Parse the JSON to extract full `TrainRecord`
   d. Add to `active_trains` map
10. For each open PR, fetch `mergeStateStatus` via GraphQL
11. Build `RepoState` with default branch, PR map, descendants index, and recovered active trains
12. **Persist state**: Write snapshot to disk (see State Persistence section)

**Recovery and completion (both paths)**

13. **Recovery check**: For any train in a non-idle `cascade_phase`, evaluate whether to resume:
    - If `preparing`: Re-run preparation (merge operations are idempotent if already pushed)
    - If `squash_pending`: Check if PR is already merged; if not, proceed with squash
    - If `reconciling`: Use `last_squash_sha` from the train record to complete reconciliation
14. Transition to `Ready` state
15. Drain queued events, process each in order

API calls are parallelized (concurrency limit ~10) to minimize bootstrap time.

**Fast path efficiency**: With disk-based bootstrap, a typical restart requires:
- 0 GitHub API calls to load cached structure (snapshot + event log replay)
- N API calls for merge state refresh (one GraphQL query per open PR, can be batched)
- 0 API calls for bot-owned state recovery (train state is local)

Compare to full bootstrap which requires O(PRs) + O(comments) API calls.

**GitHub recovery path**: If local state is lost, Phase 2 can fully recover active trains from status comments on GitHub. This is slower (requires scanning all comments) but ensures no trains are orphaned due to disk failure.

### Incremental updates

Each webhook event updates the cached state:

| Event | State Update |
|-------|--------------|
| `issue_comment` + `predecessor #N` | `pr.predecessor = Some(N)`, update descendants index |
| `issue_comment` + `start` | Create/update local train record, add to `active_trains`, begin cascade |
| `issue_comment` + `stop` | Mark train stopped in local store, cancel stack operations |
| `pull_request.opened` | Add new PR to cache |
| `pull_request.closed` (merged) | `pr.state = Merged { sha }` |
| `pull_request.closed` (not merged) | `pr.state = Closed` |
| `pull_request.synchronize` | Update `head_sha`, set `merge_state` to Unknown |
| `check_suite.completed` | Re-fetch `merge_state` via GraphQL |
| `pull_request_review` | Re-fetch `merge_state` via GraphQL |

**Train state updates during cascade**: The bot appends events to the event log and updates the status comment at each phase transition:
- Before preparation: `cascade_phase = "preparing"` → append `phase_transition` event, update status comment
- After preparation: `cascade_phase = "squash_pending"`, `predecessor_pr = <pr_number>` → append event, update comment
- After squash: `cascade_phase = "reconciling"`, `last_squash_sha = <sha>` → append event, update comment
- After reconciliation: `cascade_phase = "idle"`, advance `current_pr` → append event, update comment

These updates provide the recovery points described in the "Local train state" section. The event log enables local recovery; the status comment enables GitHub-based recovery if local state is lost.

### Handling cache misses

When an event references a PR not in cache:
1. Fetch the single PR from API, add to cache
2. Increment `miss_count`
3. If `miss_count >= 5`, trigger re-bootstrap (too much drift)

### Re-sync triggers

The bot re-bootstraps a repo when:
- **On error**: State inconsistency, cycle detected, too many cache misses, API errors
- **Periodic**: Every hour (configurable), via synthetic event in the queue
- **Stale snapshot**: Local snapshot `snapshot_at` is older than staleness threshold

Periodic re-sync is implemented by injecting a `PeriodicSync` event into the same queue as webhooks, preserving serial processing guarantees.

### State persistence

The bot persists state via an append-only event log with periodic snapshots (see "Local state storage" above).

**Event log appends:**

| Trigger | Action | fsync? |
|---------|--------|--------|
| Phase transition | Append event to `events.log` | No |
| Train start | Append event to `events.log` | Yes (critical) |
| Train stop/abort | Append event to `events.log` | Yes (critical) |
| Train completion | Append event to `events.log` | Yes (critical) |
| PR merged | Append event to `events.log` | No |
| PR state change | Append event to `events.log` | No |

**Append strategy:**

```rust
fn append_event(log: &mut File, event: &Event) -> io::Result<()> {
    writeln!(log, "{}", serde_json::to_string(event)?)?;
    if event.is_critical() {
        log.sync_all()?; // fsync on train start/stop/complete
    }
    Ok(())
}
```

**Snapshot triggers:**

| Trigger | Action | Rationale |
|---------|--------|-----------|
| Periodic timer | Compact if log size > threshold | Bound log growth |
| Full bootstrap completion | Write snapshot | Fresh state worth preserving |
| Train completion | OK to compact (train events no longer needed) | Remove stale entries |

**Atomic snapshot update:**

- Serialize JSON to a temp file in the same directory as `snapshot.json`
- `fsync` the temp file
- Rename over `snapshot.json` (atomic)
- Truncate `events.log`
- `fsync` the directory (best-effort) to persist the rename

**Failure handling**: If state persistence fails (disk full, permissions, I/O errors), the bot treats this as fatal for that repo: it logs loudly, stops advancing trains, and returns an error for new webhook deliveries for that repo until persistence is restored. This avoids silently running with state that cannot survive a restart.

### State pruning

The `prs` map in the state file can grow unboundedly as PRs accumulate over time. Without pruning, a busy repository could accumulate thousands of entries, slowing down loads and increasing disk usage.

**Retention policy:**

The state file is the authoritative local cache. Pruning doesn't need to be perfect; it just needs to keep the file bounded while retaining useful data for active trains and stack reconstruction.

| PR state | Retention |
|----------|-----------|
| Open | Always keep (needed for stack tracking) |
| Merged/closed, in active train | Always keep (train still references it) |
| Merged/closed, referenced as predecessor by a kept PR | Keep (transitive closure) |
| Merged/closed, not referenced | Prune after retention period |

**Default retention period**: 30 days for unreferenced merged/closed PRs. Configurable via `MERGE_TRAIN_PR_RETENTION_DAYS`.

**Size limit fallback**: If the `prs` map exceeds `max_prs_in_snapshot` (default: 1000), aggressively prune the oldest unreferenced merged/closed PRs (by PR number, lower = older) until under limit. This prevents unbounded growth even in pathological cases.

**Pruning algorithm** (runs before each state save):

```rust
fn prune_snapshot(snapshot: &mut PersistedRepoSnapshot, config: &Config) {
    let now = Utc::now();
    let retention = Duration::days(config.pr_retention_days as i64);

    // Collect PRs to keep
    let mut keep: HashSet<u64> = HashSet::new();

    // 1. Keep all open PRs
    for (pr_num, pr) in &snapshot.prs {
        if pr.state == "open" {
            keep.insert(pr_num.parse().unwrap());
        }
    }

    // 2. Keep all PRs in active trains
    for train in snapshot.active_trains.values() {
        keep.insert(train.current_pr.0);
    }

    // 3. Transitive closure: keep predecessors of kept PRs
    loop {
        let mut added = false;
        for (pr_num, pr) in &snapshot.prs {
            let num: u64 = pr_num.parse().unwrap();
            if keep.contains(&num) {
                if let Some(pred) = pr.predecessor {
                    if keep.insert(pred) {
                        added = true;
                    }
                }
            }
        }
        if !added { break; }
    }

    // 4. Prune unreferenced merged/closed PRs older than retention period
    snapshot.prs.retain(|pr_num, pr| {
        let num: u64 = pr_num.parse().unwrap();
        if keep.contains(&num) {
            return true;
        }
        // Check closed_at if available
        if let Some(closed_at) = &pr.closed_at {
            if let Ok(dt) = DateTime::parse_from_rfc3339(closed_at) {
                return now - dt.with_timezone(&Utc) < retention;
            }
        }
        // No closed_at timestamp - keep for now (legacy entries)
        true
    });

    // 5. Size limit fallback
    if snapshot.prs.len() > config.max_prs_in_snapshot {
        // Sort merged/closed PRs by PR number (ascending = oldest first)
        let mut prunable: Vec<u64> = snapshot.prs.iter()
            .filter(|(pr_num, pr)| {
                let num: u64 = pr_num.parse().unwrap();
                !keep.contains(&num) && pr.state != "open"
            })
            .map(|(pr_num, _)| pr_num.parse().unwrap())
            .collect();
        prunable.sort();

        // Remove oldest until under limit
        let to_remove = snapshot.prs.len() - config.max_prs_in_snapshot;
        for pr_num in prunable.into_iter().take(to_remove) {
            snapshot.prs.remove(&pr_num.to_string());
        }
    }
}
```

**Schema addition**: Add `closed_at` timestamp to `PersistedPr`:

```rust
#[derive(Serialize, Deserialize)]
struct PersistedPr {
    head_sha: String,
    base_ref: String,
    predecessor: Option<u64>,
    state: String,
    /// When the PR was merged or closed (ISO 8601). Null for open PRs.
    /// Used for retention-based pruning.
    closed_at: Option<String>,
}
```

The `closed_at` field is set when processing `pull_request.closed` events. Existing entries without this field are treated as "keep until size limit forces pruning."

**Observability**: Log pruning activity:

```
INFO repo=owner/repo pruned_prs=42 remaining_prs=158 "state pruned"
```

### State Export API

The bot exposes an HTTP endpoint for operators to inspect current state without SSH access:

```
GET /api/v1/repos/{owner}/{repo}/state
```

**Response:**

```json
{
  "schema_version": 1,
  "snapshot_at": "2024-01-15T10:30:00Z",
  "default_branch": "main",
  "prs": { ... },
  "active_trains": { ... },
  "recent_events": [ ... ]
}
```

**Fields:**

| Field | Description |
|-------|-------------|
| `schema_version` | Schema version for forward compatibility |
| `snapshot_at` | When the snapshot was last written |
| `default_branch` | Cached default branch name |
| `prs` | Map of PR number → cached PR info |
| `active_trains` | Map of root PR number → train record |
| `recent_events` | Last N events from the event log (for debugging) |

**Use cases:**

- Debugging without SSH access
- State migration (export from old host, import on new)
- Monitoring integration
- Operator visibility into bot state

**Authentication**: This endpoint should be protected by the same authentication as webhook endpoints (e.g., GitHub App installation verification or operator API key).

### Stack topology

From the cached state, stacks are computed by traversing predecessor relationships:

1. Find all root PRs (target default branch, no open predecessor)
2. Build linear chains from each root by following `descendants` index
3. Validate: no cycles, predecessors exist
4. Check `active_trains` to determine if each stack is started/stopped

**Fan-out handling**: When a PR has multiple open descendants (fan-out), the stack ends at that PR. Each descendant will become the root of its own independent stack once its predecessor merges. After the fan-out point merges:
- Each descendant is retargeted to the default branch
- Each descendant receives its own train record in the local state store (inheriting "started" status from the parent train)
- They proceed as independent trains; whichever passes CI first merges next
- The `cascade_step` returns `FanOut { descendants }` to trigger train record creation (and optional status comment updates)

---

## Event Handling

| Event | Action |
|-------|--------|
| `issue_comment` with `@merge-train predecessor #N` | Validate, record, ack |
| `issue_comment` with `@merge-train start` | Squash-merge root, begin cascade |
| `issue_comment` with `@merge-train stop` | Mark stack stopped, report state |
| `pull_request` merged | If merged PR has descendants, cascade to next |
| `pull_request` closed (not merged) | Notify descendants they're orphaned |
| `check_suite` / `status` completed | If cascade waiting on this PR, continue |
| `pull_request_review` submitted (approved) | If cascade waiting on this PR, continue |

**Fan-out discovery**: After a fan-out point merges, each descendant is discovered as a new root on subsequent events. Since each descendant now targets the default branch and its predecessor is merged, `is_root()` returns true and `compute_stacks()` includes it. No special event handling is needed — the normal cascade flow applies to each independent branch.

### Per-repo serial event processing

The server processes webhook events **serially per repository** via per-repo queues. Different repositories are processed concurrently, but events within a single repository are strictly serialized. Webhook deliveries are durably spooled to disk before being enqueued so a crash/restart cannot drop events after a `202 Accepted`.

```
                                         ┌─────────────────────────────────┐
                                         │        repo A queue             │
                                    ┌──► │  (priority: stop > others)      │ ──► worker A
                                    │    └─────────────────────────────────┘
┌─────────────┐     ┌──────────┐    │
│   axum      │ ──► │ dispatch │ ───┤    ┌─────────────────────────────────┐
│  (accepts)  │     │ by repo  │    │    │        repo B queue             │
└─────────────┘     └──────────┘    └──► │  (priority: stop > others)      │ ──► worker B
      │                                  └─────────────────────────────────┘
      │ returns 202 Accepted
      ▼
```

Deliveries are written to a per-repo spool directory keyed by `X-GitHub-Delivery` before being enqueued. The per-repo worker drains this spool on startup and during normal operation.

The HTTP handler:
1. Validates the webhook signature
2. Extracts the repository ID and `X-GitHub-Delivery` ID
3. Writes the delivery (headers + body) to the per-repo disk spool atomically (dedupe by delivery ID)
4. Classifies the delivery by priority (stop commands have higher priority)
5. Pushes the parsed event onto the **per-repo** priority queue (creating queue + worker if needed)
6. Returns `202 Accepted`

Each per-repo worker loop:
1. Ensures repo state is loaded/bootstrapped from disk (or re-syncs from GitHub if needed)
2. Drains the per-repo disk spool (replaying any `.json` files without `.done` markers)
3. Pulls the highest-priority event from its queue
4. Renames delivery file to `.proc` (claiming it for processing)
5. Applies incremental state update from the event
6. If state effects require persistence (recovery-critical event):
   a. Appends event to log + fsync
   b. Creates `.done` marker for the delivery
7. Evaluates cascade actions based on updated state
8. On cascade phase transitions or irreversible operations, persists state before proceeding
9. Loops (or shuts down after idle timeout)

**Critical invariant:** A delivery's `.done` marker is only created after all state effects from that delivery are durably persisted. This ensures that on restart, unprocessed deliveries are replayed correctly.

**Priority ordering**: Stop commands (`@merge-train stop`) are processed before all other events within that repo's queue. This ensures that a human request to halt the cascade takes effect promptly.

**Cancellation**: Cancellation is **stack-scoped**. Each stack has its own `CancellationToken`, managed by the worker. When a stop command arrives, the dispatcher:
1. Sends a `CancelStackRequest` (containing the PR number) to the worker via a separate channel
2. The worker resolves the PR to its stack root and cancels that stack's token
3. Enqueues the stop event (which will be processed immediately due to priority)

This allows long-running operations like `git merge` or `git push` to be interrupted promptly when a human requests a stop — **without affecting other stacks in the same repo**.

This design means:
- Each stack has its own git worktree (see "Per-stack worktrees" section)
- Stop on one stack doesn't interrupt operations on other stacks
- Different repos proceed independently
- No race between "check CI status" and "CI status changes"
- Straightforward reasoning about state transitions per repo
- Stop commands can interrupt in-flight operations for the target stack only
- Resilient to bot restarts — repo state and unprocessed deliveries are recovered from disk

### Event processing flow

```
receive webhook
  │
  ├─► validate signature
  ├─► extract repo_id + delivery_id
  ├─► write delivery to disk spool
  ├─► notify per-repo worker
  │
  └─► (worker) lookup repo lifecycle
        │
        ├─ Unknown ──────────► bootstrap, then drain spool
        ├─ Bootstrapping ────► queue notification (processed after bootstrap)
        ├─ Ready ────────────► drain spool, apply updates, evaluate actions
        └─ Failed ───────────► if backoff elapsed: retry bootstrap

  └─► (for Ready state) evaluate cascade:
        │
        ├─ Find frontier of each started stack
        ├─ If frontier ready and predecessor merged: cascade
        ├─ If frontier pending (BLOCKED/UNKNOWN): wait
        └─ If frontier blocked (required checks/approvals): abort, notify
```

### Polling fallback for active trains

Webhooks are the primary trigger for cascade evaluation, but they're not sufficient alone:

- **Missed webhooks**: Network issues, server downtime, or GitHub outages can cause webhook delivery failures
- **Out-of-order delivery**: Webhooks may arrive in unexpected order, causing missed state transitions
- **Incomplete event mapping**: Some events (notably `status`) don't directly map to PRs, making it hard to know which train to re-evaluate

To ensure progress even when webhooks fail, the bot polls active trains periodically as a fallback.

**Polling frequency**: Once per hour per active train (configurable). This is infrequent because:
- Webhook failures are rare
- Polling is expensive (requires API calls to refresh merge state)
- The failure mode is "cascade stalls" not "cascade breaks" — low urgency

**Implementation**: The periodic sync timer (which already exists for re-sync) also injects `PollActiveTrains` events:

```rust
enum QueuedEventPayload {
    /// Webhook from GitHub
    GitHub(GitHubEvent),
    /// Internal: trigger periodic re-sync
    PeriodicSync,
    /// Internal: poll active trains for missed webhook recovery
    PollActiveTrains,
}
```

**Poll action**: When processing `PollActiveTrains`:

1. For each active train in `active_trains`:
   a. Fetch current `mergeStateStatus` for the frontier PR via GraphQL
   b. Compare with cached `merge_state`
   c. If changed (e.g., now `CLEAN` when previously `BLOCKED`), trigger cascade evaluation
2. If any train's frontier PR is now ready but wasn't before, the missed webhook is effectively recovered

**Distributed polling**: To avoid thundering herd when multiple bot instances restart:
- Add jitter to the poll interval (e.g., 60 ± 10 minutes)
- Stagger initial poll based on hash of repo ID

**Logging**: Poll-triggered cascade evaluations are logged distinctly from webhook-triggered ones, making it easy to detect webhook delivery problems:

```
INFO repo=owner/repo train_root=123 trigger=poll "evaluating cascade (poll fallback)"
INFO repo=owner/repo train_root=123 trigger=webhook "evaluating cascade"
```

If poll-triggered evaluations are frequent, it indicates a webhook delivery problem worth investigating.

### Restart safety

Webhook deliveries are durably spooled to disk before returning `202 Accepted`. Each delivery is keyed by GitHub's `X-GitHub-Delivery` ID so retries/redeliveries can be safely deduplicated.

On restart, the bot:
1. Loads repo state (snapshot + event log replay) from disk
2. For each active train, cleans up its worktree (see "Worktree cleanup on restart" below)
3. Replays any unprocessed webhook deliveries from the spool (files without `.done` markers)
4. For each active train in a non-`idle` phase, performs recovery check (see "Recovery semantics")
5. Optionally refreshes train state from GitHub status comments (see "Supplementary GitHub recovery")

**Worktree cleanup on restart:**

If the process dies mid-git operation, worktrees may be left in a dirty state (in-progress merge, uncommitted changes). On restart, for each active train's worktree:

```bash
# Abort any in-progress merge
git merge --abort 2>/dev/null || true

# Reset to remote tracking branch (discards local changes)
git reset --hard origin/<branch>

# Remove untracked files
git clean -fd
```

If the worktree is too corrupted to clean, delete and recreate it.

**Operational requirement**: `state_dir` must be on persistent storage. If `state_dir` is lost or corrupted, in-progress trains can be recovered from GitHub status comments (slower, but works).

**Multi-instance note**: Running multiple instances against the same `state_dir` is not supported without external coordination (leader election or a shared database).

---

## Merge Operations

### Why local git is required

GitHub's merge API uses recursive/ort merge strategy, which often produces spurious conflicts or incorrect results when merging `main` into a stacked branch after a squash-merge. The content is semantically identical, but git doesn't recognise this.

The ours-strategy merge (as described at https://www.patrickstevens.co.uk/posts/2023-10-18-squash-stacked-prs/) resolves this correctly, as we now detail.

### Local git workflow

*This section uses `main` for exposition, but the implementation uses `default_branch` fetched from the GitHub API during bootstrap (see `RepoState.default_branch`). Repositories using `master`, `develop`, or other default branches work identically.*

We assume that the preceding PR in the merge train has just been squash-merged into `main`, and present the procedure to prepare the new root PR for its own squash-merge to `main`.

Firstly, we assume we know the preceding PR's number (`$PREDECESSOR_PR_NUMBER`). GitHub maintains `refs/pull/<n>/head` refs that point to a PR's head commit even after the branch is deleted — this is the reliable way to fetch PR commits (fetching by raw SHA is not guaranteed to work).
We additionally assume the GitHub API has told us the `$PREDECESSOR_SQUASH_COMMIT`, that is on `main` (possibly in the history of `main`, if someone has made an intervening commit).

```bash
# Clone (or fetch into existing clone)
git clone <repo-url> workdir
cd workdir

# Configure signing
git config user.signingkey <key-id>
git config commit.gpgsign true

# Fetch the predecessor's head via GitHub's PR ref (reliable even after branch deletion)
git fetch origin <descendant-branch> "refs/pull/$PREDECESSOR_PR_NUMBER/head:refs/remotes/origin/pr/$PREDECESSOR_PR_NUMBER"

git checkout <descendant-branch>
git merge "origin/pr/$PREDECESSOR_PR_NUMBER" --no-edit -m "Merge predecessor into <descendant-branch> (merge train)"
git merge "$PREDECESSOR_SQUASH_COMMIT"^ --no-edit -m "Merge main into <descendant-branch> (merge train)"

# Now <descendant-branch> is up to date both with main-immediately-before-merge-of-base-PR and with base-PR-immediately-before-merge.
# Assuming (incorrectly, but often true) that merges are associative and commutative, this final merge is a no-op.
git merge "$PREDECESSOR_SQUASH_COMMIT" --strategy=ours --no-edit -m "Relate main history with <descendant-branch> (merge train)"

# Now we can merge `main` into the branch: the gnarly history problem is sorted, so we just need to pick up
# any subsequent commits which may have landed in `main`.
git fetch origin main
git merge --no-edit -m "Merge main into <descendant-branch>" origin/main

git push origin <descendant-branch>
```

Then, for every PR on top of `<descendant-branch>` in the merge train, we use the GitHub API to cascade the new history back up, performing a standard recursive merge of `descendant-branch` into `PR-immediately-above-descendant-branch` and so on up the stack.

### Commit signing

All merge commits created by the bot are GPG-signed. The signing key is configured via environment/secrets (managed externally).

The squash-merge into `main` is performed via GitHub API — GitHub signs these commits itself, showing as "Verified" in the UI.

### Note on using PR refs

When a PR is merged, the PR object retains `head.sha` — the final commit on the branch before squash-merge. However, **fetching by raw SHA is unreliable** — it depends on server-side settings (`uploadpack.allowReachableSHA1InWant`) that aren't guaranteed.

Instead, the bot uses GitHub's **PR refs** (`refs/pull/<n>/head`), which:
- Are maintained by GitHub even after the PR branch is deleted
- Point to the PR's head commit reliably
- Can always be fetched via standard git protocols

This makes the bot resilient to:

- Branch deletion (GitHub's "delete branch on merge" setting)
- Manual merges outside the bot's workflow
- Crash recovery (we can resume even if the predecessor branch is gone)
- Late additions to the stack (a PR added after its predecessor was already merged)

The PR number (not the SHA) is the stable identifier used to construct these refs.

### Operation sequence

Let's assume a stack of this shape: main ← #123 ← #124 ← #125

The cascade proceeds by repeating the following for each PR, starting from the root:

```
For PR #N with descendants {#D1, #D2, ...} (may be one or multiple):

1. PREPARATION: For EACH descendant, bring it up to date with both #N and main
   a. Merge #N's head commit into descendant's branch (regular merge)
      → Local git: git merge <N.head_sha>
      → Ensures descendant has all of #N's final content
   b. Merge main into descendant's branch (regular merge)
      → Local git: git merge origin/main
      → Ensures descendant has any independent changes that landed on main
   → Both merges signed by bot
   → Push to origin
   → Must happen BEFORE squash-merging #N
   → Loop over ALL descendants before proceeding to step 2

2. SQUASH: Squash-merge #N into main
   → GitHub API, signed by GitHub
   → Record #N's head.sha for use in future recovery scenarios

3. RECONCILIATION: For EACH descendant, relate its branch with the squash commit
   → Local git: git merge <squash_commit_sha> --strategy ours
   → Uses the specific squash commit hash returned by step 2, NOT origin/main
   → Signed by bot
   → Adds the squash commit as a parent without changing tree content
   → Push to origin

4. CATCH-UP: For EACH descendant, merge any subsequent main commits
   → Local git: git merge origin/main
   → Regular merge (not ours) to properly incorporate any commits that landed on main
     after the squash commit (from other PRs, hotfixes, etc.)
   → If main hasn't advanced past the squash commit, this is a no-op
   → Push to origin

5. RETARGET: For EACH descendant, update base branch to main
   → GitHub API: PATCH /repos/{o}/{r}/pulls/{n}  { "base": "main" }

6. WAIT/BRANCH: Depends on descendant count
   → Single descendant: Update the local train record so the descendant becomes the new root, wait for CI
   → Multiple descendants (fan-out): Create a local train record for EACH descendant.
     Each becomes an independent train with its own root.
     They proceed independently; whichever passes CI first merges next.
     The others catch up via normal flow when their turn comes.

7. REPEAT: For single descendant, it becomes the new #N, loop from step 1
```

For the root PR (#123), there is no predecessor to merge, so the sequence starts at step 2.

For the final PR in the stack (#125), there is no descendant, so steps 1, 3-5 are skipped.

### Durability and commit points

Each irreversible side effect (git push, squash-merge, retarget) must be bracketed by durable state updates to ensure correct recovery after crashes.

**Commit point pattern:**

```
1. Write intent event to log + fsync (e.g., "about to push branch X")
2. Perform irreversible operation (git push, API call)
3. Write completion event to log + fsync (e.g., "pushed branch X successfully")
4. Update status comment on GitHub (provides secondary recovery source)
```

If the bot crashes:
- After step 1, before step 2: Recovery sees intent, retries the operation (must be idempotent or detectable)
- After step 2, before step 3: Recovery sees intent but no completion, checks if operation succeeded, then writes completion
- After step 3: Normal recovery, operation is recorded as complete

**Per-operation commit points:**

| Operation | Intent event | Completion event | Idempotency check |
|-----------|--------------|------------------|-------------------|
| git push (preparation) | `intent_push_prep` | `done_push_prep` | Check if remote ref matches expected SHA |
| Squash-merge | `intent_squash` | `squash_committed` | Check if PR is already merged via API |
| git push (reconciliation) | `intent_push_reconcile` | `done_push_reconcile` | Check if remote ref matches expected SHA |
| Retarget PR | `intent_retarget` | `done_retarget` | Check if PR base already equals target |

**Phase transitions with commit points:**

The `cascade_phase` transitions map to these commit points:

- `idle` → `preparing`: Write `phase_transition{preparing}` + fsync before starting preparation
- `preparing` → `squash_pending`: Write `phase_transition{squash_pending}` + fsync after all prep pushes complete
- `squash_pending` → `reconciling`: Write `squash_committed{sha}` + fsync **immediately after** receiving squash SHA from API
- `reconciling` → `idle`: Write `phase_transition{idle}` + fsync after all reconciliation pushes and retargets complete

**Critical invariant:** The `squash_committed` event with the squash SHA must be durably recorded before any reconciliation pushes occur. If the bot crashes after squash but before recording the SHA, it must re-fetch the SHA from GitHub (PR's `merge_commit_sha` field).

**Why step 1b is essential**: If main has independent changes (commits that landed outside this stack), the descendant must incorporate them before the ours-strategy merge. Otherwise, squash-merging the descendant would compute a diff against main that effectively reverts those independent changes.

**Example timeline for main ← #123 ← #124 ← #125 (linear):**

```
1.  Merge #123's head SHA into #124 (preparation 1a)
2.  Merge main into #124 (preparation 1b)
3.  Squash-merge #123 into main → returns squash_sha_123
4.  Merge squash_sha_123 into #124 (ours strategy, reconciliation)
5.  Merge origin/main into #124 (catch-up, regular merge)
6.  Retarget #124 to main
7.  Wait for #124 CI...
    [webhook fires when CI passes]
8.  Merge #124's head SHA into #125 (preparation 1a)
9.  Merge main into #125 (preparation 1b)
10. Squash-merge #124 into main → returns squash_sha_124
11. Merge squash_sha_124 into #125 (ours strategy, reconciliation)
12. Merge origin/main into #125 (catch-up, regular merge)
13. Retarget #125 to main
14. Wait for #125 CI...
    [webhook fires when CI passes]
15. Squash-merge #125 into main
16. Done
```

**Example timeline for main ← #123 ← {#124, #125} (fan-out):**

```
1.  Merge #123's head SHA into #124 (preparation 1a for first descendant)
2.  Merge main into #124 (preparation 1b for first descendant)
3.  Merge #123's head SHA into #125 (preparation 1a for second descendant)
4.  Merge main into #125 (preparation 1b for second descendant)
5.  Squash-merge #123 into main → returns squash_sha_123
6.  Merge squash_sha_123 into #124 (ours strategy, reconciliation)
7.  Merge origin/main into #124 (catch-up)
8.  Retarget #124 to main
9.  Merge squash_sha_123 into #125 (ours strategy, reconciliation)
10. Merge origin/main into #125 (catch-up)
11. Retarget #125 to main
    [#124 and #125 are now independent roots — cascade returns Complete]

    --- From here, #124 and #125 proceed independently ---

    [Assume #124 CI passes first]
12. Squash-merge #124 into main → returns squash_sha_124
13. Done with #124

    [Later, #125 CI passes]
14. Merge origin/main into #125 (picks up squash_sha_124 via normal catch-up)
15. Squash-merge #125 into main
16. Done
```

---

## Abort Conditions

The bot aborts the cascade (and comments with diagnostics) if:

| Condition | Likely cause | Recovery hint |
|-----------|--------------|---------------|
| Preparation merge fails | Conflict between predecessor/main and descendant | Resolve locally, push to descendant branch, re-trigger |
| `git push` rejected | Force-push or concurrent modification | Investigate branch state, re-trigger |
| PR closed without merge | Human intervention | Re-open or restructure stack |
| Required check fails on descendant | Code issue | Fix, push, bot will auto-continue |
| Cycle detected | Misconfigured predecessor | Fix comments |
| Approval withdrawn | Review state changed | Re-approve |
| Review dismissed | Reviewer dismissed their approval or requested changes | Re-approve; cascade will not auto-resume |
| Squash-merge API fails | Branch protection, conflicts | Check PR status |
| Branch protection blocks merge | Required checks missing, insufficient approvals, etc. | Satisfy branch protection rules, re-trigger |

**Review dismissal behaviour**: If a review is dismissed (either by the reviewer or due to new commits in repos with "dismiss stale reviews" enabled), the cascade aborts immediately. Unlike CI failure, review dismissal does **not** auto-resume — a new approval must be obtained and `@merge-train start` must be re-issued to continue.

**Branch protection behaviour**: If the target branch has protection rules that prevent the squash-merge (e.g., required status checks not yet present, insufficient approvals, unsigned commits), the GitHub API will reject the merge request. The bot treats this as a fatal error for the stack: it posts a diagnostic comment and stops handling the stack entirely. The user must satisfy the branch protection requirements and re-issue `@merge-train start`.

On abort, the bot:

1. Stops at the next opportunity (in-flight operations may complete first)
2. Posts a comment on the PR that failed, explaining what happened and suggesting recovery
3. Posts a comment on downstream PRs that the train is halted
4. Takes no further action until human intervenes or condition resolves

### Auto-resume on check fix

If the cascade aborted because `mergeStateStatus` became `BLOCKED` (required checks failing), the bot will automatically resume when:

- A `check_suite` success event fires for that PR
- The PR is still open
- The predecessor is still merged
- No `@merge-train stop` command was issued

This means "fix the CI and push" is sufficient to resume — no manual re-trigger needed.

---

## GitHub API Endpoints

| Operation | Endpoint | Method |
|-----------|----------|--------|
| Get repo metadata | `/repos/{o}/{r}` | GET |
| Squash-merge PR | `/repos/{o}/{r}/pulls/{n}/merge` | POST |
| Retarget PR base | `/repos/{o}/{r}/pulls/{n}` | PATCH |
| Get PR details | `/repos/{o}/{r}/pulls/{n}` | GET |
| List open PRs | `/repos/{o}/{r}/pulls` | GET |
| List PR comments | `/repos/{o}/{r}/issues/{n}/comments` | GET |
| Post comment | `/repos/{o}/{r}/issues/{n}/comments` | POST |
| Update comment | `/repos/{o}/{r}/issues/comments/{id}` | PATCH |
| Add reaction | `/repos/{o}/{r}/issues/comments/{id}/reactions` | POST |
| Get merge state | GraphQL `mergeStateStatus` | POST `/graphql` |

The "Get repo metadata" endpoint returns `default_branch` which is used throughout the cascade logic.

The "Update comment" endpoint is used to update optional human-readable status comments (not parsed for recovery).

**GraphQL for merge state**: The bot uses GitHub's GraphQL API to query `mergeStateStatus`, which encapsulates all branch protection checks (required status checks, required reviewers, etc.) into a single authoritative value:

```graphql
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      mergeable
      mergeStateStatus
    }
  }
}
```

---

## Authentication

### GitHub API

GitHub App installation token with permissions:

- `pull_requests`: write (merge, retarget, read, query `mergeStateStatus` via GraphQL, receive `pull_request_review` webhooks)
- `contents`: write (push via git)
- `checks`: read (receive `check_suite` webhooks to trigger re-evaluation)
- `issues`: write (comments, reactions)

### Git operations

The bot needs:

- Clone URL with authentication (HTTPS + token, or SSH + deploy key)
- GPG private key for commit signing
- Git user.name and user.email configuration

---

## Implementation Notes (Rust)

### Crate suggestions

- `octocrab` — GitHub API client
- `axum` — webhook HTTP server
- `tokio` — async runtime
- `tokio_util` — `CancellationToken` for cooperative cancellation
- `serde` — JSON (de)serialisation
- `tracing` — structured logging

We will simply shell out to `git` for Git operations.

### Core types

```rust
// ─────────────────────────────────────────────────────────────
// Newtypes for type safety
// ─────────────────────────────────────────────────────────────

/// Repository identifier
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct RepoId {
    owner: String,
    repo: String,
}

/// PR number (avoids mixing with other integers)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct PrNumber(u64);

/// Git commit SHA
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Sha(String);

// ─────────────────────────────────────────────────────────────
// State management types (see State Management section)
// ─────────────────────────────────────────────────────────────

/// State of the PR cache for a repository
enum RepoLifecycle {
    /// Never seen this repo before
    Unknown,
    /// Currently fetching full state from GitHub
    Bootstrapping {
        started_at: Instant,
        queued_events: Vec<GitHubEvent>,
    },
    /// Ready to process events incrementally
    Ready(RepoState),
    /// Bootstrap failed, will retry
    Failed {
        last_attempt: Instant,
        error: String,
    },
}

/// Per-repository cached state
struct RepoState {
    /// The repository's default branch (e.g., "main", "master", "develop")
    /// Fetched from GitHub during bootstrap; all "targets main" logic uses this
    default_branch: String,
    /// All known PRs (open + recently merged)
    prs: HashMap<PrNumber, CachedPr>,
    /// Reverse index: predecessor → set of descendants
    descendants: HashMap<PrNumber, HashSet<PrNumber>>,
	/// Active trains, keyed by root PR number.
	/// Loaded from the local state file during bootstrap.
	active_trains: HashMap<PrNumber, TrainRecord>,
    /// When bootstrap completed
    bootstrapped_at: Instant,
    /// Last re-sync time
    last_sync: Instant,
    /// Cache miss counter (triggers re-bootstrap if too high)
    miss_count: u32,
}

/// Global state across all repositories
struct GlobalState {
    repos: HashMap<RepoId, RepoLifecycle>,
}

// ─────────────────────────────────────────────────────────────
// Cached PR data
// ─────────────────────────────────────────────────────────────

/// A PR with all fields needed for cascade evaluation
struct CachedPr {
    number: PrNumber,
    head_ref: String,
    head_sha: Sha,
    base_ref: String,
    state: PrState,
    /// Merge readiness from GitHub's GraphQL `mergeStateStatus`
    merge_state: MergeStateStatus,
    /// Declared predecessor (from @merge-train predecessor comment)
    predecessor: Option<PrNumber>,
}

enum PrState {
    Open,
    Merged { merge_commit_sha: Sha },
    Closed,
}

/// GitHub's merge state status (from GraphQL `mergeStateStatus` field).
/// This encapsulates all branch protection checks into a single value.
enum MergeStateStatus {
    /// All requirements satisfied — ready to merge
    Clean,
    /// Non-required checks failing — still mergeable
    Unstable,
    /// Required checks not passing or missing approvals
    Blocked,
    /// Head branch behind base (when "require up-to-date" is enabled)
    Behind,
    /// Merge conflicts with base branch
    Dirty,
    /// GitHub Enterprise: has pre-receive hooks
    HasHooks,
    /// State not yet computed by GitHub
    Unknown,
}

	// ─────────────────────────────────────────────────────────────
	// Train state (persisted locally for restart recovery)
	// ─────────────────────────────────────────────────────────────

	/// Train record stored in the local state file.
	/// This is the authoritative source of truth for train state.
	#[derive(Serialize, Deserialize)]
	struct TrainRecord {
	    /// Schema version for forward compatibility
	    version: u32,
	    /// Current train state
	    state: TrainState,
	    /// PR currently being processed (the train's current root)
	    current_pr: PrNumber,
	    /// Current phase within the cascade step
	    cascade_phase: CascadePhase,
	    /// PR number of predecessor (for fetching via refs/pull/<n>/head during recovery)
	    predecessor_pr: Option<PrNumber>,
	    /// SHA of last squash commit (for reconciliation recovery)
	    last_squash_sha: Option<Sha>,
	    /// When the train was started (ISO 8601)
	    started_at: String,
	    /// When the train was stopped (if applicable)
	    stopped_at: Option<String>, // ISO 8601
	    /// Error details if aborted
	    error: Option<TrainError>,
	    /// Optional comment ID of a human-readable status comment (not parsed for recovery)
	    status_comment_id: Option<u64>,
	}

#[derive(Serialize, Deserialize)]
enum TrainState {
    Running,
    Stopped,
    WaitingCi,
    Aborted,
}

	#[derive(Serialize, Deserialize)]
	enum CascadePhase {
	    /// Not currently performing any operation
	    Idle,
	    /// Merging predecessor head and default branch into descendants
	    Preparing,
	    /// Preparation complete, about to squash-merge
	    SquashPending,
	    /// Squash complete, performing ours-strategy merges
	    Reconciling,
	}

#[derive(Serialize, Deserialize)]
struct TrainError {
    error_type: String,
    message: String,
}

	// ─────────────────────────────────────────────────────────────
	// Persisted repo state (stored on disk)
	// ─────────────────────────────────────────────────────────────

	/// JSON structure stored at `<state_dir>/<owner>/<repo>/snapshot.json`.
	#[derive(Serialize, Deserialize)]
	struct PersistedRepoSnapshot {
	    /// Schema version for forward-compatible migrations
	    schema_version: u32,
	    /// When this snapshot was last updated
	    snapshot_at: String, // ISO 8601
	    /// Byte offset in events.log at which this snapshot was taken.
	    /// On replay, seek to this offset and read forward.
	    log_position: u64,
	    /// Next sequence number to assign (monotonically increasing).
	    next_seq: u64,
	    /// Cached default branch name
	    default_branch: String,
	    /// Cached PR info, keyed by PR number (as string for JSON)
	    prs: HashMap<String, PersistedPr>,
	    /// Active trains, keyed by root PR number
	    active_trains: HashMap<String, TrainRecord>,
	}

	#[derive(Serialize, Deserialize)]
	struct PersistedPr {
	    /// Last known head SHA
	    head_sha: String,
	    /// Base branch name
	    base_ref: String,
	    /// Declared predecessor (from @merge-train predecessor)
	    predecessor: Option<u64>,
	    /// PR state: "open", "merged", "closed"
	    state: String,
	    /// When the PR was merged or closed (ISO 8601). Null for open PRs.
	    /// Used for retention-based pruning of the local state file.
	    closed_at: Option<String>,
	}

	// ─────────────────────────────────────────────────────────────
	// Event log entries (stored in events.log)
	// ─────────────────────────────────────────────────────────────

	/// Events appended to the event log (JSON Lines format).
	#[derive(Serialize, Deserialize)]
	struct StateEvent {
	    /// Monotonic sequence number (used for replay positioning)
	    seq: u64,
	    /// ISO 8601 timestamp
	    ts: String,
	    /// Event type discriminator
	    #[serde(flatten)]
	    payload: StateEventPayload,
	}

	#[derive(Serialize, Deserialize)]
	#[serde(tag = "type")]
	enum StateEventPayload {
	    // ─── Train lifecycle (always critical) ───
	    #[serde(rename = "train_started")]
	    TrainStarted { root_pr: u64 },
	    #[serde(rename = "train_stopped")]
	    TrainStopped { root_pr: u64 },
	    #[serde(rename = "train_completed")]
	    TrainCompleted { root_pr: u64 },
	    #[serde(rename = "train_aborted")]
	    TrainAborted { root_pr: u64, error: TrainError },

	    // ─── Phase transitions (always critical) ───
	    #[serde(rename = "phase_transition")]
	    PhaseTransition { train_root: u64, phase: String },
	    #[serde(rename = "squash_committed")]
	    SquashCommitted { train_root: u64, pr: u64, sha: String },

	    // ─── Intent/done pairs for irreversible operations ───
	    #[serde(rename = "intent_push_prep")]
	    IntentPushPrep { train_root: u64, branch: String, expected_sha: String },
	    #[serde(rename = "done_push_prep")]
	    DonePushPrep { train_root: u64, branch: String },
	    #[serde(rename = "intent_squash")]
	    IntentSquash { train_root: u64, pr: u64 },
	    #[serde(rename = "intent_push_reconcile")]
	    IntentPushReconcile { train_root: u64, branch: String, expected_sha: String },
	    #[serde(rename = "done_push_reconcile")]
	    DonePushReconcile { train_root: u64, branch: String },
	    #[serde(rename = "intent_retarget")]
	    IntentRetarget { train_root: u64, pr: u64, new_base: String },
	    #[serde(rename = "done_retarget")]
	    DoneRetarget { train_root: u64, pr: u64 },

	    // ─── Non-critical state updates (batched fsync) ───
	    #[serde(rename = "pr_merged")]
	    PrMerged { pr: u64, sha: String },
	    #[serde(rename = "pr_state_changed")]
	    PrStateChanged { pr: u64, state: String },
	    #[serde(rename = "predecessor_declared")]
	    PredecessorDeclared { pr: u64, predecessor: u64 },
	}

	impl StateEvent {
	    /// Returns true if this event should trigger fsync before continuing.
	    /// These are recovery-critical: without them, restart may corrupt state.
	    fn is_critical(&self) -> bool {
	        matches!(
	            self.payload,
	            // Train lifecycle
	            StateEventPayload::TrainStarted { .. }
	                | StateEventPayload::TrainStopped { .. }
	                | StateEventPayload::TrainCompleted { .. }
	                | StateEventPayload::TrainAborted { .. }
	            // Phase transitions
	                | StateEventPayload::PhaseTransition { .. }
	                | StateEventPayload::SquashCommitted { .. }
	            // Intent events (must be durable before performing operation)
	                | StateEventPayload::IntentPushPrep { .. }
	                | StateEventPayload::IntentSquash { .. }
	                | StateEventPayload::IntentPushReconcile { .. }
	                | StateEventPayload::IntentRetarget { .. }
	            // Done events (must be durable before considering operation complete)
	                | StateEventPayload::DonePushPrep { .. }
	                | StateEventPayload::DonePushReconcile { .. }
	                | StateEventPayload::DoneRetarget { .. }
	        )
	    }
	}

	impl PersistedRepoSnapshot {
	    /// Check if snapshot is stale (older than threshold)
	    fn is_stale(&self, threshold: Duration) -> bool {
	        let snapshot_time = DateTime::parse_from_rfc3339(&self.snapshot_at)
	            .map(|dt| dt.with_timezone(&Utc))
	            .ok();
	        match snapshot_time {
	            Some(dt) => Utc::now() - dt > chrono::Duration::from_std(threshold).unwrap_or_default(),
	            None => true, // Can't parse = treat as stale
	        }
	    }
	}

// ─────────────────────────────────────────────────────────────
// Stack topology (computed from cached state)
// ─────────────────────────────────────────────────────────────

/// A complete stack from root to tip (linear portion only).
///
/// Fan-out points terminate the stack: if a PR has multiple open descendants,
/// the stack ends at that PR. After it merges, each descendant becomes the
/// root of its own independent stack.
struct MergeStack {
    /// Ordered from root (index 0) to tip. Ends at fan-out points.
    prs: Vec<PrNumber>,
    /// Whether the stack has been started
    started: bool,
    /// Whether the stack has been stopped
    stopped: bool,
}

/// Result of evaluating what action to take on a stack
enum StackAction {
    /// Nothing to do (not started, or waiting on CI)
    Idle,
    /// Ready to merge this PR and cascade
    ReadyToMerge { pr_number: PrNumber },
    /// Cascade is blocked
    Blocked { pr_number: PrNumber, reason: BlockReason },
}

enum BlockReason {
    /// GitHub reports BLOCKED (required checks/approvals not satisfied)
    Blocked,
    /// GitHub reports BEHIND (head branch behind base, strict mode)
    Behind,
    /// GitHub reports DIRTY (merge conflicts)
    MergeConflict,
    /// User issued @merge-train stop
    Stopped,
}

/// Outcome of attempting a cascade step
enum CascadeStepOutcome {
    /// Successfully merged, cascade continues
    Merged { pr_number: PrNumber },
    /// Waiting for CI on next PR
    WaitingOnCi { pr_number: PrNumber },
    /// Stack fully merged (no descendants)
    Complete,
    /// Fan-out: multiple descendants, each becomes an independent root
    FanOut { descendants: Vec<PrNumber> },
    /// Something went wrong
    Aborted { pr_number: PrNumber, reason: AbortReason },
}

enum AbortReason {
    MergeConflict { details: String },
    PushRejected { details: String },
    PrClosed,
    CiFailed,
    CycleDetected,
    ApprovalWithdrawn,
    ApiError { details: String },
}

// ─────────────────────────────────────────────────────────────
// Events (both GitHub webhooks and internal)
// ─────────────────────────────────────────────────────────────

/// Event priority (higher = processed first)
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum EventPriority {
    /// Normal events (check_suite, pull_request, etc.)
    Normal = 0,
    /// Stop commands take priority to allow humans to halt cascades promptly
    Stop = 1,
}

/// Events that go through the per-repo processing queue
struct QueuedEvent {
    priority: EventPriority,
    payload: QueuedEventPayload,
}

enum QueuedEventPayload {
    /// Webhook from GitHub
    GitHub(GitHubEvent),
    /// Internal: trigger periodic re-sync (repo is implicit — one queue per repo)
    PeriodicSync,
    /// Internal: poll active trains for missed webhook recovery (hourly)
    PollActiveTrains,
}
```

### Event queue and server

```rust
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, Mutex};
use tokio_util::sync::CancellationToken;

// ─────────────────────────────────────────────────────────────
// Per-repo queue and worker handle
// ─────────────────────────────────────────────────────────────

/// State for a single repo's event processing
struct RepoHandle {
    /// Channel to send events to this repo's worker
    sender: mpsc::UnboundedSender<QueuedEvent>,
    /// Channel for immediate stack cancellation requests (processed via select!)
    cancel_tx: mpsc::UnboundedSender<CancelStackRequest>,
}

/// Request to cancel operations for a specific stack
struct CancelStackRequest {
    /// Any PR in the stack — the worker will look up the root
    pr_number: PrNumber,
}

/// Central dispatcher that routes events to per-repo workers
struct Dispatcher {
    /// Per-repo handles, created on demand
    repos: HashMap<RepoId, RepoHandle>,
    /// Shared application context
    ctx: AppContext,
}

impl Dispatcher {
    fn new(ctx: AppContext) -> Self {
        Self {
            repos: HashMap::new(),
            ctx,
        }
    }

    /// Get or create a handle for the given repo
    fn get_or_create_handle(&mut self, repo_id: &RepoId) -> &mut RepoHandle {
        self.repos.entry(repo_id.clone()).or_insert_with(|| {
            let (event_tx, event_rx) = mpsc::unbounded_channel();
            let (cancel_tx, cancel_rx) = mpsc::unbounded_channel();

            // Spawn a dedicated worker for this repo
            let worker_ctx = self.ctx.clone();
            let worker_repo_id = repo_id.clone();
            tokio::spawn(async move {
                repo_worker(worker_repo_id, event_rx, cancel_rx, worker_ctx).await;
            });

            RepoHandle {
                sender: event_tx,
                cancel_tx,
            }
        })
    }

    /// Dispatch an event to the appropriate repo's queue
    fn dispatch(&mut self, repo_id: &RepoId, mut event: QueuedEvent) {
        // If this is a stop command, send immediate cancel request for the target stack.
        // The worker will resolve the PR number to its stack root and cancel that stack's token.
        if let Some(pr_number) = extract_stop_pr(&event) {
            if let Some(handle) = self.repos.get(repo_id) {
                let _ = handle.cancel_tx.send(CancelStackRequest { pr_number });
            }
        }

        // Try to send; if worker died (idle timeout), recreate it
        loop {
            let handle = self.get_or_create_handle(repo_id);
            match handle.sender.send(event) {
                Ok(()) => return,
                Err(mpsc::error::SendError(returned_event)) => {
                    // Worker shut down — remove stale handle and retry with fresh worker
                    self.repos.remove(repo_id);
                    event = returned_event;
                }
            }
        }
    }
}

/// Extract PR number from a stop command event
fn extract_stop_pr(event: &QueuedEvent) -> Option<PrNumber> {
    match &event.payload {
        QueuedEventPayload::GitHub(GitHubEvent::IssueComment(c)) => {
            if parse_command(&c.body) == Some(Command::Stop) {
                Some(c.issue_number)  // PR number from issue comment
            } else {
                None
            }
        }
        _ => None,
    }
}

// ─────────────────────────────────────────────────────────────
// Per-repo priority queue (used within each worker)
// ─────────────────────────────────────────────────────────────

/// Priority queue for events within a single repo
struct RepoPriorityQueue {
    heap: BinaryHeap<Reverse<(Reverse<EventPriority>, u64, QueuedEvent)>>,
    sequence: u64,
}

impl RepoPriorityQueue {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            sequence: 0,
        }
    }

    fn push(&mut self, event: QueuedEvent) {
        let seq = self.sequence;
        self.sequence += 1;
        self.heap.push(Reverse((Reverse(event.priority), seq, event)));
    }

    fn pop(&mut self) -> Option<QueuedEvent> {
        self.heap.pop().map(|Reverse((_, _, event))| event)
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────
// Per-repo worker
// ─────────────────────────────────────────────────────────────

/// Per-stack cancellation token management
struct StackCancellation {
    tokens: HashMap<PrNumber, CancellationToken>,
}

impl StackCancellation {
    fn new() -> Self {
        Self { tokens: HashMap::new() }
    }

    /// Get or create a cancellation token for a stack (keyed by root PR)
    fn token_for_stack(&mut self, root: PrNumber) -> CancellationToken {
        self.tokens.entry(root)
            .or_insert_with(CancellationToken::new)
            .clone()
    }

    /// Cancel a stack and create a fresh token for future operations
    fn cancel(&mut self, root: PrNumber) {
        if let Some(token) = self.tokens.get(&root) {
            token.cancel();
        }
        self.tokens.insert(root, CancellationToken::new());
    }
}

/// Worker loop for a single repository
async fn repo_worker(
    repo_id: RepoId,
    mut event_rx: mpsc::UnboundedReceiver<QueuedEvent>,
    mut cancel_rx: mpsc::UnboundedReceiver<CancelStackRequest>,
    ctx: AppContext,
) {
    let mut queue = RepoPriorityQueue::new();
    let mut repo_state: Option<RepoState> = None;
    let mut stack_cancel = StackCancellation::new();

    // Idle timeout — worker shuts down if no events for this duration
    let idle_timeout = Duration::from_secs(3600);

    loop {
        // Handle any pending cancel requests immediately
        while let Ok(req) = cancel_rx.try_recv() {
            if let Some(ref state) = repo_state {
                if let Some(root) = state.find_stack_root(req.pr_number) {
                    stack_cancel.cancel(root);
                    tracing::info!(?repo_id, ?root, "cancelled stack");
                }
            }
        }

        // Restart recovery: drain any unprocessed spooled deliveries from disk.
        // (In a real implementation, this would parse the spooled payloads into QueuedEvents
        // and dedupe using the delivery ID stored in the repo state.)
        if let Ok(pending) = ctx.spool.drain_pending(&repo_id).await {
            for event in pending {
                queue.push(event);
            }
        }

        // Drain all pending events into priority queue
        while let Ok(event) = event_rx.try_recv() {
            queue.push(event);
        }

        // Process highest-priority event
        if let Some(event) = queue.pop() {
            let result = process_event_for_repo(
                &repo_id,
                event,
                &mut repo_state,
                &mut stack_cancel,
                &ctx,
            ).await;

            match result {
                Err(Error::Cancelled) => {
                    // Operation was cancelled by stop command — this is expected, not an error
                    tracing::info!(?repo_id, "operation cancelled");
                }
                Err(e) => {
                    tracing::error!(?repo_id, ?e, "failed to process event");
                }
                Ok(()) => {}
            }
            continue;
        }

        // No events in queue — wait for more (with timeout)
        // Use select! to also handle cancel requests while waiting
        tokio::select! {
            biased;

            Some(req) = cancel_rx.recv() => {
                if let Some(ref state) = repo_state {
                    if let Some(root) = state.find_stack_root(req.pr_number) {
                        stack_cancel.cancel(root);
                        tracing::info!(?repo_id, ?root, "cancelled stack");
                    }
                }
            }

            result = tokio::time::timeout(idle_timeout, event_rx.recv()) => {
                match result {
                    Ok(Some(event)) => {
                        queue.push(event);
                    }
                    Ok(None) => {
                        // Channel closed, shut down worker
                        tracing::info!(?repo_id, "repo worker shutting down: channel closed");
                        break;
                    }
                    Err(_) => {
                        // Idle timeout — shut down worker (will be recreated on next event)
                        tracing::info!(?repo_id, "repo worker shutting down: idle timeout");
                        break;
                    }
                }
            }
        }
    }
}

/// Process a single event for a repo
async fn process_event_for_repo(
    repo_id: &RepoId,
    event: QueuedEvent,
    repo_state: &mut Option<RepoState>,
    stack_cancel: &mut StackCancellation,
    ctx: &AppContext,
) -> Result<()> {
    // Ensure repo is bootstrapped (not stack-specific, no cancellation)
    let just_bootstrapped = repo_state.is_none();
    if just_bootstrapped {
        *repo_state = Some(bootstrap_repo(repo_id, ctx).await?);
    }

    let state = repo_state.as_mut().unwrap();

    // Recovery: if we just bootstrapped, check for in-progress trains that need resuming
    if just_bootstrapped {
        recover_in_progress_trains(state, stack_cancel, ctx).await?;
    }

    match event.payload {
        QueuedEventPayload::GitHub(gh_event) => {
            process_github_event(gh_event, state, stack_cancel, ctx).await
        }
        QueuedEventPayload::PeriodicSync => {
            resync_repo(repo_id, state, ctx).await
        }
        QueuedEventPayload::PollActiveTrains => {
            poll_active_trains(repo_id, state, stack_cancel, ctx).await
        }
    }
}

/// Poll active trains to recover from missed webhooks.
/// Refreshes merge state for each train's frontier PR and triggers cascade if ready.
async fn poll_active_trains(
    repo_id: &RepoId,
    state: &mut RepoState,
    stack_cancel: &mut StackCancellation,
    ctx: &AppContext,
) -> Result<()> {
    let github = ctx.github_for_repo(repo_id);

    for (root_pr, train) in &state.active_trains {
        // Skip stopped/aborted trains
        if !matches!(train.state, TrainState::Running | TrainState::WaitingCi) {
            continue;
        }

        // Find the frontier PR (the one we're waiting on)
        let frontier_pr = train.current_pr;
        let Some(cached_pr) = state.prs.get_mut(&frontier_pr) else {
            continue;
        };

        // Refresh merge state from GitHub
        let fresh_merge_state = github.get_merge_state(frontier_pr.0).await?;
        let old_merge_state = cached_pr.merge_state.clone();
        cached_pr.merge_state = fresh_merge_state.clone();

        // If state changed, log it for observability
        if old_merge_state != fresh_merge_state {
            tracing::info!(
                ?repo_id,
                train_root = root_pr.0,
                frontier_pr = frontier_pr.0,
                ?old_merge_state,
                ?fresh_merge_state,
                trigger = "poll",
                "merge state changed (detected via poll fallback)"
            );
        }

        // If now ready, evaluate cascade
        if matches!(fresh_merge_state, MergeStateStatus::Clean | MergeStateStatus::Unstable) {
            tracing::info!(
                ?repo_id,
                train_root = root_pr.0,
                trigger = "poll",
                "evaluating cascade (poll fallback)"
            );
            // Cascade evaluation would happen here
            // (In practice, this calls evaluate_cascade with the updated state)
        }
    }

    Ok(())
}

/// After bootstrap, check for trains that were mid-cascade and resume them.
async fn recover_in_progress_trains(
    repo_state: &mut RepoState,
    stack_cancel: &mut StackCancellation,
    ctx: &AppContext,
) -> Result<()> {
    // active_trains is keyed by the stack's current root PR number
    for (train_root, state) in &repo_state.active_trains {

        // Only recover trains that were actively running (not stopped/aborted)
        if !matches!(state.state, TrainState::Running | TrainState::WaitingCi) {
            continue;
        }

        // Get cancellation token for this specific stack
        let cancel = stack_cancel.token_for_stack(*train_root);

        match state.cascade_phase {
            CascadePhase::Idle => {
                // Re-evaluate readiness of current_pr
                // This will be handled by the triggering event, no special action needed
            }
            CascadePhase::Preparing => {
                // Re-run preparation (idempotent - if already pushed, merges are no-ops)
                resume_preparation(state.current_pr, repo_state, &cancel, ctx).await?;
            }
            CascadePhase::SquashPending => {
                // Check if the PR was already squash-merged
                let pr = repo_state.prs.get(&state.current_pr);
                match pr.map(|p| &p.state) {
                    Some(PrState::Merged { .. }) => {
                        // Already merged, move to reconciliation
                        resume_reconciliation(state.current_pr, state.last_squash_sha.as_ref(), repo_state, &cancel, ctx).await?;
                    }
                    Some(PrState::Open) => {
                        // Not yet merged, proceed with squash
                        resume_squash(state.current_pr, repo_state, &cancel, ctx).await?;
                    }
                    _ => {
                        // PR closed without merge or missing - abort
                        abort_train(*train_root, "PR closed or missing during recovery", repo_state, ctx).await?;
                    }
                }
            }
            CascadePhase::Reconciling => {
                // Use last_squash_sha to complete reconciliation
                let squash_sha = state.last_squash_sha.as_ref()
                    .ok_or(Error::MissingRecoverySha)?;
                resume_reconciliation(state.current_pr, Some(squash_sha), repo_state, &cancel, ctx).await?;
            }
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// HTTP handler
// ─────────────────────────────────────────────────────────────

/// The HTTP handler — validates, durably spools the delivery, then dispatches to the per-repo queue
async fn webhook_endpoint(
    State(dispatcher): State<Arc<Mutex<Dispatcher>>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Validate signature
    if !verify_signature(&headers, &body) {
        return StatusCode::UNAUTHORIZED;
    }

    // Parse event (needed for repo_id routing)
    let event = match parse_webhook_event(&headers, &body) {
        Ok(e) => e,
        Err(_) => return StatusCode::BAD_REQUEST,
    };

    let repo_id = event.repo_id();

    // Extract delivery ID for durable de-duplication
    let delivery_id = headers
        .get("X-GitHub-Delivery")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("<missing>");

    // Durably spool delivery to disk before we ACK it
    // (Clone ctx without holding the dispatcher lock across await points)
    let ctx = {
        let d = dispatcher.lock().await;
        d.ctx.clone()
    };
    if let Err(e) = ctx.spool.write_delivery(&repo_id, delivery_id, &headers, &body).await {
        tracing::error!(?repo_id, delivery_id, ?e, "failed to spool webhook delivery");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }

    // Classify priority: stop commands get higher priority
    let priority = if is_stop_command(&event) {
        EventPriority::Stop
    } else {
        EventPriority::Normal
    };

    // Dispatch to per-repo queue
    {
        let mut d = dispatcher.lock().await;
        d.dispatch(&repo_id, QueuedEvent {
            priority,
            payload: QueuedEventPayload::GitHub(event),
        });
    }

    StatusCode::ACCEPTED
}

// ─────────────────────────────────────────────────────────────
// Periodic timers
// ─────────────────────────────────────────────────────────────

/// Periodic timers for re-sync and active train polling
async fn periodic_timers(
    dispatcher: Arc<Mutex<Dispatcher>>,
    resync_interval: Duration,  // e.g., 1 hour
    poll_interval: Duration,    // e.g., 1 hour (with jitter applied per-repo)
) {
    let mut resync_ticker = tokio::time::interval(resync_interval);
    let mut poll_ticker = tokio::time::interval(poll_interval);

    loop {
        tokio::select! {
            _ = resync_ticker.tick() => {
                let mut d = dispatcher.lock().await;
                let repo_ids: Vec<RepoId> = d.repos.keys().cloned().collect();
                for repo_id in repo_ids {
                    d.dispatch(&repo_id, QueuedEvent {
                        priority: EventPriority::Normal,
                        payload: QueuedEventPayload::PeriodicSync,
                    });
                }
            }
            _ = poll_ticker.tick() => {
                // Poll active trains to recover from missed webhooks
                let mut d = dispatcher.lock().await;
                let repo_ids: Vec<RepoId> = d.repos.keys().cloned().collect();
                for repo_id in repo_ids {
                    d.dispatch(&repo_id, QueuedEvent {
                        priority: EventPriority::Normal,
                        payload: QueuedEventPayload::PollActiveTrains,
                    });
                }
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Main entry point
// ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let ctx = AppContext::from_config().await;
    let dispatcher = Arc::new(Mutex::new(Dispatcher::new(ctx)));

    // Spawn periodic timers for re-sync (hourly) and active train polling (hourly)
    let resync_interval = Duration::from_secs(3600);
    let poll_interval = Duration::from_secs(3600);
    tokio::spawn(periodic_timers(dispatcher.clone(), resync_interval, poll_interval));

    // Run the HTTP server
    let app = Router::new()
        .route("/webhook", post(webhook_endpoint))
        .with_state(dispatcher);

    axum::serve(listener, app).await.unwrap();
}
```

### Event processing

```rust
/// Process a GitHub event for a repo (called by the per-repo worker)
async fn process_github_event(
    event: GitHubEvent,
    repo_state: &mut RepoState,
    stack_cancel: &mut StackCancellation,
    ctx: &AppContext,
) -> Result<()> {
    // Apply incremental state update
    repo_state.apply_event(&event)?;

    // Compute stacks and detect missing predecessors
    let (stacks, missing_predecessors) = repo_state.compute_stacks();

    // Handle cache misses: fetch missing predecessors
    for missing_pr in missing_predecessors {
        let github = ctx.github_for_repo(&event.repo_id());
        if let Ok(pr) = github.get_pr(missing_pr).await {
            let cached = build_cached_pr_minimal(&pr);
            repo_state.prs.insert(missing_pr, cached);
        }
        repo_state.miss_count += 1;

        // Too many misses indicates significant drift — trigger re-bootstrap
        if repo_state.miss_count >= 5 {
            return Err(Error::TooManyMisses);
        }
    }

    match event {
        GitHubEvent::IssueComment(c) => {
            if let Some(cmd) = parse_command(&c.body) {
                match cmd {
                    Command::Predecessor(n) => {
                        // State already updated; just ack
                        ack_predecessor(&c, ctx).await?;
                    }
                    Command::Start => {
                        evaluate_cascade(&stacks, repo_state, stack_cancel, ctx).await?;
                    }
                    Command::Stop => {
                        // Find which stack this PR belongs to
                        let root = repo_state.find_stack_root(c.issue_number);

                        // In-flight operations were already cancelled by the dispatcher.
                        // Remove this stack's worktree to clean up any partial merge state.
                        // This only affects this stack — other stacks' worktrees are untouched.
                        if let Some(root) = root {
                            let git = ctx.git_for_repo(&repo_id);
                            git.remove_worktree(root).await?;
                        }

                        // Persist stop in local state and ack
                        ack_stop(&c, repo_state, ctx).await?;
                    }
                }
            }
        }
        GitHubEvent::PullRequest(pr) if pr.action == "closed" => {
            if pr.merged {
                evaluate_cascade(&stacks, repo_state, stack_cancel, ctx).await?;
            } else {
                notify_orphaned_descendants(pr.number, &stacks, ctx).await?;
            }
        }
        GitHubEvent::CheckSuite(cs) if cs.conclusion == Some("success") => {
            evaluate_cascade(&stacks, repo_state, stack_cancel, ctx).await?;
        }
        GitHubEvent::PullRequestReview(r) if r.action == "submitted" && r.review.state == "approved" => {
            evaluate_cascade(&stacks, repo_state, stack_cancel, ctx).await?;
        }
        _ => {}
    }

    Ok(())
}
```

### Stack computation (from cached state)

```rust
impl RepoState {
    /// Compute stacks from cached PR data.
    /// Returns both the stacks and any missing predecessor PRs that should be fetched.
    fn compute_stacks(&self) -> (Vec<MergeStack>, Vec<PrNumber>) {
        let mut missing_predecessors = Vec::new();

        // Check for missing predecessors in all open PRs
        for pr in self.prs.values().filter(|p| p.state == PrState::Open) {
            if let Some(pred) = pr.predecessor {
                if !self.prs.contains_key(&pred) {
                    missing_predecessors.push(pred);
                }
            }
        }

        // Find all root PRs (target default branch, no predecessor or predecessor merged)
        let roots: Vec<PrNumber> = self.prs.values()
            .filter(|pr| pr.state == PrState::Open)
            .filter(|pr| self.is_root(pr))
            .map(|pr| pr.number)
            .collect();

        // Build stack for each root
        let stacks = roots.into_iter()
            .filter_map(|root| self.build_stack(root))
            .collect();

        (stacks, missing_predecessors)
    }

    /// A PR is a root if it targets the default branch AND either:
    /// - Has no predecessor declaration, OR
    /// - Its predecessor has been merged (and the PR was retargeted to default branch)
    ///
    /// IMPORTANT: We always require base_ref == default_branch. A PR whose predecessor
    /// merged but which hasn't been retargeted yet is NOT a root — it's in a transitional
    /// state and should not be squash-merged until retargeting completes.
    fn is_root(&self, pr: &CachedPr) -> bool {
        // Must target the default branch to be a root
        if pr.base_ref != self.default_branch {
            return false;
        }

        match pr.predecessor {
            // No predecessor + targets default branch = root
            None => true,
            Some(pred) => {
                match self.prs.get(&pred) {
                    // Predecessor merged + we target default branch = root
                    Some(p) if matches!(p.state, PrState::Merged { .. }) => true,
                    // Predecessor exists but not merged = not a root (still stacked)
                    Some(_) => false,
                    // Predecessor missing from cache = NOT a root (data integrity issue)
                    // This triggers a cache miss and potential re-bootstrap
                    None => false,
                }
            }
        }
    }

    fn build_stack(&self, root: PrNumber) -> Option<MergeStack> {
        let mut prs = vec![root];
        let mut visited = HashSet::new();
        visited.insert(root);

        // Walk descendants using reverse index
        let mut current = root;
        while let Some(descendants) = self.descendants.get(&current) {
            // Find open descendants
            let open_descendants: Vec<_> = descendants.iter()
                .filter(|n| self.prs.get(n).map(|p| p.state == PrState::Open).unwrap_or(false))
                .collect();

            // If multiple open descendants (fan-out), stack ends here.
            // Each descendant will become its own root after this PR merges.
            if open_descendants.len() != 1 {
                break;
            }

            let &n = open_descendants[0];
            if visited.contains(&n) {
                break; // Cycle detected
            }

            prs.push(n);
            visited.insert(n);
            current = n;
        }

        // Train state is a property of the stack (stored on root), not individual PRs
        let (started, stopped) = match self.active_trains.get(&root) {
            Some(train) => (
                matches!(train.state, TrainState::Running | TrainState::WaitingCi),
                matches!(train.state, TrainState::Stopped),
            ),
            None => (false, false),
        };

        Some(MergeStack {
            prs,
            started,
            stopped,
        })
    }
}
```

### Bootstrap (two-phase: disk then fallback)

```rust
/// Bootstrap is not stack-specific — it happens before we know what stacks exist.
/// Therefore it does not support cancellation (cancellation is stack-scoped).
async fn bootstrap_repo(
    repo_id: &RepoId,
    ctx: &AppContext,
) -> Result<RepoState> {
    let github = ctx.github_for_repo(repo_id);
    let state_path = ctx.state_path_for_repo(repo_id);

    // Phase 1: Try to load from disk (fast path)
    match load_from_disk(&state_path, ctx).await {
        Ok(Some(persisted)) if !persisted.is_stale(ctx.config.staleness_threshold) => {
            let state = build_state_from_persisted(persisted)?;
            let state = refresh_merge_states(state, &github).await?;
            return Ok(state);
        }
        Ok(Some(_stale)) => {
            tracing::info!(?repo_id, "state file stale, performing full bootstrap");
        }
        Ok(None) => {
            tracing::info!(?repo_id, "state file not found, performing full bootstrap");
        }
        Err(e) => {
            tracing::warn!(?repo_id, ?e, "state file load failed, performing full bootstrap");
        }
    }

    // Phase 2: Full API crawl (fallback path)
    let state = full_bootstrap(repo_id, &github, ctx).await?;

    // Persist the freshly bootstrapped state
    persist_state_file(repo_id, &state, &state_path).await?;

    Ok(state)
}

/// Load state from disk (fast path): snapshot + event log replay
async fn load_from_disk(
    state_dir: &Path,
    _ctx: &AppContext,
) -> Result<Option<PersistedRepoSnapshot>> {
    let snapshot_path = state_dir.join("snapshot.json");
    let events_path = state_dir.join("events.log");

    // Load snapshot
    let snapshot_bytes = match tokio::fs::read(&snapshot_path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut snapshot: PersistedRepoSnapshot = serde_json::from_slice(&snapshot_bytes)?;

    // Replay events from log starting at log_position
    if let Ok(mut file) = tokio::fs::File::open(&events_path).await {
        use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};

        // Seek to the position recorded in snapshot
        file.seek(std::io::SeekFrom::Start(snapshot.log_position)).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut last_valid_pos = snapshot.log_position;
        while let Some(line) = lines.next_line().await? {
            if line.is_empty() {
                last_valid_pos += 1; // newline
                continue;
            }
            match serde_json::from_str::<StateEvent>(&line) {
                Ok(event) => {
                    apply_event(&mut snapshot, &event);
                    snapshot.next_seq = event.seq + 1;
                    last_valid_pos += line.len() as u64 + 1; // +1 for newline
                }
                Err(_) => {
                    // Partial/corrupted line at end of file (crash mid-write)
                    // Truncate at last valid position and continue
                    tracing::warn!(
                        path = %events_path.display(),
                        offset = last_valid_pos,
                        "truncating corrupted trailing line in event log"
                    );
                    tokio::fs::OpenOptions::new()
                        .write(true)
                        .open(&events_path)
                        .await?
                        .set_len(last_valid_pos)
                        .await?;
                    break;
                }
            }
        }
    }

    Ok(Some(snapshot))
}

/// Full API crawl (fallback path)
async fn full_bootstrap(
    repo_id: &RepoId,
    github: &GitHubClient,
    ctx: &AppContext,
) -> Result<RepoState> {
    // Fetch repository metadata to get default branch
    let repo_info = github.get_repo().await?;
    let default_branch = repo_info.default_branch;

    // Fetch all open PRs
    let open_prs = github.list_open_prs().await?;

    // Fetch recently merged PRs (for predecessor lookups)
    let merged_prs = github.list_recently_merged_prs().await?;

    // Parallel fetch: comments and merge state for each PR
    let all_prs: Vec<_> = open_prs.iter().chain(merged_prs.iter()).collect();

    let cached_prs = futures::stream::iter(all_prs)
        .map(|pr| async {
            let (comments, merge_state) = tokio::join!(
                github.list_comments(pr.number),
                github.get_merge_state(pr.number),
            );
            build_cached_pr(pr, comments?, merge_state?)
        })
        .buffer_unordered(10)
        .try_collect::<Vec<_>>()
        .await?;

    // Build state
    let mut prs = HashMap::new();
    let mut descendants = HashMap::new();
    let active_trains = HashMap::new();

    for (pr, comments) in cached_prs {
        if let Some(pred) = pr.predecessor {
            descendants.entry(pred).or_insert_with(HashSet::new).insert(pr.number);
        }
        prs.insert(pr.number, pr);
    }

    Ok(RepoState {
        default_branch,
        prs,
        descendants,
        active_trains,
        state_path: ctx.state_path_for_repo(repo_id),
        bootstrapped_at: Instant::now(),
        last_sync: Instant::now(),
        last_persisted: None,
        dirty: true, // Will be persisted after bootstrap
        miss_count: 0,
    })
}
```

### State file persistence

```rust
/// Persist state to disk via atomic rename (write temp → fsync → rename).
async fn persist_state_file(
    repo_id: &RepoId,
    state: &RepoState,
    state_path: &Path,
) -> Result<()> {
    let snapshot = build_snapshot(state);
    let bytes = serde_json::to_vec_pretty(&snapshot)?;
    atomic_write(state_path, &bytes).await?;
    tracing::debug!(?repo_id, path = %state_path.display(), "snapshot persisted");
    Ok(())
}

/// Convert in-memory state to its on-disk JSON representation.
fn build_snapshot(state: &RepoState) -> PersistedRepoSnapshot {
    // Omitted — straightforward mapping from RepoState → PersistedRepoSnapshot
    todo!()
}

/// Atomically write bytes to `path`.
/// (Write to a temp file in the same directory, fsync, rename, fsync dir).
async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    // Omitted — standard atomic write pattern
    todo!()
}
```

### Git operations

```rust
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

struct GitOperations {
    /// Base directory containing clone/ and worktrees/ subdirectories
    base_dir: PathBuf,
    repo_url: String,
    signing_key: String,
}

impl GitOperations {
    /// Phase 1: Preparation — bring descendant up to date with predecessor and default branch.
    /// Must be called BEFORE squash-merging the predecessor into the default branch.
    /// This ensures the descendant has all content from the default branch, so the later
    /// ours-strategy merge doesn't cause content to be lost.
    async fn prepare_descendant(
        &self,
        descendant_branch: &str,
        predecessor_pr_number: u64,
        default_branch: &str,
        cancel: &CancellationToken,
    ) -> Result<String> {
        // Fetch everything we need
        self.run_git(&["fetch", "origin", descendant_branch, default_branch], cancel).await?;

        // Fetch the predecessor's head via GitHub's PR ref (reliable even after branch deletion).
        // Fetching by raw SHA is unreliable — it depends on uploadpack.allowReachableSHA1InWant.
        let pr_ref = format!("refs/pull/{}/head", predecessor_pr_number);
        let local_ref = format!("refs/remotes/origin/pr/{}", predecessor_pr_number);
        self.run_git(&["fetch", "origin", &format!("{}:{}", pr_ref, local_ref)], cancel).await?;

        // Checkout descendant
        self.run_git(&["checkout", descendant_branch], cancel).await?;

        // Merge predecessor commit (regular merge)
        let merge_ref = format!("origin/pr/{}", predecessor_pr_number);
        self.run_git(&[
            "merge",
            &merge_ref,
            "--no-edit",
            "-m", &format!("Merge predecessor PR #{} into {} (merge-train prep)", predecessor_pr_number, descendant_branch),
        ], cancel).await?;

        // Merge default branch (regular merge) — critical for correctness!
        // Without this, any independent changes on the default branch would be reverted
        // when the descendant is later squash-merged.
        let origin_default = format!("origin/{}", default_branch);
        self.run_git(&[
            "merge",
            &origin_default,
            "--no-edit",
            "-m", &format!("Merge {} into {} (merge-train prep)", default_branch, descendant_branch),
        ], cancel).await?;

        // Push
        self.run_git(&["push", "origin", descendant_branch], cancel).await?;

        self.run_git(&["rev-parse", "HEAD"], cancel).await
    }

    /// Phase 2: Reconciliation — merge the squash commit into descendant with ours strategy.
    /// Must be called AFTER squash-merging the predecessor into the default branch.
    /// Adds the squash commit as a parent without changing tree content.
    /// IMPORTANT: Uses the specific squash commit SHA, not origin/<default_branch>, to avoid
    /// accidentally marking the branch as up-to-date with commits that landed
    /// after the squash.
    async fn reconcile_descendant(
        &self,
        descendant_branch: &str,
        squash_commit_sha: &str,
        default_branch: &str,
        cancel: &CancellationToken,
    ) -> Result<String> {
        // Fetch the squash commit and descendant branch
        self.run_git(&["fetch", "origin", descendant_branch], cancel).await?;
        self.run_git(&["fetch", "origin", squash_commit_sha], cancel).await?;

        // Checkout descendant
        self.run_git(&["checkout", descendant_branch], cancel).await?;

        // Merge the squash commit with ours strategy — keeps our tree, adds it as parent
        self.run_git(&[
            "merge",
            squash_commit_sha,
            "--strategy", "ours",
            "--no-edit",
            "-m", &format!("Relate {} with squash commit (merge-train)", descendant_branch),
        ], cancel).await?;

        // Now do a regular merge of origin/<default_branch> to pick up any subsequent commits
        let origin_default = format!("origin/{}", default_branch);
        self.run_git(&["fetch", "origin", default_branch], cancel).await?;
        self.run_git(&[
            "merge",
            &origin_default,
            "--no-edit",
            "-m", &format!("Merge {} into {} (merge-train catch-up)", default_branch, descendant_branch),
        ], cancel).await?;

        // Push
        self.run_git(&["push", "origin", descendant_branch], cancel).await?;

        self.run_git(&["rev-parse", "HEAD"], cancel).await
    }

    /// Hard-reset the local repo to the default branch.
    /// Used during stop/cancellation to clean up any partial merge state.
    /// Does NOT use cancellation token — this cleanup must complete.
    async fn reset_to_default_branch(&self, default_branch: &str) -> Result<()> {
        let origin_default = format!("origin/{}", default_branch);

        // Fetch latest default branch
        self.run_git_uncancellable(&["fetch", "origin", default_branch]).await?;

        // Abort any in-progress merge
        self.run_git_uncancellable(&["merge", "--abort"]).await.ok(); // May fail if no merge in progress

        // Hard reset to origin/default
        self.run_git_uncancellable(&["reset", "--hard", &origin_default]).await?;

        // Clean up any untracked files
        self.run_git_uncancellable(&["clean", "-fd"]).await?;

        Ok(())
    }

    /// Run a git command without cancellation support (for cleanup operations).
    async fn run_git_uncancellable(&self, args: &[&str]) -> Result<String> {
        let output = Command::new("git")
            .args(args)
            .current_dir(&self.workdir)
            .env("GIT_COMMITTER_NAME", "merge-train[bot]")
            .env("GIT_COMMITTER_EMAIL", "merge-train@example.com")
            .output()
            .await?;

        if !output.status.success() {
            return Err(GitError::CommandFailed {
                args: args.join(" "),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            }.into());
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Run a git command with cancellation support.
    /// If the token is cancelled, the child process is killed and Err(Cancelled) is returned.
    async fn run_git(&self, args: &[&str], cancel: &CancellationToken) -> Result<String> {
        let mut child = Command::new("git")
            .args(args)
            .current_dir(&self.workdir)
            .env("GIT_COMMITTER_NAME", "merge-train[bot]")
            .env("GIT_COMMITTER_EMAIL", "merge-train@example.com")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        // Race: wait for command to complete OR cancellation
        tokio::select! {
            biased;  // Check cancellation first

            _ = cancel.cancelled() => {
                // Kill the child process
                child.kill().await.ok();
                Err(Error::Cancelled)
            }

            result = child.wait_with_output() => {
                let output = result?;
                if !output.status.success() {
                    return Err(GitError::CommandFailed {
                        args: args.join(" "),
                        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                    }.into());
                }
                Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
            }
        }
    }
}
```

### Per-stack worktrees

Each stack gets its own isolated git worktree, enabling true stack isolation for the stop command and laying groundwork for potential future parallel cascade operations.

**Directory structure:**

```
/var/lib/merge-train/repos/
  owner-repo/
    clone/             # Shared bare clone (object store)
    worktrees/
      stack-123/       # Worktree for stack rooted at PR #123
      stack-456/       # Worktree for stack rooted at PR #456
```

**Benefits:**
- Stop on stack A removes stack A's worktree without affecting stack B
- No partial merge state leaks between stacks
- Each stack can have its own checked-out branch

**Implementation:**

```rust
impl GitOperations {
    /// Get or create a worktree for a stack. Called before cascade operations.
    async fn worktree_for_stack(&self, root_pr: PrNumber) -> Result<PathBuf> {
        let worktree_path = self.base_dir
            .join("worktrees")
            .join(format!("stack-{}", root_pr.0));

        if !worktree_path.exists() {
            // Create worktree from the shared clone
            self.run_git_uncancellable(&[
                "worktree", "add",
                worktree_path.to_str().unwrap(),
                "HEAD",
            ]).await?;
        }

        Ok(worktree_path)
    }

    /// Remove a stack's worktree. Called on stop or after stack completes.
    async fn remove_worktree(&self, root_pr: PrNumber) -> Result<()> {
        let worktree_path = self.base_dir
            .join("worktrees")
            .join(format!("stack-{}", root_pr.0));

        if worktree_path.exists() {
            self.run_git_uncancellable(&[
                "worktree", "remove", "--force",
                worktree_path.to_str().unwrap(),
            ]).await?;
        }

        Ok(())
    }

    /// Prune stale worktree references (housekeeping).
    async fn prune_worktrees(&self) -> Result<()> {
        self.run_git_uncancellable(&["worktree", "prune"]).await?;
        Ok(())
    }

    /// Clean up stale worktrees on startup.
    ///
    /// If the bot crashes mid-operation, worktrees may be left behind. This method
    /// removes worktrees that are either:
    /// - Not associated with an active train (orphaned)
    /// - Older than the max age threshold (stale from a previous crashed instance)
    ///
    /// Call this during startup after loading active trains from the state file.
    async fn cleanup_stale_worktrees(
        &self,
        active_train_roots: &HashSet<PrNumber>,
        max_age: Duration,
    ) -> Result<()> {
        let worktrees_dir = self.base_dir.join("worktrees");
        if !worktrees_dir.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(&worktrees_dir)? {
            let entry = entry?;
            let path = entry.path();

            // Parse "stack-NNN" directory name to extract PR number
            let Some(pr_number) = parse_stack_dir_name(&path) else {
                // Not a stack directory, skip
                continue;
            };

            // Keep if this is an active train
            if active_train_roots.contains(&pr_number) {
                continue;
            }

            // Check directory age via mtime
            let metadata = std::fs::metadata(&path)?;
            let modified = metadata.modified()?;
            let age = std::time::SystemTime::now()
                .duration_since(modified)
                .unwrap_or(Duration::MAX);

            if age > max_age {
                tracing::info!(
                    path = %path.display(),
                    age_hours = age.as_secs() / 3600,
                    pr = pr_number.0,
                    "removing stale worktree"
                );
                self.remove_worktree(pr_number).await?;
            }
        }

        // Also run git worktree prune to clean up any dangling references
        self.prune_worktrees().await?;

        Ok(())
    }
}

/// Parse a worktree directory name like "stack-123" to extract the PR number.
fn parse_stack_dir_name(path: &Path) -> Option<PrNumber> {
    let name = path.file_name()?.to_str()?;
    let num_str = name.strip_prefix("stack-")?;
    let num: u64 = num_str.parse().ok()?;
    Some(PrNumber(num))
}
```

**Worktree cleanup configuration:**

| Config | Default | Env var | Description |
|--------|---------|---------|-------------|
| `worktree_max_age` | 24 hours | `MERGE_TRAIN_WORKTREE_MAX_AGE_HOURS` | Worktrees older than this are removed on startup |

**When cleanup runs:**

1. **On bot startup**: After loading active trains from the state file, before processing any events
2. **Optionally on periodic timer**: Can be added to the hourly maintenance if desired, but startup cleanup handles the primary crash-recovery case

**Why 24 hours?** This threshold is deliberately generous:
- A healthy cascade completes in minutes to hours
- 24 hours provides ample margin for long CI pipelines or rate-limited operations
- If a worktree is 24+ hours old and not in an active train, it's almost certainly orphaned

**Observability**: Cleanup activity is logged at INFO level, making it easy to detect if orphaned worktrees are accumulating frequently (which might indicate a bug in the normal cleanup path).

The `prepare_descendant` and `reconcile_descendant` methods operate within the stack's worktree, obtained via `worktree_for_stack(root_pr)`. The `run_git` and `run_git_uncancellable` methods take a `workdir` parameter to specify which worktree to use.

**Usage in cascade logic:**

```rust
async fn cascade_step(
    pr: &PullRequest,
    descendants: &[&PullRequest],  // May be empty, one, or multiple (fan-out)
    git: &GitOperations,
    github: &GitHubClient,
    default_branch: &str,
    cancel: &CancellationToken,
) -> Result<CascadeStepOutcome> {
    // Phase 1: Prepare ALL descendants BEFORE squashing
    // Use PR number to fetch via refs/pull/<n>/head (reliable even after branch deletion)
    for desc in descendants {
        git.prepare_descendant(&desc.head_ref, pr.number, default_branch, cancel).await?;
    }

    // Phase 2: Squash-merge this PR into the default branch
    // IMPORTANT: Capture the squash commit SHA for the reconciliation step
    // Note: GitHub API calls are quick, so we don't pass cancel here
    let squash_sha = github.squash_merge(pr.number).await?;

    // Phase 3: Reconcile and retarget ALL descendants AFTER squashing
    for desc in descendants {
        git.reconcile_descendant(&desc.head_ref, &squash_sha, default_branch, cancel).await?;
        github.retarget_pr(desc.number, default_branch).await?;
    }

    // Return based on descendant count
    match descendants.len() {
        0 => Ok(CascadeStepOutcome::Complete),
        1 => Ok(CascadeStepOutcome::WaitingOnCi { pr_number: descendants[0].number }),
        _ => Ok(CascadeStepOutcome::FanOut {
            descendants: descendants.iter().map(|d| d.number).collect(),
        }),
    }
}
```

---

## Directory Structure

```
merge-train-bot/
├── Cargo.toml
├── src/
│   ├── main.rs              # Entry point, queue setup, server startup
│   ├── config.rs            # Configuration loading
│   ├── server.rs            # HTTP handlers (validate, enqueue, return 202)
│   ├── worker.rs            # Event worker loop (serial processing)
│   ├── github.rs            # GitHub API client wrapper
│   ├── git.rs               # Local git operations
│   ├── stack.rs             # Stack reconstruction and validation
│   ├── cascade.rs           # Cascade state machine
│   ├── commands.rs          # Comment command parsing
│   └── error.rs             # Error types
└── README.md
```

---

## Configuration

```toml
# config.toml
[github]
app_id = 12345
installation_id = 67890
private_key_path = "/etc/merge-train/github-app.pem"

[git]
signing_key_id = "ABCD1234"
gpg_home = "/etc/merge-train/gnupg"
clone_base_dir = "/var/lib/merge-train/repos"

[state]
# Local persistence directory (must be on persistent storage)
state_dir = "/var/lib/merge-train/state"

[server]
bind_address = "0.0.0.0:8080"
webhook_secret = "${WEBHOOK_SECRET}"  # env var substitution

[behavior]
# Future: auto_start = true

[housekeeping]
# State pruning: how long to retain merged/closed PRs before pruning
pr_retention_days = 30  # or env: MERGE_TRAIN_PR_RETENTION_DAYS

# Maximum PRs to store in the snapshot (hard limit to prevent unbounded growth)
max_prs_in_snapshot = 1000  # or env: MERGE_TRAIN_MAX_PRS_IN_SNAPSHOT

# Worktree cleanup: remove worktrees older than this on startup
worktree_max_age_hours = 24  # or env: MERGE_TRAIN_WORKTREE_MAX_AGE_HOURS
```

---

## Future: Automatic Triggering

The design supports easy migration to automatic triggering:

1. Add `auto_start: bool` to config
2. On `check_suite` success for a root PR (one targeting the default branch with descendants):
   - If `auto_start` enabled and PR is approved and mergeable
   - Behave as if `@merge-train start` was issued

The event-driven architecture means no other changes are needed — the cascade logic is identical whether triggered manually or automatically.

## Future: Monitoring Dashboard

A monitoring/ops dashboard is planned to provide visibility into active trains, cascade progress, and historical metrics.

---

## Security Considerations

- **Command authorization**: Commands (`@merge-train predecessor`, `start`, `stop`) are only accepted when:
  1. The comment is on a **pull request**, not an issue. The `issue_comment` webhook fires for both PRs and issues; the bot checks the payload's `issue.pull_request` field exists before processing.
  2. The comment author is **the user who opened the PR**. The bot compares `comment.user.id` against the PR's `user.id` (available in the webhook payload or cached PR metadata). This prevents other users—even those with write access—from hijacking someone else's merge train.
- **Webhook validation**: Verify `X-Hub-Signature-256` header against webhook secret.
- **Signing key protection**: GPG private key should be stored securely (e.g., mounted secret, not in repo).
- **Clone isolation**: Each repo gets its own workdir; clean up after operations.
- **Local state protection**: `state_dir` should be readable/writable only by the bot user; losing it means losing restart recovery.
- **Rate limiting**: Respect GitHub API rate limits; back off on 403/429.
- **Audit logging**: Log all merge operations with PR numbers, SHAs, timestamps.
