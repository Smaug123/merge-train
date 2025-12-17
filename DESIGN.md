# Merge Train Bot Design

## Overview

A GitHub bot that orchestrates sequential squash-merging of stacked PRs into `main`. The bot maintains in-memory state per repository, bootstrapping from GitHub on first contact (or restart) and updating incrementally from webhooks thereafter. It uses local git for merge operations (required for ours-strategy merges) and the GitHub API for everything else.

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

1. Walk the linked list to find all descendants (including any added after this command)
2. Validate the root PR is approved and CI is green (see below)
3. Squash-merge the root PR
4. Begin the cascade

**Definition of "CI green / approved"**: A PR is considered ready to merge when:
- **Approved**: Has at least one approving review and no changes-requested reviews
- **CI green**: All check suites that exist have completed with status `success`, AND all required status checks (as configured in branch protection) are present and passing

The bot will not merge a PR if any required check is missing, even if all existing checks pass.

Once started, the cascade proceeds automatically through all descendants. New PRs that declare themselves as descendants mid-cascade will be picked up when the cascade reaches their predecessor.

### Stopping

```
@merge-train stop
```

Immediately halts the cascade for the stack containing this PR. The bot will:

1. Mark the stack as stopped (via a `@merge-train stopped` comment on the root PR)
2. Leave a comment describing the current state
3. Take no further action on this stack until `@merge-train start` is issued again

The stop command is scoped to a single stack — other independent stacks in the same repo are unaffected.

### Aborting

The bot automatically aborts a cascade when it encounters an error (merge conflict, CI failure, etc.). This is distinct from a manual stop:

- **Abort**: Caused by an error condition. The bot posts diagnostics and waits for the condition to resolve. Some conditions (like CI failure) auto-resume when fixed.
- **Stop**: Explicit human request. The cascade will not resume until `@merge-train start` is issued again.

---

## Data Model

The stack is a singly-linked list stored in PR comments:

```
main ← PR #123 ← PR #124 ← PR #125
        (root)
```

Each non-root PR has exactly one `@merge-train predecessor #N` comment. The root PR targets `main` directly and has no predecessor comment.

### Stack membership

A PR is "in a train" if:

- It has a `@merge-train predecessor` comment pointing to another PR, and
- That predecessor chain eventually reaches a PR targeting `main`

Being in a train does not require the train to have started. Once the root receives `@merge-train start`, all current and future descendants will be processed.

---

## State Management

The bot maintains in-memory state per repository to avoid expensive API calls on every event. State is bootstrapped once per repo (on first webhook or restart) and updated incrementally from subsequent webhooks.

### Repository lifecycle

Each repo follows this state machine:

```
                    first webhook
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
- **Bootstrapping**: Fetching full snapshot from GitHub; incoming events are queued
- **Ready**: Processing events incrementally
- **Failed**: Bootstrap failed; retry with backoff

### Per-repo state

```rust
pub struct RepoState {
    /// All known PRs (open + recently merged)
    prs: HashMap<PrNumber, CachedPr>,

    /// Reverse index: predecessor → descendants
    descendants: HashMap<PrNumber, HashSet<PrNumber>>,

    /// When bootstrap completed
    bootstrapped_at: Instant,

    /// For periodic re-sync
    last_sync: Instant,

    /// Cache miss counter for drift detection
    miss_count: u32,
}
```

### Bootstrap algorithm

On first webhook for a repo (or on re-sync):

1. Transition to `Bootstrapping` state (queue incoming events)
2. Fetch repository metadata: `GET /repos/{o}/{r}` → extract `default_branch`
3. Fetch all open PRs: `GET /repos/{o}/{r}/pulls?state=open`
4. Fetch recently merged PRs: `GET /repos/{o}/{r}/pulls?state=closed&sort=updated`
5. For each PR, fetch comments to find `@merge-train` commands
6. For each open PR, fetch check suites and reviews
7. Build `RepoState` with default branch, PR map, and descendants index
8. Transition to `Ready` state
9. Drain queued events, process each in order

API calls are parallelized (concurrency limit ~10) to minimize bootstrap time.

### Incremental updates

Each webhook event updates the cached state:

| Event | State Update |
|-------|--------------|
| `issue_comment` + `predecessor #N` | `pr.predecessor = Some(N)`, update descendants index |
| `issue_comment` + `start` | `pr.train_started = true` |
| `issue_comment` + `stop` | Mark all PRs in stack as stopped |
| `pull_request.opened` | Add new PR to cache |
| `pull_request.closed` (merged) | `pr.state = Merged { sha }` |
| `pull_request.closed` (not merged) | `pr.state = Closed` |
| `pull_request.synchronize` | Update `head_sha`, reset `check_status` to Pending |
| `check_suite.completed` | Update `check_status` for matching PR(s) |
| `pull_request_review` | Re-evaluate `is_approved` |

### Handling cache misses

When an event references a PR not in cache:
1. Fetch the single PR from API, add to cache
2. Increment `miss_count`
3. If `miss_count >= 5`, trigger re-bootstrap (too much drift)

### Re-sync triggers

The bot re-bootstraps a repo when:
- **On error**: State inconsistency, cycle detected, too many cache misses, API errors
- **Periodic**: Every hour (configurable), via synthetic event in the queue

Periodic re-sync is implemented by injecting a `PeriodicSync` event into the same queue as webhooks, preserving serial processing guarantees.

### Stack topology

From the cached state, stacks are computed by traversing predecessor relationships:

1. Find all open PRs with `train_started = true` on any ancestor
2. Build linked lists by following `predecessor` pointers
3. Validate: no cycles, predecessors exist
4. Identify the "frontier" of each started stack

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

### Serial event processing

The server processes webhook events **strictly serially** via an in-memory priority queue. This eliminates race conditions between concurrent events (e.g., two `check_suite` events arriving simultaneously) without requiring distributed locking or database transactions.

```
┌─────────────┐     ┌────────────────────┐     ┌─────────────┐
│   axum      │ ──► │  priority queue    │ ──► │   worker    │
│  (accepts)  │     │ (stop > others)    │     │ (processes) │
└─────────────┘     └────────────────────┘     └─────────────┘
      │                                               │
      │ returns 202 Accepted                          │ one at a time
      ▼                                               ▼
```

The HTTP handler:
1. Validates the webhook signature
2. Classifies the event by priority (stop commands have higher priority)
3. Pushes the event onto the priority queue
4. Immediately returns `202 Accepted`

The worker loop:
1. Pulls the highest-priority event from the queue
2. Ensures repo state is bootstrapped (or queues event if bootstrapping)
3. Applies incremental state update from the event
4. Evaluates cascade actions based on updated state
5. Loops

**Priority ordering**: Stop commands (`@merge-train stop`) are processed before all other events. This ensures that a human request to halt the cascade takes effect promptly, even if there's a backlog of check_suite or other events in the queue.

This design means:
- No concurrent access to git working directories
- No race between "check CI status" and "CI status changes"
- Straightforward reasoning about state transitions
- Resilient to bot restarts — on startup, repos bootstrap on first webhook

### Event processing flow

```
receive event
  │
  ├─► extract repo_id from event
  │
  ├─► lookup repo lifecycle
  │     │
  │     ├─ Unknown ──────────► start bootstrap, queue event
  │     │
  │     ├─ Bootstrapping ────► queue event (processed after bootstrap)
  │     │
  │     ├─ Ready ────────────► apply incremental update, evaluate actions
  │     │
  │     └─ Failed ───────────► if backoff elapsed: retry bootstrap
  │
  └─► (for Ready state) evaluate cascade:
        │
        ├─ Find frontier of each started stack
        ├─ If frontier ready and predecessor merged: cascade
        ├─ If frontier CI pending: wait
        └─ If frontier CI failed: abort, notify
```

---

## Merge Operations

### Why local git is required

GitHub's merge API uses recursive/ort merge strategy, which often produces spurious conflicts or incorrect results when merging `main` into a stacked branch after a squash-merge. The content is semantically identical, but git doesn't recognise this.

The ours-strategy merge (as described at https://www.patrickstevens.co.uk/posts/2023-10-18-squash-stacked-prs/) resolves this correctly, as we now detail.

### Local git workflow

*This section uses `main` for exposition, but the implementation uses `default_branch` fetched from the GitHub API during bootstrap (see `RepoState.default_branch`). Repositories using `master`, `develop`, or other default branches work identically.*

We assume that the preceding PR in the merge train has just been squash-merged into `main`, and present the procedure to prepare the new root PR for its own squash-merge to `main`.

Firstly, we assume that we've used the GitHub API to obtain the preceding PR's head.sha (which will still exist in GitHub even if the branch has been deleted), and that it's stored as `$PREDECESSOR_HEAD_SHA`.
We additionally assume the GitHub API has told us the `$PREDECESSOR_SQUASH_COMMIT`, that is on `main` (possibly in the history of `main`, if someone has made an intervening commit).

```bash
# Clone (or fetch into existing clone)
git clone --depth=50 <repo-url> workdir
cd workdir

# Configure signing
git config user.signingkey <key-id>
git config commit.gpgsign true

git fetch origin <descendant-branch> "$PREDECESSOR_HEAD_SHA"

git checkout <descendant-branch>
git merge "$PREDECESSOR_HEAD_SHA" --no-edit -m "Merge predecessor into <descendant-branch> (merge train)"
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

### Note on using commit hashes

When a PR is merged, the PR object retains `head.sha` — the final commit on the branch before squash-merge. This commit continues to exist in the repo even if the branch is deleted. By using this SHA instead of branch names, the bot is resilient to:

- Branch deletion (GitHub's "delete branch on merge" setting)
- Manual merges outside the bot's workflow
- Crash recovery (we can resume even if the predecessor branch is gone)
- Late additions to the stack (a PR added after its predecessor was already merged)

### Operation sequence

Let's assume a stack of this shape: main ← #123 ← #124 ← #125

The cascade proceeds by repeating the following for each PR, starting from the root:

```
For PR #N with descendant #N+1:

1. PREPARATION: Bring #N+1 up to date with both #N and main
   a. Merge #N's head commit into #N+1's branch (regular merge)
      → Local git: git merge <N.head_sha>
      → Ensures #N+1 has all of #N's final content
   b. Merge main into #N+1's branch (regular merge)
      → Local git: git merge origin/main
      → Ensures #N+1 has any independent changes that landed on main
   → Both merges signed by bot
   → Push to origin
   → Must happen BEFORE squash-merging #N

2. SQUASH: Squash-merge #N into main
   → GitHub API, signed by GitHub
   → Record #N's head.sha for use in future recovery scenarios

3. RECONCILIATION: Relate #N+1's branch with the squash commit (ours strategy)
   → Local git: git merge <squash_commit_sha> --strategy ours
   → Uses the specific squash commit hash returned by step 2, NOT origin/main
   → Signed by bot
   → Adds the squash commit as a parent without changing tree content
   → Push to origin

4. CATCH-UP: Merge any subsequent main commits into #N+1's branch
   → Local git: git merge origin/main
   → Regular merge (not ours) to properly incorporate any commits that landed on main
     after the squash commit (from other PRs, hotfixes, etc.)
   → If main hasn't advanced past the squash commit, this is a no-op
   → Push to origin

5. RETARGET: Update #N+1's base branch to main
   → GitHub API: PATCH /repos/{o}/{r}/pulls/{n}  { "base": "main" }

6. WAIT: CI runs on #N+1 (event-driven)
   → On check_suite success webhook, continue to next iteration
   → On failure, abort cascade

7. REPEAT: #N+1 becomes the new #N, loop from step 1
```

For the root PR (#123), there is no predecessor to merge, so the sequence starts at step 2.

For the final PR in the stack (#125), there is no descendant, so steps 1, 3-5 are skipped.

**Why step 1b is essential**: If main has independent changes (commits that landed outside this stack), the descendant must incorporate them before the ours-strategy merge. Otherwise, squash-merging the descendant would compute a diff against main that effectively reverts those independent changes.

**Example timeline for main ← #123 ← #124 ← #125:**

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

---

## Abort Conditions

The bot aborts the cascade (and comments with diagnostics) if:

| Condition | Likely cause | Recovery hint |
|-----------|--------------|---------------|
| Preparation merge fails | Conflict between predecessor/main and descendant | Resolve locally, push to descendant branch, re-trigger |
| `git push` rejected | Force-push or concurrent modification | Investigate branch state, re-trigger |
| PR closed without merge | Human intervention | Re-open or restructure stack |
| CI fails on a descendant | Code issue | Fix, push, bot will auto-continue |
| Cycle detected | Misconfigured predecessor | Fix comments |
| Approval withdrawn | Review state changed | Re-approve |
| Review dismissed | Reviewer dismissed their approval or requested changes | Re-approve; cascade will not auto-resume |
| Squash-merge API fails | Branch protection, conflicts | Check PR status |
| Branch protection blocks merge | Required checks missing, insufficient approvals, etc. | Satisfy branch protection rules, re-trigger |

**Review dismissal behaviour**: If a review is dismissed (either by the reviewer or due to new commits in repos with "dismiss stale reviews" enabled), the cascade aborts immediately. Unlike CI failure, review dismissal does **not** auto-resume — a new approval must be obtained and `@merge-train start` must be re-issued to continue.

**Branch protection behaviour**: If the target branch has protection rules that prevent the squash-merge (e.g., required status checks not yet present, insufficient approvals, unsigned commits), the GitHub API will reject the merge request. The bot treats this as a fatal error for the stack: it posts a diagnostic comment and stops handling the stack entirely. The user must satisfy the branch protection requirements and re-issue `@merge-train start`.

On abort, the bot:

1. Stops immediately
2. Posts a comment on the PR that failed, explaining what happened and suggesting recovery
3. Posts a comment on downstream PRs that the train is halted
4. Takes no further action until human intervenes or condition resolves

### Auto-resume on CI fix

If the cascade aborted due to CI failure, the bot will automatically resume when:

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
| Add reaction | `/repos/{o}/{r}/issues/comments/{id}/reactions` | POST |
| Get check suites | `/repos/{o}/{r}/commits/{ref}/check-suites` | GET |

The "Get repo metadata" endpoint returns `default_branch` which is used throughout the cascade logic.

---

## Authentication

### GitHub API

GitHub App installation token with permissions:

- `pull_requests`: write (merge, retarget, read)
- `contents`: write (push via git)
- `checks`: read (wait for CI)
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
    check_status: CheckStatus,
    is_approved: bool,
    /// Declared predecessor (from @merge-train predecessor comment)
    predecessor: Option<PrNumber>,
    /// True if @merge-train start was issued
    train_started: bool,
    /// True if @merge-train stop was issued
    train_stopped: bool,
}

enum PrState {
    Open,
    Merged { merge_commit_sha: Sha },
    Closed,
}

enum CheckStatus {
    Pending,
    Success,
    Failure,
    Unknown, // Before any check_suite event
}

// ─────────────────────────────────────────────────────────────
// Stack topology (computed from cached state)
// ─────────────────────────────────────────────────────────────

/// A complete stack from root to tip
struct MergeStack {
    /// Ordered from root (index 0) to tip
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
    CiFailing,
    NotApproved,
    MergeConflict,
    Stopped,
}

/// Outcome of attempting a cascade step
enum CascadeStepOutcome {
    /// Successfully merged, cascade continues
    Merged { pr_number: PrNumber },
    /// Waiting for CI on next PR
    WaitingOnCi { pr_number: PrNumber },
    /// Stack fully merged
    Complete,
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

/// Events that go through the processing queue
struct QueuedEvent {
    priority: EventPriority,
    payload: QueuedEventPayload,
}

enum QueuedEventPayload {
    /// Webhook from GitHub
    GitHub(GitHubEvent),
    /// Internal: trigger periodic re-sync
    PeriodicSync(RepoId),
}
```

### Event queue and server

```rust
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};

/// Priority queue wrapper for events
struct PriorityEventQueue {
    /// Min-heap by Reverse(priority, sequence) so highest priority + oldest comes first
    heap: BinaryHeap<Reverse<(EventPriority, u64, QueuedEvent)>>,
    /// Monotonic sequence number for FIFO ordering within same priority
    sequence: u64,
    /// Notifies worker when events are available
    notify: Arc<Notify>,
}

impl PriorityEventQueue {
    fn new(notify: Arc<Notify>) -> Self {
        Self {
            heap: BinaryHeap::new(),
            sequence: 0,
            notify,
        }
    }

    fn push(&mut self, event: QueuedEvent) {
        let seq = self.sequence;
        self.sequence += 1;
        // Use Reverse so BinaryHeap (max-heap) gives us highest priority first,
        // and within same priority, lowest sequence (oldest) first
        self.heap.push(Reverse((Reverse(event.priority), seq, event)));
        self.notify.notify_one();
    }

    fn pop(&mut self) -> Option<QueuedEvent> {
        self.heap.pop().map(|Reverse((_, _, event))| event)
    }
}

/// The HTTP handler — validates, classifies priority, and enqueues
async fn webhook_endpoint(
    State(queue): State<Arc<Mutex<PriorityEventQueue>>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // Validate signature
    if !verify_signature(&headers, &body) {
        return StatusCode::UNAUTHORIZED;
    }

    // Parse event
    let event = match parse_webhook_event(&headers, &body) {
        Ok(e) => e,
        Err(_) => return StatusCode::BAD_REQUEST,
    };

    // Classify priority: stop commands get higher priority
    let priority = if is_stop_command(&event) {
        EventPriority::Stop
    } else {
        EventPriority::Normal
    };

    // Enqueue for processing
    {
        let mut q = queue.lock().await;
        q.push(QueuedEvent {
            priority,
            payload: QueuedEventPayload::GitHub(event),
        });
    }

    StatusCode::ACCEPTED
}

/// The worker loop — processes highest-priority events first
async fn event_worker(
    queue: Arc<Mutex<PriorityEventQueue>>,
    notify: Arc<Notify>,
    state: Arc<Mutex<GlobalState>>,
    ctx: AppContext,
) {
    loop {
        // Wait for events
        let event = {
            let mut q = queue.lock().await;
            q.pop()
        };

        let event = match event {
            Some(e) => e,
            None => {
                notify.notified().await;
                continue;
            }
        };

        let result = match event.payload {
            QueuedEventPayload::GitHub(gh_event) => {
                process_github_event(gh_event, &state, &ctx).await
            }
            QueuedEventPayload::PeriodicSync(repo_id) => {
                resync_repo(repo_id, &state, &ctx).await
            }
        };

        if let Err(e) = result {
            tracing::error!(?e, "failed to process event");
        }
    }
}

/// Periodic sync timer — injects PeriodicSync events into the queue
async fn periodic_sync_timer(
    queue: Arc<Mutex<PriorityEventQueue>>,
    state: Arc<Mutex<GlobalState>>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let repos: Vec<RepoId> = {
            let state = state.lock().await;
            state.repos.keys()
                .filter(|_| true) // Only Ready repos, but simplified here
                .cloned()
                .collect()
        };
        for repo_id in repos {
            let mut q = queue.lock().await;
            q.push(QueuedEvent {
                priority: EventPriority::Normal,
                payload: QueuedEventPayload::PeriodicSync(repo_id),
            });
        }
    }
}

/// Main entry point
#[tokio::main]
async fn main() {
    let notify = Arc::new(Notify::new());
    let queue = Arc::new(Mutex::new(PriorityEventQueue::new(notify.clone())));
    let ctx = AppContext::from_config().await;
    let state = Arc::new(Mutex::new(GlobalState::default()));

    // Spawn the single worker
    tokio::spawn(event_worker(queue.clone(), notify.clone(), state.clone(), ctx.clone()));

    // Spawn periodic sync timer (e.g., every hour)
    let sync_interval = Duration::from_secs(3600);
    tokio::spawn(periodic_sync_timer(queue.clone(), state.clone(), sync_interval));

    // Run the HTTP server
    let app = Router::new()
        .route("/webhook", post(webhook_endpoint))
        .with_state(queue);

    axum::serve(listener, app).await.unwrap();
}
```

### Event processing with state management

```rust
async fn process_github_event(
    event: GitHubEvent,
    state: &Arc<Mutex<GlobalState>>,
    ctx: &AppContext,
) -> Result<()> {
    let repo_id = event.repo_id();

    // Ensure repo is bootstrapped (or queue event if bootstrapping)
    let repo_state = ensure_bootstrapped(&repo_id, &event, state, ctx).await?;

    // Apply incremental state update
    let update_result = repo_state.apply_event(&event);

    // Handle errors that trigger re-sync
    if let Err(e) = &update_result {
        if e.should_trigger_resync() {
            trigger_resync(&repo_id, state, ctx).await?;
            return Ok(()); // Event will be reprocessed after resync
        }
    }

    // Compute stacks from cached state and evaluate actions
    let stacks = repo_state.compute_stacks();

    match event {
        GitHubEvent::IssueComment(c) => {
            if let Some(cmd) = parse_command(&c.body) {
                match cmd {
                    Command::Predecessor(n) => {
                        // State already updated; just ack
                        ack_predecessor(&c, ctx).await?;
                    }
                    Command::Start => {
                        evaluate_cascade(&stacks, &repo_state, ctx).await?;
                    }
                    Command::Stop => {
                        // State already updated; just ack
                        ack_stop(&c, ctx).await?;
                    }
                }
            }
        }
        GitHubEvent::PullRequest(pr) if pr.action == "closed" => {
            if pr.merged {
                evaluate_cascade(&stacks, &repo_state, ctx).await?;
            } else {
                notify_orphaned_descendants(pr.number, &stacks, ctx).await?;
            }
        }
        GitHubEvent::CheckSuite(cs) if cs.conclusion == Some("success") => {
            evaluate_cascade(&stacks, &repo_state, ctx).await?;
        }
        _ => {}
    }

    Ok(())
}

/// Ensure repo is bootstrapped; returns Ready state or queues event
async fn ensure_bootstrapped(
    repo_id: &RepoId,
    event: &GitHubEvent,
    state: &Arc<Mutex<GlobalState>>,
    ctx: &AppContext,
) -> Result<&mut RepoState> {
    let mut global = state.lock().await;
    let lifecycle = global.repos.entry(repo_id.clone())
        .or_insert(RepoLifecycle::Unknown);

    match lifecycle {
        RepoLifecycle::Unknown => {
            // Start bootstrap
            *lifecycle = RepoLifecycle::Bootstrapping {
                started_at: Instant::now(),
                queued_events: vec![event.clone()],
            };
            drop(global); // Release lock during API calls

            let repo_state = bootstrap_repo(repo_id, ctx).await?;

            let mut global = state.lock().await;
            let queued = match global.repos.remove(repo_id) {
                Some(RepoLifecycle::Bootstrapping { queued_events, .. }) => queued_events,
                _ => vec![],
            };
            global.repos.insert(repo_id.clone(), RepoLifecycle::Ready(repo_state));

            // Process queued events (recursive)
            for queued_event in queued {
                process_github_event(queued_event, state, ctx).await?;
            }

            // Return the now-ready state
            match global.repos.get_mut(repo_id) {
                Some(RepoLifecycle::Ready(s)) => Ok(s),
                _ => unreachable!(),
            }
        }
        RepoLifecycle::Bootstrapping { queued_events, .. } => {
            queued_events.push(event.clone());
            Err(EventQueued) // Signal that event was queued, not processed
        }
        RepoLifecycle::Ready(repo_state) => Ok(repo_state),
        RepoLifecycle::Failed { last_attempt, .. } => {
            if last_attempt.elapsed() > BOOTSTRAP_RETRY_DELAY {
                // Retry bootstrap
                *lifecycle = RepoLifecycle::Bootstrapping {
                    started_at: Instant::now(),
                    queued_events: vec![event.clone()],
                };
                // ... same as Unknown case
            }
            Err(RepoNotReady)
        }
    }
}
```

### Stack computation (from cached state)

```rust
impl RepoState {
    /// Compute stacks from cached PR data — no API calls needed
    fn compute_stacks(&self) -> Vec<MergeStack> {
        // Find all root PRs (target main, no predecessor, or predecessor is merged)
        let roots: Vec<PrNumber> = self.prs.values()
            .filter(|pr| pr.state == PrState::Open)
            .filter(|pr| self.is_root(pr))
            .map(|pr| pr.number)
            .collect();

        // Build stack for each root
        roots.into_iter()
            .filter_map(|root| self.build_stack(root))
            .collect()
    }

    fn is_root(&self, pr: &CachedPr) -> bool {
        match pr.predecessor {
            None => pr.base_ref == self.default_branch,
            Some(pred) => {
                // Root if predecessor is merged
                self.prs.get(&pred)
                    .map(|p| matches!(p.state, PrState::Merged { .. }))
                    .unwrap_or(true) // Missing predecessor = treat as root
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
            // Find the one open descendant (if multiple, pick first — shouldn't happen)
            let next = descendants.iter()
                .filter(|n| self.prs.get(n).map(|p| p.state == PrState::Open).unwrap_or(false))
                .next();

            match next {
                Some(&n) if !visited.contains(&n) => {
                    prs.push(n);
                    visited.insert(n);
                    current = n;
                }
                _ => break,
            }
        }

        let root_pr = self.prs.get(&root)?;
        Some(MergeStack {
            prs,
            started: root_pr.train_started,
            stopped: root_pr.train_stopped,
        })
    }
}
```

### Bootstrap (fetches full state from GitHub)

```rust
async fn bootstrap_repo(repo_id: &RepoId, ctx: &AppContext) -> Result<RepoState> {
    let github = ctx.github_for_repo(repo_id);

    // Fetch repository metadata to get default branch
    let repo_info = github.get_repo().await?;
    let default_branch = repo_info.default_branch;

    // Fetch all open PRs
    let open_prs = github.list_open_prs().await?;

    // Fetch recently merged PRs (for predecessor lookups)
    let merged_prs = github.list_recently_merged_prs().await?;

    // Parallel fetch: comments, check suites, reviews for each PR
    let all_prs: Vec<_> = open_prs.iter().chain(merged_prs.iter()).collect();

    let cached_prs = futures::stream::iter(all_prs)
        .map(|pr| async {
            let (comments, checks, reviews) = tokio::join!(
                github.list_comments(pr.number),
                github.get_check_status(&pr.head_sha),
                github.get_reviews(pr.number),
            );
            build_cached_pr(pr, comments?, checks?, reviews?)
        })
        .buffer_unordered(10) // Concurrency limit
        .try_collect::<Vec<_>>()
        .await?;

    // Build state
    let mut prs = HashMap::new();
    let mut descendants = HashMap::new();

    for pr in cached_prs {
        if let Some(pred) = pr.predecessor {
            descendants.entry(pred).or_insert_with(HashSet::new).insert(pr.number);
        }
        prs.insert(pr.number, pr);
    }

    Ok(RepoState {
        default_branch,
        prs,
        descendants,
        bootstrapped_at: Instant::now(),
        last_sync: Instant::now(),
        miss_count: 0,
    })
}
```

### Git operations

```rust
struct GitOperations {
    workdir: PathBuf,
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
        predecessor_head_sha: &str,
        default_branch: &str,
    ) -> Result<String> {
        // Fetch everything we need
        self.run_git(&["fetch", "origin", descendant_branch, default_branch]).await?;
        // Fetch the specific predecessor commit (may not be on any branch)
        self.run_git(&["fetch", "origin", predecessor_head_sha]).await?;

        // Checkout descendant
        self.run_git(&["checkout", descendant_branch]).await?;

        // Merge predecessor commit (regular merge)
        self.run_git(&[
            "merge",
            predecessor_head_sha,
            "--no-edit",
            "-m", &format!("Merge predecessor into {} (merge-train prep)", descendant_branch),
        ]).await?;

        // Merge default branch (regular merge) — critical for correctness!
        // Without this, any independent changes on the default branch would be reverted
        // when the descendant is later squash-merged.
        let origin_default = format!("origin/{}", default_branch);
        self.run_git(&[
            "merge",
            &origin_default,
            "--no-edit",
            "-m", &format!("Merge {} into {} (merge-train prep)", default_branch, descendant_branch),
        ]).await?;

        // Push
        self.run_git(&["push", "origin", descendant_branch]).await?;

        self.run_git(&["rev-parse", "HEAD"]).await
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
    ) -> Result<String> {
        // Fetch the squash commit and descendant branch
        self.run_git(&["fetch", "origin", descendant_branch]).await?;
        self.run_git(&["fetch", "origin", squash_commit_sha]).await?;

        // Checkout descendant
        self.run_git(&["checkout", descendant_branch]).await?;

        // Merge the squash commit with ours strategy — keeps our tree, adds it as parent
        self.run_git(&[
            "merge",
            squash_commit_sha,
            "--strategy", "ours",
            "--no-edit",
            "-m", &format!("Relate {} with squash commit (merge-train)", descendant_branch),
        ]).await?;

        // Now do a regular merge of origin/<default_branch> to pick up any subsequent commits
        let origin_default = format!("origin/{}", default_branch);
        self.run_git(&["fetch", "origin", default_branch]).await?;
        self.run_git(&[
            "merge",
            &origin_default,
            "--no-edit",
            "-m", &format!("Merge {} into {} (merge-train catch-up)", default_branch, descendant_branch),
        ]).await?;

        // Push
        self.run_git(&["push", "origin", descendant_branch]).await?;

        self.run_git(&["rev-parse", "HEAD"]).await
    }

    async fn run_git(&self, args: &[&str]) -> Result<String> {
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
}
```

**Usage in cascade logic:**

```rust
async fn cascade_step(
    pr: &PullRequest,
    descendant: Option<&PullRequest>,
    git: &GitOperations,
    github: &GitHubClient,
    default_branch: &str,
) -> Result<CascadeStepOutcome> {
    // Phase 1: Prepare descendant (if any) BEFORE squashing
    if let Some(desc) = descendant {
        git.prepare_descendant(&desc.head_ref, &pr.head_sha, default_branch).await?;
    }

    // Phase 2: Squash-merge this PR into the default branch
    // IMPORTANT: Capture the squash commit SHA for the reconciliation step
    let squash_sha = github.squash_merge(pr.number).await?;

    // Phase 3: Reconcile descendant (if any) AFTER squashing
    // Uses the specific squash commit SHA, not origin/<default_branch>
    if let Some(desc) = descendant {
        git.reconcile_descendant(&desc.head_ref, &squash_sha, default_branch).await?;
        github.retarget_pr(desc.number, default_branch).await?;
        return Ok(CascadeStepOutcome::WaitingOnCi { pr_number: desc.number });
    }

    Ok(CascadeStepOutcome::Complete)
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

[server]
bind_address = "0.0.0.0:8080"
webhook_secret = "${WEBHOOK_SECRET}"  # env var substitution

[behavior]
# Future: auto_start = true
```

---

## Future: Automatic Triggering

The design supports easy migration to automatic triggering:

1. Add `auto_start: bool` to config
2. On `check_suite` success for a root PR (one targeting the default branch with descendants):
   - If `auto_start` enabled and PR is approved and mergeable
   - Behave as if `@merge-train start` was issued

The event-driven architecture means no other changes are needed — the cascade logic is identical whether triggered manually or automatically.

---

## Security Considerations

- **Command authorization**: Only users with write access to the repo may issue commands. Validate via GitHub API before acting.
- **Webhook validation**: Verify `X-Hub-Signature-256` header against webhook secret.
- **Signing key protection**: GPG private key should be stored securely (e.g., mounted secret, not in repo).
- **Clone isolation**: Each repo gets its own workdir; clean up after operations.
- **Rate limiting**: Respect GitHub API rate limits; back off on 403/429.
- **Audit logging**: Log all merge operations with PR numbers, SHAs, timestamps.
