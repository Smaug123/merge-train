# Merge Train Bot Design

## Overview

A GitHub bot that orchestrates sequential squash-merging of stacked PRs into `main`. The bot maintains in-memory state per repository, bootstrapping from GitHub on first contact (or restart) and updating incrementally from webhooks thereafter. Operational state (train started/stopped, cascade progress) is persisted in bot-owned "state comments" on PRs, enabling recovery after restarts. It uses local git for merge operations (required for ours-strategy merges) and the GitHub API for everything else.

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

1. Create a state comment on this PR (the root) to claim ownership of the train
2. Walk the linked list to find all descendants (including any added after this command)
3. Validate the root PR is approved and CI is green (see below)
4. Squash-merge the root PR
5. Begin the cascade (updating the state comment at each phase transition)

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

Once started, the cascade proceeds automatically through all descendants. New PRs that declare themselves as descendants mid-cascade will be picked up when the cascade reaches their predecessor.

### Stopping

```
@merge-train stop
```

Requests a halt of the cascade for the stack containing this PR. Due to inherent race conditions, the stop takes effect at the next opportunity — any in-flight git operation or API call may complete before the halt is observed. The bot will:

1. Update the state comment on the root PR to `"state": "stopped"`
2. Update the human-readable portion of the state comment describing the current state
3. Take no further action on this stack until `@merge-train start` is issued again

The stop command is scoped to a single stack — other independent stacks in the same repo are unaffected.

### Aborting

The bot automatically aborts a cascade when it encounters an error (merge conflict, CI failure, etc.). This is distinct from a manual stop:

- **Abort**: Caused by an error condition. The bot posts diagnostics and waits for the condition to resolve. Some conditions (like CI failure) auto-resume when fixed.
- **Stop**: Explicit human request. The cascade will not resume until `@merge-train start` is issued again.

---

## Data Model

The stack structure is stored in PR comments via predecessor declarations:

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

### State comment

When the bot takes ownership of a stack (via `@merge-train start`), it creates a **state comment** on the root PR containing machine-readable JSON. This comment is the authoritative source of truth for the train's operational state, enabling the bot to recover after restarts without relying on in-memory state or the presence of specific human comments over time.

**Format:**

```markdown
<!-- merge-train-state
{
  "version": 1,
  "state": "running",
  "root_pr": 123,
  "current_pr": 124,
  "cascade_phase": "idle",
  "predecessor_head_sha": null,
  "last_squash_sha": null,
  "stopped_at": null,
  "error": null
}
-->
**Merge Train Status**

🚂 Train running — processing PR #124, waiting for CI
```

**JSON fields:**

| Field | Type | Description |
|-------|------|-------------|
| `version` | `number` | Schema version (currently `1`) |
| `state` | `string` | One of: `running`, `stopped`, `waiting_ci`, `aborted` |
| `root_pr` | `number` | PR number where the train was started |
| `current_pr` | `number` | PR currently being processed in the cascade |
| `cascade_phase` | `string` | One of: `idle`, `preparing`, `squash_pending`, `reconciling` |
| `predecessor_head_sha` | `string?` | Head SHA of predecessor before it was squash-merged (for recovery) |
| `last_squash_sha` | `string?` | SHA of last squash commit (for reconciliation recovery) |
| `stopped_at` | `string?` | ISO 8601 timestamp if stopped |
| `error` | `object?` | Error details if aborted: `{ "type": "...", "message": "..." }` |

**Cascade phases:**

- `idle`: Not currently performing any operation; waiting for CI or next event
- `preparing`: Merging predecessor head and main into descendants (before squash)
- `squash_pending`: Preparation complete; about to squash-merge current PR
- `reconciling`: Squash complete; performing ours-strategy merges into descendants

**Recovery semantics:**

If the bot crashes mid-cascade, the `cascade_phase` indicates where to resume:

| Phase | Recovery action |
|-------|-----------------|
| `idle` | Re-evaluate current PR's readiness |
| `preparing` | Re-run preparation (idempotent if already pushed) |
| `squash_pending` | Check if squash already happened; if not, perform it |
| `reconciling` | Use `last_squash_sha` to complete reconciliation |

The bot updates the state comment atomically at each phase transition. The human-readable portion below the JSON comment is updated to reflect current status but is not parsed — only the JSON is authoritative.

**Comment ownership:**

The bot identifies its own state comments by the `<!-- merge-train-state` marker. There is exactly one state comment per active train, always on the original root PR (even after that PR merges). When a train completes or is stopped, the comment is updated (not deleted) to reflect the final state.

**Limitation**: If the bot is offline for an extended period and the root PR is no longer returned in "recently merged PRs", train state cannot be recovered. The train must be manually restarted.

---

## State Management

### Design decision: ephemeral repo tracking

The bot does **not** persist the set of repositories it monitors. On restart, it has no memory of which repos it was previously tracking. Instead:

1. The bot waits for webhook events to arrive
2. On first webhook for a repo, it bootstraps that repo's state from GitHub
3. State comments on PRs provide the authoritative train state

This design is intentional:

- **Simpler deployment**: No database or persistent storage required
- **Self-healing**: If a webhook is missed, the next webhook for that repo triggers re-bootstrap
- **GitHub as source of truth**: All durable state lives in PR comments, not in the bot
- **Recovery window**: State comments live on the original root PR. If the bot is offline long enough that this PR falls outside the "recently merged" window (controlled by GitHub's pagination), train state will be lost and must be manually restarted. In practice, this window is large (hundreds of PRs) and only affects extended outages.

The tradeoff is that after a restart, a repo won't be processed until a relevant webhook arrives. In practice this is fine — CI completions, PR updates, or human comments will quickly trigger re-engagement. For repos with active trains, the `check_suite` webhooks from ongoing CI provide natural re-triggering.

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
5. For each PR, fetch comments to find:
   - `@merge-train predecessor` declarations (from humans)
   - `<!-- merge-train-state` state comments (from bot) — parse JSON to recover train state
6. For each open PR, fetch `mergeStateStatus` via GraphQL
7. Build `RepoState` with default branch, PR map, descendants index, and recovered train state
8. **Recovery check**: For any train in a non-idle `cascade_phase`, evaluate whether to resume:
   - If `preparing`: Re-run preparation (merge operations are idempotent if already pushed)
   - If `squash_pending`: Check if PR is already merged; if not, proceed with squash
   - If `reconciling`: Use `last_squash_sha` from state comment to complete reconciliation
9. Transition to `Ready` state
10. Drain queued events, process each in order

API calls are parallelized (concurrency limit ~10) to minimize bootstrap time.

**State comment parsing**: During comment scanning (step 5), the bot looks for comments containing `<!-- merge-train-state`. The JSON between the marker and `-->` is parsed to recover the train state (`running`, `stopped`, etc.), `current_pr`, `cascade_phase`, and recovery SHAs. This enables seamless continuation of in-progress cascades after a restart.

### Incremental updates

Each webhook event updates the cached state:

| Event | State Update |
|-------|--------------|
| `issue_comment` + `predecessor #N` | `pr.predecessor = Some(N)`, update descendants index |
| `issue_comment` + `start` | Create/update state comment on root PR, add to `active_trains` |
| `issue_comment` + `stop` | Update state comment on root PR with `stopped` |
| `pull_request.opened` | Add new PR to cache |
| `pull_request.closed` (merged) | `pr.state = Merged { sha }` |
| `pull_request.closed` (not merged) | `pr.state = Closed` |
| `pull_request.synchronize` | Update `head_sha`, set `merge_state` to Unknown |
| `check_suite.completed` | Re-fetch `merge_state` via GraphQL |
| `pull_request_review` | Re-fetch `merge_state` via GraphQL |

**State comment updates during cascade**: The bot updates its state comment at each phase transition:
- Before preparation: `cascade_phase = "preparing"`
- After preparation: `cascade_phase = "squash_pending"`, `predecessor_head_sha = <sha>`
- After squash: `cascade_phase = "reconciling"`, `last_squash_sha = <sha>`
- After reconciliation: `cascade_phase = "idle"`, advance `current_pr`

These updates are atomic (single API call) and provide the recovery points described in the state comment specification.

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

1. Find all root PRs (target default branch, no open predecessor)
2. Build linear chains from each root by following `descendants` index
3. Validate: no cycles, predecessors exist
4. Check `active_trains` to determine if each stack is started/stopped

**Fan-out handling**: When a PR has multiple open descendants (fan-out), the stack ends at that PR. Each descendant is not yet part of a computable stack — it will become the root of its own independent stack once its predecessor merges. After the fan-out point merges:
- Each descendant is retargeted to the default branch
- Each descendant is discovered as a new root via normal stack computation
- They proceed independently; whichever passes CI first merges next

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

The server processes webhook events **serially per repository** via per-repo queues. Different repositories are processed concurrently, but events within a single repository are strictly serialized. This eliminates race conditions without blocking unrelated repos.

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

The HTTP handler:
1. Validates the webhook signature
2. Extracts the repository ID from the event
3. Classifies the event by priority (stop commands have higher priority)
4. Pushes the event onto the **per-repo** priority queue (creating queue + worker if needed)
5. Immediately returns `202 Accepted`

Each per-repo worker loop:
1. Pulls the highest-priority event from its queue
2. Ensures repo state is bootstrapped (or queues event if bootstrapping)
3. Applies incremental state update from the event
4. Evaluates cascade actions based on updated state
5. Loops (or shuts down after idle timeout)

**Priority ordering**: Stop commands (`@merge-train stop`) are processed before all other events within that repo's queue. This ensures that a human request to halt the cascade takes effect promptly.

**Cancellation**: Each repo has an associated `CancellationToken`. When a stop command arrives, the dispatcher:
1. Cancels the current token (aborting any in-flight git operations)
2. Creates a fresh token for subsequent operations
3. Enqueues the stop event (which will be processed immediately due to priority)

This allows long-running operations like `git clone` or `git fetch` to be interrupted promptly when a human requests a stop.

This design means:
- No concurrent access to git working directories *within a repo*
- Different repos proceed independently
- No race between "check CI status" and "CI status changes"
- Straightforward reasoning about state transitions per repo
- Stop commands can interrupt in-flight operations
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
   → Single descendant: Wait for CI, then continue cascade with that descendant
   → Multiple descendants (fan-out): Each is now an independent root targeting main.
     They proceed independently; whichever passes CI first merges next.
     The others pick up that merge via normal catch-up flow when their turn comes.

7. REPEAT: For single descendant, it becomes the new #N, loop from step 1
```

For the root PR (#123), there is no predecessor to merge, so the sequence starts at step 2.

For the final PR in the stack (#125), there is no descendant, so steps 1, 3-5 are skipped.

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
| CI fails on a descendant | Code issue | Fix, push, bot will auto-continue |
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
| Update comment | `/repos/{o}/{r}/issues/comments/{id}` | PATCH |
| Add reaction | `/repos/{o}/{r}/issues/comments/{id}/reactions` | POST |
| Get merge state | GraphQL `mergeStateStatus` | POST `/graphql` |

The "Get repo metadata" endpoint returns `default_branch` which is used throughout the cascade logic.

The "Update comment" endpoint is used to update state comments atomically during cascade phase transitions.

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
    /// Recovered from state comments during bootstrap.
    active_trains: HashMap<PrNumber, StateCommentRef>,
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
// State comment (persisted in PR comments for restart recovery)
// ─────────────────────────────────────────────────────────────

/// JSON structure stored in bot-owned state comments.
/// This is the authoritative source of truth for train state.
#[derive(Serialize, Deserialize)]
struct StateComment {
    /// Schema version for forward compatibility
    version: u32,
    /// Current train state
    state: TrainState,
    /// PR number where the train was started (root)
    root_pr: PrNumber,
    /// PR currently being processed in the cascade
    current_pr: PrNumber,
    /// Current phase within the cascade step
    cascade_phase: CascadePhase,
    /// Head SHA of predecessor before squash (for recovery)
    predecessor_head_sha: Option<Sha>,
    /// SHA of last squash commit (for reconciliation recovery)
    last_squash_sha: Option<Sha>,
    /// When the train was stopped (if applicable)
    stopped_at: Option<String>, // ISO 8601
    /// Error details if aborted
    error: Option<TrainError>,
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
    /// Merging predecessor head and main into descendants
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

/// Comment ID for the state comment (used for updates)
struct StateCommentRef {
    comment_id: u64,
    /// Parsed state from the comment
    state: StateComment,
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
    /// Watch channel to send fresh cancellation tokens to the worker
    token_sender: watch::Sender<CancellationToken>,
    /// Current token (also sent to worker via watch channel)
    cancellation_token: CancellationToken,
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
            let token = CancellationToken::new();
            let (token_tx, token_rx) = watch::channel(token.clone());

            // Spawn a dedicated worker for this repo
            let worker_ctx = self.ctx.clone();
            let worker_repo_id = repo_id.clone();
            tokio::spawn(async move {
                repo_worker(worker_repo_id, event_rx, token_rx, worker_ctx).await;
            });

            RepoHandle {
                sender: event_tx,
                token_sender: token_tx,
                cancellation_token: token,
            }
        })
    }

    /// Dispatch an event to the appropriate repo's queue
    fn dispatch(&mut self, repo_id: &RepoId, event: QueuedEvent) {
        let is_stop = event.priority == EventPriority::Stop;

        // If this is a stop command, cancel in-flight operations first
        if is_stop {
            if let Some(handle) = self.repos.get_mut(repo_id) {
                // Cancel current token (interrupts any in-flight git operations)
                handle.cancellation_token.cancel();
                // Create fresh token for subsequent operations
                let new_token = CancellationToken::new();
                handle.cancellation_token = new_token.clone();
                // Send fresh token to worker (it will use this for new operations)
                let _ = handle.token_sender.send(new_token);
            }
        }

        let handle = self.get_or_create_handle(repo_id);
        let _ = handle.sender.send(event);
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

/// Worker loop for a single repository
async fn repo_worker(
    repo_id: RepoId,
    mut event_rx: mpsc::UnboundedReceiver<QueuedEvent>,
    mut token_rx: watch::Receiver<CancellationToken>,
    ctx: AppContext,
) {
    let mut queue = RepoPriorityQueue::new();
    let mut repo_state: Option<RepoState> = None;

    // Idle timeout — worker shuts down if no events for this duration
    let idle_timeout = Duration::from_secs(3600);

    loop {
        // Drain all pending events into priority queue
        while let Ok(event) = event_rx.try_recv() {
            queue.push(event);
        }

        // Process highest-priority event
        if let Some(event) = queue.pop() {
            // Get the current cancellation token (may have been refreshed by dispatcher)
            let cancel = token_rx.borrow().clone();

            let result = process_event_for_repo(
                &repo_id,
                event,
                &mut repo_state,
                &cancel,
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
        match tokio::time::timeout(idle_timeout, event_rx.recv()).await {
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

/// Process a single event for a repo
async fn process_event_for_repo(
    repo_id: &RepoId,
    event: QueuedEvent,
    repo_state: &mut Option<RepoState>,
    cancel: &CancellationToken,
    ctx: &AppContext,
) -> Result<()> {
    // Ensure repo is bootstrapped
    let just_bootstrapped = repo_state.is_none();
    if just_bootstrapped {
        *repo_state = Some(bootstrap_repo(repo_id, cancel, ctx).await?);
    }

    let state = repo_state.as_mut().unwrap();

    // Recovery: if we just bootstrapped, check for in-progress trains that need resuming
    if just_bootstrapped {
        recover_in_progress_trains(state, cancel, ctx).await?;
    }

    match event.payload {
        QueuedEventPayload::GitHub(gh_event) => {
            process_github_event(gh_event, state, cancel, ctx).await
        }
        QueuedEventPayload::PeriodicSync => {
            resync_repo(repo_id, state, cancel, ctx).await
        }
    }
}

/// After bootstrap, check for trains that were mid-cascade and resume them.
async fn recover_in_progress_trains(
    repo_state: &mut RepoState,
    cancel: &CancellationToken,
    ctx: &AppContext,
) -> Result<()> {
    for (root_pr, state_ref) in &repo_state.active_trains {
        let state = &state_ref.state;

        // Only recover trains that were actively running (not stopped/aborted)
        if !matches!(state.state, TrainState::Running | TrainState::WaitingCi) {
            continue;
        }

        match state.cascade_phase {
            CascadePhase::Idle => {
                // Re-evaluate readiness of current_pr
                // This will be handled by the triggering event, no special action needed
            }
            CascadePhase::Preparing => {
                // Re-run preparation (idempotent - if already pushed, merges are no-ops)
                resume_preparation(state.current_pr, repo_state, cancel, ctx).await?;
            }
            CascadePhase::SquashPending => {
                // Check if the PR was already squash-merged
                let pr = repo_state.prs.get(&state.current_pr);
                match pr.map(|p| &p.state) {
                    Some(PrState::Merged { .. }) => {
                        // Already merged, move to reconciliation
                        resume_reconciliation(state.current_pr, state.last_squash_sha.as_ref(), repo_state, cancel, ctx).await?;
                    }
                    Some(PrState::Open) => {
                        // Not yet merged, proceed with squash
                        resume_squash(state.current_pr, repo_state, cancel, ctx).await?;
                    }
                    _ => {
                        // PR closed without merge or missing - abort
                        abort_train(*root_pr, "PR closed or missing during recovery", repo_state, ctx).await?;
                    }
                }
            }
            CascadePhase::Reconciling => {
                // Use last_squash_sha to complete reconciliation
                let squash_sha = state.last_squash_sha.as_ref()
                    .ok_or(Error::MissingRecoverySha)?;
                resume_reconciliation(state.current_pr, Some(squash_sha), repo_state, cancel, ctx).await?;
            }
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────
// HTTP handler
// ─────────────────────────────────────────────────────────────

/// The HTTP handler — validates, classifies priority, and dispatches to per-repo queue
async fn webhook_endpoint(
    State(dispatcher): State<Arc<Mutex<Dispatcher>>>,
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

    let repo_id = event.repo_id();

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
// Periodic sync timer
// ─────────────────────────────────────────────────────────────

/// Periodic sync timer — injects PeriodicSync events into per-repo queues
async fn periodic_sync_timer(
    dispatcher: Arc<Mutex<Dispatcher>>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let mut d = dispatcher.lock().await;
        let repo_ids: Vec<RepoId> = d.repos.keys().cloned().collect();
        for repo_id in repo_ids {
            d.dispatch(&repo_id, QueuedEvent {
                priority: EventPriority::Normal,
                payload: QueuedEventPayload::PeriodicSync,
            });
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

    // Spawn periodic sync timer (e.g., every hour)
    let sync_interval = Duration::from_secs(3600);
    tokio::spawn(periodic_sync_timer(dispatcher.clone(), sync_interval));

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
    cancellation_token: &CancellationToken,
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
                        evaluate_cascade(&stacks, repo_state, cancellation_token, ctx).await?;
                    }
                    Command::Stop => {
                        // State already updated; just ack
                        // Note: in-flight operations were already cancelled by the dispatcher
                        ack_stop(&c, ctx).await?;
                    }
                }
            }
        }
        GitHubEvent::PullRequest(pr) if pr.action == "closed" => {
            if pr.merged {
                evaluate_cascade(&stacks, repo_state, cancellation_token, ctx).await?;
            } else {
                notify_orphaned_descendants(pr.number, &stacks, ctx).await?;
            }
        }
        GitHubEvent::CheckSuite(cs) if cs.conclusion == Some("success") => {
            evaluate_cascade(&stacks, repo_state, cancellation_token, ctx).await?;
        }
        GitHubEvent::PullRequestReview(r) if r.action == "submitted" && r.review.state == "approved" => {
            evaluate_cascade(&stacks, repo_state, cancellation_token, ctx).await?;
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
            Some(state_ref) => (
                matches!(state_ref.state.state, TrainState::Running | TrainState::WaitingCi),
                matches!(state_ref.state.state, TrainState::Stopped),
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

### Bootstrap (fetches full state from GitHub)

```rust
async fn bootstrap_repo(
    repo_id: &RepoId,
    cancellation_token: &CancellationToken,
    ctx: &AppContext,
) -> Result<RepoState> {
    let github = ctx.github_for_repo(repo_id);

    // Check for cancellation before starting
    cancellation_token.cancelled().now_or_never();
    if cancellation_token.is_cancelled() {
        return Err(Error::Cancelled);
    }

    // Fetch repository metadata to get default branch
    let repo_info = github.get_repo().await?;
    let default_branch = repo_info.default_branch;

    // Fetch all open PRs
    let open_prs = github.list_open_prs().await?;

    // Fetch recently merged PRs (for predecessor lookups)
    let merged_prs = github.list_recently_merged_prs().await?;

    // Check for cancellation before the expensive parallel fetch
    if cancellation_token.is_cancelled() {
        return Err(Error::Cancelled);
    }

    // Parallel fetch: comments and merge state for each PR
    let all_prs: Vec<_> = open_prs.iter().chain(merged_prs.iter()).collect();

    let cached_prs = futures::stream::iter(all_prs)
        .map(|pr| async {
            let (comments, merge_state) = tokio::join!(
                github.list_comments(pr.number),
                github.get_merge_state(pr.number),  // GraphQL query
            );
            build_cached_pr(pr, comments?, merge_state?)
        })
        .buffer_unordered(10) // Concurrency limit
        .try_collect::<Vec<_>>()
        .await?;

    // Build state
    let mut prs = HashMap::new();
    let mut descendants = HashMap::new();
    let mut active_trains = HashMap::new();

    for (pr, comments) in cached_prs {
        // Look for state comments from the bot
        if let Some(state_comment) = find_state_comment(&comments) {
            active_trains.insert(state_comment.state.root_pr, state_comment);
        }

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
        bootstrapped_at: Instant::now(),
        last_sync: Instant::now(),
        miss_count: 0,
    })
}

/// Parse a state comment from a list of comments
fn find_state_comment(comments: &[Comment]) -> Option<StateCommentRef> {
    for comment in comments {
        if let Some(start) = comment.body.find("<!-- merge-train-state\n") {
            if let Some(end) = comment.body[start..].find("\n-->") {
                let json_start = start + "<!-- merge-train-state\n".len();
                let json_str = &comment.body[json_start..start + end];
                if let Ok(state) = serde_json::from_str::<StateComment>(json_str) {
                    return Some(StateCommentRef {
                        comment_id: comment.id,
                        state,
                    });
                }
            }
        }
    }
    None
}
```

### Git operations

```rust
use tokio::process::Command;
use tokio_util::sync::CancellationToken;

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
        cancel: &CancellationToken,
    ) -> Result<String> {
        // Fetch everything we need
        self.run_git(&["fetch", "origin", descendant_branch, default_branch], cancel).await?;
        // Fetch the specific predecessor commit (may not be on any branch)
        self.run_git(&["fetch", "origin", predecessor_head_sha], cancel).await?;

        // Checkout descendant
        self.run_git(&["checkout", descendant_branch], cancel).await?;

        // Merge predecessor commit (regular merge)
        self.run_git(&[
            "merge",
            predecessor_head_sha,
            "--no-edit",
            "-m", &format!("Merge predecessor into {} (merge-train prep)", descendant_branch),
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
    for desc in descendants {
        git.prepare_descendant(&desc.head_ref, &pr.head_sha, default_branch, cancel).await?;
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
        _ => Ok(CascadeStepOutcome::Complete), // Fan-out: descendants are now independent roots
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

## Future: Monitoring Dashboard

A monitoring/ops dashboard is planned to provide visibility into active trains, cascade progress, and historical metrics.

---

## Security Considerations

- **Command authorization**: Only users with write access to the repo may issue commands. Validate via GitHub API before acting.
- **Webhook validation**: Verify `X-Hub-Signature-256` header against webhook secret.
- **Signing key protection**: GPG private key should be stored securely (e.g., mounted secret, not in repo).
- **Clone isolation**: Each repo gets its own workdir; clean up after operations.
- **Rate limiting**: Respect GitHub API rate limits; back off on 403/429.
- **Audit logging**: Log all merge operations with PR numbers, SHAs, timestamps.
