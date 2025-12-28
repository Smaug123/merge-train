# Merge Train Bot Design

## Overview

A GitHub bot that orchestrates sequential squash-merging of stacked PRs into the repository's default branch (usually `main`). The bot maintains in-memory state per repository and persists it to a local on-disk state store so it can recover cleanly after restarts. GitHub is used as the command surface (comments/webhooks) and as the system of record for PRs, branches, and checks. The bot's own operational state is persisted primarily to a local filesystem; status comments on PRs contain a backup copy of train state for recovery when local state is lost. It uses local git for reconciliation merge operations (required for ours-strategy merges) and the GitHub API for squash-merges into the default branch and other operations.

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
- Repos with "dismiss stale approvals" enabled (the bot pushes merge commits to PR branches during cascade operations; if branch protection dismisses approvals when new commits are pushed, each cascade step would invalidate approvals. Without mitigation the train would stall waiting for re-approval; instead, the bot aborts immediately on review dismissal events — see "Dismiss stale approvals preflight check" and "Exception — review dismissal events" below)

---

## User Interface

### Declaring stack membership

On a PR whose base branch is *not* `main`, comment:

```
@merge-train predecessor #123
```

This declares that the current PR is stacked on top of PR #123. The bot will:

1. Validate that #123 either targets `main` or itself has a predecessor declaration
2. Validate that the current PR's base branch matches #123's head branch (the branch the predecessor PR would merge FROM). This ensures the PR is actually stacked on the predecessor, not just claiming to be. Without this check, a PR based on `main` could declare a predecessor and the cascade would merge unrelated history.
3. Acknowledge with a reaction (👍) or error comment

**Base branch mismatch**: If the PR's base branch doesn't match the predecessor's head branch, the bot rejects the declaration with an error: "PR #N declares predecessor #123, but its base branch 'X' doesn't match #123's head branch 'Y'. The PR must be based on the predecessor's branch."

**Continuous base branch validation**: Base branch matching is validated not just at declaration time, but also:

1. **On `pull_request.edited` events**: When a PR's base branch changes (retargeting), the bot re-validates the predecessor relationship. If the new base no longer matches the predecessor's head branch **and the predecessor is still open**:
   - The PR is excluded from any active cascade while the mismatch exists
   - The bot posts a warning: "PR #N was retargeted to 'X', which no longer matches predecessor #123's head branch 'Y'. This PR will not be included in the cascade until the base branch is corrected."

   **Exception — merged predecessor**: If the predecessor is already merged, a base branch mismatch is expected. After the bot squash-merges a predecessor and retargets its descendants to main, the descendant's base (`main`) no longer matches the predecessor's former head branch. This is the normal post-cascade state — the predecessor relationship is considered **resolved**, not invalid. No warning is posted, and the PR is eligible to be discovered as a new root (see `is_root()` below).

2. **At cascade time**: Before entering the `Preparing` phase for any descendant, the bot re-validates that the descendant's base branch matches the predecessor's head branch. This catches any retargeting that occurred between declaration and cascade. If validation fails (and the predecessor is still open), the cascade aborts for that PR with a clear error.

This prevents stale predecessor links from causing merges into wrong branches.

A PR with a valid predecessor declaration is automatically part of any train rooted at its ancestor.

### Triggering the merge train

When the root PR (the one targeting `main`) is ready to merge, comment:

```
@merge-train start
```

The bot will:

1. Persist a "train started" record for this stack in the local state store
2. Walk the linked list to find all current descendants (the set is frozen per-PR when entering each PR's Preparing phase — see "Descendant set freezing")
3. Validate the root PR is mergeable (approved, required checks passing — see below)
4. Begin the cascade: prepare the root's descendants, squash-merge the root, then reconcile, catch-up, and retarget descendants (persisting phase transitions to disk and posting/updating a status comment — see "Status comments" and "Operation sequence")

**Definition of "ready to merge"**: The bot uses GitHub's `mergeStateStatus` field (via GraphQL) to determine readiness. A PR is ready when `mergeStateStatus` is `CLEAN` or `UNSTABLE`:

| Status | Meaning | Bot action |
|--------|---------|------------|
| `CLEAN` | All requirements satisfied | ✅ Proceed with merge |
| `UNSTABLE` | Non-required checks failing | ✅ Proceed with merge |
| `BLOCKED` | Required checks not passing or missing approvals | ⏳ Wait |
| `BEHIND` | Head branch behind base (strict mode) | ⏳ Update (see below) |
| `DIRTY` | Merge conflicts | ❌ Abort cascade |
| `UNKNOWN` | State not yet computed | ⏳ Wait and re-check |

This delegates all branch protection logic to GitHub, ensuring the bot respects required status checks, required reviewers, and any other protection rules without duplicating that logic.

**Draft PRs**: Draft PRs (`isDraft: true` in GraphQL) are treated as follows:

1. **On `@merge-train start`**: If the root PR is a draft, the bot **rejects the command** and posts an error: "Cannot start merge train: PR #N is a draft. Please mark it as ready for review first." This is checked explicitly via the `isDraft` field, not via `mergeStateStatus`.

2. **Descendants**: Draft descendants are allowed in the stack — they will be processed when the cascade reaches them. If a descendant is still a draft when the cascade arrives:
   - The bot waits (similar to `BLOCKED` status)
   - When the PR is marked ready for review, the bot re-evaluates and continues

3. **Why explicit check?**: GitHub's `mergeStateStatus` for a draft PR may be `CLEAN` (if CI passes), but the merge API will reject it with "Pull request is in draft state." Checking `isDraft` at start time provides a clearer error message than waiting for the API rejection.

**Handling BEHIND status**: When `mergeStateStatus` is `BEHIND`, the PR's head branch is not up-to-date with its base branch (typically because "Require branches to be up to date before merging" is enabled in branch protection). The bot's response depends on context:

1. **Root PR (base = default branch)**: The bot merges the default branch into the PR branch:
   ```
   git fetch origin main
   git checkout <pr_branch>
   git merge origin/main -m "Merge main to satisfy branch protection"
   git push origin <pr_branch>
   ```
   This is safe because the root has no predecessor — there's no cascade state to protect. After pushing, the bot waits for CI to run on the updated branch before proceeding.

   **If the merge conflicts**: The bot aborts with `git merge --abort`, cleans the worktree (see "Worktree cleanup on abort"), transitions the train to `aborted` state, and posts a comment: "Cannot update PR #N: merge conflicts with main. Please resolve conflicts locally and push, then re-issue `@merge-train start`." This matches the `DIRTY` status handling — the train cannot proceed until the human resolves the conflict.

2. **Descendant during PREPARATION phase** (before predecessor squash): Should not happen — the descendant's base is the predecessor's branch, and we just merged the predecessor's head. If this occurs:
   - Log warning: "Descendant #N is BEHIND after preparation — unexpected state"
   - Re-merge the predecessor's head (idempotent if already merged)
   - If still BEHIND: Abort with error. Someone may have pushed to the predecessor branch after we prepared.

3. **Descendant during RECONCILIATION or CATCH-UP phase** (after predecessor squash, before retarget): The descendant's base branch (`base_ref`) still refers to the predecessor's branch, which no longer exists (squash-merged and deleted). GitHub may report `BEHIND`, `UNKNOWN`, or even stale status since the base branch reference is invalid. The bot should:
   - **Ignore `mergeStateStatus` entirely** during these phases — it's meaningless when the base branch doesn't exist
   - Continue with reconciliation/catch-up operations as normal (these operate on git history, not GitHub's mergeability status)
   - The base will be corrected to `main` during the RETARGET step (step 5 in the operation sequence)

4. **Descendant after RETARGET** (base = main, waiting for CI): The bot has retargeted the PR to main and pushed updates. If `BEHIND`, main advanced after our catch-up push. Re-run catch-up merge (`git merge origin/main`) and wait for CI again.

**CRITICAL**: The bot NEVER merges main into a descendant BEFORE the predecessor is squash-merged. Doing so would cause lost commits (see "Why merging $SQUASH_SHA^ is essential"). The handling above only applies when the descendant's base is already main (post-retarget) or for the root PR.

**Eventual consistency caveat**: GitHub's `mergeStateStatus` is eventually consistent. After a push (including pushes from the bot itself during preparation or reconciliation), the field may reflect stale state for several seconds — even after `headRefOid` is updated to reflect the new head. The `headRefOid` field and `mergeStateStatus` field are **not atomically consistent**: `headRefOid` is updated by the git layer, while `mergeStateStatus` is computed by a separate mergeability engine that may lag behind. To avoid acting on stale data:

1. **After a bot-initiated push**: The bot must wait for CI completion events **for the new head SHA** before trusting `mergeStateStatus`. The bot accepts EITHER:
   - `check_suite.completed` (GitHub Checks API — most modern CI systems)
   - `status` context updates with state `success`/`failure`/`error` (legacy GitHub Status API — some CI systems still use this)

   The bot tracks which SHA it pushed and ignores events for older SHAs. Only when the appropriate event arrives for the NEW head does the bot re-query `mergeStateStatus`.

   **Why `check_suite.created`/`requested` is insufficient**: The mergeability engine is asynchronous. When `check_suite.created` fires, you may query `mergeStateStatus` and receive `CLEAN` — but this `CLEAN` is cached from the OLD head's check results, not computed against the new head. The new head's checks haven't finished yet, so the mergeability engine has nothing new to report and returns stale data.

   **Why both event types are needed**: Repositories may use the Checks API (check suites), the Status API (status contexts), or a mix of both — and this can vary per-PR due to path-based CI triggers or workflow conditions. A PR using only status contexts would never receive `check_suite.completed`, causing a deadlock if we only listened for that event. Conversely, treating an early status update as sufficient when check suites are still running risks proceeding before all checks finish.

   **Per-PR tracking**: The bot tracks which CI mechanism(s) are active **per-PR, per-SHA** by observing events as they arrive:
   - If `check_suite.created` or `check_suite.requested` is received for a SHA → the bot must wait for `check_suite.completed` for that SHA
   - If a `status` context update with state `pending` is received → the bot must wait for that context to reach a terminal state (`success`/`failure`/`error`)
   - If both mechanisms are observed, the bot waits for both to complete

   Only when all observed mechanisms have reached terminal states does the bot re-query `mergeStateStatus`. This per-PR approach avoids deadlocks (waiting for events that will never arrive) and avoids premature merges (proceeding before all checks complete).

2. **Before squash-merging**: The bot must verify that `headRefOid` matches the expected head SHA recorded when the train was started or when preparation completed. If they differ, someone pushed to the branch — abort and notify.
3. **On `UNKNOWN` status**: This explicitly means GitHub hasn't computed the status yet. Wait for CI completion events (check_suite or status) for the current head SHA.

This prevents races where: (a) the bot pushes a reconciliation commit, (b) `check_suite.created` fires, (c) bot queries `mergeStateStatus` and gets `CLEAN` based on the OLD head's cached results, (d) merges before required checks have run on the NEW head.

**Handling repos with no required checks**: Before entering the CI wait loop, the bot queries branch protection rules to check for required status checks. If no required checks are configured, the bot skips the CI wait entirely and proceeds based on `mergeStateStatus` alone. This uses the same per-repo branch protection cache (1-hour TTL) already used for the "dismiss stale approvals" preflight check. This is distinct from the dismiss stale approvals check — that one blocks `@merge-train start` entirely, while this one simply skips the CI wait when there are no checks to wait for.

**Note on "check failure"**: Throughout this document, "check failure" or "required check failure" refers to `mergeStateStatus` transitioning to `BLOCKED`. During the CI wait, the bot does not query branch protection rules to distinguish which specific checks are required vs non-required — it relies entirely on GitHub's `mergeStateStatus` computation. (The branch protection query described above for skipping CI wait when no required checks exist is a separate, one-time check at cascade start.) This means:
- If a non-required check fails → `UNSTABLE` → bot proceeds
- If a required check fails → `BLOCKED` → bot enters `waiting_ci` state
- If approval is withdrawn (detected via `mergeStateStatus` polling) → `BLOCKED` → bot enters `waiting_ci` state

When the bot detects `BLOCKED` via `mergeStateStatus` polling (without knowing the specific cause), it enters `waiting_ci` state — assuming the condition is potentially transient (flaky tests, CI outages, or code that will be fixed). The cascade auto-resumes when `mergeStateStatus` becomes `CLEAN` or `UNSTABLE`. Users can issue `@merge-train stop` if the failure is known to be permanent.

**Exception — review dismissal events**: If the bot receives a `pull_request_review.dismissed` webhook (explicit review dismissal, or automatic dismissal due to "dismiss stale reviews" on new commits), it immediately transitions to `aborted` state rather than `waiting_ci`. This is because review dismissals often indicate a deliberate action requiring human attention, not a transient condition. See "Pause Conditions" for the full table of abort vs waiting conditions.

**Note on webhook vs polling detection**: The distinction between webhook-detected review dismissal (`aborted`) and polling-detected missing approval (`waiting_ci`) is intentional. Webhook events indicate an explicit action occurred; polling only reveals current state. If the bot misses a `pull_request_review.dismissed` webhook (e.g., due to a brief outage), it may subsequently detect the missing approval via `mergeStateStatus` polling and enter `waiting_ci` instead of `aborted`. This is acceptable: in both cases, a new approval is required before the cascade can proceed. The only difference is whether the restart is automatic (on new approval, if `waiting_ci`) or requires re-issuing `@merge-train start` (if `aborted`).

Once started, the cascade proceeds automatically through all descendants. New PRs that declare themselves as descendants mid-cascade will be picked up when the cascade reaches their predecessor — but with an important constraint: see "Descendant set freezing" below.

### Dismiss stale approvals preflight check

Before starting a train, the bot checks whether the repository has "dismiss stale approvals" (also called "dismiss stale pull request approvals when new commits are pushed") enabled in branch protection. This setting is **incompatible** with the merge train bot because:

1. The bot pushes merge commits to descendant PR branches during cascade (preparation, reconciliation, catch-up)
2. Each push invalidates existing approvals on those PRs
3. The PR's `mergeStateStatus` flips from `CLEAN` to `BLOCKED`
4. The cascade would stall waiting for re-approval that never comes automatically (however, the bot intercepts the review dismissal event and aborts — see "Interaction with review dismissal events" below)

**Detection**: On `@merge-train start`, the bot queries branch protection rules via the GitHub API:

```
GET /repos/{owner}/{repo}/branches/{default_branch}/protection
```

If the response includes `"dismiss_stale_reviews": true` in `required_pull_request_reviews`, the bot **refuses to start** and posts an error:

```
Cannot start merge train: this repository has "Dismiss stale pull request approvals when new commits are pushed" enabled in branch protection.

The merge train bot pushes merge commits to PR branches during cascade operations, which would invalidate approvals after each step. Please either:

1. Disable "Dismiss stale pull request approvals" in branch protection settings, OR
2. Use a different workflow for stacked PRs in this repository

See: https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches#require-pull-request-reviews-before-merging
```

**Rulesets**: GitHub rulesets can also enforce "dismiss stale reviews" independently of branch protection. The bot also queries:

```
GET /repos/{owner}/{repo}/rulesets?includes_parents=true
```

And checks each ruleset for `dismiss_stale_reviews_on_push: true` in the `pull_request` rule. If found on any ruleset targeting the default branch, the bot refuses to start with the same error message.

**API permission**: The bot requires `administration:read` permission (or higher) to query branch protection rules, and `repository_metadata:read` for rulesets. If permissions are missing:

1. The bot logs a **warning** and posts a visible notice on the PR: "⚠️ Unable to verify branch protection settings (missing permissions). If 'dismiss stale approvals' is enabled, the cascade may abort. Consider granting `administration:read` permission."
2. The bot **proceeds anyway** — this is a deliberate choice to avoid blocking workflows entirely when the admin hasn't granted full permissions.
3. If the cascade later aborts due to dismissed approvals, the error message explicitly suggests checking this setting.

This "warn and proceed" approach balances operational flexibility with user awareness. Admins who want strict enforcement can grant the permission; those who know their settings are compatible can proceed without it.

**Caching**: Branch protection and ruleset queries are cached per-repo for 1 hour (configurable via `MERGE_TRAIN_BRANCH_PROTECTION_CACHE_TTL_MINS`). This cache is appropriate because:

1. Branch protection changes during an active cascade are rare
2. If the setting IS changed mid-cascade, the cascade aborts (see below)
3. The user can then fix the setting, or `@merge-train stop` and re-evaluate

**Interaction with review dismissal events**: When "dismiss stale reviews" is enabled and the bot pushes a merge commit, GitHub dismisses the existing approvals **and** emits `pull_request_review.dismissed` webhook events. The bot's event handler intercepts these events and transitions the cascade to `aborted` state (see "Review dismissal behaviour" in the Error Handling section). This means:

1. If the setting is enabled mid-cascade, the cascade **aborts immediately** via the dismissed event — it does not merely stall on `BLOCKED`.
2. The abort reason will be `ReviewDismissed`, and the user must re-approve and issue `@merge-train start` to resume.
3. When handling the abort, the bot **invalidates the protection cache** and re-queries branch protection. If it now detects `dismiss_stale_reviews: true`, it posts an error message explaining that the setting is incompatible (rather than a generic "review was dismissed" message).

This is intentional: review dismissal is treated as a deliberate human action that should not auto-resume. The preflight check exists to prevent this situation from occurring in the first place.

### Merge method preflight check

Before starting a train, the bot verifies that the repository is configured for squash-only merges. The bot **requires** squash merge and **prohibits** merge commits and rebase merges — other merge methods would not maintain the linear history that the cascade logic depends on, and would cause incorrect results in late-addition scenarios.

**Detection**: On `@merge-train start`, the bot queries repository settings:

```
GET /repos/{owner}/{repo}
```

The response includes `allow_squash_merge`, `allow_merge_commit`, and `allow_rebase_merge`. The bot requires ALL of:

- `allow_squash_merge: true`
- `allow_merge_commit: false`
- `allow_rebase_merge: false`

If any condition fails, the bot **refuses to start** and posts an error:

```
Cannot start merge train: this repository must be configured for squash-only merges.

The merge train bot requires:
  ✓ "Allow squash merging" enabled
  ✗ "Allow merge commits" disabled
  ✗ "Allow rebase merging" disabled

Current settings:
  allow_squash_merge: {value}
  allow_merge_commit: {value}
  allow_rebase_merge: {value}

Please update repository settings to allow only squash merging.
```

**Why squash-only?**: The cascade logic relies on squash commits having a single parent that is the prior default branch HEAD. Merge commits have two parents (breaking the parent assumption), and rebase merges add multiple commits (making `merge_commit_sha^` point to another rebased commit, not the prior main HEAD). Late-addition reconciliation would produce incorrect results if a predecessor was merged with a non-squash method.

**Caching**: This check uses the same per-repo cache as branch protection queries (1 hour TTL). Repository merge method settings rarely change.

**No warn-and-proceed**: Unlike the "dismiss stale approvals" check (which can warn and proceed if permissions are missing), the merge method check is **hard requirement**. The cascade fundamentally cannot work without squash merge — attempting to proceed would fail immediately on the first merge with `METHOD_NOT_ALLOWED`.

### Stopping

```
@merge-train stop
```

Requests a halt of the cascade for the stack containing this PR. Unlike `start` and `predecessor`, the `stop` command can be issued by the PR author OR any repository admin/maintainer — this allows intervention when the author is unavailable. See "Command authorization" in Security Considerations for details.

Due to inherent race conditions, the stop takes effect at the next opportunity — any in-flight git operation or API call may complete before the halt is observed. The bot will:

1. Cancel any in-flight git operations
2. Remove the stack's dedicated worktree (abort any in-progress merge first, then remove via `git worktree remove --force`)
3. Persist the stack as `"state": "stopped"` in the local state store
4. Post/update the status comment to reflect `stopped` state (required for recovery consistency)
5. Take no further action on this stack until `@merge-train start` is issued again

The stop command is scoped to a single stack — other independent stacks in the same repo are unaffected. Each stack has its own isolated worktree (see "Per-stack worktrees" section), so stopping one stack has no effect on others.

**Worktree removal on stop**: The stop command removes the stack's worktree entirely via `remove_worktree`. This is intentional: a stopped train is expected to be restarted from scratch if needed, and removing the worktree ensures clean state. When the train restarts, a fresh worktree is created.

### Aborting and Waiting

The bot pauses a cascade when it encounters an error. The pause state depends on whether the condition can auto-resolve:

- **Waiting (`waiting_ci`)**: Transient conditions that can resolve without user action — check failures, approval temporarily missing. The bot monitors for relevant events (`check_suite.completed`, `pull_request_review.submitted`) and auto-resumes when `mergeStateStatus` becomes `CLEAN` or `UNSTABLE`.
- **Aborted (`aborted`)**: Permanent conditions that require explicit user intervention — merge conflicts, review dismissed, permanent API failures. The cascade will not resume until `@merge-train start` is re-issued.
- **Stopped (`stopped`)**: Explicit human request via `@merge-train stop`. The cascade will not resume until `@merge-train start` is issued again.

---

## Data Model

The stack structure is declared via PR comments (predecessor declarations) and persisted in the local state file:

```
main ← PR #123 ← PR #124 ← PR #125      (linear)
        (root)

main ← PR #123 ←┬─ PR #124              (fan-out)
        (root)  └─ PR #125
```

Each non-root PR has exactly one `@merge-train predecessor #N` comment. A root PR targets the default branch directly and either has no predecessor declaration, or has a predecessor declaration pointing to a PR that has already been merged (a "resolved" predecessor — see `is_root()` below). In the latter case, the predecessor comment is preserved but the relationship is considered resolved; the PR is eligible to be the root of a new train.

Multiple PRs may declare the same predecessor (fan-out). Each non-root PR has exactly one predecessor, but a PR may have multiple descendants.

### Stack membership

A PR is "in a train" if:

- It has a `@merge-train predecessor` comment pointing to an open predecessor PR, and
- That predecessor chain eventually reaches a root PR (one targeting `main` with no open predecessor)

A PR whose predecessor was closed without merge is **orphaned**, not "in a train" — it has a broken predecessor chain and will not be processed until the predecessor is re-opened or the declaration is updated.

Being in a train does not require the train to have started. Once the root receives `@merge-train start`, all current and future descendants will be processed.

### Local train state (primary)

When the bot takes ownership of a stack (via `@merge-train start`), it persists the train's operational state to disk. This local record is the primary persistence target. See "Recovery precedence" below for how conflicts between local and GitHub state are resolved during restart.

**Train record fields:**

| Field | Type | Description |
|-------|------|-------------|
| `version` | `number` | Schema version (currently `1`) |
| `recovery_seq` | `number` | Monotonic counter incremented on each state change; used to determine which record (local vs GitHub) is "ahead" during recovery (see "Recovery precedence") |
| `state` | `string` | One of: `running`, `stopped`, `waiting_ci`, `aborted`, `needs_manual_review` |
| `original_root_pr` | `number` | The PR that originated this train. For the initial train, this is the PR that received `@merge-train start`. For trains created by fan-out, this is the descendant PR that became a new root. Constant **within** a single train's lifetime, but fan-out creates new trains with different values (see "Fan-out handling" below). |
| `current_pr` | `number` | PR currently being processed (advances as each PR merges) |
| `cascade_phase` | `object` | Phase with `completed`, `skipped`, and `frozen_descendants` lists (see below) |
| `predecessor_pr` | `number?` | PR number of predecessor (for fetching via `refs/pull/<n>/head` during recovery) |
| `predecessor_head_sha` | `string?` | Head SHA of predecessor at preparation time (for verifying preparation during recovery) |
| `last_squash_sha` | `string?` | SHA of last squash commit (for reconciliation recovery) |
| `started_at` | `string` | ISO 8601 timestamp when started |
| `stopped_at` | `string?` | ISO 8601 timestamp if stopped |
| `error` | `object?` | Error details if aborted: `{ "type": "...", "message": "..." }` |

**Cascade phases** (serialized as `{ "PhaseName": {...} }` for phases with data, or `"PhaseName"` for unit variants):

- `Idle`: Not currently performing any operation; waiting for CI or next event
- `Preparing`: Merging predecessor head into descendants (before squash) — NOT main, only predecessor head. Captures `frozen_descendants` at entry (see "Descendant set freezing").
- `SquashPending`: Preparation complete; about to squash-merge current PR. Carries forward `frozen_descendants` and `skipped` from `Preparing`.
- `Reconciling`: Squash complete; performing ours-strategy merges into descendants
- `CatchingUp`: Ours-merge complete; performing regular merge of origin/main
- `Retargeting`: Catch-up complete; retargeting descendant PRs to default branch

Phases with multiple descendants (`Preparing`, `Reconciling`, `CatchingUp`, `Retargeting`) include:
- `completed`: Which descendants have finished this phase
- `skipped`: Descendants that failed during processing (PR closed, branch deleted, etc.) — not retried
- `frozen_descendants`: The descendant set captured when entering `Preparing`; carried through ALL subsequent phases

**CRITICAL**: Recovery MUST use `frozen_descendants` from the persisted state, NOT re-query `repo_state.descendants`. Between crash and recovery, new descendants may have been added via `predecessor_declared` events during spool replay. These new descendants were never prepared and must not be reconciled in the current cascade step — they'll be handled when the next PR becomes the root.

**Recovery semantics:**

If the bot crashes mid-cascade, the `cascade_phase` indicates where to resume:

| Phase | Recovery action |
|-------|-----------------|
| `Idle` | Re-evaluate current PR's readiness |
| `Preparing` | Skip descendants in `completed`, re-run for remaining (idempotent if already pushed) |
| `SquashPending` | Check if squash already happened; if not, perform it |
| `Reconciling` | Verify preparation, then skip descendants in `completed`, use `last_squash_sha` to complete for remaining |
| `CatchingUp` | Skip descendants in `completed`, re-run merge of origin/main for remaining |
| `Retargeting` | Skip descendants in `completed`, retarget remaining (idempotent via API check) |

**Verifying preparation before reconciliation:**

Reconciliation assumes all descendants were prepared (predecessor head merged into them) before the squash. This invariant can be violated if:
- Someone manually merges the root PR via GitHub UI (bypassing the bot)
- The bot crashes between preparation and squash, and on recovery finds the PR already merged
- A race condition where preparation partially completed

**On recovery to `Reconciling` phase**, before proceeding, the bot MUST verify for each descendant not in `completed`:

1. Fetch the descendant's head SHA and the predecessor's pre-squash head SHA (from `predecessor_head_sha` field, or by fetching `refs/pull/<predecessor_pr>/head` via `git ls-remote` as fallback)
2. Check if the predecessor head is an ancestor of the descendant head: `git merge-base --is-ancestor <predecessor_head> <descendant_head>`
3. If NOT an ancestor: This descendant was never prepared. The bot cannot safely reconcile — the descendant doesn't have the predecessor's content.
   - **Recovery action**: Abort with error: "Descendant #N was not prepared before squash. Manual intervention required: merge main into the descendant branch or rebase."
   - Do NOT attempt to "fix" this automatically — the descendant may have diverged in ways that make automatic merge incorrect.

This verification is fast (local git operation after fetching refs) and prevents silent data loss where reconciliation would create ours-merges for content that was never actually integrated.

**Reconciling recovery with missing `last_squash_sha`:**

If recovery finds `cascade_phase = "reconciling"` but `last_squash_sha` is null (lost due to crash after squash but before durable write):

1. Fetch `current_pr` from GitHub API (the PR that was just squash-merged)
2. If `state == "merged"`, extract `merge_commit_sha` — this is the squash SHA
3. **If `merge_commit_sha` is null**: GitHub's API has eventual consistency — the merge may have succeeded but `merge_commit_sha` may not be populated yet. Retry with exponential backoff:
   - Initial delay: 1 second
   - Max delay: 30 seconds (cap)
   - Max attempts: 10
   - Total max wait: ~150 seconds (1+2+4+8+16+30+30+30+30)

   ```rust
   async fn fetch_merge_commit_sha_with_retry(
       github: &GitHubClient,
       pr_number: PrNumber,
   ) -> Result<Option<String>> {
       let mut delay = Duration::from_secs(1);
       let max_delay = Duration::from_secs(30);
       let max_attempts = 10;

       for attempt in 1..=max_attempts {
           let pr = github.get_pr(pr_number).await?;
           if let Some(sha) = pr.merge_commit_sha {
               return Ok(Some(sha));
           }
           if attempt == max_attempts {
               break;
           }
           tracing::info!(
               pr = pr_number.0,
               attempt,
               delay_ms = delay.as_millis(),
               "merge_commit_sha not yet available, retrying"
           );
           tokio::time::sleep(delay).await;
           delay = std::cmp::min(delay * 2, max_delay);
       }
       Ok(None)
   }
   ```

   If still null after all retries, log a warning and abort with a recoverable error. The user can retry recovery later.

4. Continue reconciliation with the recovered SHA
5. If `current_pr` is not merged, the squash didn't actually happen — revert to `SquashPending`

This derives the squash SHA from GitHub rather than hard-failing, since the squash-merge is recorded in GitHub's PR state even if our local record was lost.

**Why ~150 seconds total?** GitHub's API typically propagates `merge_commit_sha` within a few seconds, but under heavy load or during incidents, propagation can take 10-30 seconds. A 150-second total wait covers the vast majority of cases without making recovery unacceptably slow. The exponential backoff (1s, 2s, 4s, 8s, 16s, then 30s capped for the remaining attempts) front-loads fast retries for the common case while backing off for edge cases.

**GitHub-based recovery**: If the local state files are missing or corrupted, the bot can recover train state from GitHub during bootstrap by scanning for status comments on open and recently merged PRs that target the default branch. The status comment contains the full `TrainRecord` as machine-readable JSON, enabling precise recovery without inference. See "Status comments" below.

**Supplementary GitHub recovery**: Even when local state exists, the bot may consult GitHub status comments to fill gaps caused by crashes between event log writes and status comment updates. On restart with existing local state:

1. For each active train, fetch the status comment from GitHub
2. Compare `cascade_phase` and `last_squash_sha` between local and GitHub
3. If GitHub has a more advanced phase or SHA that local state lacks:
   - The local write was lost (crash before fsync or during write)
   - Use the GitHub values to update local state
4. Continue recovery from the merged state

This handles the scenario where: local state shows `SquashPending`, but GitHub shows `Reconciling` with `last_squash_sha`. The squash succeeded and was recorded to GitHub, but the local write was lost. Without this, recovery would attempt to squash again (which would fail or duplicate).

**Recovery precedence** (determined by comparing `recovery_seq`):
- If GitHub's `recovery_seq` > local: Use GitHub's state (operation succeeded, local lost the record)
- If local's `recovery_seq` > GitHub: Use local (status comment update was delayed/lost)
- If both agree: Normal recovery

This eliminates ambiguity in "which is ahead" — the monotonic sequence provides a total ordering.

**Note:** This recovery precedence rule supersedes any references to the local event log being "authoritative" or "source of truth." Those terms describe where state is *written* during normal operation, not where it is *read* during recovery.

### Status comments (required, non-authoritative for normal operation)

The bot MUST post and update a status comment on the **original root PR** (the PR that received `@merge-train start`) at each phase transition. The comment remains on this PR for the train's entire lifecycle, even as `current_pr` advances through descendants. This provides a single stable location for observability and recovery; users watching descendant PRs can follow the link to the root PR's status comment for current state.

This comment includes:

- Full machine-readable train state in `<!-- merge-train-state {...} -->` JSON
- Human-readable status for user visibility

**Format:**

```markdown
<!-- merge-train-state
{
  "version": 1,
  "recovery_seq": 42,
  "state": "running",
  "original_root_pr": 123,
  "current_pr": 124,
  "cascade_phase": {
    "Reconciling": {
      "completed": [125],
      "skipped": [],
      "frozen_descendants": [125, 126]
    }
  },
  "predecessor_pr": 123,
  "last_squash_sha": "abc123def456",
  "started_at": "2024-01-15T09:00:00Z"
}
-->
**Merge Train Status**

Train running — reconciling PR #124 after squash (1/2 descendants complete)
```

**JSON fields:** Same as the `TrainRecord` fields documented in "Local train state" above, **except** `status_comment_id` (which is only known locally after the comment is created): `version`, `recovery_seq`, `state`, `original_root_pr`, `current_pr`, `cascade_phase` (as full `CascadePhase` object), `predecessor_pr`, `predecessor_head_sha`, `last_squash_sha`, `started_at`, `stopped_at`, `error`. The `recovery_seq` is a monotonic counter incremented on each state change, used to determine which record is "ahead" during recovery. Deserialization must tolerate a missing `status_comment_id` field.

**Critical for recovery:** The `cascade_phase` MUST include the full object with `completed`, `skipped`, AND `frozen_descendants`. Without `frozen_descendants`, GitHub-based recovery cannot correctly identify which descendants were promised preparation — it might include new descendants added after the freeze point, leading to unprepared PRs being reconciled (data loss). Without `skipped`, recovery would endlessly retry failed descendants.

**Comment size limits and train size validation:** GitHub comments have a 65536-character limit. Since the status comment contains the full `TrainRecord` JSON (including `frozen_descendants` and `completed` lists), excessively large trains would exceed this limit.

Rather than attempting truncated recovery (which is fragile and can violate freeze invariants), the bot refuses to operate on trains that are too large:

1. **Train size limit**: The bot enforces a maximum of **50 PRs per train** (configurable via `MERGE_TRAIN_MAX_STACK_SIZE`, default 50). This limit is conservative enough to always fit within comment size limits with margin.

2. **Validation on start**: When `@merge-train start` is issued, the bot walks the descendant tree to count total PRs. If the count exceeds the limit:
   - The command is rejected
   - The bot posts an error comment: "Train too large: found N PRs, maximum allowed is 50. Please split this into smaller stacks."
   - No train record is created

3. **Validation during cascade**: Before entering the `Preparing` phase for any PR, the bot re-validates the descendant count. If new PRs have been added that push the total over the limit:
   - The cascade is aborted
   - The bot posts to the current root PR: "Train has grown too large (N PRs, maximum 50). New PRs were added during the cascade. Please split the stack or remove excess PRs, then restart."
   - The train transitions to `aborted` state

4. **Why 50 PRs?** A train with 50 PRs produces status comment JSON of approximately 20-30KB (depending on branch names and SHA lengths), well under the 60KB safe threshold. This leaves ample headroom for human-readable content and any future schema additions.

5. **Unbounded field truncation**: The `error.message` field and other variable-length strings (git command output, API error responses) are truncated before serialization to ensure the total JSON stays within bounds:
   - `error.message`: Maximum 4KB (truncated with "... [truncated]" suffix)
   - `error.stderr`: Maximum 2KB
   - Branch names: Maximum 255 characters each (GitHub's limit)

   Truncation happens **before** JSON serialization, ensuring the size estimate remains valid. The full error details are preserved in the local event log for debugging.

6. **Final size check**: Before posting/updating the status comment, the bot verifies the serialized JSON is under 60KB. If it exceeds this (which should not happen with the above limits):
   - First, aggressively truncate `error.message` and `error.stderr` to 500 characters each and retry
   - If STILL too large after aggressive truncation, this indicates a bug in the size estimation (the 50 PR limit with truncation should always fit). The bot MUST NOT post a minimal comment without JSON, as this would silently disable GitHub-based recovery:
     - Abort the train with error: "Status comment size limit exceeded unexpectedly. This is a bug — please report it with the train configuration. Train aborted to prevent recovery data loss."
     - Transition to `aborted` state
     - Log the full serialized JSON size and structure for debugging
   - The local state remains authoritative; the user can retry after the bug is investigated

**Why not degrade gracefully?** Posting a status comment without the embedded JSON would allow the cascade to continue, but if the bot crashes, GitHub-based recovery cannot determine which descendants were frozen, which were skipped, or what phase was interrupted. The train would transition to `needs_manual_review` on restart, forcing manual intervention anyway — but with the added risk that the cascade made progress that can't be safely resumed. It's better to fail fast when we detect the size limit is exceeded.

This approach is simpler and safer than truncation-based recovery, which would require reconstructing `frozen_descendants` from current declarations — a process that violates the freeze invariant by potentially including PRs added after preparation began or missing PRs that were removed via comment edits.

**Primary vs recovery sources:** During normal operation, the local event log is the primary target for state changes. Status comments mirror this state for disaster recovery. On restart, "Recovery precedence" (above) determines which source to use when they disagree. Status comments serve as:
- User-facing observability (what is the bot doing?)
- Fallback recovery source if local state is lost

**Security:** The bot verifies that the comment author's user ID matches its own before parsing, preventing injection of fake state by malicious users.

**Deletion handling:** If a user deletes the status comment, the bot recreates it on the next phase transition.

**Recovery without status comment:** If local state is lost AND the status comment is missing (deleted or bot identity changed), GitHub-based recovery cannot determine precise cascade state. The bot falls back to **inference-based recovery**:

1. **Scan for active trains**: Query open PRs with predecessor declarations. Build the stack topology from these declarations.

2. **Check merge status**: For each stack, identify which PRs have already been merged (from GitHub PR state).

3. **Infer cascade position**: The "current PR" is the first non-merged PR in the stack. If all PRs are merged, the train is complete.

4. **Cannot infer cascade phase**: Without the status comment, the bot cannot know which phase (`Preparing`, `Reconciling`, etc.) was interrupted. The bot MUST:
   - Mark the train as `needs_manual_review` (a new state)
   - Post a NEW status comment explaining the situation: "Train state was lost. Manual review required. The bot has identified [list of descendants] but cannot safely resume mid-operation. Options: (a) Issue `@merge-train stop` then `@merge-train start` to restart from current position, (b) Manually complete the cascade."
   - Do NOT attempt to auto-resume — the risk of data loss (e.g., skipping preparation) is too high.

5. **Bot identity change**: If the bot's GitHub identity changes (different app installation, new bot user), it cannot locate its own status comments by author check. The bot should log this condition and treat it as "status comment missing."

This fallback is lossy — some in-progress work may need to be repeated — but it prevents permanent inability to recover. The key insight is that stack TOPOLOGY can be reconstructed from predecessor declarations, but operational STATE (which phase, which descendants completed) cannot.

### Local state storage

The bot persists per-repo state using an append-only event log with periodic snapshots:

**Directory structure (default):**

```
/var/lib/merge-train/state/
  owner/
    repo/
      generation        # current generation number (single integer)
      snapshot.0.json   # snapshot for generation 0
      events.0.log      # event log for generation 0 (JSON Lines)
      snapshot.1.json   # snapshot for generation 1 (after compaction)
      events.1.log      # event log for generation 1
      spool/            # webhook delivery spool (separate, transient)
        <delivery-id>.json
```

Only the current generation's files are active; older generations are deleted after compaction completes.

**Webhook spool lifecycle:**

The spool is a durable queue, not just a transient buffer. Deliveries progress through states using **separate marker files** (not renames):

```
<delivery-id>.json       → pending (just received, contains payload)
<delivery-id>.json.proc  → processing (empty marker: worker claimed it)
<delivery-id>.json.done  → processed (empty marker: state effects durably persisted)
```

**File semantics:**
- `.json` file: Contains the actual webhook payload (headers + body). Created atomically via temp file + rename + fsync + directory fsync.
- `.json.proc` marker: Empty file indicating a worker is processing this delivery. Created atomically. If present without `.done`, the processing was interrupted.
- `.json.done` marker: Empty file indicating the delivery's state effects are durably persisted. Created atomically after fsync of the event log.

**Why separate marker files instead of renaming the payload file:**
1. **Crash atomicity**: Creating an empty file is simpler to make atomic than renaming a file while preserving its content.
2. **Debugging**: The original `.json` payload remains readable even after processing, aiding diagnosis.
3. **No content loss risk**: A crash during rename could corrupt the payload; marker files avoid this.

**Cleanup/GC**: A delivery is deleted after BOTH:
1. Its `.done` marker exists, AND
2. A grace period (default: 1 hour) has passed

This allows debugging of recently processed events.

**Dedupe strategy:** Deliveries are deduped by multiple stable identifiers, not just `X-GitHub-Delivery`:
- For `issue_comment.created` events: `(PR number, comment ID, "created")` — a comment can only be created once
- For `issue_comment.edited` events: `(PR number, comment ID, "edited", updated_at)` — each edit has a distinct timestamp; using just `(comment ID, "edited")` would incorrectly drop subsequent edits
- For `pull_request` events: `(PR number, action, head SHA)` for most actions; for `pull_request.edited`: `(PR number, "edited", updated_at)` — edits that don't change head SHA (base retargets, title changes) must not be deduplicated against each other
- For `check_suite` events: `(check suite ID, action, updated_at)` — reruns reuse the same check suite ID, so `updated_at` is needed to distinguish subsequent completions

This handles GitHub's redelivery with new delivery IDs for the same logical event.

**Dedupe key persistence:** The seen dedupe keys are stored in the snapshot to survive restarts:

```json
{
  "seen_dedupe_keys": {
    "issue_comment:123:456789": "2024-01-15T10:00:00Z",
    "pull_request:124:opened:abc123": "2024-01-15T10:01:00Z"
  }
}
```

Keys are pruned after 24 hours (configurable via `MERGE_TRAIN_DEDUPE_TTL_HOURS`). Before processing any event, check the dedupe key against this set; if present, skip processing and immediately mark `.done`.

**Idempotency beyond dedupe:** Even with dedupe, some actions may be retried due to crashes (the dedupe keys live in the snapshot, and snapshots are periodic). All external actions must be idempotent:
- Before git push: check if remote ref already has expected tree content (see "Git push idempotency details")
- Before squash-merge: check if PR is already merged
- Before retarget: check if PR base already equals target
- Before posting ack reaction (👍): check if bot already reacted to that comment (via `GET /repos/{o}/{r}/issues/comments/{id}/reactions`)
- Before posting/updating status comment: check if comment already exists with matching content; use comment ID recorded in train state to find existing comment
- Before posting error/notification comments: include a unique identifier (e.g., event sequence number) in the comment body so duplicates can be detected and skipped

The event log is persistent (state changes are retained for recovery/debugging).

**Event log format (`events.<N>.log`):**

Newline-delimited JSON (JSON Lines), one event per line. Each event includes a monotonic sequence number. **Critical**: All fields needed to reconstruct train state must be durably logged — the event log is the primary persistence mechanism. During recovery, GitHub status comments may supersede local state when `recovery_seq` indicates they are ahead (see "Recovery precedence").

```json
{"seq":1,"ts":"2024-01-15T10:00:00Z","type":"train_started","root_pr":123,"current_pr":123}
{"seq":2,"ts":"2024-01-15T10:01:00Z","type":"phase_transition","train_root":123,"current_pr":123,"predecessor_pr":null,"last_squash_sha":null,"phase":{"Preparing":{"completed":[],"skipped":[],"frozen_descendants":[124]}}}
{"seq":3,"ts":"2024-01-15T10:01:30Z","type":"phase_transition","train_root":123,"current_pr":123,"predecessor_pr":null,"last_squash_sha":null,"phase":{"SquashPending":{"frozen_descendants":[124],"skipped":[]}}}
{"seq":4,"ts":"2024-01-15T10:02:00Z","type":"squash_committed","train_root":123,"pr":123,"sha":"abc123"}
{"seq":5,"ts":"2024-01-15T10:02:30Z","type":"phase_transition","train_root":123,"current_pr":123,"predecessor_pr":null,"last_squash_sha":"abc123","phase":{"Reconciling":{"completed":[],"skipped":[],"frozen_descendants":[124]}}}
{"seq":6,"ts":"2024-01-15T10:03:00Z","type":"phase_transition","train_root":123,"current_pr":124,"predecessor_pr":123,"last_squash_sha":"abc123","phase":"Idle"}
{"seq":7,"ts":"2024-01-15T10:10:00Z","type":"train_completed","root_pr":123}
```

**Required fields per event type:**
- `train_started`: `root_pr`, `current_pr`
- `phase_transition`: ALL of `train_root`, `current_pr`, `predecessor_pr`, `last_squash_sha`, `phase` (as full `CascadePhase` object including `completed` lists for multi-descendant phases)
- `squash_committed`: `train_root`, `pr`, `sha`
- `train_completed`/`train_stopped`/`train_aborted`: `root_pr`
- `intent_push_prep`/`intent_push_reconcile`/`intent_push_catchup`: `train_root`, `branch`, `pre_push_sha`, `expected_tree`
- `done_push_prep`/`done_push_reconcile`/`done_push_catchup`: `train_root`, `branch`
- `intent_squash`: `train_root`, `pr`
- `intent_retarget`: `train_root`, `pr`, `new_base`
- `done_retarget`: `train_root`, `pr`

Without these fields durably logged, recovery cannot determine which PR the train was processing or what SHA to use for reconciliation.

**Snapshot format (`snapshot.<N>.json`):**

```json
{
  "schema_version": 1,
  "snapshot_at": "2024-01-15T10:30:00Z",
  "log_generation": 1,
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
      "recovery_seq": 5,
      "state": "running",
      "original_root_pr": 123,
      "current_pr": 123,
      "cascade_phase": "Idle",
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

1. Read the `generation` file to find current generation `N`
2. Load `snapshot.<N>.json` if it exists (fall back to `snapshot.<N-1>.json` if crash during compaction)
3. Seek to byte offset `log_position` in `events.<N>.log` and replay from there
4. Skip any trailing partial line (incomplete JSON due to crash mid-append)
5. Rebuild in-memory state

**Partial line handling:** On replay, if the final line doesn't parse as valid JSON, truncate the log file at the start of that line and continue. This handles crashes mid-append without failing boot.

**Compaction:**

Compaction cannot atomically update both the snapshot and truncate the log — a crash between these operations would either replay duplicates or lose events. Instead, use a generation-based scheme:

- Periodically (e.g., every hour) or when log exceeds size threshold
- Increment the generation number (stored in a `generation` file, starting at 0)
- Write snapshot to `snapshot.<gen>.json` (e.g., `snapshot.1.json`) with `log_generation = <gen>` and `log_position = 0`
- fsync the snapshot file, then fsync the directory
- Start a new log file `events.<gen>.log` (new events append here)
- Update `generation` file to the new generation, fsync, fsync directory
- **Only after** the new generation is durable, delete the old snapshot and log files

On startup:
1. Read the `generation` file to find the current generation `N`
2. Load `snapshot.<N>.json` (or `snapshot.<N-1>.json` if N doesn't exist — crash during compaction)
3. Replay from the corresponding `events.<N>.log` starting at `log_position`
4. Clean up any stale files from older generations

This ensures that at any crash point, either the old or new generation is complete and consistent. Events for completed/stopped trains can be omitted from the snapshot.

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
- `phase_transition` (any phase change: `Preparing`, `SquashPending`, `Reconciling`, `Idle`)
- `squash_committed` (records the squash SHA needed for reconciliation recovery)

**Relationship to operation commit points**: These recovery-critical events are **phase-level** markers — they record state machine transitions, not individual operations. Irreversible operations (git push, squash-merge, retarget) use a separate **intent/done commit-point pattern** described in "Durability and commit points" below. The distinction:
- Phase-level events (above): Recorded once per phase transition, enable recovery to the correct phase
- Operation-level commit points: Bracket each irreversible operation with intent→operation→done, enabling idempotent retry

Note that `squash_committed` serves a dual role: it's both the completion event for the squash-merge operation (part of the intent/done pattern) AND a recovery-critical event requiring immediate fsync.

Non-critical events (fsync batched for performance):
- `pr_merged`, `pr_state_changed`, `predecessor_declared`

**Snapshot update strategy**:

Use the **generation-based compaction** approach (see "Compaction" in the Data Model section and "Snapshot update" in State Persistence). The naive "rename snapshot then truncate log" approach is **not crash-atomic** — a crash between these operations would either replay duplicates or lose events. Generation-based compaction provides correct crash recovery by maintaining complete generations that can be switched atomically.

**Critical**: All directory fsyncs (for both spool and snapshot directories) are **mandatory**, not best-effort. Without directory fsync, a power loss can drop files even after the file contents were fsynced, because the directory entry wasn't persisted.

**Concurrency**: This design assumes a single active bot process per `state_dir`. Multi-instance deployment is a non-goal.

**Startup lock**: To prevent accidental multi-instance split-brain (which could cause double-merges), the bot acquires an exclusive `flock` on `<state_dir>/lock` at startup:

```rust
use std::fs::File;
use fs2::FileExt;

fn acquire_state_lock(state_dir: &Path) -> Result<File> {
    let lock_path = state_dir.join("lock");
    let lock_file = File::create(&lock_path)?;
    lock_file.try_lock_exclusive().map_err(|_| {
        Error::AnotherInstanceRunning(lock_path)
    })?;
    Ok(lock_file) // Hold this for the lifetime of the process
}
```

If the lock is already held, the bot fails immediately with a clear error message. The lock is released automatically when the process exits.

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

    /// Active trains, keyed by original_root_pr (the PR that received @merge-train start).
    /// This key is STABLE throughout the cascade — it doesn't change as PRs merge.
    /// Use TrainRecord.current_pr to find which PR is currently being processed.
    active_trains: HashMap<PrNumber, TrainRecord>,

    /// Filesystem path for this repo's state directory
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
   a. Read the `generation` file to find current generation `N`
   b. Load `<state_dir>/<owner>/<repo>/snapshot.<N>.json` if it exists (fall back to `snapshot.<N-1>.json` if crash during compaction)
   c. Replay events from `events.<N>.log` starting at byte offset `log_position` (not by timestamp — timestamps can drift or be non-monotonic)
   d. Rebuild in-memory state
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
9. **Train state recovery**: For each PR (open OR recently merged) that targets the default branch:
   a. If a bot-owned status comment with `<!-- merge-train-state {...} -->` is found:
   b. Verify comment author matches bot's user ID (security check)
   c. Parse the JSON to extract full `TrainRecord`
   d. Compare `recovery_seq` to determine which record is most recent (see below)
   e. Add to `active_trains` map (most recent wins if duplicates exist)
10. For each open PR, fetch `mergeStateStatus` via GraphQL
11. Build `RepoState` with default branch, PR map, descendants index, and recovered active trains
12. **Persist state**: Write snapshot to disk (see State Persistence section)

**Recovery and completion (both paths)**

13. **Recovery check**: For any train in a non-`Idle` `cascade_phase`, evaluate whether to resume:
    - If `Preparing`: Re-run preparation (merge operations are idempotent if already pushed)
    - If `SquashPending`: Check if PR is already merged; if not, proceed with squash
    - If `Reconciling`: Use `last_squash_sha` from the train record to complete reconciliation
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
| `pull_request.edited` (base changed) | Update `base_branch`, re-validate predecessor relationship (see "Continuous base branch validation") |
| `pull_request.synchronize` | Update `head_sha`, set `merge_state` to Unknown |
| `check_suite.completed` | Re-fetch `merge_state` via GraphQL |
| `pull_request_review` | Re-fetch `merge_state` via GraphQL |

**Train state updates during cascade**: The bot appends events to the event log and updates the status comment at each phase transition:
- Before preparation: `cascade_phase = "Preparing"` → append `phase_transition` event, update status comment
- After preparation: `cascade_phase = "SquashPending"`, `predecessor_pr = <pr_number>` → append event, update comment
- After squash: `cascade_phase = "Reconciling"`, `last_squash_sha = <sha>` → append event, update comment
- After reconciliation: `cascade_phase = "CatchingUp"` → append event, update comment
- After catch-up: `cascade_phase = "Retargeting"` → append event, update comment
- After retargeting: `cascade_phase = "Idle"`, advance `current_pr` → append event, update comment

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
| Phase transition | Append event to `events.log` | Yes (critical) |
| Train start | Append event to `events.log` | Yes (critical) |
| Train stop/abort | Append event to `events.log` | Yes (critical) |
| Train completion | Append event to `events.log` | Yes (critical) |
| Squash committed | Append event to `events.log` | Yes (critical) |
| PR merged | Append event to `events.log` | No (batched) |
| PR state change | Append event to `events.log` | No (batched) |

**Append strategy:**

```rust
fn append_event(log: &mut File, event: &Event) -> io::Result<()> {
    writeln!(log, "{}", serde_json::to_string(event)?)?;
    if event.is_critical() {
        log.sync_all()?; // fsync on phase transitions, train lifecycle, squash commits
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

**Snapshot update (generation-based):**

See "Compaction" in the Data Model section for the full algorithm. In brief:

- Write new snapshot to `snapshot.<N+1>.json` with `log_generation = N+1`, `log_position = 0`
- fsync the file, then fsync the directory
- Start new log file `events.<N+1>.log`
- Update `generation` file to `N+1`, fsync, fsync directory
- Delete old `snapshot.<N>.json` and `events.<N>.log` only after new generation is durable

This avoids the crash-atomicity problem of "rename snapshot, then truncate log" — at any crash point, at least one complete generation exists.

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
    /// SHA of the predecessor's squash commit that this PR was reconciled against.
    /// Set after normal cascade or late-addition reconciliation completes.
    /// Required for is_root() to return true when predecessor is merged.
    predecessor_squash_reconciled: Option<String>,
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

1. Find all root PRs (target default branch, no predecessor or predecessor merged — see `is_root()` below)
2. Build linear chains from each root by following `descendants` index
3. Validate: no cycles, predecessors exist
4. Check `active_trains` to determine if each stack is started/stopped

**Fan-out handling**: When a PR has multiple open descendants (fan-out), the stack ends at that PR. Each descendant will become the root of its own independent stack once its predecessor merges. After the fan-out point merges:
- Each descendant is retargeted to the default branch
- Each descendant receives its own train record in the local state store (inheriting "started" status from the parent train)
- They proceed as independent trains; whichever passes CI first merges next
- The `cascade_step` returns `FanOut { descendants }` to trigger train record creation and status comment posting on each new root

Note: `FanOut` is a **cascade-internal** mechanism — it's a return value from the cascade step function, not a webhook event type. The cascade that processes the fan-out point handles train record creation synchronously before returning.

---

## Event Handling

| Event | Action |
|-------|--------|
| `issue_comment.created` with `@merge-train predecessor #N` | Validate, record, ack |
| `issue_comment.created` with `@merge-train start` | Squash-merge root, begin cascade |
| `issue_comment.created` with `@merge-train stop` | Mark stack stopped, report state |
| `issue_comment.edited` with `@merge-train predecessor #N` | Validate (same as new), update predecessor if valid |
| `issue_comment.edited` where authoritative predecessor comment no longer contains command | Remove predecessor relationship (see below) |
| `issue_comment.deleted` | If deleted comment was a predecessor declaration, remove predecessor relationship |
| `pull_request` merged | If merged PR has descendants, cascade to next |
| `pull_request` closed (not merged) | Notify descendants they're orphaned |
| `check_suite.completed` / `status` (terminal state) | If cascade waiting on this PR AND event SHA matches expected head, re-evaluate `mergeStateStatus` (see "Eventual consistency caveat") |
| `pull_request_review` submitted (approved) | If cascade waiting on this PR, re-evaluate readiness (but see "Review dismissal behaviour" — dismissed reviews require `@merge-train start` to resume) |
| `pull_request.ready_for_review` | If cascade waiting on this PR due to draft status, re-evaluate readiness |

**Comment edit handling**: When a predecessor declaration is edited, the bot handles several cases:

1. **Command changed** (e.g., `#123` to `#456`):
   - **Re-run full validation** (same as new declarations):
     a. Validate that `#456` either targets the default branch or itself has a predecessor declaration
     b. Validate that the current PR's base branch matches `#456`'s head branch (unless `#456` is already merged — see "Continuous base branch validation")
   - If validation **passes**: Update the predecessor relationship to point to the new PR. React with 👍 on the edited comment.
   - If validation **fails**: **Reject the edit**. Post an error comment explaining the validation failure. The predecessor relationship remains unchanged (still pointing to the old predecessor). This prevents edited comments from creating invalid stacks that only fail mid-cascade.
2. **Command removed** (e.g., editing to remove `@merge-train predecessor` entirely): If this was the authoritative predecessor comment for the PR (tracked via comment ID), remove the predecessor relationship. The PR becomes orphaned unless another predecessor comment exists (which would be an error state — see "Multiple predecessor comments").
3. **Command added to non-predecessor comment**: Rejected if the PR already has a predecessor declaration.

The dedupe key for edits is `(PR number, comment ID, "edited", updated_at)` — the `updated_at` timestamp distinguishes different edits of the same comment, preventing later edits from being incorrectly dropped. If a train is already started and the predecessor is changed or removed, the bot aborts with an error (cannot safely change stack structure mid-cascade).

**Comment deletion handling**: When a comment is deleted, the bot checks if it was the authoritative predecessor declaration for a PR. If so:
1. Remove the predecessor relationship from the cached state
2. If a train is running that involves this PR, abort with error: "Predecessor declaration deleted mid-cascade"
3. The PR becomes orphaned (no longer part of any stack) unless another predecessor comment exists

To identify predecessor comments for deletion handling, the bot tracks `(PR number, comment ID)` for each predecessor declaration in the event log. On `issue_comment.deleted`, look up whether that comment ID was a predecessor declaration.

**Multiple predecessor comments**: A PR should have exactly one predecessor declaration. If multiple `@merge-train predecessor` comments are created:
1. The FIRST valid declaration (by comment creation time) is authoritative
2. Subsequent declarations are rejected with error: "PR already has predecessor declaration in comment #C pointing to #N. Edit that comment to change predecessors, or delete it first."
3. This prevents conflicting/ambiguous stack topology

If the authoritative predecessor comment is deleted and other predecessor comments exist, the bot does NOT automatically promote another comment — the PR becomes orphaned. The user must create a fresh declaration to re-establish the relationship. This avoids silently switching predecessors based on comment ordering races.

**Fan-out discovery**: After a fan-out point merges, each descendant is discovered as a new root on subsequent events. Since each descendant now targets the default branch and its predecessor is merged, `is_root()` returns true and `compute_stacks()` includes it. No special **webhook event handling** is needed — the normal `pr_merged` webhook triggers the cascade which internally handles fan-out via the `FanOut` return value (see "Fan-out handling" above). After that, each new independent branch follows the normal cascade flow.

### Per-repo serial event processing

The server processes webhook events **serially per repository** via per-repo queues. Different repositories are processed concurrently, but events within a single repository are strictly serialized. Webhook deliveries are durably spooled to disk before being enqueued so a crash/restart cannot drop events after a `202 Accepted`.

**Important distinction**: "Serial event processing" refers to **webhook event dispatch**, not stack execution. Multiple stacks can make progress concurrently within a single repo worker through timer-based yielding (see "Non-blocking polling" below). Events are dequeued one at a time, but each event handler yields control during long waits, allowing other stacks' timers to fire.

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
3. Writes the delivery (headers + body) to the per-repo disk spool:
   a. Write to a temp file in the spool directory
   b. fsync the temp file
   c. Rename to `<delivery-id>.json` (atomic on POSIX)
   d. fsync the spool directory (ensures the directory entry is durable — without this, a power loss could drop the file even though it was written)
4. Classifies the delivery by priority (stop commands have higher priority)
5. Pushes the parsed event onto the **per-repo** priority queue (creating queue + worker if needed)
6. Returns `202 Accepted`

**Critical:** Step 3d (fsync directory) is required for durability. A successful write and file rename does NOT guarantee the directory entry survives a power loss — the directory must also be synced. Only after this completes can we safely return `202 Accepted`.

Each per-repo worker loop:
1. Ensures repo state is loaded/bootstrapped from disk (or re-syncs from GitHub if needed)
2. Drains the per-repo disk spool:
   a. First, clean up interrupted processing: delete any `.json.proc` markers without corresponding `.done` markers — these are deliveries that were being processed when a crash occurred, and need to be reprocessed
   b. Then replay any `.json` files without corresponding `.done` markers (parse and enqueue)
3. Pulls the highest-priority event from its queue
4. Creates `.json.proc` marker for the delivery (claiming it for processing)
5. Applies incremental state update from the event
6. Appends event to log
7. If event is recovery-critical:
   a. fsync the log immediately
   b. Create `.done` marker for the delivery (empty file, created atomically)
   c. fsync the spool directory (required — without this, the `.done` marker may not survive power loss, causing replay of an already-processed event)
   d. Delete the `.json.proc` marker (optional, for cleanliness)
8. If event uses batched fsync:
   a. Add delivery ID to pending batch set
   b. On next batch fsync (or before any irreversible operation), fsync the log
   c. Create `.done` markers for all deliveries in the pending batch
   d. fsync the spool directory once (after all `.done` files are created)
   e. Delete `.json.proc` markers for all deliveries in the batch (optional)
   f. Clear the pending batch set
9. Evaluates cascade actions based on updated state
10. On cascade phase transitions or irreversible operations, flush pending batch first
11. Loops (or shuts down after idle timeout)

**Marker file atomicity:**
```rust
// Creating a marker file atomically
fn create_marker(path: &Path) -> io::Result<()> {
    // Create marker with a unique temp name first.
    // IMPORTANT: Append ".tmp" rather than replacing the extension, so that
    // <id>.json.proc and <id>.json.done get distinct temp files:
    //   <id>.json.proc -> <id>.json.proc.tmp
    //   <id>.json.done -> <id>.json.done.tmp
    let mut temp_name = path.as_os_str().to_owned();
    temp_name.push(".tmp");
    let temp = PathBuf::from(temp_name);

    let file = File::create(&temp)?;
    file.sync_all()?;  // fsync the (empty) file
    std::fs::rename(&temp, path)?;  // atomic rename
    // Directory fsync happens separately after all markers in batch
    Ok(())
}
```

**Critical invariant:** A delivery's `.done` marker is only created after all state effects from that delivery are durably persisted (fsynced). This ensures that on restart, unprocessed deliveries are replayed correctly. For batched events, `.done` is deferred until the batch fsync completes.

**Priority ordering**: Stop commands (`@merge-train stop`) are processed before all other events within that repo's queue. This ensures that a human request to halt the cascade takes effect promptly.

**Cancellation**: Cancellation is **stack-scoped**. Each stack has its own `CancellationToken`, managed by the worker. When a stop command arrives, the dispatcher:
1. Sends a `CancelStackRequest` (containing the PR number) to the worker via a separate channel
2. The worker resolves the PR to its stack root and cancels that stack's token
3. Enqueues the stop event (which will be processed immediately due to priority)

This allows long-running operations like `git merge` or `git push` to be interrupted promptly when a human requests a stop — **without affecting other stacks in the same repo**.

**Non-blocking polling**: When the bot needs to wait for GitHub state to propagate (e.g., `headRefOid` to match after a push, or `mergeStateStatus` to transition from `UNKNOWN`), it does **not** block the event queue:

1. **Timer-based re-evaluation**: Instead of synchronous polling loops, the bot:
   - Records what condition it's waiting for in the train state (e.g., `waiting_for: { headRefOid: "<expected_sha>" }`)
   - Sets a timer (e.g., 5s) to re-check
   - **Yields control** back to the event loop, allowing other events (including stop commands) to be processed
   - When the timer fires, re-evaluates the condition and either proceeds or schedules another timer

2. **Event-driven updates**: Many conditions resolve via webhook events rather than polling:
   - `check_suite.completed` / `status` → re-evaluate `mergeStateStatus`
   - `pull_request.synchronize` → `headRefOid` updated
   - The bot processes these events and checks if the waiting condition is satisfied

3. **Timeout handling**: After a maximum wait time (configurable, default 5 minutes), the bot:
   - Logs a warning: "Timed out waiting for GitHub state propagation"
   - Treats this as a transient failure (see retry logic above)
   - Does NOT abort permanently — GitHub may be experiencing delays

This ensures stop commands are processed promptly (within one timer interval) even while waiting for state propagation, and independent stacks in the same repo can proceed concurrently. (This is how "per-repo serial event processing" coexists with concurrent stack execution — see the "Important distinction" note in that section.)

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
        ├─ If frontier ready (CLEAN/UNSTABLE) and predecessor merged: cascade
        ├─ If frontier waiting (BLOCKED/UNKNOWN/isDraft): enter waiting_ci, poll for resolution
        └─ If frontier has permanent failure (DIRTY, review dismissed): abort, notify
```

### Polling fallback for active trains

Webhooks are the primary trigger for cascade evaluation, but they're not sufficient alone:

- **Missed webhooks**: Network issues, server downtime, or GitHub outages can cause webhook delivery failures
- **Out-of-order delivery**: Webhooks may arrive in unexpected order, causing missed state transitions
- **Incomplete event mapping**: Some events (notably `status`) don't directly map to PRs, making it hard to know which train to re-evaluate

To ensure progress even when webhooks fail, the bot polls active trains periodically as a fallback.

**Polling frequency**: Once per 10 minutes per active train (configurable via `MERGE_TRAIN_POLL_INTERVAL_MINS`, default 10). This is relatively infrequent because:
- Webhook failures are rare
- Polling is expensive (requires API calls to refresh merge state)
- The failure mode is "cascade stalls" not "cascade breaks"

The 10-minute default balances responsiveness (not waiting an hour for a missed webhook) against API cost. For high-traffic installations, increase this; for critical workflows, decrease it.

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
   a. Fetch current `mergeStateStatus` and `isDraft` for the frontier PR via GraphQL
   b. Compare with cached `merge_state` and `is_draft`
   c. If changed (e.g., now `CLEAN` when previously `BLOCKED`, or `isDraft` changed from `true` to `false`), trigger cascade evaluation
2. If any train's frontier PR is now ready but wasn't before, the missed webhook is effectively recovered

**Distributed polling**: To avoid thundering herd when multiple bot instances restart:
- Add jitter to the poll interval (e.g., 10 ± 2 minutes)
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
4. For each active train in a non-`Idle` phase, performs recovery check (see "Recovery semantics")
5. Optionally refreshes train state from GitHub status comments (see "Supplementary GitHub recovery")

**Worktree cleanup on restart:**

If the process dies mid-git operation, worktrees may be left in a dirty state (in-progress merge, uncommitted changes). Since worktrees use **detached HEAD mode** (not checked-out branches), and branches may be deleted after PR merge, the cleanup approach must not rely on `origin/<branch>` being available.

**Cleanup strategy:**

```rust
async fn cleanup_worktree_on_restart(
    worktree_path: &Path,
    train_state: &TrainRecord,
    ctx: &AppContext,
) -> Result<()> {
    // First, try to abort any in-progress merge
    run_git_in_worktree(worktree_path, &["merge", "--abort"]).await.ok();

    // Try to clean up uncommitted changes
    let cleanup_result = run_git_in_worktree(worktree_path, &["reset", "--hard"]).await
        .and_then(|_| run_git_in_worktree(worktree_path, &["clean", "-fd"]).await);

    match cleanup_result {
        Ok(_) => {
            // Worktree is clean, but we're in detached HEAD at an unknown commit.
            // The recovery logic will fetch and checkout the appropriate ref.
            Ok(())
        }
        Err(_) => {
            // Worktree is too corrupted — delete and recreate
            tracing::warn!(
                ?worktree_path,
                train_root = train_state.original_root_pr.0,
                "worktree corrupted, deleting and recreating"
            );
            delete_worktree(worktree_path).await?;
            // Worktree will be recreated on first cascade operation
            Ok(())
        }
    }
}
```

**Why not `git reset --hard origin/<branch>`:**
1. **Detached HEAD**: Worktrees use detached HEAD mode to avoid branch locking issues across multiple worktrees.
2. **Branch deletion**: After a PR merges, GitHub may delete the branch (if "delete branch on merge" is enabled).
3. **Unknown state**: On restart, we don't know which branch the worktree was working on — the recovery logic determines this from the `TrainRecord`.

**Recovery flow after cleanup:**
1. Worktree cleanup just ensures no uncommitted changes or in-progress merges
2. Recovery logic (see `recover_in_progress_trains`) determines what state we need
3. Recovery fetches the appropriate refs (via `refs/pull/<n>/head` which is always available)
4. Recovery checks out and resumes from the correct point

**Fallback — delete and recreate:**
If a worktree is too corrupted to clean (e.g., `.git` file corrupted, missing index), the safest approach is to delete and recreate it. Since worktrees don't contain any state that isn't recoverable from the remote and the `TrainRecord`, this is always safe.

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

This workflow is performed for EACH descendant PR (i.e., for every PR that had the just-merged PR as its predecessor). All descendants are processed using local git — the GitHub API is NOT used for these reconciliation merges because GitHub's recursive/ort merge strategy would cause spurious conflicts or incorrect results (see "Why local git is required" above). The one exception is the squash-merge into `main`, which uses the GitHub API (see "Commit signing" below). Once all descendants have been prepared, reconciled, caught up, and retargeted, the cascade proceeds to the next level as described in "Operation sequence" below.

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

**PR ref availability**: On github.com, PR refs are retained indefinitely and never garbage-collected. However, on GitHub Enterprise Server (GHES) instances, administrators may configure aggressive ref garbage collection that removes PR refs after the PR is closed. If the bot fails to fetch a PR ref:

1. The fetch operation fails with a clear git error ("couldn't find remote ref")
2. The bot aborts the cascade with an actionable error message:
   ```
   Failed to fetch predecessor PR #123's head ref (refs/pull/123/head).

   This ref may have been garbage-collected. On github.com this should not happen;
   on GitHub Enterprise Server, check your instance's ref retention settings.

   The cascade cannot proceed without the predecessor's commit history.
   ```
3. The train is marked as aborted; the user must manually resolve (potentially by re-opening/re-creating the predecessor PR to restore its refs)

### Operation sequence

Let's assume a stack of this shape: main ← #123 ← #124 ← #125

**REQUIRED TESTS**: The correctness of this operation sequence is verified by Properties 1-3 in the "Property-based testing with real git" section:
- Property 1 (`descendant_content_preserved_after_cascade`): Verifies all content from predecessor, descendant, and intervening main commits is preserved
- Property 2 (`intervening_main_commits_preserved`): Verifies commits pushed to main during the cascade (that don't conflict) survive the train
- Property 3 (`squash_parent_ordering_prevents_lost_commits`): Verifies the $SQUASH_SHA^ ordering is essential and that the naive (wrong) approach loses commits

The cascade proceeds by repeating the following for each PR, starting from the root:

```
For PR #N with descendants {#D1, #D2, ...} (may be one or multiple):

1. PREPARATION: For EACH descendant, merge predecessor head ONLY (NOT main!)
   → Local git: git merge <N.head_sha>
   → Ensures descendant has all of #N's final content
   → Signed by bot
   → Push to origin
   → Must happen BEFORE squash-merging #N
   → Loop over ALL descendants before proceeding to step 2

   CRITICAL: Do NOT merge main here! See "Why merging $SQUASH_SHA^ is essential" below.
   Merging main before we know the squash SHA causes lost commits.

2. SQUASH: Squash-merge #N into main
   → GitHub API, signed by GitHub
   → Returns squash_commit_sha (the commit on main)

3. RECONCILIATION: For EACH descendant, incorporate the squash into history
   a. Merge $SQUASH_SHA^ (parent of squash = main state just before squash)
      → Local git: git merge <squash_commit_sha>^
      → Incorporates all main content up to (but not including) the squash
   b. Ours-merge the squash commit itself
      → Local git: git merge <squash_commit_sha> --strategy ours
      → Marks squash as ancestor without changing tree (which already has the content)
   → Both merges signed by bot
   → Push to origin

4. CATCH-UP: For EACH descendant, merge any subsequent main commits
   → Local git: git merge origin/main
   → Regular merge (not ours) to incorporate commits that landed AFTER the squash
   → If main hasn't advanced past the squash commit, this is a no-op
   → **If merge conflicts**: Genuine conflict between descendant's work and new main content.
     Abort with specific error: "Conflict with commits on main that landed after the squash."
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

For the root PR (#123), there is no predecessor whose content needs to be merged *into* the root (since the root's base is main). However, step 1 still applies: if #123 has descendants, they must be prepared (by merging #123's head into them) before squashing #123.

For the final PR in the stack (#125), there is no descendant, so steps 1, 3-5 are skipped.

### Descendant set freezing

**Invariant**: The descendant set for PR #N is frozen at the moment we begin preparing descendants for #N's squash. Any new PRs that declare #N as their predecessor after this point are **not** included in the current cascade step for #N. However, new descendants for PRs that haven't been processed yet (further down the stack) ARE still discovered when the cascade reaches them — this is a per-PR freeze, not a per-train freeze.

**Why this matters**: Preparation must happen for ALL descendants BEFORE squashing #N (step 1 completes entirely before step 2). If a new descendant appears after preparation started but before the squash, that descendant:
- Would not have been prepared (no merge of #N's head)
- Would break after the squash (its base becomes invalid)

**Freeze point and logging**: When entering the `Preparing` phase for PR #N:
1. Query the current descendant set from the `descendants` index
2. Log the `phase_transition` event with the descendant list frozen in the `frozen_descendants` field
3. Only process descendants that were captured at this moment
4. New descendants declared **after the freeze** (whether before or after #N merges) are effectively "late additions" — by the time the cascade could process them, their predecessor is already merged. They're caught by either (a) the `predecessor_declared` event handler checking if the predecessor is already merged, or (b) the polling-based catch-up mechanism that scans for PRs with merged predecessors but unretargeted `base_ref`. See "Late additions recovery" below.

**Late additions recovery**: A "late addition" is any PR whose predecessor is already merged by the time the cascade could process it. This includes PRs declared after the freeze (whether before or after the predecessor merges) and PRs declared after the predecessor has already merged.

The late descendant's `base_ref` still points to the predecessor's branch (not the default branch), so `is_root()` returns false and `compute_stacks()` won't discover it. This requires explicit detection:

**Detection trigger 1 — event-driven**: When processing a `predecessor_declared` event:
1. Look up the predecessor PR from the cache
2. If predecessor is `Merged`, this is a late addition — trigger immediate reconciliation

**Detection trigger 2 — polling-based catch-up**: During `PollActiveTrains`, scan for PRs whose predecessor is merged but whose `base_ref` hasn't been retargeted. This catches late additions where the `predecessor_declared` event arrived before the predecessor merged (and thus wasn't detected at event time).

**Late addition reconciliation flow**:

**IMPORTANT**: Late additions require a DIFFERENT flow than normal cascading!
- In normal cascading, we prepare BEFORE squash (merge predecessor head only)
- For late additions, the predecessor is ALREADY merged, so we go directly to reconciliation
- We cannot use `prepare_descendant` because that's for pre-squash preparation

```rust
async fn handle_late_addition(
    late_pr: PrNumber,
    late_pr_branch: &str,
    merged_predecessor: PrNumber,
    default_branch: &str,
    git: &GitOperations,
    github: &GitHubClient,
) -> Result<()> {
    // 1. Fetch predecessor's final head via GitHub's PR ref
    let pr_ref = format!("refs/pull/{}/head", merged_predecessor.0);
    git.fetch_ref(&pr_ref).await?;

    // 2. Get the squash commit SHA from the merged PR (with retry for eventual consistency)
    //    CRITICAL: Also validate the merge method — see below.
    let squash_sha = fetch_merge_commit_sha_with_retry(github, merged_predecessor)
        .await?
        .ok_or(Error::MissingMergeCommitSha)?;

    // 3. CRITICAL: Validate this was a squash merge, not merge/rebase!
    //    The ours-merge strategy only works correctly for squash merges.
    //    If predecessor was merged with merge/rebase, the merge_commit_sha points to
    //    a different commit structure and the ours-merge will drop or duplicate changes.
    //
    //    We use TWO checks:
    //    a) Query GitHub API for the merge method (authoritative but may be unavailable)
    //    b) Verify commit structure on the default branch (fallback)

    // Check 1: Query GitHub for the merge method via GraphQL
    // The `mergedBy` and `mergeCommit` fields are available, and we can infer the method
    // from commit structure. But GitHub's REST API exposes the merge method directly
    // in the timeline events or via the merge_commit_sha structure.
    //
    // IMPORTANT: GitHub's `merge_commit_sha` semantics differ by merge method:
    // - Squash: Points to the single squash commit on the default branch
    // - Merge: Points to the merge commit (two parents)
    // - Rebase: Points to the LAST rebased commit on the default branch (single parent,
    //   but parent is NOT the prior default branch HEAD — it's the previous rebased commit)

    git.fetch_commit(&squash_sha).await?;
    let commit = git.get_commit_info(&squash_sha).await?;

    // Check for merge commits (easy case)
    if commit.parents.len() != 1 {
        return Err(Error::NonSquashMerge {
            pr: merged_predecessor,
            message: format!(
                "Predecessor #{} was merged with merge (not squash). \
                 Late-addition reconciliation requires squash merge. \
                 Manual intervention required: rebase the late PR onto main.",
                merged_predecessor.0
            ),
        });
    }

    // Defense in depth: The preflight check requires squash-only repo config, but
    // settings can change after preflight or be bypassed by force-push. We MUST
    // validate the parent is the prior default branch HEAD (not just single-parent).
    let parent_sha = &commit.parents[0];

    // CRITICAL: Verify parent is the prior default branch HEAD.
    // For a valid squash: the parent of squash_sha should be where main pointed
    // just before the merge. For a rebase merge, the parent would be the previous
    // rebased commit (NOT on main's history), which would cause reconciliation
    // to use $SHA^ incorrectly.
    git.fetch_ref(&format!("refs/heads/{}", default_branch)).await?;

    // Check: is parent_sha an ancestor of current main HEAD?
    // If not, this wasn't a squash onto main — it's a rebase or force-push.
    let main_head = git.rev_parse(&format!("origin/{}", default_branch)).await?;
    if !git.is_ancestor(parent_sha, &main_head).await? {
        return Err(Error::NonSquashMerge {
            pr: merged_predecessor,
            message: format!(
                "Predecessor #{}'s merge commit parent {} is not in the default branch history. \
                 This may indicate a rebase merge or force-push. \
                 Late-addition reconciliation requires a squash merge. \
                 Manual intervention required: rebase the late PR onto main.",
                merged_predecessor.0, parent_sha
            ),
        });
    }

    // Additional check: verify parent_sha is the commit just before squash_sha on main.
    // This catches edge cases where parent is on main's history but isn't the immediate
    // predecessor (e.g., if other commits landed between the merge and our check).
    // squash_sha should be a direct child of parent_sha AND squash_sha should be
    // reachable from main_head.
    if !git.is_ancestor(&squash_sha, &main_head).await? {
        return Err(Error::NonSquashMerge {
            pr: merged_predecessor,
            message: format!(
                "Predecessor #{}'s merge_commit_sha {} is not in the default branch history. \
                 This is unexpected for a merged PR. Manual intervention required.",
                merged_predecessor.0, squash_sha
            ),
        });
    }

    // 4. Fetch and checkout the late PR branch
    git.fetch_and_checkout_detached(late_pr_branch).await?;

    // 5. Merge predecessor's final head (what they had just before merge)
    // This is analogous to normal preparation, but we already have the squash SHA
    let pred_head_ref = format!("origin/pr/{}", merged_predecessor.0);
    git.merge(&pred_head_ref, "Merge predecessor head (late addition)").await?;

    // 6. Reconcile using the same logic as normal cascading:
    //    - Merge $SQUASH_SHA^ (parent of squash)
    //    - ours-merge $SQUASH_SHA
    //    - Merge origin/main (catch-up)
    //
    // We can reuse reconcile_descendant because it does exactly this.
    git.reconcile_descendant(late_pr_branch, &squash_sha, default_branch).await?;

    // 7. Retarget the PR to the default branch
    github.update_pr_base(late_pr, default_branch).await?;

    // 8. Record that reconciliation completed — this is CRITICAL for is_root()
    //    Without this, is_root() would return false even after reconciliation,
    //    because predecessor_squash_reconciled would still be None.
    repo_state.update_pr_reconciliation(late_pr, squash_sha.clone());

    // 9. Now the PR has base_ref == default_branch AND predecessor_squash_reconciled
    //    is set, so is_root() will return true.
    Ok(())
}
```

**Why this is different from normal flow**:
- Normal flow: prepare (merge predecessor head only) → squash → reconcile (merge $SHA^, ours-merge $SHA, catch-up)
- Late addition: validate squash → merge predecessor head → reconcile (merge $SHA^, ours-merge $SHA, catch-up)

The key insight is that `reconcile_descendant` already does the correct ours-merge dance, so we can reuse it. We just need to first merge the predecessor's head (which prepare_descendant would have done before the squash).

**CRITICAL: Squash validation for late additions**: The ours-merge strategy assumes `merge_commit_sha` is a squash commit (single parent pointing to the prior main HEAD). If the predecessor was merged with merge/rebase, the reconciliation would use the wrong parent and produce incorrect results:

- **Merge commit**: Has two parents, so `$SHA^` doesn't mean "main before squash" — it could be the PR branch head. The ours-merge would incorporate wrong history.
- **Rebase merge**: GitHub's `merge_commit_sha` points to the LAST rebased commit. This has a single parent, but that parent is the PREVIOUS rebased commit, not the prior default branch HEAD. Using `$SHA^` would incorporate the wrong commit, potentially dropping changes or creating duplicates.

The validation uses a two-pronged approach:

1. **Preflight enforcement** (see "Merge method preflight check"): The bot refuses to start on repositories that allow non-squash merge methods. By requiring `allow_merge_commit: false` and `allow_rebase_merge: false` at the repository level, we make non-squash merges unlikely during normal operation. However, this check can be bypassed (e.g., by re-enabling other merge methods after preflight, or by force-pushing directly to the default branch) — see README for caveats.

2. **Parent validation** (required, not optional): We verify that `merge_commit_sha` has exactly one parent AND that parent is the prior default branch HEAD. This catches: (a) repository settings changed after preflight, (b) force-pushed commits, (c) multi-commit rebase or fast-forward merges. Without this check, reconciliation would use `$SHA^` incorrectly and could lose commits.

**REQUIRED TESTS**: The property-based testing section verifies merge method handling:
- Property 4 (`squash_merge_detection_accepts_squash`): Validates that legitimate squash merges (single parent, parent is prior main HEAD) pass validation
- Property 5 (`squash_merge_detection_rejects_merge_commit`): Validates that true merge commits (two parents) are rejected
- Property 6 (`squash_merge_detection_rejects_wrong_parent`): Validates that commits whose parent is NOT the prior main HEAD are rejected (catches multi-commit rebase/fast-forward)
- Preflight test (`squash_only_preflight_check`): Validates that the bot refuses to start when `allow_merge_commit` or `allow_rebase_merge` is true (see "Merge method preflight check" section)

**Polling fallback**: During `PollActiveTrains`, also scan for "orphaned" PRs:
```rust
// Find PRs whose predecessor is merged but base_ref != default_branch
for pr in repo_state.prs.values() {
    if pr.state != PrState::Open { continue; }
    if pr.base_ref == repo_state.default_branch { continue; }

    if let Some(pred) = pr.predecessor {
        if let Some(pred_pr) = repo_state.prs.get(&pred) {
            if matches!(pred_pr.state, PrState::Merged { .. }) {
                // Late addition detected — trigger reconciliation
                handle_late_addition(pr.number, pred, repo_state, ctx).await?;
            }
        }
    }
}
```

This ensures late additions are handled promptly (on predecessor_declared) and caught during polling if the event was missed. After reconciliation and retargeting, `is_root()` returns true and the PR joins normal cascade processing.

**Manually-retargeted PR detection**: A parallel scan catches PRs that were manually retargeted to the default branch before reconciliation could run:

```rust
// Find PRs that were manually retargeted but NOT reconciled
// These are dangerous: base_ref == default_branch but ours-merge was never done
for pr in repo_state.prs.values() {
    if pr.state != PrState::Open { continue; }
    if pr.base_ref != repo_state.default_branch { continue; }

    if let Some(pred) = pr.predecessor {
        if let Some(pred_pr) = repo_state.prs.get(&pred) {
            if matches!(pred_pr.state, PrState::Merged { .. })
                && pr.predecessor_squash_reconciled.is_none()
            {
                // PR was manually retargeted before reconciliation!
                // Post warning and trigger reconciliation.
                github.post_comment(pr.number, &format!(
                    "⚠️ **Warning**: This PR was retargeted to `{}` but has not been \
                     reconciled with predecessor #{}'s squash commit. This PR cannot be \
                     safely merged until reconciliation completes.\n\n\
                     The bot will now attempt reconciliation automatically.",
                    repo_state.default_branch, pred
                )).await?;

                // Reconciliation still works even though base_ref is already main.
                // The git operations (ours-merge) are what matter, not the GitHub base_ref.
                handle_late_addition(pr.number, pred, repo_state, ctx).await?;
            }
        }
    }
}
```

This scan prevents manually-retargeted PRs from being discovered as roots (via `is_root()`) until proper reconciliation completes. The warning comment alerts the PR author to the unusual state.

**Event log format for frozen descendants**:
```json
{"seq":5,"ts":"...","type":"phase_transition","train_root":123,"current_pr":123,
 "predecessor_pr":null,"last_squash_sha":null,
 "phase":{"Preparing":{"completed":[],"skipped":[],"frozen_descendants":[124,125]}}}
```

The `frozen_descendants` field captures the exact set that will be processed. Recovery uses this list to avoid re-querying descendants (which might have changed).

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
| git push (preparation) | `intent_push_prep` (with `pre_push_sha`, `expected_tree`) | `done_push_prep` | Compare tree SHA + verify parent chain (see below) |
| Squash-merge | `intent_squash` | `squash_committed` | Check if PR is already merged via API |
| git push (reconciliation) | `intent_push_reconcile` (with `pre_push_sha`, `expected_tree`) | `done_push_reconcile` | Compare tree SHA + verify parent chain (see below) |
| git push (catch-up) | `intent_push_catchup` (with `pre_push_sha`, `expected_tree`) | `done_push_catchup` | Compare tree SHA + verify parent chain (see below) |
| Retarget PR | `intent_retarget` | `done_retarget` | Check if PR base already equals target |

**Git push idempotency details:**

Merge commits are **not reproducible** across retries: timestamps, GPG signatures, and other metadata vary between runs. Therefore, recovery cannot simply compare "expected SHA" against "remote SHA" — a SHA mismatch doesn't mean the push failed.

The intent event records `pre_push_sha` (the remote ref before our push) and `expected_tree` (the tree SHA we expect). On recovery:

1. Query GitHub API: `GET /repos/{o}/{r}/git/ref/heads/{branch}` to get current remote SHA
2. Fetch the commit object and extract its tree SHA
3. If the tree matches `expected_tree` **and** the commit's parent chain includes `pre_push_sha`: push already succeeded, write completion event
4. If tree differs or parent chain doesn't include `pre_push_sha`: fetch the remote state, then:
   a. If remote is a fast-forward from `pre_push_sha` but with different content: someone else pushed — abort with conflict
   b. Otherwise: re-run the merge operations locally, then push
5. If push fails with "non-fast-forward": someone else pushed — abort with conflict

The tree SHA is deterministic (same file content = same tree), so it serves as a stable comparison point even when commit metadata varies. Recording `pre_push_sha` lets us verify the push actually happened (our commit is reachable from the remote and the remote has advanced past the pre-push state).

**Phase transitions with commit points:**

The `cascade_phase` transitions map to these commit points:

- `Idle` → `Preparing`: Write `phase_transition{Preparing{...}}` + fsync before starting preparation
- `Preparing` → `SquashPending`: Write `phase_transition{SquashPending{...}}` + fsync after all prep pushes complete
- `SquashPending` → `Reconciling`: Write `squash_committed{sha}` + `phase_transition{Reconciling{...}}` + fsync **immediately after** receiving squash SHA from API
- `Reconciling` → `CatchingUp`: Write `phase_transition{CatchingUp{...}}` + fsync after all ours-merge pushes complete
- `CatchingUp` → `Retargeting`: Write `phase_transition{Retargeting{...}}` + fsync after all catch-up pushes complete
- `Retargeting` → `Idle`: Write `phase_transition{Idle}` + fsync after all retarget API calls complete

**Per-phase intent/done events:**

Each phase that performs irreversible operations uses intent/done pairs:

| Phase | Intent event | Done event | Per-descendant? |
|-------|--------------|------------|-----------------|
| Preparing | `intent_push_prep` | `done_push_prep` | Yes |
| Reconciling | `intent_push_reconcile` | `done_push_reconcile` | Yes |
| CatchingUp | `intent_push_catchup` | `done_push_catchup` | Yes |
| Retargeting | `intent_retarget` | `done_retarget` | Yes |

Each descendant in the `frozen_descendants` set requires its own intent/done cycle. The `completed` vector in each phase tracks which descendants have finished, enabling crash recovery to skip already-completed work.

**Critical invariant:** The `squash_committed` event with the squash SHA must be durably recorded before any reconciliation pushes occur. If the bot crashes after squash but before recording the SHA, it must re-fetch the SHA from GitHub (PR's `merge_commit_sha` field).

**Critical invariant:** The `frozen_descendants` set must be carried through all phases (SquashPending, Reconciling, CatchingUp, Retargeting). Recovery must use this set, not re-query `repo_state.descendants`, which may have changed during spool replay.

### Expected state tracking

Rather than pre-validating state before each operation (which creates TOCTOU bugs), we track what we *expect* to be true after each operation. When reality diverges from expectations, we diagnose and handle the divergence at the point of failure.

**Philosophy**: Attempt the operation, handle failure gracefully. Since we never force-push, we cannot silently clobber human changes — at worst, our push fails with "non-fast-forward", which is loud and recoverable.

**What we track**:

| After operation | Expected state | Stored in |
|-----------------|----------------|-----------|
| Preparation push | Branch X has tree Y, parent chain includes pre-push SHA | `intent_push_prep.expected_tree`, `intent_push_prep.pre_push_sha` |
| Squash-merge | PR is merged, merge_commit_sha is set | `squash_committed.sha` |
| Reconciliation push | Branch X has tree Z, parent chain includes squash SHA | `intent_push_reconcile.expected_tree` |
| Catch-up push | Branch X has tree W, parent chain includes origin/main | `intent_push_catchup.expected_tree` |
| Retarget | PR base_ref equals default_branch | Verified via API after `done_retarget` |

**Diagnosing divergence**:

When an operation fails or state doesn't match expectations:

1. **Push fails non-fast-forward**: Someone pushed to the branch since we last fetched.
   - Fetch the new remote state
   - If the remote tree matches our expected tree: someone pushed the same content (rare but possible) — treat as success
   - If the remote tree differs: genuine conflict — abort with clear error: "Branch was modified during cascade. Remote has tree X, we expected Y."

2. **PR already merged when we try to squash**: Either we crashed after squashing (recovery case) or a human merged it manually.
   - If `merge_commit_sha` matches our expected flow: treat as success, continue to reconciliation
   - If `merge_commit_sha` differs or merge method wasn't squash: abort with error: "PR was merged outside the cascade."

3. **Descendant PR closed/deleted when we try to prepare/reconcile**: Human intervention.
   - Skip this descendant with a warning: "Descendant #N was closed during cascade, skipping."
   - Continue with remaining descendants in `frozen_descendants`
   - If ALL descendants are gone, the cascade completes (nothing left to process)

4. **Branch deleted when we try to push**: PR was closed and branch auto-deleted.
   - Same handling as "descendant closed" — skip with warning

**Why not pre-validate**: Checking "is this PR still open?" before preparing it is useless — it could close between the check and the operation. By handling failure at the operation site, we get:
- Simpler code (no duplicated state checks)
- Correct handling of races (the operation itself is the authoritative check)
- Clear diagnostics (we know exactly what failed and why)

**Frozen descendants and failure handling**: When a descendant in `frozen_descendants` fails (closed, branch deleted, etc.), we:
1. Log the failure clearly
2. Add it to the `skipped` list for this phase (persisted alongside `completed` and `frozen_descendants`)
3. Continue with remaining descendants

The `completed` list tracks successfully processed descendants; the `skipped` list tracks descendants that failed during processing. On recovery, we skip descendants in `completed` OR `skipped`, and only attempt `remaining = frozen_descendants - completed - skipped`. Failed descendants are NOT retried automatically — the user must manually resolve the issue (reopen the PR, restore the branch, etc.) and restart the train.

**Why merging $SQUASH_SHA^ is essential**: If main has independent changes (commits that landed outside this stack), the descendant must incorporate them. This happens in reconciliation by merging `$SQUASH_SHA^` (the parent of the squash commit, i.e., main immediately before the squash). The ours-merge then marks the squash commit as an ancestor without changing the tree (which already has all pre-squash content).

**CRITICAL**: We do NOT merge main during preparation (before squash). See `reconcile_descendant` for why this ordering prevents lost commits.

**Example timeline for main ← #123 ← #124 ← #125 (linear):**

```
1.  Merge #123's head SHA into #124 (preparation — predecessor head only, NOT main)
2.  Squash-merge #123 into main → returns squash_sha_123
3.  Merge squash_sha_123^ into #124 (parent of squash = pre-squash main)
4.  Merge squash_sha_123 into #124 (ours strategy — marks as merged)
5.  Merge origin/main into #124 (catch-up for anything after squash)
6.  Retarget #124 to main
7.  Wait for #124 CI...
    [webhook fires when CI passes]
8.  Merge #124's head SHA into #125 (preparation — predecessor head only)
9.  Squash-merge #124 into main → returns squash_sha_124
10. Merge squash_sha_124^ into #125 (parent of squash)
11. Merge squash_sha_124 into #125 (ours strategy)
12. Merge origin/main into #125 (catch-up)
13. Retarget #125 to main
14. Wait for #125 CI...
    [webhook fires when CI passes]
15. Squash-merge #125 into main
16. Done
```

**Example timeline for main ← #123 ← {#124, #125} (fan-out):**

```
1.  Merge #123's head SHA into #124 (preparation for first descendant)
2.  Merge #123's head SHA into #125 (preparation for second descendant)
3.  Squash-merge #123 into main → returns squash_sha_123
4.  Merge squash_sha_123^ into #124 (parent of squash)
5.  Merge squash_sha_123 into #124 (ours strategy)
6.  Merge origin/main into #124 (catch-up)
7.  Retarget #124 to main
8.  Merge squash_sha_123^ into #125 (parent of squash)
9.  Merge squash_sha_123 into #125 (ours strategy)
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

## Pause Conditions

The bot pauses the cascade (and comments with diagnostics) when it encounters issues. The resulting state (`waiting_ci` or `aborted`) determines whether the cascade can auto-resume:

| Condition | Resulting state | Recovery |
|-----------|-----------------|----------|
| Required check fails on descendant | `waiting_ci` | Fix, push — bot auto-resumes on `check_suite.completed` |
| Approval withdrawn (temporarily missing) | `waiting_ci` | Re-approve — bot auto-resumes on `pull_request_review.submitted` |
| Squash-merge API fails (transient) | `waiting_ci` | Auto-retry with backoff; waits for status event if retries exhausted |
| Preparation merge fails | `aborted` | Resolve conflict locally, push, then `@merge-train start` |
| `git push` rejected (non-fast-forward) | depends on phase | During preparation: retry if no conflict, else `aborted`. After reconciliation: always `aborted`. See "Concurrent push handling" below |
| Root PR closed without merge | `aborted` | Re-open or restructure stack |
| Descendant PR closed during cascade | continues | Descendant skipped with warning; cascade proceeds with remaining descendants (see "Diagnosing divergence" above) |
| Cycle detected | `aborted` | Fix predecessor comments |
| Review dismissed | `aborted` | Re-approve, then `@merge-train start` (no auto-resume) |
| Squash-merge API fails (permanent) | `aborted` | Check PR status, satisfy requirements, then `@merge-train start` |

**Review dismissal behaviour**: If a review is dismissed (either by the reviewer or due to new commits in repos with "dismiss stale reviews" enabled), the cascade aborts immediately. Unlike CI failure, review dismissal does **not** auto-resume — a new approval must be obtained and `@merge-train start` must be re-issued to continue.

**Branch protection behaviour**: If the target branch has protection rules that prevent the squash-merge, the GitHub API will reject the merge request. The bot distinguishes between **transient** and **permanent** failures:

1. **Transient failures** (retry with backoff):
   - "Required status check is expected" — status propagation delay after bot's push
   - "Base branch was modified" — someone pushed to main during merge attempt
   - HTTP 5xx errors, timeouts, rate limits
   - The bot retries up to 3 times with exponential backoff (2s, 4s, 8s) before treating as permanent

2. **Permanent failures** (abort immediately):
   - "Pull request is not mergeable" with `mergeStateStatus: DIRTY` — genuine conflict
   - "Approving review required" — missing approval (won't fix itself)
   - "Changes must be signed" — commit signing required but not present
   - 4xx errors other than rate limits

This distinction is critical because the eventual consistency caveat (see above) means `mergeStateStatus` may briefly show `CLEAN` while GitHub's merge machinery still sees stale state. The retry window allows propagation to complete.

On **transient** failure after exhausting retries, the bot transitions to `waiting_ci` state (not `aborted`) and waits for the next `check_suite` or `status` event to re-attempt. This prevents false aborts from brief propagation delays.

On **permanent** failure, the bot posts a diagnostic comment and stops handling the stack entirely. The user must satisfy the branch protection requirements and re-issue `@merge-train start`.

**Concurrent push handling**: If the bot's push is rejected because someone else pushed to the descendant branch during the cascade:

1. **Detection**: The push fails with "non-fast-forward" error
2. **Before reconciliation**: If this happens during preparation, the bot:
   - Fetches the new remote state
   - Checks if the new commits conflict with the merge
   - If no conflict: re-runs preparation to incorporate the new commits, then pushes
   - If conflict: aborts with a message explaining that someone pushed conflicting changes
3. **After reconciliation**: If this happens during reconciliation/catch-up, the bot aborts — the ours-strategy merge was computed against a different base
4. **Recovery**: The user can either revert their push and re-trigger, or stop the train, push their changes, and start a new cascade

On abort, the bot:

1. Stops at the next opportunity (in-flight operations may complete first)
2. **Cleans up the worktree** (see below)
3. Posts a comment on the PR that failed, explaining what happened and suggesting recovery
4. Posts a comment on downstream PRs that the train is halted
5. Takes no further action until human intervenes or condition resolves

**Worktree cleanup on abort**: When an abort occurs (especially due to merge conflicts), the worktree may be left in an unmerged state with conflict markers in the index. This would cause subsequent `git checkout --detach` calls to fail, wedging the train. The bot MUST clean up the worktree before transitioning to aborted state:

```rust
async fn cleanup_worktree_on_abort(worktree_path: &Path) -> Result<()> {
    // 1. Abort any in-progress merge (clears MERGE_HEAD and unmerged index entries)
    run_git_in_worktree(worktree_path, &["merge", "--abort"]).await.ok();

    // 2. Reset index to HEAD (clears any staged changes)
    run_git_in_worktree(worktree_path, &["reset", "--hard", "HEAD"]).await?;

    // 3. Clean untracked files (conflict artifacts, temp files)
    run_git_in_worktree(worktree_path, &["clean", "-fd"]).await?;

    Ok(())
}
```

**Why this is critical**: Git refuses to checkout a different ref when the index contains unmerged entries. Without this cleanup:
- The next cascade operation (after user fixes the issue) would call `git checkout --detach origin/<branch>`
- This would fail with "error: you need to resolve your current index first"
- The train would be stuck until manual worktree cleanup or worktree deletion

The cleanup is called synchronously as part of the abort transition, before persisting the `aborted` state. This ensures the worktree is always clean when the train is in an aborted state, ready for the next resume attempt.

### Auto-resume on check fix

If the cascade is waiting because `mergeStateStatus` is `BLOCKED`, the bot will automatically re-evaluate and potentially resume when:

- A `check_suite` or `status` completed event fires for that PR
- The PR is still open
- The predecessor is still merged (if applicable)
- No `@merge-train stop` command was issued

This means "fix the CI and push" is sufficient to resume — no manual re-trigger needed.

**Distinguishing BLOCKED causes**: GitHub's `mergeStateStatus` returns `BLOCKED` for both check failures AND missing approvals — it doesn't distinguish between them. The bot handles this by:

1. **Event-driven re-evaluation**: When `check_suite.completed` or `pull_request_review.submitted` fires, the bot re-queries `mergeStateStatus`. If now `CLEAN` or `UNSTABLE`, the cascade continues regardless of what caused the prior `BLOCKED`.

2. **Review dismissal is special**: When a review is dismissed (detected via `pull_request_review.dismissed` event), the cascade transitions to `aborted` state with a specific reason. This state does NOT auto-resume on subsequent approval — the user must re-issue `@merge-train start`. See "Review dismissal behaviour" above.

3. **Why the asymmetry?**: Check failures are typically transient (CI flakes, missing dependencies) and fixing + pushing is the natural workflow. Review dismissal is an explicit human action that may indicate the reviewer wants changes — auto-resuming could bypass that intent.

---

## GitHub API Endpoints

| Operation | Endpoint | Method |
|-----------|----------|--------|
| Get repo metadata | `/repos/{o}/{r}` | GET |
| Squash-merge PR | `/repos/{o}/{r}/pulls/{n}/merge` | PUT |
| Retarget PR base | `/repos/{o}/{r}/pulls/{n}` | PATCH |
| Get PR details | `/repos/{o}/{r}/pulls/{n}` | GET |
| List open PRs | `/repos/{o}/{r}/pulls` | GET |
| List PR comments | `/repos/{o}/{r}/issues/{n}/comments` | GET |
| Post comment | `/repos/{o}/{r}/issues/{n}/comments` | POST |
| Update comment | `/repos/{o}/{r}/issues/comments/{id}` | PATCH |
| Add reaction | `/repos/{o}/{r}/issues/comments/{id}/reactions` | POST |
| Get merge state | GraphQL `mergeStateStatus` | POST `/graphql` |

The "Get repo metadata" endpoint returns `default_branch` which is used throughout the cascade logic.

The "Update comment" endpoint is used to update status comments (which contain machine-readable JSON for disaster recovery — see "Status comments" section).

**Squash-merge with SHA guard (CRITICAL)**:

When calling the merge endpoint, **always** pass the expected head SHA:

```json
PUT /repos/{o}/{r}/pulls/{n}/merge
{
  "merge_method": "squash",
  "sha": "<expected_head_sha>"
}
```

The `sha` parameter is a **race condition guard**. GitHub will reject the merge with HTTP 409 if the PR's head SHA doesn't match. This prevents:

1. **Merging unreviewed commits**: If someone pushes to the PR branch after we evaluated readiness (CI passed, approvals obtained), without the SHA guard we'd merge those new commits without review.

2. **TOCTOU bugs**: Between "check if ready" and "merge", the PR state can change. The SHA guard makes the operation atomic.

```rust
async fn squash_merge(
    &self,
    pr_number: PrNumber,
    expected_head_sha: &str,
) -> Result<String> {
    let response = self.client
        .put(format!("/repos/{}/{}/pulls/{}/merge", owner, repo, pr_number.0))
        .json(&json!({
            "merge_method": "squash",
            "sha": expected_head_sha,  // CRITICAL: prevents merging unreviewed commits
        }))
        .send()
        .await?;

    match response.status() {
        StatusCode::OK => {
            let body: MergeResponse = response.json().await?;
            Ok(body.sha)  // This is the squash commit SHA
        }
        StatusCode::CONFLICT => {
            // PR head changed since we checked — abort and re-evaluate
            Err(Error::HeadShaChanged)
        }
        StatusCode::METHOD_NOT_ALLOWED => {
            // PR is not mergeable (conflicts, branch protection, etc.)
            Err(Error::NotMergeable)
        }
        _ => Err(Error::ApiError(response.status())),
    }
}
```

**Handling 409 Conflict**: If the merge returns 409, the bot should:
1. Log the SHA mismatch
2. Abort the current cascade step
3. Re-fetch the PR state and re-evaluate readiness
4. If still ready with the new head SHA, retry (the new commits may have been benign)
5. If no longer ready (CI failing, reviews dismissed), wait for the appropriate webhook

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
- `administration`: read (query branch protection rules for dismiss-stale-approvals detection) — **optional but recommended**
- `metadata`: read (query rulesets) — **optional but recommended**

**Note on optional permissions:** Without `administration:read` and `metadata:read`, the bot cannot detect "dismiss stale approvals" settings during preflight. It will warn and proceed, risking mid-cascade aborts if the setting is enabled. See "Dismiss stale approvals preflight check" section.

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

/// Per-repository cached state (see canonical definition in "Per-repo state" section)
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
    /// Filesystem path for this repo's state directory
    state_path: PathBuf,
    /// When bootstrap completed
    bootstrapped_at: Instant,
    /// Last re-sync time
    last_sync: Instant,
    /// When state was last persisted to disk
    last_persisted: Option<Instant>,
    /// Whether in-memory state has changed since last persistence
    dirty: bool,
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
    /// SHA of the predecessor's squash commit that this PR was reconciled against.
    /// Set after:
    /// - Normal cascade reconciliation completes (reconcile_descendant)
    /// - Late addition reconciliation completes (handle_late_addition)
    /// Used by is_root() to verify proper reconciliation before allowing
    /// the PR to become a new root. Prevents manually-retargeted PRs from
    /// bypassing the ours-merge chain.
    predecessor_squash_reconciled: Option<Sha>,
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
    /// Merge conflicts with base branch — abort cascade
    Dirty,
    /// GitHub Enterprise pre-receive hooks or merge queue enabled — abort cascade
    /// (incompatible with merge-train, see non-goals)
    HasHooks,
    /// State not yet computed by GitHub
    Unknown,
}

	// ─────────────────────────────────────────────────────────────
	// Train state (persisted locally for restart recovery)
	// ─────────────────────────────────────────────────────────────
	//
	// TRAIN IDENTITY MODEL:
	//
	// A train has TWO PR numbers that serve different purposes:
	//
	// 1. `original_root_pr`: The PR that received `@merge-train start`.
	//    - STABLE throughout the cascade — never changes
	//    - Used as the key in `active_trains` HashMap
	//    - Used to name the worktree (`stack-<original_root_pr>/`)
	//    - Used to look up the train when a stop command arrives
	//
	// 2. `current_pr`: The PR currently being processed.
	//    - CHANGES as PRs merge and the cascade advances
	//    - Initially equals `original_root_pr`
	//    - After #123 merges, becomes #124 (its descendant)
	//    - Used for merge operations, CI checks, status updates
	//
	// Example: Stack is main ← #123 ← #124 ← #125, user runs `@merge-train start` on #123
	//   - original_root_pr = 123 (forever)
	//   - current_pr = 123 → 124 → 125 as cascade advances
	//   - active_trains key = 123 (forever)
	//   - worktree path = stack-123/ (forever)
	//
	// FAN-OUT HANDLING:
	// When #123 has multiple descendants (#124 and #125):
	//   - Original train (original_root_pr=123) completes after #123 merges
	//   - Two NEW trains are created: one with original_root_pr=124, one with original_root_pr=125
	//   - Each gets its own worktree, its own entry in active_trains
	//   - The old train (keyed by 123) is removed from active_trains
	//
	// STATUS COMMENT HANDOFF AT FAN-OUT:
	// When fan-out completes successfully:
	//   1. Update #123's status comment to final state: "Train completed. Descendants #124, #125
	//      are now independent trains."
	//   2. Create new TrainRecords for #124 and #125 with their own original_root_pr values
	//   3. Post initial status comments on #124 and #125
	//   4. Remove #123 from active_trains
	//
	// FAN-OUT RECOVERY:
	// If the bot crashes during fan-out (between removing the old train and creating all new trains):
	//   1. On recovery, scan for status comments on merged PRs with state = "running"
	//   2. If a merged PR (#123) has a running status comment:
	//      - The train was mid-fan-out when the crash occurred
	//      - Query #123's descendants from predecessor declarations in the PR cache
	//      - For each descendant targeting main (already retargeted): create a new train if none exists
	//      - For each descendant NOT targeting main: these weren't fully processed; mark needs_manual_review
	//   3. Update #123's status comment to indicate completion (or the error state)
	//
	// The status comment on #123 serves as a breadcrumb that fan-out was in progress. The new trains
	// (#124, #125) get their own status comments once created.

	/// Train record stored in the local state file.
	/// During normal operation, state is written here first, then mirrored to GitHub.
	/// During recovery, the record with higher `recovery_seq` wins (see "Recovery precedence").
	#[derive(Serialize, Deserialize)]
	struct TrainRecord {
	    /// Schema version for forward compatibility
	    version: u32,
	    /// Monotonic sequence number, incremented on each state change.
	    /// Used to determine which record is "ahead" during recovery when
	    /// comparing local state vs GitHub status comments.
	    recovery_seq: u64,
	    /// Current train state
	    state: TrainState,
	    /// PR that originated this train. For the initial train, this is the PR that received
	    /// @merge-train start. For trains created by fan-out, this is the descendant PR that
	    /// became a new root. Used as the key in active_trains and for worktree naming.
	    original_root_pr: PrNumber,
	    /// PR currently being processed (the train's current root)
	    current_pr: PrNumber,
	    /// Current phase within the cascade step
	    cascade_phase: CascadePhase,
	    /// PR number of predecessor (for fetching via refs/pull/<n>/head during recovery)
	    predecessor_pr: Option<PrNumber>,
	    /// Head SHA of predecessor at preparation time (for verifying preparation during recovery)
	    predecessor_head_sha: Option<Sha>,
	    /// SHA of last squash commit (for reconciliation recovery)
	    last_squash_sha: Option<Sha>,
	    /// When the train was started (ISO 8601)
	    started_at: String,
	    /// When the train was stopped (if applicable)
	    stopped_at: Option<String>, // ISO 8601
	    /// Error details if aborted
	    error: Option<TrainError>,
	    /// Comment ID of the status comment on `original_root_pr` (not `current_pr`).
	    /// The comment stays on the original root for the train's lifetime — see "Status comments".
	    status_comment_id: Option<u64>,
	}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TrainState {
    Running,
    Stopped,
    WaitingCi,
    Aborted,
    NeedsManualReview,
}

	#[derive(Serialize, Deserialize)]
	enum CascadePhase {
	    /// Not currently performing any operation; waiting for CI or next event
	    Idle,
	    /// Merging predecessor head into descendants (NOT default branch — see "Why merging $SQUASH_SHA^ is essential").
	    /// `frozen_descendants` is captured at phase entry and used for recovery —
	    /// new descendants that arrive mid-phase are NOT included (see "Descendant set freezing").
	    Preparing {
	        completed: Vec<PrNumber>,
	        skipped: Vec<PrNumber>,
	        frozen_descendants: Vec<PrNumber>,
	    },
	    /// Preparation complete, about to squash-merge
	    SquashPending {
	        /// Carried forward from Preparing for subsequent phases
	        frozen_descendants: Vec<PrNumber>,
	        /// Descendants that failed during preparation (won't be processed in later phases)
	        skipped: Vec<PrNumber>,
	    },
	    /// Squash complete, performing ours-strategy merges into descendants
	    Reconciling {
	        completed: Vec<PrNumber>,
	        skipped: Vec<PrNumber>,
	        /// Carried forward from Preparing — recovery MUST use this, not repo_state.descendants
	        frozen_descendants: Vec<PrNumber>,
	    },
	    /// Ours-merge complete, performing regular merge of origin/main (catch-up)
	    CatchingUp {
	        completed: Vec<PrNumber>,
	        skipped: Vec<PrNumber>,
	        /// Carried forward from Preparing
	        frozen_descendants: Vec<PrNumber>,
	    },
	    /// Catch-up complete, retargeting descendant PRs to default branch
	    Retargeting {
	        completed: Vec<PrNumber>,
	        skipped: Vec<PrNumber>,
	        /// Carried forward from Preparing
	        frozen_descendants: Vec<PrNumber>,
	    },
	}

	// The `completed` vectors track which descendants have finished each phase.
	// The `skipped` vectors track descendants that failed (PR closed, branch deleted, etc.).
	// On recovery, we process: frozen_descendants - completed - skipped.
	// This ensures we don't endlessly retry failed descendants.
	//
	// CRITICAL: `frozen_descendants` is captured once in `Preparing` and carried through
	// ALL subsequent phases. Recovery MUST use this list, NOT repo_state.descendants.
	//
	// Why this matters for correctness:
	// 1. After restart, the bot replays the spool BEFORE performing recovery (see Restart safety)
	// 2. During spool replay, new `predecessor_declared` events may add descendants
	// 3. If recovery re-queried repo_state.descendants, it would see descendants that weren't
	//    present when we entered the Preparing phase
	// 4. Those new descendants never got preparation (predecessor head merge)
	// 5. Their branches don't contain the predecessor's changes, so reconciliation would
	//    produce incorrect results (missing the predecessor's work)
	//
	// By carrying frozen_descendants through all phases, recovery always knows exactly which
	// descendants were promised preparation and which still need reconciliation/catch-up/retarget.
	// The `skipped` set ensures we don't retry descendants that permanently failed.
	//
	// REQUIRED TEST: Property 8 (`recovery_uses_frozen_descendants`) in the "Property-based
	// testing with real git" section verifies that recovery uses the frozen set, not the
	// current descendants index.

#[derive(Serialize, Deserialize)]
struct TrainError {
    error_type: String,
    message: String,
}

	// ─────────────────────────────────────────────────────────────
	// Persisted repo state (stored on disk)
	// ─────────────────────────────────────────────────────────────

	/// JSON structure stored at `<state_dir>/<owner>/<repo>/snapshot.<gen>.json`.
	/// The `<gen>` suffix is the current generation number (see "Compaction" section).
	#[derive(Serialize, Deserialize)]
	struct PersistedRepoSnapshot {
	    /// Schema version for forward-compatible migrations
	    schema_version: u32,
	    /// When this snapshot was last updated
	    snapshot_at: String, // ISO 8601
	    /// The generation number this snapshot belongs to (matches filename suffix)
	    log_generation: u64,
	    /// Byte offset in events.<log_generation>.log at which this snapshot was taken.
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
	    /// Seen dedupe keys with timestamps for TTL-based pruning.
	    /// Key format: "event_type:pr:id" or "event_type:pr:action:sha"
	    seen_dedupe_keys: HashMap<String, String>, // key -> ISO 8601 timestamp
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
	    /// SHA of the predecessor's squash commit that this PR was reconciled against.
	    /// Set after normal cascade or late-addition reconciliation completes.
	    /// Required for is_root() to return true when predecessor is merged.
	    predecessor_squash_reconciled: Option<String>,
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
	    TrainStarted { root_pr: u64, current_pr: u64 },
	    #[serde(rename = "train_stopped")]
	    TrainStopped { root_pr: u64 },
	    #[serde(rename = "train_completed")]
	    TrainCompleted { root_pr: u64 },
	    #[serde(rename = "train_aborted")]
	    TrainAborted { root_pr: u64, error: TrainError },

	    // ─── Phase transitions (always critical) ───
	    //
	    // Phase transitions must include all fields needed for restart-safe recovery:
	    // - current_pr: which PR the train is currently processing
	    // - predecessor_pr: for fetching via refs/pull/<n>/head during recovery
	    // - last_squash_sha: for reconciliation recovery
	    // - phase: the CascadePhase including completed descendant lists
	    //
	    // Without these fields durably logged, recovery cannot determine which PR
	    // the train was processing or what SHA to use for reconciliation.
	    #[serde(rename = "phase_transition")]
	    PhaseTransition {
	        train_root: u64,
	        current_pr: u64,
	        predecessor_pr: Option<u64>,
	        last_squash_sha: Option<String>,
	        /// Full CascadePhase including completed lists for multi-descendant phases
	        phase: CascadePhase,
	    },
	    #[serde(rename = "squash_committed")]
	    SquashCommitted { train_root: u64, pr: u64, sha: String },

	    // ─── Intent/done pairs for irreversible operations ───
	    //
	    // Push intents record `pre_push_sha` (remote ref before our push) and `expected_tree`
	    // (tree SHA we expect after the merge). On recovery:
	    // 1. Fetch current remote SHA
	    // 2. If remote's tree matches expected_tree AND remote's parent chain includes pre_push_sha:
	    //    push already succeeded → write completion event
	    // 3. Otherwise: re-run merge operations and push
	    //
	    // We use tree SHA (not commit SHA) because merge commits aren't reproducible across
	    // retries (timestamps, signatures vary), but the tree content is deterministic.
	    #[serde(rename = "intent_push_prep")]
	    IntentPushPrep {
	        train_root: u64,
	        branch: String,
	        /// Remote ref SHA before we push (for verifying push actually happened)
	        pre_push_sha: String,
	        /// Expected tree SHA after merge (deterministic, unlike commit SHA)
	        expected_tree: String,
	    },
	    #[serde(rename = "done_push_prep")]
	    DonePushPrep { train_root: u64, branch: String },
	    #[serde(rename = "intent_squash")]
	    IntentSquash { train_root: u64, pr: u64 },
	    #[serde(rename = "intent_push_reconcile")]
	    IntentPushReconcile {
	        train_root: u64,
	        branch: String,
	        /// Remote ref SHA before we push
	        pre_push_sha: String,
	        /// Expected tree SHA after merge
	        expected_tree: String,
	    },
	    #[serde(rename = "done_push_reconcile")]
	    DonePushReconcile { train_root: u64, branch: String },
	    #[serde(rename = "intent_push_catchup")]
	    IntentPushCatchup {
	        train_root: u64,
	        branch: String,
	        /// Remote ref SHA before we push
	        pre_push_sha: String,
	        /// Expected tree SHA after catch-up merge
	        expected_tree: String,
	    },
	    #[serde(rename = "done_push_catchup")]
	    DonePushCatchup { train_root: u64, branch: String },
	    #[serde(rename = "intent_retarget")]
	    IntentRetarget { train_root: u64, pr: u64, new_base: String },
	    #[serde(rename = "done_retarget")]
	    DoneRetarget { train_root: u64, pr: u64 },

	    // ─── Fan-out (atomic update of train records) ───
	    #[serde(rename = "fan_out_completed")]
	    FanOutCompleted {
	        old_root: u64,               // Original train root being retired
	        new_roots: Vec<u64>,         // New train roots (the descendants)
	        original_root_pr: u64,       // For worktree management
	    },

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
	                | StateEventPayload::IntentPushCatchup { .. }
	                | StateEventPayload::IntentRetarget { .. }
	            // Done events (must be durable before considering operation complete)
	                | StateEventPayload::DonePushPrep { .. }
	                | StateEventPayload::DonePushReconcile { .. }
	                | StateEventPayload::DonePushCatchup { .. }
	                | StateEventPayload::DoneRetarget { .. }
	            // Fan-out (atomic train record updates)
	                | StateEventPayload::FanOutCompleted { .. }
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
    /// User issued @merge-train stop
    Stopped,
    /// PR is still a draft
    Draft,
    /// GitHub reports UNKNOWN (state not yet computed)
    Unknown,
}
// Note: DIRTY (merge conflicts) is NOT a BlockReason—it causes an immediate
// Aborted { reason: AbortReason::MergeConflict { .. } } since conflicts require
// explicit user intervention and cannot auto-resolve.

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
    /// Cascade is blocked, waiting for condition to change
    Blocked { pr_number: PrNumber, reason: BlockReason },
    /// Something went wrong, cascade aborted
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
    /// Repository has merge hooks or merge queue enabled (HAS_HOOKS status)
    MergeHooksEnabled,
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
    /// Internal: poll active trains for missed webhook recovery (every 10 minutes)
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
        // Use select! to handle cancel requests DURING event processing.
        // This is critical: without it, cancel tokens wouldn't be set until
        // processing completes, making "prompt" interruption impossible.
        if let Some(event) = queue.pop() {
            let processing = process_event_for_repo(
                &repo_id,
                event,
                &mut repo_state,
                &mut stack_cancel,
                &ctx,
            );
            tokio::pin!(processing);

            let result = loop {
                tokio::select! {
                    biased;

                    // Priority: handle cancel requests immediately
                    Some(req) = cancel_rx.recv() => {
                        if let Some(ref state) = repo_state {
                            if let Some(root) = state.find_stack_root(req.pr_number) {
                                stack_cancel.cancel(root);
                                tracing::info!(?repo_id, ?root, "cancelled stack during processing");
                            }
                        }
                        // Continue processing — the cancelled token will cause
                        // the in-flight operation to return Err(Cancelled)
                    }

                    // Process the event (will check cancel token periodically)
                    result = &mut processing => {
                        break result;
                    }
                }
            };

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
    // active_trains is keyed by original_root_pr (stable throughout cascade)
    for (original_root, train) in &repo_state.active_trains {

        // Only recover trains that were actively running (not stopped/aborted)
        if !matches!(train.state, TrainState::Running | TrainState::WaitingCi) {
            continue;
        }

        // Get cancellation token for this specific stack (keyed by original_root)
        let cancel = stack_cancel.token_for_stack(*original_root);

        match &train.cascade_phase {
            CascadePhase::Idle => {
                // Re-evaluate readiness of current_pr
                // This will be handled by the triggering event, no special action needed
            }
            CascadePhase::Preparing { completed, frozen_descendants, .. } => {
                // CRITICAL: Use frozen_descendants from the logged phase_transition, NOT
                // repo_state.descendants. The descendants index may have changed during spool
                // replay (step 3 of restart), but we must only prepare the descendants that
                // were frozen when we entered this phase.
                resume_preparation(
                    train.current_pr,
                    frozen_descendants,
                    completed,
                    repo_state,
                    &cancel,
                    ctx
                ).await?;
            }
            CascadePhase::SquashPending { frozen_descendants, .. } => {
                // Check if the PR was already squash-merged
                let pr = repo_state.prs.get(&train.current_pr);
                match pr.map(|p| &p.state) {
                    Some(PrState::Merged { .. }) => {
                        // Already merged — recover the squash SHA from GitHub.
                        // We can't trust train.last_squash_sha here because we may have
                        // crashed after the squash but before durably writing the SHA.
                        // See "Reconciling recovery with missing last_squash_sha".
                        let squash_sha = fetch_merge_commit_sha_with_retry(github, train.current_pr)
                            .await?
                            .ok_or(Error::MissingRecoverySha)?;
                        resume_reconciliation(
                            train.current_pr,
                            &squash_sha,
                            frozen_descendants,
                            repo_state,
                            &cancel,
                            ctx
                        ).await?;
                    }
                    Some(PrState::Open) => {
                        // Not yet merged, proceed with squash (frozen_descendants carried forward)
                        resume_squash(train.current_pr, frozen_descendants, repo_state, &cancel, ctx).await?;
                    }
                    _ => {
                        // PR closed without merge or missing - abort
                        abort_train(*original_root, "PR closed or missing during recovery", repo_state, ctx).await?;
                    }
                }
            }
            CascadePhase::Reconciling { completed, frozen_descendants, .. } => {
                // Use last_squash_sha and frozen_descendants to complete reconciliation
                // for remaining descendants (those in frozen_descendants but not in completed)
                //
                // If last_squash_sha is missing (crash after squash but before durable write),
                // recover it from GitHub. During Reconciling, current_pr is the PR that was
                // just squash-merged, so we fetch its merge_commit_sha.
                // See "Reconciling recovery with missing last_squash_sha".
                let squash_sha = match train.last_squash_sha.as_ref() {
                    Some(sha) => sha.clone(),
                    None => {
                        fetch_merge_commit_sha_with_retry(github, train.current_pr)
                            .await?
                            .ok_or(Error::MissingRecoverySha)?
                    }
                };
                resume_reconciliation_with_completed(
                    train.current_pr,
                    &squash_sha,
                    frozen_descendants,
                    completed,
                    repo_state,
                    &cancel,
                    ctx
                ).await?;
            }
            CascadePhase::CatchingUp { completed, frozen_descendants, .. } => {
                // Resume catch-up (merge origin/main) for remaining descendants
                //
                // Same recovery logic as Reconciling — if last_squash_sha is missing,
                // fetch it from GitHub. current_pr is the PR that was just squash-merged.
                let squash_sha = match train.last_squash_sha.as_ref() {
                    Some(sha) => sha.clone(),
                    None => {
                        fetch_merge_commit_sha_with_retry(github, train.current_pr)
                            .await?
                            .ok_or(Error::MissingRecoverySha)?
                    }
                };
                resume_catch_up(
                    train.current_pr,
                    &squash_sha,
                    frozen_descendants,
                    completed,
                    repo_state,
                    &cancel,
                    ctx
                ).await?;
            }
            CascadePhase::Retargeting { completed, frozen_descendants, .. } => {
                // Resume retargeting for remaining descendants
                resume_retargeting(
                    train.current_pr,
                    frozen_descendants,
                    completed,
                    repo_state,
                    &cancel,
                    ctx
                ).await?;
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
    poll_interval: Duration,    // e.g., 10 minutes (with jitter applied per-repo)
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

    // Spawn periodic timers for re-sync (hourly) and active train polling (10 min default)
    let resync_interval = Duration::from_secs(3600);
    let poll_interval = Duration::from_secs(600); // 10 minutes, configurable via MERGE_TRAIN_POLL_INTERVAL_MINS
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
        GitHubEvent::CheckSuite(cs) if cs.conclusion.is_some() => {
            // Re-evaluate on ANY check completion, not just success.
            // The bot determines mergeability via `mergeStateStatus`, not individual check
            // conclusions. Non-required checks failing yields UNSTABLE (which is mergeable),
            // so we must re-evaluate even on "failure" or "neutral" conclusions.
            evaluate_cascade(&stacks, repo_state, stack_cancel, ctx).await?;
        }
        GitHubEvent::PullRequestReview(r) if r.action == "submitted" && r.review.state == "approved" => {
            evaluate_cascade(&stacks, repo_state, stack_cancel, ctx).await?;
        }
        GitHubEvent::PullRequestReview(r) if r.action == "dismissed" => {
            // Review dismissal aborts the train (does NOT auto-resume on subsequent approval)
            // See "Review dismissal behaviour" section
            if let Some(root) = repo_state.find_stack_root(r.pull_request.number) {
                if let Some(train) = repo_state.active_trains.get(&root) {
                    abort_train(train, AbortReason::ReviewDismissed, repo_state, ctx).await?;
                }
            }
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
    /// - Its predecessor has been merged AND reconciliation has completed
    ///
    /// The second case represents a "resolved" predecessor relationship — see "Continuous
    /// base branch validation" in the User Interface section. After a cascade step completes,
    /// the descendant targets main but still has its predecessor declaration. This is NOT
    /// an invalid state; it's the expected post-cascade state where the PR becomes a new root.
    ///
    /// IMPORTANT: We always require base_ref == default_branch. A PR whose predecessor
    /// merged but which hasn't been retargeted yet is NOT a root — it's in a transitional
    /// state and should not be squash-merged until retargeting completes.
    ///
    /// CRITICAL: For the merged-predecessor case, we also require `predecessor_squash_reconciled`
    /// to be set. This prevents a race where someone manually retargets a late descendant to
    /// main before the bot's reconciliation (handle_late_addition) runs. Without this check,
    /// such a PR would be discovered as a root and squash-merged WITHOUT the ours-merge chain,
    /// risking lost predecessor content or conflicts.
    ///
    /// PRIVATE: This function is for stack *discovery* only — finding new stacks that
    /// aren't yet being processed. Once a train is active, all progress is driven by the
    /// TrainRecord (which tracks current_pr, cascade_phase, frozen_descendants, etc.).
    /// Never call is_root() to decide what to do next in an active cascade; consult the
    /// TrainRecord instead.
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
                    // Predecessor merged — but ONLY a root if reconciliation completed.
                    // This prevents manually-retargeted PRs from bypassing ours-merge.
                    Some(p) if matches!(p.state, PrState::Merged { .. }) => {
                        pr.predecessor_squash_reconciled.is_some()
                    }
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

        // Train state lookup: use find_stack_root to handle the case where the train
        // has advanced (root PR merged, cascade moved to a descendant). The train is
        // keyed by original_root_pr (stable), but the discovered root here is the
        // *current* stack root which may differ after advancement.
        let (started, stopped) = match self.find_stack_root(root)
            .and_then(|original_root| self.active_trains.get(&original_root))
        {
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

    /// Given any PR number, find the original_root_pr of its active train (if any).
    /// Returns None if this PR is not part of an active train.
    ///
    /// This is used to map stop commands (which reference any PR in the stack)
    /// to the train's stable identifier (original_root_pr).
    fn find_stack_root(&self, pr: PrNumber) -> Option<PrNumber> {
        // Check if this PR is itself an original_root_pr
        if self.active_trains.contains_key(&pr) {
            return Some(pr);
        }

        // Check if this PR is the current_pr of any active train
        for (original_root, train) in &self.active_trains {
            if train.current_pr == pr {
                return Some(*original_root);
            }
            // Also check if it's in the frozen_descendants of any phase
            // (the PR might be mid-cascade but not yet the current_pr)
            if self.is_pr_in_train(pr, train) {
                return Some(*original_root);
            }
        }

        None
    }

    /// Check if a PR is part of a train (either in frozen_descendants or reachable via
    /// the predecessor chain from train.current_pr when Idle).
    fn is_pr_in_train(&self, pr: PrNumber, train: &TrainRecord) -> bool {
        match &train.cascade_phase {
            CascadePhase::Preparing { frozen_descendants, .. } |
            CascadePhase::SquashPending { frozen_descendants, .. } |
            CascadePhase::Reconciling { frozen_descendants, .. } |
            CascadePhase::CatchingUp { frozen_descendants, .. } |
            CascadePhase::Retargeting { frozen_descendants, .. } => {
                frozen_descendants.contains(&pr)
            }
            CascadePhase::Idle => {
                // When Idle (e.g., waiting on CI), the frozen_descendants aren't populated yet.
                // Walk the predecessor chain from `pr` to see if it leads to train.current_pr.
                // This handles stop commands on future descendants that haven't been frozen.
                self.is_descendant_of(pr, train.current_pr)
            }
        }
    }

    /// Check if `pr` is a descendant of `ancestor` by walking the predecessor chain.
    /// Returns true if walking up from `pr` eventually reaches `ancestor`.
    fn is_descendant_of(&self, pr: PrNumber, ancestor: PrNumber) -> bool {
        let mut current = pr;
        let mut visited = HashSet::new();

        while let Some(cached_pr) = self.prs.get(&current) {
            if !visited.insert(current) {
                return false; // Cycle detected
            }
            if let Some(pred) = cached_pr.predecessor {
                if pred == ancestor {
                    return true;
                }
                current = pred;
            } else {
                return false; // Reached a root (no predecessor)
            }
        }
        false
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
    let state_dir = ctx.state_dir_for_repo(repo_id);

    // Phase 1: Try to load from disk (fast path)
    match load_from_disk(&state_dir, ctx).await {
        Ok(Some(persisted)) if !persisted.is_stale(ctx.config.staleness_threshold) => {
            let state = build_state_from_persisted(persisted, state_dir.clone())?;
            let state = refresh_merge_states(state, &github).await?;
            return Ok(state);
        }
        Ok(Some(_stale)) => {
            tracing::info!(?repo_id, "state stale, performing full bootstrap");
        }
        Ok(None) => {
            tracing::info!(?repo_id, "state not found, performing full bootstrap");
        }
        Err(e) => {
            tracing::warn!(?repo_id, ?e, "state load failed, performing full bootstrap");
        }
    }

    // Phase 2: Full API crawl (fallback path)
    let state = full_bootstrap(repo_id, &github, ctx).await?;

    // Persist the freshly bootstrapped state
    persist_state_file(repo_id, &state, &state_dir).await?;

    Ok(state)
}

/// Load state from disk (fast path): snapshot + event log replay
/// Uses generation-based file naming: snapshot.<gen>.json and events.<gen>.log
async fn load_from_disk(
    state_dir: &Path,
    _ctx: &AppContext,
) -> Result<Option<PersistedRepoSnapshot>> {
    // Read the current generation number
    let gen_path = state_dir.join("generation");
    let gen: u64 = match tokio::fs::read_to_string(&gen_path).await {
        Ok(s) => s.trim().parse().unwrap_or(0),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    // Load snapshot for current generation (with fallback to previous if crash during compaction)
    let snapshot_path = state_dir.join(format!("snapshot.{}.json", gen));
    let snapshot_bytes = match tokio::fs::read(&snapshot_path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && gen > 0 => {
            // Crash during compaction — try previous generation
            let prev_snapshot = state_dir.join(format!("snapshot.{}.json", gen - 1));
            tokio::fs::read(&prev_snapshot).await?
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut snapshot: PersistedRepoSnapshot = serde_json::from_slice(&snapshot_bytes)?;

    // Replay events from the corresponding log file starting at log_position
    let events_path = state_dir.join(format!("events.{}.log", snapshot.log_generation));
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

/// Convert persisted snapshot to in-memory RepoState.
/// IMPORTANT: Rebuilds derived indexes (like `descendants`) that aren't persisted.
fn build_state_from_persisted(
    snapshot: PersistedRepoSnapshot,
    state_dir: PathBuf,
) -> Result<RepoState> {
    // Convert persisted PRs to CachedPr
    let prs: HashMap<PrNumber, CachedPr> = snapshot.prs
        .into_iter()
        .map(|(num_str, pr)| {
            let num = PrNumber(num_str.parse().unwrap());
            (num, CachedPr::from_persisted(num, pr))
        })
        .collect();

    // Rebuild the descendants reverse index (predecessor → set of descendants)
    // This is a derived index, not stored in the snapshot
    let mut descendants: HashMap<PrNumber, HashSet<PrNumber>> = HashMap::new();
    for (pr_num, pr) in &prs {
        if let Some(pred) = pr.predecessor {
            descendants.entry(pred).or_default().insert(*pr_num);
        }
    }

    // Convert active_trains keys from String to PrNumber
    let active_trains: HashMap<PrNumber, TrainRecord> = snapshot.active_trains
        .into_iter()
        .map(|(k, v)| (PrNumber(k.parse().unwrap()), v))
        .collect();

    Ok(RepoState {
        default_branch: snapshot.default_branch,
        prs,
        descendants,
        active_trains,
        state_path: state_dir,
        bootstrapped_at: Instant::now(),
        last_sync: Instant::now(),
        last_persisted: Some(Instant::now()),
        dirty: false,
        miss_count: 0,
    })
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
        state_path: ctx.state_dir_for_repo(repo_id),
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
/// Persist state to disk using generation-based naming.
/// Writes snapshot.<gen>.json under state_dir, matching load_from_disk's expectations.
async fn persist_state_file(
    repo_id: &RepoId,
    state: &RepoState,
    state_dir: &Path,
) -> Result<()> {
    // Read current generation (or start at 0)
    let gen_path = state_dir.join("generation");
    let (gen, gen_existed): (u64, bool) = match tokio::fs::read_to_string(&gen_path).await {
        Ok(s) => (s.trim().parse().unwrap_or(0), true),
        Err(_) => (0, false),
    };

    let snapshot = build_snapshot(state);
    let bytes = serde_json::to_vec_pretty(&snapshot)?;
    let snapshot_path = state_dir.join(format!("snapshot.{}.json", gen));
    atomic_write(&snapshot_path, &bytes).await?;

    // Write generation file if it didn't exist (first persist).
    // This ensures load_from_disk finds the generation file on restart.
    if !gen_existed {
        atomic_write(&gen_path, b"0\n").await?;
    }

    tracing::debug!(?repo_id, path = %snapshot_path.display(), gen, "snapshot persisted");
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
    /// Phase 1: Preparation — merge predecessor's head into descendant.
    /// Must be called BEFORE squash-merging the predecessor into the default branch.
    ///
    /// **CRITICAL**: We do NOT merge the default branch here. That happens in
    /// reconciliation AFTER the squash, using $SQUASH_SHA^ (parent of squash commit).
    /// See reconcile_descendant for why this ordering is essential for correctness.
    ///
    /// **Detached HEAD strategy**: We use detached HEAD to avoid git's restriction that
    /// a branch can only be checked out in one worktree at a time.
    async fn prepare_descendant(
        &self,
        descendant_branch: &str,
        predecessor_pr_number: u64,
        cancel: &CancellationToken,
    ) -> Result<String> {
        // Fetch descendant branch only — we don't need default branch yet
        self.run_git(&["fetch", "origin", descendant_branch], cancel).await?;

        // Fetch the predecessor's head via GitHub's PR ref (reliable even after branch deletion).
        let pr_ref = format!("refs/pull/{}/head", predecessor_pr_number);
        let local_ref = format!("refs/remotes/origin/pr/{}", predecessor_pr_number);
        self.run_git(&["fetch", "origin", &format!("{}:{}", pr_ref, local_ref)], cancel).await?;

        // Checkout in DETACHED HEAD mode to avoid branch locking issues.
        let origin_descendant = format!("origin/{}", descendant_branch);
        self.run_git(&["checkout", "--detach", &origin_descendant], cancel).await?;

        // Merge predecessor commit ONLY — do NOT merge main here!
        // Merging main before we know the squash SHA causes lost commits.
        let merge_ref = format!("origin/pr/{}", predecessor_pr_number);
        self.run_git(&[
            "merge",
            &merge_ref,
            "--no-edit",
            "-m", &format!("Merge predecessor PR #{} into {} (merge-train prep)", predecessor_pr_number, descendant_branch),
        ], cancel).await?;

        // Push HEAD to the remote branch
        let push_ref = format!("HEAD:refs/heads/{}", descendant_branch);
        self.run_git(&["push", "origin", &push_ref], cancel).await?;

        self.run_git(&["rev-parse", "HEAD"], cancel).await
    }

    /// Phase 2: Reconciliation — incorporate the squash commit into descendant.
    /// Must be called AFTER squash-merging the predecessor into the default branch.
    ///
    /// This performs the ours-merge dance in the correct order:
    /// 1. Merge $SQUASH_SHA^ (parent of squash) — incorporates main up to just before squash
    /// 2. ours-merge $SQUASH_SHA — marks squash as ancestor without changing tree
    /// 3. Merge origin/<default_branch> — picks up any commits after squash
    ///
    /// **Why merging $SQUASH_SHA^ (not origin/main before squash) is critical**:
    ///
    /// WRONG approach (merging main in preparation):
    /// 1. Prep: merge origin/main (at commit A)
    /// 2. Someone pushes commit B to main
    /// 3. Squash predecessor → creates commit S (parent is B)
    /// 4. Reconcile: ours-merge S
    /// 5. Catch-up: merge origin/main (now at S)
    ///
    /// In step 5, git thinks S is already merged (via ours-merge), so the merge is
    /// a no-op. But descendant's tree doesn't have B's content! B's changes are lost.
    ///
    /// CORRECT approach (this implementation):
    /// 1. Prep: merge predecessor head only
    /// 2. Squash predecessor → creates commit S (parent is B)
    /// 3. Reconcile: merge S^ (which is B) — gets B's content
    /// 4. Reconcile: ours-merge S — marks as merged, tree already has B's content
    /// 5. Catch-up: merge origin/main — gets anything after S
    ///
    /// By merging S^ AFTER the squash, we guarantee we incorporate exactly the main
    /// state up to (but not including) the squash, regardless of when commits landed.
    ///
    /// **REQUIRED TEST**: Property 3 (`squash_parent_ordering_prevents_lost_commits`) in the
    /// "Property-based testing with real git" section verifies this by demonstrating the bug
    /// in the WRONG approach and confirming the CORRECT approach preserves late commits.
    async fn reconcile_descendant(
        &self,
        descendant_branch: &str,
        squash_commit_sha: &str,
        default_branch: &str,
        cancel: &CancellationToken,
    ) -> Result<String> {
        // Fetch default branch (which contains the squash commit) and descendant branch.
        self.run_git(&["fetch", "origin", default_branch, descendant_branch], cancel).await?;

        // Checkout in DETACHED HEAD mode
        let origin_descendant = format!("origin/{}", descendant_branch);
        self.run_git(&["checkout", "--detach", &origin_descendant], cancel).await?;

        // Step 1: Merge the PARENT of the squash commit ($SQUASH_SHA^)
        // This incorporates all of main up to (but not including) the squash.
        // This is the critical fix — we merge the exact pre-squash state.
        let squash_parent = format!("{}^", squash_commit_sha);
        self.run_git(&[
            "merge",
            &squash_parent,
            "--no-edit",
            "-m", &format!("Merge {} (pre-squash) into {} (merge-train)", default_branch, descendant_branch),
        ], cancel).await?;

        // Step 2: ours-merge the squash commit itself
        // Adds squash as parent without changing tree (which now has all pre-squash content).
        self.run_git(&[
            "merge",
            squash_commit_sha,
            "--strategy", "ours",
            "--no-edit",
            "-m", &format!("Relate {} with squash commit (merge-train)", descendant_branch),
        ], cancel).await?;

        // Step 3: Catch-up merge of origin/<default_branch>
        // Picks up any commits that landed on main AFTER the squash commit.
        let origin_default = format!("origin/{}", default_branch);
        self.run_git(&[
            "merge",
            &origin_default,
            "--no-edit",
            "-m", &format!("Merge {} into {} (merge-train catch-up)", default_branch, descendant_branch),
        ], cancel).await?;

        // Push HEAD to the remote branch
        let push_ref = format!("HEAD:refs/heads/{}", descendant_branch);
        self.run_git(&["push", "origin", &push_ref], cancel).await?;

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
            .args(&["-c", &format!("user.signingkey={}", self.signing_key)])
            .args(&["-c", "commit.gpgsign=true"])
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
            .args(&["-c", &format!("user.signingkey={}", self.signing_key)])
            .args(&["-c", "commit.gpgsign=true"])
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

**Worktree lifecycle:**

Worktrees are keyed by the **original** root PR number (the PR that received `@merge-train start`), not the current root. This provides a stable identifier throughout the cascade:

1. **Creation**: When `@merge-train start` is issued on PR #123, create `stack-123/`
2. **During cascade**: As the cascade advances (#123 merges, #124 becomes new root), continue using `stack-123/`
3. **Completion**: When the final PR in the stack merges, remove `stack-123/`
4. **Stop**: When `@merge-train stop` is issued, remove the worktree immediately

For **fan-out**, when PR #123 has multiple descendants (#124, #125):
1. Original worktree `stack-123/` is used until #123 merges
2. After #123 merges, #124 and #125 become independent roots
3. **Remove `stack-123/` FIRST** (before creating new worktrees)
4. Create new worktrees `stack-124/` and `stack-125/` for each

**Critical ordering**: The old worktree must be removed before creating new worktrees. Although we use detached HEAD mode (avoiding branch locking), removing the old worktree first ensures:
- No risk of stale worktree references causing git errors
- Clean state for the new independent trains
- Prevents accumulation of worktrees during rapid fan-out cascades

This ensures worktrees are cleaned up promptly rather than waiting for the 24-hour timeout.

**REQUIRED TEST**: Property 7 (`fanout_worktree_ordering`) in the "Property-based testing with real git" section verifies this ordering by asserting the old worktree is removed before new worktrees are created during fan-out.

**Implementation:**

```rust
impl GitOperations {
    /// Get or create a worktree for a stack. Called before cascade operations.
    ///
    /// Worktrees are created in detached HEAD mode to avoid git's restriction that
    /// a branch can only be checked out in one worktree at a time. Operations within
    /// the worktree use `git checkout --detach origin/<branch>` and push with
    /// `git push origin HEAD:refs/heads/<branch>`.
    async fn worktree_for_stack(&self, root_pr: PrNumber) -> Result<PathBuf> {
        let worktree_path = self.base_dir
            .join("worktrees")
            .join(format!("stack-{}", root_pr.0));

        if !worktree_path.exists() {
            // Create worktree from the shared clone in detached HEAD mode.
            // Using --detach ensures no branch is "checked out" in this worktree,
            // avoiding conflicts with other worktrees or the main clone.
            self.run_git_uncancellable(&[
                "worktree", "add", "--detach",
                worktree_path.to_str().unwrap(),
                "HEAD",
            ]).await?;
        }

        Ok(worktree_path)
    }

    /// Remove a stack's worktree. Called after stack completes successfully,
    /// on stop command, or when cleanup_worktree_on_abort fails (worktree corrupted beyond repair).
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
    // ONLY merges predecessor head — does NOT merge main (that happens in reconcile)
    for desc in descendants {
        git.prepare_descendant(&desc.head_ref, pr.number, cancel).await?;
    }

    // Phase 2: Squash-merge this PR into the default branch
    // CRITICAL: Pass expected_head_sha to prevent merging unreviewed commits!
    // If someone pushes to the PR branch after we evaluated readiness, this will fail.
    let squash_sha = github.squash_merge(pr.number, &pr.head_sha).await?;

    // Phase 3: Reconcile ALL descendants AFTER squashing
    // This merges $SQUASH_SHA^ (parent of squash), then ours-merges squash, then catches up
    for desc in descendants {
        git.reconcile_descendant(&desc.head_ref, &squash_sha, default_branch, cancel).await?;
        github.retarget_pr(desc.number, default_branch).await?;
        // Record that reconciliation completed — CRITICAL for is_root() to return true
        // Without this, a manually-retargeted PR could be discovered as a root
        // without the ours-merge chain being applied.
        repo_state.update_pr_reconciliation(desc.number, squash_sha.clone());
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

# Poll interval for active trains (fallback for missed webhooks)
# Lower = faster recovery from missed webhooks, but more API calls
poll_interval_mins = 10  # or env: MERGE_TRAIN_POLL_INTERVAL_MINS
```

---

## Testing

### Crash-injection testing

The durability and recovery guarantees documented above must be verified with crash-injection tests. Without these tests, subtle bugs in fsync ordering, event log format, or recovery logic could cause data corruption or duplicate operations in production.

**Commit point boundaries**: Inject crashes (simulated process kill or SIGKILL) at each of these points:

| Phase | Crash point | Expected recovery behavior |
|-------|-------------|---------------------------|
| Intent fsync | After writing intent event, before external operation | Recovery sees intent, retries operation (must be idempotent or detectable) |
| External operation | During git push or API call | Operation may or may not have completed; recovery must detect and handle both |
| Done fsync | After external op succeeded, before writing completion event | Recovery sees intent without completion, verifies operation succeeded, writes completion |
| Spool .done marker | After completion event, before marking webhook as processed | Webhook may replay; all state changes must be idempotent |
| Generation rollover | During compaction (between any two fsyncs) | Either old or new generation must be complete and usable |

**Required test assertions**:

1. **Each PR squashed at most once**: After any crash/recovery sequence, verify the PR was merged exactly once (not zero, not twice). Use GitHub API to check merge status.

2. **No silent branch divergence**: After recovery, verify that each descendant branch's tree matches what it would have been with no crash. Compute expected tree independently and compare.

3. **Monotonic progress**: After recovery, the `recovery_seq` must be >= what it was before the crash. State should not "rewind" to an earlier phase.

4. **Frozen descendants honored**: If crash occurs mid-preparation, recovery must use the frozen_descendants from the logged phase_transition, not re-query the (possibly changed) descendants index.

5. **No orphaned worktrees**: After recovery, all worktrees must be either associated with an active train or cleaned up within the configured max_age.

**Crash-injection test harness**:

```rust
/// Trait for injecting crashes at specific points
trait CrashPoint {
    /// Called before each fsync. If returns Err(Crash), the process "dies".
    fn before_fsync(&self, ctx: &FsyncContext) -> Result<(), Crash>;

    /// Called before each external operation (git push, API call)
    fn before_external_op(&self, ctx: &OpContext) -> Result<(), Crash>;
}

/// Test harness that runs the full cascade with crash injection
async fn test_crash_recovery(
    scenario: CrashScenario,
    crash_points: &[CrashPointConfig],
) -> TestResult {
    for crash_point in crash_points {
        // 1. Set up initial state (stack of PRs, train started)
        let initial_state = setup_test_scenario(&scenario).await;

        // 2. Run cascade with crash injection enabled
        let crash_injector = CrashInjector::new(crash_point);
        let result = run_cascade_with_crashes(&crash_injector).await;

        // 3. Simulate process restart
        let recovered_state = restart_and_recover().await;

        // 4. Verify assertions
        assert_squashed_at_most_once(&initial_state, &recovered_state);
        assert_no_branch_divergence(&initial_state, &recovered_state);
        assert_monotonic_progress(&initial_state, &recovered_state);

        // 5. Let cascade complete
        let final_state = continue_cascade_to_completion().await;
        assert_cascade_correct(&scenario, &final_state);
    }
}
```

**Minimum test matrix**:

- Linear stack (main ← #1 ← #2 ← #3): Crash at each phase boundary
- Fan-out (main ← #1 ← {#2, #3}): Crash during each descendant's preparation
- Concurrent descendant addition: Crash after frozen_descendants logged but before squash
- Power loss simulation: Kill -9 without clean shutdown
- Partial fsync: Simulate fsync succeeding for file but not directory

**Property-based testing**: Use property-based testing to generate arbitrary stack topologies and crash points:

```rust
proptest! {
    #[test]
    fn crash_recovery_preserves_invariants(
        stack: StackTopology,
        crash_points: Vec<CrashPoint>,
    ) {
        for crash_point in crash_points {
            let result = run_crash_test(&stack, crash_point);
            prop_assert!(result.squash_count <= 1);
            prop_assert!(result.no_branch_divergence);
            prop_assert!(result.recovery_seq_monotonic);
        }
    }
}
```

### Integration testing with GitHub

For end-to-end testing against real GitHub (or a GitHub Enterprise test instance):

1. **Test repository setup**: Create a dedicated test repository with branch protection rules matching production
2. **Automated stack creation**: Script that creates PRs in various topologies
3. **Cascade verification**: After cascade completes, verify:
   - All PRs are merged
   - Commit history on main is correct (linear, squash commits only)
   - No orphaned branches
4. **Failure injection**: Use GitHub's rate limiting or a mock server to test API error handling

### Property-based testing with real git (REQUIRED)

The cascade correctness properties are subtle and must be verified against real git, not mocks. These tests shell out to actual git commands and verify properties over generated scenarios.

**Test infrastructure:**

```rust
/// A generated file change for property-based tests.
/// Files are identified by path; content is arbitrary bytes.
#[derive(Clone, Debug, Arbitrary)]
struct FileChange {
    path: FilePath,      // e.g., "src/foo.rs", max depth 3, max 50 files
    content: Vec<u8>,    // arbitrary content, max 10KB per file
}

/// A generated commit containing one or more file changes.
#[derive(Clone, Debug, Arbitrary)]
struct GeneratedCommit {
    changes: Vec<FileChange>,  // 1-10 file changes
    message: String,           // arbitrary, for debugging
}

/// A generated branch containing a sequence of commits.
#[derive(Clone, Debug, Arbitrary)]
struct GeneratedBranch {
    commits: Vec<GeneratedCommit>,  // 1-5 commits
}

/// File paths touched by a branch (for disjointness checking).
fn files_touched(branch: &GeneratedBranch) -> HashSet<FilePath> {
    branch.commits.iter()
        .flat_map(|c| c.changes.iter().map(|ch| ch.path.clone()))
        .collect()
}

/// Check if two branches touch disjoint file sets.
fn branches_disjoint(a: &GeneratedBranch, b: &GeneratedBranch) -> bool {
    files_touched(a).is_disjoint(&files_touched(b))
}

/// Apply a generated branch to a git repo, returning the resulting HEAD SHA.
fn apply_branch(repo: &Path, branch_name: &str, base: &str, branch: &GeneratedBranch) -> Sha {
    run_git(repo, &["checkout", "-b", branch_name, base]);
    for commit in &branch.commits {
        for change in &commit.changes {
            let file_path = repo.join(&change.path);
            std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();
            std::fs::write(&file_path, &change.content).unwrap();
            run_git(repo, &["add", &change.path]);
        }
        run_git(repo, &["commit", "-m", &commit.message]);
    }
    get_head_sha(repo)
}

/// Get the tree contents as a map of path → content for comparison.
fn tree_contents(repo: &Path, ref_name: &str) -> HashMap<FilePath, Vec<u8>> {
    // Uses git ls-tree -r and git cat-file to extract all file contents
    // Returns complete snapshot of the tree at that ref
    todo!()
}
```

#### REQUIRED: Cascade content preservation

**Property 1: Descendant content is preserved after cascade**

After a cascade where predecessor and descendant touch disjoint files, and main receives arbitrary commits that also touch disjoint files, the final main must contain:
- All content from the predecessor branch (squashed)
- All content from the descendant branch (squashed)
- All content from the intervening main commits

```rust
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    #[test]
    fn descendant_content_preserved_after_cascade(
        predecessor_branch: GeneratedBranch,
        descendant_branch: GeneratedBranch,
        intervening_main_commits: Vec<GeneratedCommit>,  // 0-3 commits
    ) {
        // Skip if branches aren't disjoint (conflict case is separate)
        let pred_files = files_touched(&predecessor_branch);
        let desc_files = files_touched(&descendant_branch);
        let main_files: HashSet<_> = intervening_main_commits.iter()
            .flat_map(|c| c.changes.iter().map(|ch| ch.path.clone()))
            .collect();

        prop_assume!(pred_files.is_disjoint(&desc_files));
        prop_assume!(pred_files.is_disjoint(&main_files));
        prop_assume!(desc_files.is_disjoint(&main_files));

        // Set up repo with main branch
        let repo = TempRepo::new();

        // Create predecessor branch from main
        let pred_tip = apply_branch(&repo, "predecessor", "main", &predecessor_branch);

        // Create descendant branch from predecessor
        let desc_tip = apply_branch(&repo, "descendant", "predecessor", &descendant_branch);

        // Record expected final content: descendant tip + intervening main commits
        let expected_desc_content = tree_contents(&repo, "descendant");

        // === Simulate cascade ===

        // 1. Preparation: merge predecessor into descendant (already done - descendant is based on it)

        // 2. Someone pushes intervening commits to main (BEFORE squash)
        run_git(&repo, &["checkout", "main"]);
        for commit in &intervening_main_commits {
            for change in &commit.changes {
                write_file(&repo, &change.path, &change.content);
                run_git(&repo, &["add", &change.path]);
            }
            run_git(&repo, &["commit", "-m", &commit.message]);
        }
        let main_before_squash = get_head_sha(&repo);

        // 3. Squash-merge predecessor into main
        run_git(&repo, &["merge", "--squash", "predecessor"]);
        run_git(&repo, &["commit", "-m", "Squash predecessor"]);
        let squash_sha = get_head_sha(&repo);

        // 4. Reconcile descendant using the ours-merge dance
        run_git(&repo, &["checkout", "descendant"]);

        // 4a. Merge $SQUASH_SHA^ (parent of squash = main_before_squash)
        run_git(&repo, &["merge", &format!("{}^", squash_sha), "-m", "Merge pre-squash main"]);

        // 4b. Ours-merge $SQUASH_SHA
        run_git(&repo, &["merge", &squash_sha, "--strategy", "ours", "-m", "Ours-merge squash"]);

        // 4c. Catch-up merge main
        run_git(&repo, &["merge", "main", "-m", "Catch-up"]);

        // 5. Squash-merge descendant into main
        run_git(&repo, &["checkout", "main"]);
        run_git(&repo, &["merge", "--squash", "descendant"]);
        run_git(&repo, &["commit", "-m", "Squash descendant"]);

        // === Verify properties ===

        let final_main = tree_contents(&repo, "main");

        // All descendant files present with correct content
        for (path, content) in &expected_desc_content {
            prop_assert_eq!(
                final_main.get(path),
                Some(content),
                "Descendant file {} missing or wrong content", path
            );
        }

        // All intervening main commit files present
        for commit in &intervening_main_commits {
            for change in &commit.changes {
                prop_assert_eq!(
                    final_main.get(&change.path),
                    Some(&change.content),
                    "Intervening main file {} missing or wrong content", change.path
                );
            }
        }
    }
}
```

**Property 2: Intervening main commits survive the train**

Commits pushed to main that don't intersect with any branch in the train must have their content preserved after the entire train merges.

```rust
proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]
    #[test]
    fn intervening_main_commits_preserved(
        train_branches: Vec<GeneratedBranch>,  // 1-3 branches forming a linear stack
        intervening_commits: Vec<(usize, GeneratedCommit)>,  // (insert_after_branch_idx, commit)
    ) {
        prop_assume!(!train_branches.is_empty());

        // Collect all files touched by train
        let train_files: HashSet<_> = train_branches.iter()
            .flat_map(|b| files_touched(b))
            .collect();

        // Filter intervening commits to only those disjoint from train
        let disjoint_intervening: Vec<_> = intervening_commits.iter()
            .filter(|(_, commit)| {
                commit.changes.iter().all(|ch| !train_files.contains(&ch.path))
            })
            .collect();

        prop_assume!(!disjoint_intervening.is_empty());

        let repo = TempRepo::new();

        // Build the stack
        let mut branch_names = vec![];
        let mut base = "main".to_string();
        for (i, branch) in train_branches.iter().enumerate() {
            let name = format!("branch-{}", i);
            apply_branch(&repo, &name, &base, branch);
            branch_names.push(name.clone());
            base = name;
        }

        // Run the cascade, inserting intervening commits at specified points
        let mut intervening_by_insertion: HashMap<usize, Vec<&GeneratedCommit>> = HashMap::new();
        for (idx, commit) in &disjoint_intervening {
            intervening_by_insertion.entry(*idx).or_default().push(commit);
        }

        for (i, branch_name) in branch_names.iter().enumerate() {
            // Push any intervening commits scheduled for this point
            if let Some(commits) = intervening_by_insertion.get(&i) {
                run_git(&repo, &["checkout", "main"]);
                for commit in commits {
                    for change in &commit.changes {
                        write_file(&repo, &change.path, &change.content);
                        run_git(&repo, &["add", &change.path]);
                    }
                    run_git(&repo, &["commit", "-m", &commit.message]);
                }
            }

            // Prepare descendants (for branches after this one)
            // ... (cascade logic)

            // Squash this branch
            run_git(&repo, &["checkout", "main"]);
            run_git(&repo, &["merge", "--squash", branch_name]);
            run_git(&repo, &["commit", "-m", &format!("Squash {}", branch_name)]);
            let squash_sha = get_head_sha(&repo);

            // Reconcile remaining branches
            // ... (full ours-merge dance for each remaining branch)
        }

        // Verify all disjoint intervening commits are present
        let final_main = tree_contents(&repo, "main");
        for (_, commit) in &disjoint_intervening {
            for change in &commit.changes {
                prop_assert_eq!(
                    final_main.get(&change.path),
                    Some(&change.content),
                    "Intervening commit file {} was lost", change.path
                );
            }
        }
    }
}
```

**Property 3: The $SQUASH_SHA^ ordering is essential**

This test verifies that merging main during preparation (WRONG) loses commits, while merging $SQUASH_SHA^ during reconciliation (CORRECT) preserves them. This is a regression test for the bug described in "Why merging $SQUASH_SHA^ is essential".

```rust
proptest! {
    #[test]
    fn squash_parent_ordering_prevents_lost_commits(
        predecessor_branch: GeneratedBranch,
        descendant_branch: GeneratedBranch,
        late_main_commit: GeneratedCommit,  // Pushed to main AFTER prep, BEFORE squash
    ) {
        // Ensure late_main_commit touches files disjoint from both branches
        let pred_files = files_touched(&predecessor_branch);
        let desc_files = files_touched(&descendant_branch);
        let late_files: HashSet<_> = late_main_commit.changes.iter()
            .map(|ch| ch.path.clone())
            .collect();

        prop_assume!(pred_files.is_disjoint(&late_files));
        prop_assume!(desc_files.is_disjoint(&late_files));
        prop_assume!(pred_files.is_disjoint(&desc_files));
        prop_assume!(!late_main_commit.changes.is_empty());

        let repo = TempRepo::new();

        // Setup
        apply_branch(&repo, "predecessor", "main", &predecessor_branch);
        apply_branch(&repo, "descendant", "predecessor", &descendant_branch);

        // === WRONG approach (what we must NOT do) ===
        let wrong_repo = repo.clone_to_temp();
        {
            // Preparation: merge main into descendant (WRONG - merging main too early)
            run_git(&wrong_repo, &["checkout", "descendant"]);
            run_git(&wrong_repo, &["merge", "main", "-m", "Prep merge main (WRONG)"]);

            // Late commit arrives on main
            run_git(&wrong_repo, &["checkout", "main"]);
            for change in &late_main_commit.changes {
                write_file(&wrong_repo, &change.path, &change.content);
                run_git(&wrong_repo, &["add", &change.path]);
            }
            run_git(&wrong_repo, &["commit", "-m", "Late main commit"]);

            // Squash predecessor
            run_git(&wrong_repo, &["merge", "--squash", "predecessor"]);
            run_git(&wrong_repo, &["commit", "-m", "Squash predecessor"]);
            let squash_sha = get_head_sha(&wrong_repo);

            // Reconcile descendant (ours-merge squash, then catch-up)
            run_git(&wrong_repo, &["checkout", "descendant"]);
            run_git(&wrong_repo, &["merge", &squash_sha, "--strategy", "ours", "-m", "Ours"]);
            run_git(&wrong_repo, &["merge", "main", "-m", "Catch-up"]);

            // Squash descendant
            run_git(&wrong_repo, &["checkout", "main"]);
            run_git(&wrong_repo, &["merge", "--squash", "descendant"]);
            run_git(&wrong_repo, &["commit", "-m", "Squash descendant"]);

            // BUG: late_main_commit content is LOST
            let final_main = tree_contents(&wrong_repo, "main");
            for change in &late_main_commit.changes {
                // This WILL FAIL - the late commit is lost
                // We assert it IS lost to confirm the bug exists
                prop_assert!(
                    final_main.get(&change.path) != Some(&change.content),
                    "WRONG approach should lose late commit, but it was preserved"
                );
            }
        }

        // === CORRECT approach (what we DO) ===
        {
            // Preparation: merge predecessor only (NOT main)
            // (descendant is already based on predecessor, so this is a no-op here)

            // Late commit arrives on main
            run_git(&repo, &["checkout", "main"]);
            for change in &late_main_commit.changes {
                write_file(&repo, &change.path, &change.content);
                run_git(&repo, &["add", &change.path]);
            }
            run_git(&repo, &["commit", "-m", "Late main commit"]);
            let main_with_late = get_head_sha(&repo);

            // Squash predecessor
            run_git(&repo, &["merge", "--squash", "predecessor"]);
            run_git(&repo, &["commit", "-m", "Squash predecessor"]);
            let squash_sha = get_head_sha(&repo);

            // Reconcile descendant: merge $SQUASH_SHA^ (CORRECT)
            run_git(&repo, &["checkout", "descendant"]);
            // $SQUASH_SHA^ is main_with_late, which includes the late commit
            run_git(&repo, &["merge", &format!("{}^", squash_sha), "-m", "Merge pre-squash main"]);
            run_git(&repo, &["merge", &squash_sha, "--strategy", "ours", "-m", "Ours"]);
            run_git(&repo, &["merge", "main", "-m", "Catch-up"]);

            // Squash descendant
            run_git(&repo, &["checkout", "main"]);
            run_git(&repo, &["merge", "--squash", "descendant"]);
            run_git(&repo, &["commit", "-m", "Squash descendant"]);

            // CORRECT: late_main_commit content is PRESERVED
            let final_main = tree_contents(&repo, "main");
            for change in &late_main_commit.changes {
                prop_assert_eq!(
                    final_main.get(&change.path),
                    Some(&change.content),
                    "CORRECT approach must preserve late commit file {}", change.path
                );
            }
        }
    }
}
```

#### REQUIRED: Squash-vs-rebase detection for late additions

The late addition flow must correctly distinguish squash merges from rebase merges. Using the wrong merge type causes incorrect reconciliation.

**Property 4: Squash merge detection accepts valid squash merges**

```rust
proptest! {
    #[test]
    fn squash_merge_detection_accepts_squash(
        branch: GeneratedBranch,
    ) {
        prop_assume!(!branch.commits.is_empty());

        let repo = TempRepo::new();
        apply_branch(&repo, "feature", "main", &branch);

        // Record main HEAD before squash
        let main_head_before = get_head_sha(&repo);

        // Perform a squash merge
        run_git(&repo, &["checkout", "main"]);
        run_git(&repo, &["merge", "--squash", "feature"]);
        run_git(&repo, &["commit", "-m", "Squash feature"]);
        let squash_sha = get_head_sha(&repo);

        // Validate using the detection logic from handle_late_addition
        let commit_info = get_commit_info(&repo, &squash_sha);

        // The detection logic accepts commits with exactly one parent
        prop_assert_eq!(commit_info.parents.len(), 1, "Squash merge should have one parent");

        // AND that parent is the prior main HEAD
        prop_assert_eq!(
            &commit_info.parents[0], &main_head_before,
            "Squash merge parent should be prior main HEAD"
        );

        // Verify acceptance: full validation passes
        let is_valid = commit_info.parents.len() == 1
            && commit_info.parents[0] == main_head_before;
        prop_assert!(is_valid, "Detection should ACCEPT squash merge");
    }
}
```

**Property 5: Squash merge detection rejects merge commits**

```rust
proptest! {
    #[test]
    fn squash_merge_detection_rejects_merge_commit(
        branch: GeneratedBranch,
        main_commit: GeneratedCommit,  // Creates divergence requiring true merge
    ) {
        prop_assume!(!branch.commits.is_empty());

        // Ensure main_commit touches different files than branch
        let branch_files = files_touched(&branch);
        prop_assume!(main_commit.changes.iter().all(|ch| !branch_files.contains(&ch.path)));
        prop_assume!(!main_commit.changes.is_empty());

        let repo = TempRepo::new();
        apply_branch(&repo, "feature", "main", &branch);

        // Add divergent commit on main
        run_git(&repo, &["checkout", "main"]);
        for change in &main_commit.changes {
            write_file(&repo, &change.path, &change.content);
            run_git(&repo, &["add", &change.path]);
        }
        run_git(&repo, &["commit", "-m", "Divergent main commit"]);

        // Perform a TRUE merge (not squash)
        run_git(&repo, &["merge", "feature", "-m", "Merge feature"]);
        let merge_sha = get_head_sha(&repo);

        // Validate using the detection logic
        let commit_info = get_commit_info(&repo, &merge_sha);

        // Merge commits have two parents
        prop_assert_eq!(commit_info.parents.len(), 2, "Merge commit should have two parents");

        // Verify rejection: parent count check fails
        let is_valid_squash = commit_info.parents.len() == 1;
        prop_assert!(!is_valid_squash, "Detection should REJECT merge commit");
    }
}
```

**Property 6: Squash merge detection rejects wrong parent**

This catches multi-commit rebase merges and multi-commit fast-forward merges, where the commit has a single parent but that parent is NOT the prior main HEAD.

```rust
proptest! {
    #[test]
    fn squash_merge_detection_rejects_wrong_parent(
        branch: GeneratedBranch,
    ) {
        prop_assume!(branch.commits.len() >= 2); // Multi-commit branch required

        let repo = TempRepo::new();
        apply_branch(&repo, "feature", "main", &branch);

        // Record the current main HEAD (what parent SHOULD be)
        let main_head_before = get_head_sha_for_branch(&repo, "main");

        // Simulate a rebase merge: rebase the branch onto main, then fast-forward main
        run_git(&repo, &["checkout", "feature"]);
        run_git(&repo, &["rebase", "main"]);
        run_git(&repo, &["checkout", "main"]);
        run_git(&repo, &["merge", "--ff-only", "feature"]);
        let result_sha = get_head_sha(&repo);

        // Get parent of the result
        let commit_info = get_commit_info(&repo, &result_sha);

        // Single parent (looks like squash at first glance)
        prop_assert_eq!(commit_info.parents.len(), 1, "Rebase result should have one parent");

        // But parent is NOT the prior main HEAD — it's the second-to-last rebased commit
        let parent_sha = &commit_info.parents[0];
        prop_assert_ne!(
            parent_sha, &main_head_before,
            "Rebase/fast-forward parent should NOT be prior main HEAD"
        );

        // Verify rejection: full validation fails
        let is_valid = commit_info.parents.len() == 1
            && commit_info.parents[0] == main_head_before;
        prop_assert!(!is_valid, "Detection should REJECT multi-commit rebase/fast-forward");
    }
}
```

**Property 7: Preflight check rejects non-squash-only repositories**

The bot requires squash-only merge configuration at the repository level. This is enforced by the preflight check, not by post-hoc detection of rebase merges.

```rust
proptest! {
    #[test]
    fn preflight_rejects_non_squash_only(
        allow_squash: bool,
        allow_merge: bool,
        allow_rebase: bool,
    ) {
        // The bot should only accept repos where squash is the ONLY allowed method
        let should_accept = allow_squash && !allow_merge && !allow_rebase;

        let repo_settings = RepoSettings {
            allow_squash_merge: allow_squash,
            allow_merge_commit: allow_merge,
            allow_rebase_merge: allow_rebase,
        };

        let result = check_merge_method_preflight(&repo_settings);

        if should_accept {
            prop_assert!(result.is_ok(), "Should accept squash-only config");
        } else {
            prop_assert!(result.is_err(), "Should reject non-squash-only config");

            // Verify error message mentions the specific issue
            let err = result.unwrap_err();
            if !allow_squash {
                prop_assert!(err.contains("squash merge disabled"));
            } else if allow_merge {
                prop_assert!(err.contains("merge commit") || err.contains("allow_merge_commit"));
            } else if allow_rebase {
                prop_assert!(err.contains("rebase") || err.contains("allow_rebase_merge"));
            }
        }
    }
}
```

#### REQUIRED: Worktree cleanup ordering for fan-out

When a PR with multiple descendants completes, the old worktree must be removed before new worktrees are created.

**Property 8: Fan-out worktree lifecycle**

```rust
proptest! {
    #[test]
    fn fanout_worktree_ordering(
        root_branch: GeneratedBranch,
        descendant_branches: Vec<GeneratedBranch>,  // 2-4 descendants (fan-out)
    ) {
        prop_assume!(descendant_branches.len() >= 2);
        prop_assume!(descendant_branches.len() <= 4);

        // Ensure all branches are disjoint
        let root_files = files_touched(&root_branch);
        for (i, desc) in descendant_branches.iter().enumerate() {
            prop_assume!(root_files.is_disjoint(&files_touched(desc)));
            for (j, other) in descendant_branches.iter().enumerate() {
                if i != j {
                    prop_assume!(files_touched(desc).is_disjoint(&files_touched(other)));
                }
            }
        }

        let repo = TempRepo::new();
        let worktree_base = TempDir::new();

        // Setup: root PR and multiple descendants
        apply_branch(&repo, "root", "main", &root_branch);
        for (i, desc) in descendant_branches.iter().enumerate() {
            apply_branch(&repo, &format!("desc-{}", i), "root", desc);
        }

        // Simulate train started on root
        let root_worktree = worktree_base.path().join("stack-1");  // PR #1
        run_git(&repo, &["worktree", "add", "--detach", root_worktree.to_str().unwrap(), "HEAD"]);
        prop_assert!(root_worktree.exists(), "Root worktree should exist");

        // Prepare all descendants
        for i in 0..descendant_branches.len() {
            run_git_in_worktree(&root_worktree, &["checkout", "--detach", &format!("origin/desc-{}", i)]);
            run_git_in_worktree(&root_worktree, &["merge", "origin/root", "-m", "Prep"]);
            // push would happen here
        }

        // Squash root
        run_git(&repo, &["checkout", "main"]);
        run_git(&repo, &["merge", "--squash", "root"]);
        run_git(&repo, &["commit", "-m", "Squash root"]);
        let squash_sha = get_head_sha(&repo);

        // Reconcile all descendants
        for i in 0..descendant_branches.len() {
            run_git_in_worktree(&root_worktree, &["checkout", "--detach", &format!("origin/desc-{}", i)]);
            run_git_in_worktree(&root_worktree, &["merge", &format!("{}^", squash_sha), "-m", "Pre-squash"]);
            run_git_in_worktree(&root_worktree, &["merge", &squash_sha, "--strategy", "ours", "-m", "Ours"]);
            run_git_in_worktree(&root_worktree, &["merge", "origin/main", "-m", "Catch-up"]);
            // push and retarget would happen here
        }

        // === CRITICAL ORDERING: Remove old worktree FIRST ===

        // Old worktree must be removed before creating new ones
        run_git(&repo, &["worktree", "remove", "--force", root_worktree.to_str().unwrap()]);
        prop_assert!(!root_worktree.exists(), "Old worktree should be removed before creating new ones");

        // Now create new worktrees for each descendant
        let new_worktrees: Vec<_> = (0..descendant_branches.len())
            .map(|i| {
                let path = worktree_base.path().join(format!("stack-{}", i + 10));  // PRs #10, #11, etc.
                run_git(&repo, &["worktree", "add", "--detach", path.to_str().unwrap(), "HEAD"]);
                path
            })
            .collect();

        // All new worktrees should exist
        for wt in &new_worktrees {
            prop_assert!(wt.exists(), "New worktree {:?} should exist", wt);
        }

        // Old worktree should still not exist
        prop_assert!(!root_worktree.exists(), "Old worktree should remain removed");
    }
}
```

#### REQUIRED: Frozen descendants invariant

**Property 9: Recovery uses frozen descendants, not current state**

```rust
proptest! {
    #[test]
    fn recovery_uses_frozen_descendants(
        initial_descendants: Vec<GeneratedBranch>,  // 1-3 descendants at train start
        late_descendants: Vec<GeneratedBranch>,     // 0-2 descendants added mid-cascade
    ) {
        prop_assume!(!initial_descendants.is_empty());
        prop_assume!(initial_descendants.len() <= 3);
        prop_assume!(late_descendants.len() <= 2);

        // All branches must be disjoint for this test
        let all_branches: Vec<_> = initial_descendants.iter()
            .chain(late_descendants.iter())
            .collect();
        for (i, a) in all_branches.iter().enumerate() {
            for (j, b) in all_branches.iter().enumerate() {
                if i != j {
                    prop_assume!(files_touched(a).is_disjoint(&files_touched(b)));
                }
            }
        }

        // Simulate state at Preparing phase entry
        let frozen_descendants: Vec<usize> = (0..initial_descendants.len()).collect();

        // Simulate late additions (would be added via predecessor_declared events during spool replay)
        let current_descendants: Vec<usize> = (0..(initial_descendants.len() + late_descendants.len())).collect();

        // Recovery MUST use frozen_descendants, not current_descendants
        let to_process = frozen_descendants.clone();

        // Verify we're processing the right set
        prop_assert_eq!(to_process.len(), initial_descendants.len());
        prop_assert!(to_process.iter().all(|&i| i < initial_descendants.len()));

        // Late descendants should NOT be in the processing set
        for i in initial_descendants.len()..current_descendants.len() {
            prop_assert!(
                !to_process.contains(&i),
                "Late descendant {} should not be in frozen set", i
            );
        }

        // Late descendants will be handled in the NEXT cascade step
        // (when one of the initial descendants becomes the new root)
    }
}
```

#### Required unit tests (non-property-based)

In addition to property-based tests, the following specific scenarios require explicit unit tests:

**Rebase-vs-squash detection edge cases:**

The detection criteria is: **the merge result must have exactly one parent, and that parent must be the prior main HEAD**. This is the invariant that makes the $SQUASH_SHA^ reconciliation logic work correctly.

| Scenario | Expected result |
|----------|-----------------|
| Single-commit branch, squash merged | Accept (single parent, parent is prior main HEAD) |
| Single-commit branch, rebase merged | Accept (indistinguishable from squash for single commit) |
| Multi-commit branch, squash merged | Accept |
| Multi-commit branch, rebase merged | Reject (parent is previous rebased commit, not on main) |
| True merge (two parents) | Reject |
| Single-commit fast-forward | Accept (indistinguishable from squash — parent is prior main HEAD) |
| Multi-commit fast-forward | Reject (parent is in-branch commit, not prior main HEAD — same issue as multi-commit rebase) |

**Worktree cleanup scenarios:**

| Scenario | Expected behavior |
|----------|-------------------|
| Normal completion (single descendant) | Worktree removed after final PR merges |
| Fan-out (multiple descendants) | Old worktree removed, then new worktrees created for each |
| Stop command | Worktree removed immediately |
| Abort (conflict) | Worktree cleaned (merge --abort, reset --hard) but not removed |
| Crash during fan-out worktree creation | Old worktree gone, some new worktrees exist; cleanup removes orphans |

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

- **Command authorization**: Commands (`@merge-train predecessor`, `start`, `stop`) are accepted when the comment is on a **pull request** (not an issue) AND the author has appropriate permissions:

  | Command | Who can issue |
  |---------|---------------|
  | `predecessor` | PR author only |
  | `start` | PR author only |
  | `stop` | PR author OR repository admin/maintainer |
  | `stop --force` | Repository admin only |

  **Authorization checks**:
  1. The comment is on a PR (check `issue.pull_request` field in webhook payload).
  2. For author-only commands: `comment.user.id == pr.user.id`.
  3. For admin/maintainer commands: Query `GET /repos/{o}/{r}/collaborators/{username}/permission` and check `permission` is `admin` or `maintain`.

  **Rationale for stop permissions**: If a train enters a bad state (e.g., stuck on a failing check, endless loop) and the PR author is unavailable (vacation, left company, different timezone), maintainers need the ability to halt the cascade. Without this, a stuck train blocks the entire stack indefinitely.

  **Force stop**: `@merge-train stop --force` performs additional admin actions beyond normal stop:
  - Closes the status comment with "Train forcibly stopped by admin"
  - Optionally closes the affected PRs (if `--close-prs` flag added)

  This is restricted to admins because closing PRs is destructive. Normal `stop` also clears train state (a subsequent `start` begins a fresh cascade from the current stack topology), but does not close PRs or post admin-specific messaging.

  **Audit trail**: All commands are logged with: timestamp, command, issuer user ID, issuer permission level, PR number, outcome. This provides accountability for admin overrides.
- **Webhook validation**: Verify `X-Hub-Signature-256` header against webhook secret.
- **Signing key protection**: GPG private key should be stored securely (e.g., mounted secret, not in repo).
- **Clone isolation**: Each repo gets its own workdir; clean up after operations.
- **Local state protection**: `state_dir` should be readable/writable only by the bot user; losing it means losing restart recovery.
- **Rate limiting**: Respect GitHub API rate limits; back off on 403/429.
- **Audit logging**: Log all merge operations with PR numbers, SHAs, timestamps.
