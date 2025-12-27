# Merge train

This bot implements a merge train, following [my standard workflow](https://www.patrickstevens.co.uk/posts/2023-10-18-squash-stacked-prs/) for stacked PRs in a squash-merge-to-main world.

# Gotchas

* If the root PR is a draft, the bot rejects `@merge-train start` with an error asking you to mark the PR as ready for review first. Draft PRs elsewhere in the stack are allowed: when the cascade reaches them, it waits (similar to waiting for CI) rather than failing immediately. However, draft PRs cannot be merged into main, so they must be marked ready before the cascade can complete.

# Limitations by design

* The bot works against GitHub only, and is only designed to be run against github.com. (There is some functionality implemented for GitHub Enterprise, but I have no real way of testing it; you should consider that to be unsupported.)
* The bot performs a check to ensure the repository is configured for squash-only merges (via the repository settings `allow_squash_merge`, `allow_merge_commit`, `allow_rebase_merge` - not branch protection rules). It's possible to fool this check by e.g. force-pushing a rebase commit manually to the default branch, or by quickly re-enabling other merge methods after one of our preflight checks has run; if you do that, you may silently get incorrect results, including data loss!
* Of course, don't attempt to run any other merge train software like GitHub's own merge queue.
* The system is incompatible with the combination "dismiss stale approvals" and "approvals are required before merge" branch protection, because the system will push fresh commits to each branch, invalidating approvals on that branch. If approvals are required, this will prevent merging. The bot attempts to detect this setting on startup (requires `administration:read` permission), but if it lacks permissions, it will warn and proceed — the cascade may abort mid-flight if approvals are dismissed.
* The bot doesn't work across forks, because it pushes to the incoming branches.
* Hard cap of 50 PRs per merge train, for internal implementation reasons (we store some state in GitHub comments, which limits that state's size).
* You can add leaf nodes to the DAG that is a merge train, but if you change the internal structure of the train while the stack is in flight, expect it to abort: we generally assume that the DAG of PRs remains connected, for example.
* The "stop" command is inherently pretty racy, so in-flight operations may or may not actually stop precisely when you ask them to.

# Non-goals

* Speed/efficiency. This system is designed for a world in which human wall-clock time is not particularly valuable and CI checks are free, so (for example) we're fine running CI checks quadratically many times in the length of the merge train.
* Automatic resolution of problems. The system aims to be correct and to bail out (requiring manual intervention) if it finds that it's in an unrecognised scenario. For example, merge conflicts with main will abort the train.
* Distributed deployment. There's nothing in the design to account for consensus. State is locally persisted to a filesystem, so it aims to be restart-safe, and it aims to recover from total destruction of local state by re-scraping all the information from GitHub, but that hasn't had much testing.

# Licence

Licensed to you under the MIT licence, a copy of which can be found at [LICENCE.md](LICENCE.md).
