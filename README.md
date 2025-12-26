# Merge train

This bot implements a merge train, following [my standard workflow](https://www.patrickstevens.co.uk/posts/2023-10-18-squash-stacked-prs/) for stacked PRs in a squash-merge-to-main world.

# Gotchas

* The merge train will proceed with draft PRs, but will fail at merge time to merge into main if the PR we're merging is still in draft state at that point. Probably don't use merge trains in conjunction with draft PRs.

# Limitations by design

* The bot works against GitHub only, and is only designed to be run against github.com (though it may work against GitHub Enterprise by coincidence).
* The bot performs a check to ensure there's a branch protection rule on the default branch specifying that "squash merge" is the only allowed merge method: no rebase-merge, no merge commits. It's possible to fool this check by e.g. force-pushing a rebase commit manually to the default branch, or by quickly switching the branch protection rule off after one of our preflight checks has run; if you do that, you may silently get incorrect results, including data loss!
* Of course, don't attempt to run any other merge train software like GitHub's own merge queue.
* The system is incompatible with "dismiss stale approvals" branch protection, because the system will push to each branch, invalidating approvals on that branch.
* The bot doesn't work across forks, because it pushes to the incoming branches.
* Hard cap of 50 PRs per merge train, for internal implementation reasons (we store some state in GitHub comments, which limits that state's size).
* If you change the structure of the train while the stack is in flight, expect it to abort: we generally assume that the DAG of PRs remains connected.
* The "stop" command is inherently pretty racy, so in-flight operations may or may not actually stop precisely when you ask them to.

# Non-goals

* Speed/efficiency. This system is designed for a world in which human wall-clock time is not particularly valuable and CI checks are free, so (for example) we're fine running CI checks quadratically many times in the length of the merge train.
* Automatic resolution of problems. The system aims to be correct and to bail out (requiring manual intervention) if it finds that it's in an unrecognised scenario. For example, merge conflicts with main will abort the train.
* Distributed deployment. There's nothing in the design to account for consensus. State is locally persisted to a filesystem, so it aims to be restart-safe, and it aims to recover from total destruction of local state by re-scraping all the information from GitHub, but that hasn't had much testing.

# Licence

Licenced to you under the MIT licence, a copy of which can be found at [LICENCE.md](LICENCE.md).
