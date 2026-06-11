# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Test Commands

Standard Rust (2024 edition) cargo toolchain.
Always run `cargo clippy -- -D warnings` and `cargo fmt` after finishing a change.

Property-based tests use `proptest`.
The tests take a long time; a 10min timeout should be sufficient.

## Architecture

This is a GitHub bot for orchestrating squash-merge cascades of stacked PRs. Key architectural decisions:

### Effects-as-Data Pattern

The core follows a functional core / imperative shell design:
- **Pure functions** compute state transitions and return `Effect` values (see `src/effects/`)
- **Interpreters** execute effects against GitHub API and git (see `src/effects/interpreter.rs`, `src/github/interpreter.rs`)
- This enables testing without I/O and crash recovery by knowing what was attempted

### Cascade State Machine

The cascade phase transitions (`src/state/transitions.rs`) move a train through phases:
```
Idle → Preparing → SquashPending → Reconciling → CatchingUp → Retargeting → Idle
```

Key invariant: `frozen_descendants` is captured when entering `Preparing` and carried through all subsequent phases. Recovery uses this frozen set, never re-queries.

**The cascade engine that drives these transitions does not exist yet** (IMPLEMENTATION_PLAN.md Stage 10, plus the per-repo worker and bootstrap of Stages 15/17/18). The modules below are built and tested but largely unwired: nothing yet drains the webhook spool, constructs effects, or executes cascade steps.

### Module Structure

- `types/` - Core domain types with type-system-enforced invariants (newtypes for `PrNumber`, `Sha`, `CommentId`, etc.)
- `state/` - Pure state logic: topology, descendants index, phase transitions, validation
- `effects/` - Effect types (`GitEffect`, `GitHubEffect`) and interpreters
- `github/` - GitHub API client, retry logic, error handling
- `git/` - Local git operations (worktrees, merges, push)
- `persistence/` - Crash-safe event log + snapshot persistence with generation-based compaction
- `spool/` - Webhook delivery queue with filesystem-based crash safety
- `commands/` - Command parsing (`@merge-train start/stop/predecessor`)
- `status/` - Status comment formatting and parsing (backup state in comments)
- `preflight/` - Pre-start validation (merge method, branch protection checks)

### Persistence

Uses event sourcing with periodic snapshots:
- Event log: append-only JSON Lines
- Snapshots: periodic full-state captures
- Generations: crash-safe compaction via generation-based file naming
- All writes use fsync on files and directories

### Key Design Constraints

- Squash-merge only (required for linear history cascade logic)
- No cross-fork PRs (bot pushes to branches)
- Hard cap of 50 PRs per train (state stored in GitHub comments)
- Designed for correctness over speed; CI checks may run quadratically
