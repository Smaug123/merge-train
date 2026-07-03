//! The dumb effect executor (M2 amendment 6): runs a plan's effects in order,
//! stops at the first observed failure, runs best-effort effects ignoring
//! failures, and hands the outcomes back. All domain knowledge — which
//! outcome carries the observation, what a failure means — lives in the pure
//! `cascade::observe`/`advance`; nothing here branches on effect semantics.
//!
//! Execution is synchronous. The worker thread calls [`execute_batch`] on a
//! spawned *executor thread* (so multi-minute git sagas never block intake)
//! and receives the outcomes back through its own mailbox; tests call it
//! inline for deterministic, crash-injectable drives.

use tracing::warn;

use crate::cascade::{EffectError, EffectOutcome, EffectResponse};
use crate::effects::{Effect, GitHubEffect, GitHubResponse};
use crate::git::interpreter::{WorktreeGitInterpreter, classify_git_error};
use crate::git::{GitConfig, GitError, run_git_sync};
use crate::github::{OctocrabClient, classify_github_error};

/// Ensures the repo's shared clone exists and is correctly configured,
/// cloning on first use (M5 integration decision: repos are cloned lazily
/// when the first git effect needs them, not at startup). Idempotent; races
/// are impossible because only one saga executes per repo at a time.
///
/// A *pre-existing* clone (operator-created, or from an older build whose
/// origin URL carried credentials) is normalized too: the credential helper
/// is (re-)installed and, when a `clone_url` is configured, the origin URL
/// is pinned to it — otherwise the auth change silently doesn't apply to
/// exactly the repos that existed before it (Codex M5 round 17).
pub fn ensure_clone(config: &GitConfig, clone_url: Option<&str>) -> Result<(), GitError> {
    let clone_dir = config.clone_dir();
    if clone_dir.exists() {
        run_git_sync(
            &clone_dir,
            &["config", "credential.helper", CREDENTIAL_HELPER],
        )?;
        if let Some(url) = clone_url {
            run_git_sync(&clone_dir, &["remote", "set-url", "origin", url])?;
        }
        return Ok(());
    }
    let Some(url) = clone_url else {
        return Err(GitError::WorktreeError {
            details: format!(
                "repo clone missing at {} and no clone URL configured",
                clone_dir.display()
            ),
        });
    };
    let repo_dir = config.repo_dir();
    std::fs::create_dir_all(&repo_dir)?;
    let clone_dir_str = clone_dir.to_str().ok_or_else(|| GitError::WorktreeError {
        details: format!("clone path is not valid UTF-8: {}", clone_dir.display()),
    })?;
    // `--config` persists the helper into the new clone (taking effect for
    // the initial fetch too), so every later fetch/push authenticates the
    // same way without the URL ever carrying credentials.
    let helper = format!("credential.helper={CREDENTIAL_HELPER}");
    run_git_sync(
        &repo_dir,
        &["clone", "--config", &helper, url, clone_dir_str],
    )?;
    Ok(())
}

/// The credential helper wired into fresh clones: it reads the token from
/// the process environment at each fetch/push, so the secret never appears
/// in command lines, git error messages, or the on-disk origin URL (a failed
/// clone would otherwise print the token into the worker's logs — Codex M5
/// review, P1). `GITHUB_TOKEN` is guaranteed present in production: startup
/// refuses to run without it, and git subprocesses inherit the environment.
const CREDENTIAL_HELPER: &str =
    r#"!f() { echo "username=x-access-token"; echo "password=${GITHUB_TOKEN}"; }; f"#;

/// How the worker reaches GitHub. The real arm blocks on the async octocrab
/// interpreter (the worker and executor are plain OS threads); the test arm
/// drives the real-git-backed [`FakeGitHub`](crate::github::test_support::FakeGitHub)
/// world, mirroring PR refs after every effect exactly as conformance does.
#[derive(Clone)]
pub enum GitHubExec {
    /// The production octocrab client, with a handle to the server's tokio
    /// runtime for `block_on`.
    Real {
        /// The repo-scoped client.
        client: OctocrabClient,
        /// The async runtime that owns the client's connections.
        handle: tokio::runtime::Handle,
    },
    /// The conformance-style fake, shared with the test for inspection.
    #[cfg(test)]
    Fake(std::sync::Arc<std::sync::Mutex<crate::github::test_support::FakeGitHub>>),
}

impl GitHubExec {
    /// The server's async runtime, when there is one: the worker loop
    /// uses it for a TIMED mailbox wait (the poll cadence) instead of a
    /// dedicated timer thread per repo. The fake has none, and tests drive
    /// polls directly.
    pub fn runtime(&self) -> Option<&tokio::runtime::Handle> {
        match self {
            GitHubExec::Real { handle, .. } => Some(handle),
            #[cfg(test)]
            GitHubExec::Fake(_) => None,
        }
    }

    /// Executes one GitHub effect, classifying failures into the engine's
    /// [`EffectError`] vocabulary.
    pub fn execute(&self, effect: GitHubEffect) -> Result<GitHubResponse, EffectError> {
        match self {
            GitHubExec::Real { client, handle } => handle
                .block_on(client.interpret(effect))
                .map_err(|e| classify_github_error(&e)),
            #[cfg(test)]
            GitHubExec::Fake(fake) => fake.lock().unwrap().execute(&effect),
        }
    }

    /// Post-effect hook: the fake mirrors `refs/pull/<n>/head` after every
    /// effect (GitHub tracks PR branches continuously; the fake must too, or
    /// the engine's `RefetchPr` reads stale heads after the bot's own pushes).
    /// A no-op in production.
    fn after_effect(&self) {
        #[cfg(test)]
        if let GitHubExec::Fake(fake) = self {
            fake.lock().unwrap().sync_pr_refs();
        }
    }
}

/// One plan's effects, extracted for execution while the worker keeps
/// servicing intake. `feedback` is true for `Control::Continue` plans, whose
/// outcomes feed `observe` → `advance`; terminal plans (`Park`/`Done`/
/// `FanOut`) still carry best-effort cleanup to run but nothing to observe.
#[derive(Debug)]
pub struct SagaBatch {
    /// The train root, which scopes the git worktree.
    pub root: crate::types::PrNumber,
    /// Observed effects, run in order, stopping at the first failure.
    pub effects: Vec<Effect>,
    /// Best-effort effects (status comments, cleanup); failures are logged
    /// and never influence the cascade.
    pub best_effort: Vec<Effect>,
    /// Whether the outcomes must be fed back into the engine.
    pub feedback: bool,
    /// This is the root's first batch since its train was recovered from a
    /// dead process: run the one-time worktree restart cleanup first (a
    /// mid-git-op crash leaves in-progress merges and dirt that no later
    /// operation may see — DESIGN §Worktree cleanup on restart).
    pub restart_cleanup: bool,
}

/// Executes one batch: the one-time worktree restart cleanup if the batch
/// asks for it, then observed effects in order (stop at first failure),
/// then best-effort effects (failures logged). Returns the observed
/// outcomes.
pub fn execute_batch(
    git: &WorktreeGitInterpreter,
    github: &GitHubExec,
    batch: &SagaBatch,
) -> Vec<EffectOutcome> {
    if batch.restart_cleanup
        && let Err(e) = crate::git::recovery::cleanup_worktree_on_restart(git.config(), batch.root)
    {
        // Cleanup self-heals hard cases (delete-and-recreate), so an error
        // here is exotic (filesystem trouble). The worktree cannot be
        // trusted: fail the first observed effect as transient so the
        // engine parks and recovery re-runs, rather than doing git work in
        // a dirty tree.
        warn!(root = %batch.root, error = %e, "worktree restart cleanup failed");
        return batch
            .effects
            .first()
            .map(|first| {
                vec![EffectOutcome {
                    effect: first.clone(),
                    result: Err(EffectError::Transient {
                        detail: format!("worktree restart cleanup failed: {e}"),
                    }),
                }]
            })
            .unwrap_or_default();
    }
    let mut outcomes = Vec::with_capacity(batch.effects.len());
    for effect in &batch.effects {
        let result = execute_one(git, github, effect);
        let failed = result.is_err();
        outcomes.push(EffectOutcome {
            effect: effect.clone(),
            result,
        });
        if failed {
            break;
        }
    }
    for effect in &batch.best_effort {
        if let Err(e) = execute_one(git, github, effect) {
            warn!(?effect, error = ?e, "best-effort effect failed (ignored)");
        }
    }
    outcomes
}

/// Executes a single effect through the right interpreter, classifying
/// failures.
fn execute_one(
    git: &WorktreeGitInterpreter,
    github: &GitHubExec,
    effect: &Effect,
) -> Result<EffectResponse, EffectError> {
    let result = match effect {
        Effect::Git(g) => git
            .interpret(g)
            .map(EffectResponse::Git)
            .map_err(|e| classify_git_error(&e)),
        Effect::GitHub(g) => github.execute(g.clone()).map(EffectResponse::GitHub),
    };
    github.after_effect();
    result
}
