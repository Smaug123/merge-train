//! Local git operations for cascade (prepare, reconcile, catch-up).
//!
//! This module implements the git operations needed for the merge train cascade:
//! - Worktree management (per-stack isolation)
//! - Merge operations (prepare, reconcile, catch-up)
//! - Push operations (detached HEAD to remote branch)
//! - Recovery (cleanup corrupted worktrees, prune stale worktrees)
//!
//! All worktree operations use **detached HEAD mode** to avoid git's restriction
//! that a branch can only be checked out in one worktree at a time. Pushes use
//! `HEAD:refs/heads/<branch>` refspecs.

pub mod merge;
pub mod push;
pub mod recovery;
pub mod worktree;

#[cfg(test)]
mod property_tests;
#[cfg(test)]
pub(crate) mod test_support;

// Re-export commonly used types from submodules
pub use push::{PushCompletion, PushIntent, PushResult, is_push_completed};

use std::path::{Path, PathBuf};
use std::process::Output;

use thiserror::Error;

use crate::types::{PrNumber, Sha};

/// Errors from git operations.
#[derive(Debug, Error)]
pub enum GitError {
    /// Git command failed.
    #[error("git command failed: {command}\nstderr: {stderr}")]
    CommandFailed { command: String, stderr: String },

    /// Worktree operation failed.
    #[error("worktree error: {details}")]
    WorktreeError { details: String },

    /// Invalid SHA format.
    #[error("invalid SHA: {0}")]
    InvalidSha(String),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Failed to fetch a ref.
    #[error("failed to fetch ref {refspec}: {details}")]
    FetchFailed { refspec: String, details: String },

    /// Checkout target is not in a form that is safe to pass to
    /// `git checkout --detach` (see [`checkout_detached`]).
    #[error("checkout target must be refs/..., origin/..., or a SHA: {target}")]
    InvalidCheckoutTarget { target: String },

    /// The supplied commit is not a valid squash-merge of the expected parent.
    #[error("invalid squash commit {squash_sha}: {details}")]
    InvalidSquashCommit { squash_sha: Sha, details: String },

    /// Reconciliation refused: the descendant does not contain the
    /// predecessor's pre-squash head, so the ours-merge would record the
    /// squash as merged while the tree lacks its content.
    #[error(
        "preparation missing or stale for {descendant_branch}: predecessor pre-squash head \
         {predecessor_head} is not an ancestor of descendant head {descendant_head}"
    )]
    PreparationNotCompleted {
        descendant_branch: String,
        predecessor_head: Sha,
        descendant_head: Sha,
    },

    /// Reconciliation refused: the head that was actually squash-merged (per
    /// the predecessor's PR ref, which is frozen at merge time) differs from
    /// the head preparation merged into the descendant. The predecessor was
    /// force-pushed between preparation and squash; the ours-merge would
    /// record the squash as merged while the descendant lacks its content.
    #[error(
        "predecessor PR #{predecessor_pr} head changed after preparation: \
         prepared {prepared}, but {squashed} was squash-merged"
    )]
    PredecessorHeadChanged {
        predecessor_pr: u64,
        prepared: Sha,
        squashed: Sha,
    },
}

/// Result type for git operations.
pub type GitResult<T> = Result<T, GitError>;

/// Result of a merge operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeResult {
    /// Merge completed successfully, producing a new commit.
    Success {
        /// The SHA of the merge commit.
        commit_sha: Sha,
    },

    /// Merge resulted in a conflict.
    Conflict {
        /// Files with conflicts.
        conflicting_files: Vec<String>,
    },

    /// Merge was a no-op (already up-to-date).
    AlreadyUpToDate,
}

/// Identity used for creating commits.
///
/// This is passed via `-c` flags to git commands, ensuring commits can be
/// created even when global/system git config is disabled. This avoids
/// relying on per-repo `.git/config` settings.
#[derive(Debug, Clone)]
pub struct CommitIdentity {
    /// The committer/author name (git `user.name`).
    pub name: String,

    /// The committer/author email (git `user.email`).
    pub email: String,

    /// GPG signing key ID. If present, commits will be signed with `-S`
    /// and this key will be used via `-c user.signingkey=<key>`.
    pub signing_key: Option<String>,
}

/// Configuration for git operations.
#[derive(Debug, Clone)]
pub struct GitConfig {
    /// Base directory for all repos (e.g., `/var/lib/merge-train/repos`).
    pub base_dir: PathBuf,

    /// Owner of the repository.
    pub owner: String,

    /// Name of the repository.
    pub repo: String,

    /// The default branch name (e.g., "main").
    pub default_branch: String,

    /// Maximum age for stale worktree cleanup (default: 24 hours).
    pub worktree_max_age: std::time::Duration,

    /// Identity for creating commits (merge commits during cascade).
    pub commit_identity: CommitIdentity,
}

impl GitConfig {
    /// Returns the path to the repo directory (owner-repo/).
    pub fn repo_dir(&self) -> PathBuf {
        self.base_dir.join(format!("{}-{}", self.owner, self.repo))
    }

    /// Returns the path to the shared bare clone.
    pub fn clone_dir(&self) -> PathBuf {
        self.repo_dir().join("clone")
    }

    /// Returns the path to the worktrees directory.
    pub fn worktrees_dir(&self) -> PathBuf {
        self.repo_dir().join("worktrees")
    }

    /// Returns the path to a specific stack's worktree.
    pub fn worktree_path(&self, root_pr: PrNumber) -> PathBuf {
        self.worktrees_dir().join(format!("stack-{}", root_pr.0))
    }
}

/// Parse a worktree directory name like "stack-123" to extract the PR number.
pub fn parse_stack_dir_name(path: &Path) -> Option<PrNumber> {
    let name = path.file_name()?.to_str()?;
    let num_str = name.strip_prefix("stack-")?;
    let num: u64 = num_str.parse().ok()?;
    Some(PrNumber(num))
}

/// Create a git Command with a scrubbed environment, pinned locale, and no
/// system/user config.
///
/// The environment is cleared; only `PATH` is propagated (required to locate
/// git and the subprocesses it spawns). Inherited `GIT_*` variables would
/// otherwise redirect operations away from `workdir` (`GIT_DIR`,
/// `GIT_WORK_TREE`, `GIT_INDEX_FILE`), re-enable the config injection that
/// `GIT_CONFIG_NOSYSTEM`/`GIT_CONFIG_GLOBAL` block (`GIT_CONFIG_PARAMETERS`,
/// `GIT_CONFIG_COUNT`), or override the `-c user.name`/`-c user.email`
/// identity (`GIT_AUTHOR_*`, `GIT_COMMITTER_*`). `HOME` is not needed:
/// `GIT_CONFIG_GLOBAL=/dev/null` replaces both `~/.gitconfig` and the XDG
/// config path.
///
/// `LC_ALL=C` pins git's output language. Error classification that
/// string-matches git output (e.g. fetch's "couldn't find remote ref")
/// depends on this.
///
/// Terminal prompts are disabled so commands fail instead of hanging on
/// credential prompts.
///
/// Security: local hooks are disabled via `-c core.hooksPath=/dev/null` to
/// prevent untrusted repos from executing code during merge/commit operations.
pub(crate) fn git_command(workdir: &Path) -> std::process::Command {
    use std::process::Command;

    let mut cmd = Command::new("git");
    cmd.current_dir(workdir);

    cmd.env_clear();
    // Allowlist, not denylist: anything not named here is scrubbed.
    // - PATH: to find git (and its transport helpers).
    // - HOME: ssh needs it for ~/.ssh/config, keys, and known_hosts. Git
    //   config injection via ~/.gitconfig is still blocked: GIT_CONFIG_GLOBAL
    //   and GIT_CONFIG_NOSYSTEM below are pinned regardless of HOME.
    // - SSH_AUTH_SOCK: agent-based SSH auth for fetch/push/ls-remote.
    // Deliberately NOT preserved: GIT_SSH/GIT_SSH_COMMAND (arbitrary command
    // execution; deployments needing a custom SSH wrapper must configure it
    // explicitly when such a knob exists) and credential-helper overrides.
    for var in ["PATH", "HOME", "SSH_AUTH_SOCK"] {
        if let Some(value) = std::env::var_os(var) {
            cmd.env(var, value);
        }
    }

    cmd.env("LC_ALL", "C");

    cmd.env("GIT_CONFIG_NOSYSTEM", "1");
    cmd.env("GIT_CONFIG_GLOBAL", "/dev/null");
    cmd.env("GIT_CONFIG_COUNT", "0");

    cmd.env("GIT_TERMINAL_PROMPT", "0");

    cmd.args(["-c", "core.hooksPath=/dev/null"]);

    cmd
}

/// Create a git Command configured for commit operations.
///
/// This extends [`git_command`] with identity configuration passed via `-c` flags.
/// All config is per-command (no persistent `.git/config` changes required).
///
/// The returned command has these `-c` flags prepended:
/// - `-c user.name=<name>`
/// - `-c user.email=<email>`
/// - If `identity.signing_key` is Some: `-c user.signingkey=<key>`
///
/// Callers should use `-S` to actually sign commits when `identity.signing_key` is set.
pub(crate) fn git_commit_command(
    workdir: &Path,
    identity: &CommitIdentity,
) -> std::process::Command {
    let mut cmd = git_command(workdir);

    // Identity config (required for commits)
    cmd.arg("-c");
    cmd.arg(format!("user.name={}", identity.name));
    cmd.arg("-c");
    cmd.arg(format!("user.email={}", identity.email));

    // Signing key config (the actual signing is done via -S flag by caller)
    if let Some(ref key) = identity.signing_key {
        cmd.arg("-c");
        cmd.arg(format!("user.signingkey={}", key));
    }

    cmd
}

/// Run a git command in the given working directory.
///
/// Returns the command output on success, or a GitError on failure.
/// Uses clean git environment (no system/user config) for consistent behavior.
pub fn run_git_sync(workdir: &Path, args: &[&str]) -> GitResult<Output> {
    let output = git_command(workdir).args(args).output()?;

    if output.status.success() {
        Ok(output)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let command = format!("git {}", args.join(" "));
        Err(GitError::CommandFailed { command, stderr })
    }
}

/// Run a git command and return stdout as a string.
pub fn run_git_stdout(workdir: &Path, args: &[&str]) -> GitResult<String> {
    let output = run_git_sync(workdir, args)?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Check if one commit is an ancestor of another.
pub fn is_ancestor(workdir: &Path, potential_ancestor: &Sha, descendant: &Sha) -> GitResult<bool> {
    let output = git_command(workdir)
        .args([
            "merge-base",
            "--is-ancestor",
            potential_ancestor.as_str(),
            descendant.as_str(),
        ])
        .output()?;

    // Exit 0 = is ancestor, exit 1 = not ancestor, other = error
    match output.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            Err(GitError::CommandFailed {
                command: format!(
                    "git merge-base --is-ancestor {} {}",
                    potential_ancestor, descendant
                ),
                stderr,
            })
        }
    }
}

/// Get the SHA of a revision.
pub fn rev_parse(workdir: &Path, rev: &str) -> GitResult<Sha> {
    let sha_str = run_git_stdout(workdir, &["rev-parse", rev])?;
    Sha::parse(&sha_str).map_err(|_| GitError::InvalidSha(sha_str))
}

/// Get the tree SHA for a commit (useful for comparing content across commits).
pub fn get_tree_sha(workdir: &Path, commit: &str) -> GitResult<Sha> {
    let tree_str = run_git_stdout(workdir, &["rev-parse", &format!("{}^{{tree}}", commit)])?;
    Sha::parse(&tree_str).map_err(|_| GitError::InvalidSha(tree_str))
}

/// Get the parent SHA(s) for a commit.
pub fn get_parents(workdir: &Path, commit: &str) -> GitResult<Vec<Sha>> {
    let output = run_git_stdout(workdir, &["rev-parse", &format!("{}^@", commit)])?;
    if output.is_empty() {
        return Ok(vec![]);
    }
    output
        .lines()
        .map(|line| Sha::parse(line).map_err(|_| GitError::InvalidSha(line.to_string())))
        .collect()
}

/// Fetch refs from origin.
///
/// Uses `--` to separate options from refspecs, preventing branch names
/// starting with `-` from being interpreted as flags.
pub fn fetch(workdir: &Path, refspecs: &[&str]) -> GitResult<()> {
    let mut args = vec!["fetch", "origin", "--"];
    args.extend(refspecs);
    run_git_sync(workdir, &args)?;
    Ok(())
}

/// Checkout a target in detached HEAD mode.
///
/// `git checkout --detach <target>` parses a leading `-` in the target as a
/// flag, and a `--` separator would turn the target into a pathspec rather
/// than a commit, so flag injection cannot be prevented positionally. The
/// target must therefore be in a form that can never begin with `-`: a
/// `refs/...` ref, an `origin/`-prefixed remote-tracking name, or a SHA.
pub fn checkout_detached(workdir: &Path, target: &str) -> GitResult<()> {
    let fully_qualified =
        target.starts_with("refs/") || target.starts_with("origin/") || Sha::parse(target).is_ok();
    if !fully_qualified {
        return Err(GitError::InvalidCheckoutTarget {
            target: target.to_string(),
        });
    }
    run_git_sync(workdir, &["checkout", "--detach", target])?;
    Ok(())
}

/// Worktree paths are passed to git as command-line arguments, which requires
/// valid UTF-8.
pub(crate) fn worktree_path_str(path: &Path) -> GitResult<&str> {
    path.to_str().ok_or_else(|| GitError::WorktreeError {
        details: format!("worktree path is not valid UTF-8: {}", path.display()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_stack_dir_name_valid() {
        let path = PathBuf::from("/some/path/worktrees/stack-123");
        assert_eq!(parse_stack_dir_name(&path), Some(PrNumber(123)));
    }

    #[test]
    fn parse_stack_dir_name_different_numbers() {
        let path = PathBuf::from("stack-1");
        assert_eq!(parse_stack_dir_name(&path), Some(PrNumber(1)));

        let path = PathBuf::from("stack-999999");
        assert_eq!(parse_stack_dir_name(&path), Some(PrNumber(999999)));
    }

    #[test]
    fn parse_stack_dir_name_invalid_prefix() {
        let path = PathBuf::from("worktree-123");
        assert_eq!(parse_stack_dir_name(&path), None);

        let path = PathBuf::from("stack123");
        assert_eq!(parse_stack_dir_name(&path), None);
    }

    #[test]
    fn parse_stack_dir_name_non_numeric() {
        let path = PathBuf::from("stack-abc");
        assert_eq!(parse_stack_dir_name(&path), None);

        let path = PathBuf::from("stack-");
        assert_eq!(parse_stack_dir_name(&path), None);
    }

    #[test]
    fn git_config_paths() {
        let config = GitConfig {
            base_dir: PathBuf::from("/var/lib/merge-train/repos"),
            owner: "owner".to_string(),
            repo: "repo".to_string(),
            default_branch: "main".to_string(),
            worktree_max_age: std::time::Duration::from_secs(24 * 3600),
            commit_identity: CommitIdentity {
                name: "Test".to_string(),
                email: "test@test.com".to_string(),
                signing_key: None,
            },
        };

        assert_eq!(
            config.repo_dir(),
            PathBuf::from("/var/lib/merge-train/repos/owner-repo")
        );
        assert_eq!(
            config.clone_dir(),
            PathBuf::from("/var/lib/merge-train/repos/owner-repo/clone")
        );
        assert_eq!(
            config.worktrees_dir(),
            PathBuf::from("/var/lib/merge-train/repos/owner-repo/worktrees")
        );
        assert_eq!(
            config.worktree_path(PrNumber(123)),
            PathBuf::from("/var/lib/merge-train/repos/owner-repo/worktrees/stack-123")
        );
    }

    #[test]
    fn checkout_detached_rejects_unqualified_targets() {
        let dir = std::env::temp_dir();
        for target in ["-evil", "--", "main", "HEAD", "feature/x", ""] {
            let err = checkout_detached(&dir, target).unwrap_err();
            assert!(
                matches!(err, GitError::InvalidCheckoutTarget { .. }),
                "target {:?} must be rejected without running git, got {:?}",
                target,
                err
            );
        }
    }
}
