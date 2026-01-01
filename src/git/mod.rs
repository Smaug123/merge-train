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

    /// Merge conflict occurred.
    #[error("merge conflict: {details}")]
    MergeConflict { details: String },

    /// Push was rejected (non-fast-forward).
    #[error("push rejected: {details}")]
    PushRejected { details: String },

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

    /// Ref not found (e.g., PR ref was garbage-collected).
    #[error("ref not found: {refspec}")]
    RefNotFound { refspec: String },
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

impl MergeResult {
    /// Returns true if the merge was successful (created a merge commit).
    pub fn is_success(&self) -> bool {
        matches!(self, MergeResult::Success { .. })
    }

    /// Returns true if the merge completed without conflict.
    /// This includes both `Success` (created a merge commit) and `AlreadyUpToDate` (no-op).
    pub fn is_ok(&self) -> bool {
        matches!(
            self,
            MergeResult::Success { .. } | MergeResult::AlreadyUpToDate
        )
    }

    /// Returns true if the merge resulted in a conflict.
    pub fn is_conflict(&self) -> bool {
        matches!(self, MergeResult::Conflict { .. })
    }
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

/// Create a git Command with clean environment (no system/user config).
///
/// This ensures consistent behavior across different machines by ignoring
/// system and user git configuration (e.g., rerere, hooks, aliases).
pub(crate) fn git_command(workdir: &Path) -> std::process::Command {
    use std::process::Command;

    let mut cmd = Command::new("git");
    cmd.current_dir(workdir);

    // Disable system and user config for reproducible behavior
    cmd.env("GIT_CONFIG_NOSYSTEM", "1");
    cmd.env("GIT_CONFIG_GLOBAL", "/dev/null");

    // Disable terminal prompts
    cmd.env("GIT_TERMINAL_PROMPT", "0");

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
pub fn fetch(workdir: &Path, refspecs: &[&str]) -> GitResult<()> {
    let mut args = vec!["fetch", "origin"];
    args.extend(refspecs);
    run_git_sync(workdir, &args)?;
    Ok(())
}

/// Checkout a target in detached HEAD mode.
pub fn checkout_detached(workdir: &Path, target: &str) -> GitResult<()> {
    run_git_sync(workdir, &["checkout", "--detach", target])?;
    Ok(())
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
    fn merge_result_predicates() {
        let success = MergeResult::Success {
            commit_sha: Sha::parse("a".repeat(40)).unwrap(),
        };
        assert!(success.is_success());
        assert!(success.is_ok());
        assert!(!success.is_conflict());

        let conflict = MergeResult::Conflict {
            conflicting_files: vec!["foo.rs".to_string()],
        };
        assert!(!conflict.is_success());
        assert!(!conflict.is_ok());
        assert!(conflict.is_conflict());

        let up_to_date = MergeResult::AlreadyUpToDate;
        assert!(!up_to_date.is_success());
        assert!(up_to_date.is_ok());
        assert!(!up_to_date.is_conflict());
    }
}
