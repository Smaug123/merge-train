//! Shared fixtures for git module tests.
//!
//! These helpers build real repositories on disk: a bare "remote" clone plus
//! whatever branches/refs a test needs. They are consolidated here because the
//! per-module copies had drifted (notably: whether the bare clone itself has
//! an `origin` remote registered).

use std::time::Duration;

use tempfile::TempDir;

use crate::types::Sha;

use super::{CommitIdentity, GitConfig, run_git_stdout, run_git_sync};

/// Test identity for merge commits (no signing).
pub(crate) fn test_identity() -> CommitIdentity {
    CommitIdentity {
        name: "Test".to_string(),
        email: "test@test.com".to_string(),
        signing_key: None,
    }
}

/// Create a minimal test repo: a bare clone with an initial commit on `main`.
///
/// Returns the temp dir guard, the `GitConfig` pointing at the repo layout,
/// and the SHA of the initial commit.
///
/// The bare clone has **no** `origin` remote of its own; worktrees created
/// from it can still operate locally. Use [`create_test_repo_with_origin`]
/// when commands run inside the clone or its worktrees need to push/fetch
/// via `origin`.
pub(crate) fn create_test_repo() -> (TempDir, GitConfig, Sha) {
    create_test_repo_impl(false)
}

/// Like [`create_test_repo`], but the bare clone also has an `origin` remote
/// pointing at itself, so worktrees created from it can push/fetch `origin`.
pub(crate) fn create_test_repo_with_origin() -> (TempDir, GitConfig, Sha) {
    create_test_repo_impl(true)
}

fn create_test_repo_impl(origin_on_clone: bool) -> (TempDir, GitConfig, Sha) {
    let temp_dir = TempDir::new().unwrap();
    let base_dir = temp_dir.path().to_path_buf();

    let config = GitConfig {
        base_dir: base_dir.clone(),
        owner: "test".to_string(),
        repo: "repo".to_string(),
        default_branch: "main".to_string(),
        worktree_max_age: Duration::from_secs(24 * 3600),
        commit_identity: test_identity(),
    };

    // Create the clone directory and initialize a bare repo
    let clone_dir = config.clone_dir();
    std::fs::create_dir_all(&clone_dir).unwrap();
    run_git_sync(&clone_dir, &["init", "--bare"]).unwrap();

    if origin_on_clone {
        // Add origin pointing to itself so worktrees can push/fetch
        run_git_sync(
            &clone_dir,
            &["remote", "add", "origin", clone_dir.to_str().unwrap()],
        )
        .unwrap();
    }

    // Create a temporary working repo to make an initial commit
    let work_dir = temp_dir.path().join("work");
    std::fs::create_dir_all(&work_dir).unwrap();
    run_git_sync(&work_dir, &["init"]).unwrap();
    run_git_sync(&work_dir, &["config", "user.email", "test@test.com"]).unwrap();
    run_git_sync(&work_dir, &["config", "user.name", "Test"]).unwrap();

    // Create initial commit
    std::fs::write(work_dir.join("README.md"), "# Test").unwrap();
    run_git_sync(&work_dir, &["add", "."]).unwrap();
    run_git_sync(&work_dir, &["commit", "-m", "Initial commit"]).unwrap();

    let initial_sha = run_git_stdout(&work_dir, &["rev-parse", "HEAD"]).unwrap();
    let initial_sha = Sha::parse(&initial_sha).unwrap();

    // Push to the bare repo
    run_git_sync(
        &work_dir,
        &["remote", "add", "origin", clone_dir.to_str().unwrap()],
    )
    .unwrap();
    run_git_sync(&work_dir, &["push", "-u", "origin", "HEAD:main"]).unwrap();

    // Update HEAD in the bare repo
    run_git_sync(&clone_dir, &["symbolic-ref", "HEAD", "refs/heads/main"]).unwrap();

    (temp_dir, config, initial_sha)
}

/// Create a branch with a single file committed on top of `base_branch`,
/// pushed to the bare clone. Returns the new branch head SHA.
///
/// Passing `branch == base_branch` (e.g. both `"main"`) advances that branch.
pub(crate) fn create_branch_with_file(
    config: &GitConfig,
    branch: &str,
    filename: &str,
    content: &str,
    base_branch: &str,
) -> Sha {
    let clone_dir = config.clone_dir();

    // Create a temporary worktree for making the branch
    let temp_work = clone_dir.parent().unwrap().join(format!("temp_{}", branch));
    std::fs::create_dir_all(&temp_work).unwrap();
    run_git_sync(
        &clone_dir,
        &[
            "worktree",
            "add",
            "--detach",
            temp_work.to_str().unwrap(),
            &format!("refs/heads/{}", base_branch),
        ],
    )
    .unwrap();

    run_git_sync(&temp_work, &["config", "user.email", "test@test.com"]).unwrap();
    run_git_sync(&temp_work, &["config", "user.name", "Test"]).unwrap();

    // Create the file and commit
    std::fs::write(temp_work.join(filename), content).unwrap();
    run_git_sync(&temp_work, &["add", filename]).unwrap();
    run_git_sync(&temp_work, &["commit", "-m", &format!("Add {}", filename)]).unwrap();

    let sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();

    // Push the new branch
    run_git_sync(
        &temp_work,
        &["push", "origin", &format!("HEAD:refs/heads/{}", branch)],
    )
    .unwrap();

    // Cleanup
    run_git_sync(
        &clone_dir,
        &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
    )
    .unwrap();

    Sha::parse(&sha).unwrap()
}

/// Create a PR ref in the bare repo (simulates GitHub's `refs/pull/<n>/head`).
///
/// Needed because `prepare_descendant` fetches via PR refs.
pub(crate) fn create_pr_ref(config: &GitConfig, pr_number: u64, sha: &Sha) {
    let clone_dir = config.clone_dir();
    run_git_sync(
        &clone_dir,
        &[
            "update-ref",
            &format!("refs/pull/{}/head", pr_number),
            sha.as_str(),
        ],
    )
    .unwrap();
}

/// Result of a squash merge: the squash commit SHA and the prior main HEAD.
pub(crate) struct SquashResult {
    pub(crate) squash_sha: Sha,
    pub(crate) prior_main_head: Sha,
}

/// Perform a squash merge of a branch into main.
/// Returns both the squash commit SHA and the main HEAD before the squash.
pub(crate) fn squash_merge_to_main(config: &GitConfig, branch_sha: &Sha) -> SquashResult {
    let clone_dir = config.clone_dir();

    // Capture main HEAD before the squash - this is the expected squash parent
    let prior_main_head = run_git_stdout(&clone_dir, &["rev-parse", "refs/heads/main"]).unwrap();
    let prior_main_head = Sha::parse(&prior_main_head).unwrap();

    let temp_work = clone_dir.parent().unwrap().join("temp_squash");
    std::fs::create_dir_all(&temp_work).unwrap();
    run_git_sync(
        &clone_dir,
        &[
            "worktree",
            "add",
            "--detach",
            temp_work.to_str().unwrap(),
            "refs/heads/main",
        ],
    )
    .unwrap();
    run_git_sync(&temp_work, &["config", "user.email", "test@test.com"]).unwrap();
    run_git_sync(&temp_work, &["config", "user.name", "Test"]).unwrap();

    // Squash merge
    run_git_sync(&temp_work, &["merge", "--squash", branch_sha.as_str()]).unwrap();
    run_git_sync(&temp_work, &["commit", "-m", "Squash merge"]).unwrap();

    let squash_sha = run_git_stdout(&temp_work, &["rev-parse", "HEAD"]).unwrap();

    run_git_sync(&temp_work, &["push", "origin", "HEAD:refs/heads/main"]).unwrap();
    run_git_sync(
        &clone_dir,
        &["worktree", "remove", "--force", temp_work.to_str().unwrap()],
    )
    .unwrap();

    SquashResult {
        squash_sha: Sha::parse(&squash_sha).unwrap(),
        prior_main_head,
    }
}
