//! Manual integration test for the GitHub API interpreter.
//!
//! This example exercises the GitHub API client against a real repository to verify
//! that all effect interpretations work correctly.
//!
//! # Usage
//!
//! 1. Set the `GITHUB_TOKEN` environment variable to a personal access token with
//!    `repo` scope (or a fine-grained token with appropriate permissions).
//!
//! 2. Set `TEST_REPO` to the repository to test against (e.g., `owner/repo`).
//!
//! 3. Optionally set `TEST_PR` to a specific PR number to test PR operations.
//!
//! 4. Run: `cargo run --example github_integration_test`
//!
//! # Required Token Permissions
//!
//! - `repo` scope for classic tokens
//! - For fine-grained tokens:
//!   - Contents: Read and Write (for push operations)
//!   - Pull requests: Read and Write
//!   - Issues: Read and Write (comments are on issues API)
//!   - Metadata: Read
//!
//! # Note
//!
//! This test performs real API calls. Some operations (like squash-merge) are
//! intentionally skipped unless explicitly enabled to avoid unintended changes.

use std::env;

use merge_train::effects::{GitHubEffect, GitHubInterpreter, GitHubResponse};
use merge_train::github::OctocrabClient;
use merge_train::types::{PrNumber, RepoId};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing for visibility into what's happening
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,merge_train=debug".into()),
        )
        .init();

    // Get configuration from environment
    let token = env::var("GITHUB_TOKEN")
        .map_err(|_| anyhow::anyhow!("GITHUB_TOKEN environment variable not set"))?;

    let test_repo = env::var("TEST_REPO").map_err(|_| {
        anyhow::anyhow!("TEST_REPO environment variable not set (e.g., owner/repo)")
    })?;

    // Strip GitHub URL prefix if present (accept both "owner/repo" and "https://github.com/owner/repo")
    let test_repo = test_repo
        .strip_prefix("https://github.com/")
        .or_else(|| test_repo.strip_prefix("http://github.com/"))
        .or_else(|| test_repo.strip_prefix("github.com/"))
        .unwrap_or(&test_repo);

    let (owner, repo) = test_repo
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("TEST_REPO must be in owner/repo format"))?;

    let test_pr: Option<PrNumber> = env::var("TEST_PR")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(PrNumber);

    // Create the client
    let repo_id = RepoId::new(owner, repo);
    let client = OctocrabClient::from_token(token, repo_id.clone())?;

    println!("\n=== GitHub Integration Test ===\n");
    println!("Repository: {}", repo_id);
    if let Some(pr) = test_pr {
        println!("Test PR: {}", pr);
    } else {
        println!("Test PR: (none - some tests will be skipped)");
    }
    println!();

    // Run tests
    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;

    // ─── Repository Settings ─────────────────────────────────────────────────

    println!("--- Repository Settings ---");

    match test_get_repo_settings(&client).await {
        Ok(()) => {
            println!("  [PASS] GetRepoSettings");
            passed += 1;
        }
        Err(e) => {
            println!("  [FAIL] GetRepoSettings: {}", e);
            failed += 1;
        }
    }

    match test_get_branch_protection(&client).await {
        Ok(()) => {
            println!("  [PASS] GetBranchProtection");
            passed += 1;
        }
        Err(e) => {
            println!("  [FAIL] GetBranchProtection: {}", e);
            failed += 1;
        }
    }

    match test_get_rulesets(&client).await {
        Ok(()) => {
            println!("  [PASS] GetRulesets");
            passed += 1;
        }
        Err(e) => {
            println!("  [FAIL] GetRulesets: {}", e);
            failed += 1;
        }
    }

    // ─── PR Listing ──────────────────────────────────────────────────────────

    println!("\n--- PR Listing ---");

    match test_list_open_prs(&client).await {
        Ok(count) => {
            println!("  [PASS] ListOpenPrs ({} PRs found)", count);
            passed += 1;
        }
        Err(e) => {
            println!("  [FAIL] ListOpenPrs: {}", e);
            failed += 1;
        }
    }

    match test_list_recently_merged_prs(&client).await {
        Ok(count) => {
            println!("  [PASS] ListRecentlyMergedPrs ({} PRs found)", count);
            passed += 1;
        }
        Err(e) => {
            println!("  [FAIL] ListRecentlyMergedPrs: {}", e);
            failed += 1;
        }
    }

    // ─── Single PR Operations ────────────────────────────────────────────────

    println!("\n--- Single PR Operations ---");

    if let Some(pr) = test_pr {
        match test_get_pr(&client, pr).await {
            Ok(()) => {
                println!("  [PASS] GetPr");
                passed += 1;
            }
            Err(e) => {
                println!("  [FAIL] GetPr: {}", e);
                failed += 1;
            }
        }

        match test_get_merge_state(&client, pr).await {
            Ok(status) => {
                println!("  [PASS] GetMergeState (status: {:?})", status);
                passed += 1;
            }
            Err(e) => {
                println!("  [FAIL] GetMergeState: {}", e);
                failed += 1;
            }
        }

        match test_list_comments(&client, pr).await {
            Ok(count) => {
                println!("  [PASS] ListComments ({} comments found)", count);
                passed += 1;
            }
            Err(e) => {
                println!("  [FAIL] ListComments: {}", e);
                failed += 1;
            }
        }
    } else {
        println!("  [SKIP] GetPr (no TEST_PR set)");
        println!("  [SKIP] GetMergeState (no TEST_PR set)");
        println!("  [SKIP] ListComments (no TEST_PR set)");
        skipped += 3;
    }

    // ─── Mutating Operations ─────────────────────────────────────────────────

    println!("\n--- Mutating Operations ---");

    // Mutating operations require explicit opt-in
    let enable_mutations = env::var("ENABLE_MUTATIONS").is_ok();

    if enable_mutations && test_pr.is_some() {
        let pr = test_pr.unwrap();

        // Post a comment, then update it, then add a reaction
        match test_comment_operations(&client, pr).await {
            Ok(comment_id) => {
                println!("  [PASS] PostComment (id: {})", comment_id);
                println!("  [PASS] UpdateComment");
                println!("  [PASS] AddReaction");
                passed += 3;
            }
            Err(e) => {
                println!("  [FAIL] Comment operations: {}", e);
                failed += 3;
            }
        }

        // RetargetPr and SquashMerge are too dangerous for automated testing.
        // See test_retarget_pr() and test_squash_merge() at the end of this file
        // for manual testing instructions.
        println!("  [SKIP] RetargetPr (see test_retarget_pr for manual testing)");
        println!("  [SKIP] SquashMerge (see test_squash_merge for manual testing)");
        skipped += 2;

        // Uncomment these lines to manually test dangerous operations:
        // let pr_data = client.interpret(GitHubEffect::GetPr { pr }).await?;
        // let head_sha = match pr_data {
        //     GitHubResponse::Pr(data) => data.head_sha.as_str().to_string(),
        //     _ => anyhow::bail!("Failed to get PR data"),
        // };
        // test_retarget_pr(&client, pr, "some-other-branch").await?;
        // test_squash_merge(&client, pr, &head_sha).await?;
    } else if test_pr.is_some() {
        println!("  [SKIP] PostComment (ENABLE_MUTATIONS not set)");
        println!("  [SKIP] UpdateComment (ENABLE_MUTATIONS not set)");
        println!("  [SKIP] AddReaction (ENABLE_MUTATIONS not set)");
        println!("  [SKIP] RetargetPr (ENABLE_MUTATIONS not set)");
        println!("  [SKIP] SquashMerge (ENABLE_MUTATIONS not set)");
        skipped += 5;
    } else {
        println!("  [SKIP] All mutating operations (no TEST_PR set)");
        skipped += 5;
    }

    // ─── Summary ─────────────────────────────────────────────────────────────

    println!("\n=== Summary ===\n");
    println!("Passed:  {}", passed);
    println!("Failed:  {}", failed);
    println!("Skipped: {}", skipped);

    if failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

// ─── Test Functions ────────────────────────────────────────────────────────────

async fn test_get_repo_settings(client: &OctocrabClient) -> anyhow::Result<()> {
    let response = client.interpret(GitHubEffect::GetRepoSettings).await?;
    match response {
        GitHubResponse::RepoSettings(data) => {
            tracing::debug!(
                default_branch = %data.default_branch,
                allow_squash = data.allow_squash_merge,
                allow_merge = data.allow_merge_commit,
                allow_rebase = data.allow_rebase_merge,
                "Got repo settings"
            );
            Ok(())
        }
        other => anyhow::bail!("Unexpected response: {:?}", other),
    }
}

async fn test_get_branch_protection(client: &OctocrabClient) -> anyhow::Result<()> {
    // Get the default branch first
    let settings = client.interpret(GitHubEffect::GetRepoSettings).await?;
    let default_branch = match settings {
        GitHubResponse::RepoSettings(data) => data.default_branch,
        _ => "main".to_string(),
    };

    let response = client
        .interpret(GitHubEffect::GetBranchProtection {
            branch: default_branch.clone(),
        })
        .await?;

    match response {
        GitHubResponse::BranchProtection(data) => {
            tracing::debug!(
                branch = %default_branch,
                dismiss_stale_reviews = data.dismiss_stale_reviews,
                required_checks = ?data.required_status_checks,
                "Got branch protection"
            );
            Ok(())
        }
        GitHubResponse::BranchProtectionUnknown => {
            // Per DESIGN.md, this is a valid "warn and proceed" outcome when
            // permissions are missing or the branch has no protection rules.
            tracing::warn!(
                branch = %default_branch,
                "Branch protection unknown (missing permissions or no rules)"
            );
            Ok(())
        }
        other => anyhow::bail!("Unexpected response: {:?}", other),
    }
}

async fn test_get_rulesets(client: &OctocrabClient) -> anyhow::Result<()> {
    let response = client.interpret(GitHubEffect::GetRulesets).await?;
    match response {
        GitHubResponse::Rulesets(data) => {
            tracing::debug!(count = data.len(), "Got rulesets");
            for ruleset in &data {
                tracing::debug!(
                    name = %ruleset.name,
                    dismiss_stale_on_push = ruleset.dismiss_stale_reviews_on_push,
                    "Ruleset"
                );
            }
            Ok(())
        }
        GitHubResponse::RulesetsUnknown => {
            // Per DESIGN.md, this is a valid "warn and proceed" outcome when
            // permissions are missing or rulesets are not available.
            tracing::warn!("Rulesets unknown (missing permissions or not available)");
            Ok(())
        }
        other => anyhow::bail!("Unexpected response: {:?}", other),
    }
}

async fn test_list_open_prs(client: &OctocrabClient) -> anyhow::Result<usize> {
    let response = client.interpret(GitHubEffect::ListOpenPrs).await?;
    match response {
        GitHubResponse::PrList(prs) => {
            for pr in &prs {
                tracing::debug!(
                    number = pr.number.0,
                    head_ref = %pr.head_ref,
                    base_ref = %pr.base_ref,
                    is_draft = pr.is_draft,
                    "Open PR"
                );
            }
            Ok(prs.len())
        }
        other => anyhow::bail!("Unexpected response: {:?}", other),
    }
}

async fn test_list_recently_merged_prs(client: &OctocrabClient) -> anyhow::Result<usize> {
    let response = client
        .interpret(GitHubEffect::ListRecentlyMergedPrs { since_days: 7 })
        .await?;
    match response {
        GitHubResponse::RecentlyMergedPrList {
            prs,
            may_be_incomplete,
        } => {
            if may_be_incomplete {
                tracing::warn!("Recently merged PR list may be incomplete (pagination limit hit)");
            }
            for pr in &prs {
                tracing::debug!(
                    number = pr.number.0,
                    head_ref = %pr.head_ref,
                    base_ref = %pr.base_ref,
                    "Recently merged PR"
                );
            }
            Ok(prs.len())
        }
        other => anyhow::bail!("Unexpected response: {:?}", other),
    }
}

async fn test_get_pr(client: &OctocrabClient, pr: PrNumber) -> anyhow::Result<()> {
    let response = client.interpret(GitHubEffect::GetPr { pr }).await?;
    match response {
        GitHubResponse::Pr(data) => {
            tracing::debug!(
                number = data.number.0,
                head_sha = %data.head_sha,
                head_ref = %data.head_ref,
                base_ref = %data.base_ref,
                state = ?data.state,
                is_draft = data.is_draft,
                "Got PR"
            );
            Ok(())
        }
        other => anyhow::bail!("Unexpected response: {:?}", other),
    }
}

async fn test_get_merge_state(
    client: &OctocrabClient,
    pr: PrNumber,
) -> anyhow::Result<merge_train::types::MergeStateStatus> {
    let response = client.interpret(GitHubEffect::GetMergeState { pr }).await?;
    match response {
        GitHubResponse::MergeState(status) => {
            tracing::debug!(pr = pr.0, ?status, "Got merge state");
            Ok(status)
        }
        other => anyhow::bail!("Unexpected response: {:?}", other),
    }
}

async fn test_list_comments(client: &OctocrabClient, pr: PrNumber) -> anyhow::Result<usize> {
    let response = client.interpret(GitHubEffect::ListComments { pr }).await?;
    match response {
        GitHubResponse::Comments(comments) => {
            for comment in &comments {
                tracing::debug!(
                    id = comment.id.0,
                    author_id = comment.author_id,
                    body_len = comment.body.len(),
                    "Comment"
                );
            }
            Ok(comments.len())
        }
        other => anyhow::bail!("Unexpected response: {:?}", other),
    }
}

async fn test_comment_operations(client: &OctocrabClient, pr: PrNumber) -> anyhow::Result<u64> {
    use merge_train::effects::Reaction;

    // Post a comment
    let response = client
        .interpret(GitHubEffect::PostComment {
            pr,
            body: "[merge-train integration test] This comment will be updated and then deleted."
                .to_string(),
        })
        .await?;

    let comment_id = match response {
        GitHubResponse::CommentPosted { id } => id,
        other => anyhow::bail!("Unexpected response from PostComment: {:?}", other),
    };

    tracing::debug!(id = comment_id.0, "Posted comment");

    // Update the comment
    let response = client
        .interpret(GitHubEffect::UpdateComment {
            comment_id,
            body: "[merge-train integration test] Comment updated! This comment should be deleted."
                .to_string(),
        })
        .await?;

    match response {
        GitHubResponse::CommentUpdated => {}
        other => anyhow::bail!("Unexpected response from UpdateComment: {:?}", other),
    }

    tracing::debug!(id = comment_id.0, "Updated comment");

    // Add a reaction
    let response = client
        .interpret(GitHubEffect::AddReaction {
            comment_id,
            reaction: Reaction::Rocket,
        })
        .await?;

    match response {
        GitHubResponse::ReactionAdded => {}
        other => anyhow::bail!("Unexpected response from AddReaction: {:?}", other),
    }

    tracing::debug!(id = comment_id.0, "Added reaction");

    // Note: We intentionally don't delete the comment so the test is visible
    // in the PR history. Users should manually clean up after testing.

    Ok(comment_id.0)
}

// ─── Dangerous Operations (uncomment for manual testing) ───────────────────────
//
// These functions are commented out because they mutate PRs in ways that may be
// difficult to undo. To manually test:
//
// 1. Create a test PR on a branch you control
// 2. Set TEST_PR=<number> and ENABLE_MUTATIONS=1
// 3. Uncomment the test_retarget_pr and/or test_squash_merge calls in main()
// 4. Run: cargo run --example github_integration_test
//
// For SquashMerge, the expected_sha parameter is critical: it ensures we don't
// merge stale content. If the HEAD SHA doesn't match, GitHub returns HTTP 409.
// This is the SHA-guarded merge behavior described in DESIGN.md.

#[allow(dead_code)]
async fn test_retarget_pr(
    client: &OctocrabClient,
    pr: PrNumber,
    new_base: &str,
) -> anyhow::Result<()> {
    let response = client
        .interpret(GitHubEffect::RetargetPr {
            pr,
            new_base: new_base.to_string(),
        })
        .await?;

    match response {
        GitHubResponse::Retargeted => {
            tracing::info!(pr = pr.0, new_base = %new_base, "Retargeted PR");
            Ok(())
        }
        other => anyhow::bail!("Unexpected response from RetargetPr: {:?}", other),
    }
}

#[allow(dead_code)]
async fn test_squash_merge(
    client: &OctocrabClient,
    pr: PrNumber,
    expected_sha: &str,
) -> anyhow::Result<()> {
    use merge_train::types::Sha;

    let sha = Sha::parse(expected_sha).map_err(|e| anyhow::anyhow!("Invalid SHA: {}", e))?;

    let response = client
        .interpret(GitHubEffect::SquashMerge {
            pr,
            expected_sha: sha,
        })
        .await?;

    match response {
        GitHubResponse::Merged { sha } => {
            tracing::info!(
                pr = pr.0,
                merge_sha = %sha.as_str(),
                "Squash-merged PR"
            );
            Ok(())
        }
        other => anyhow::bail!("Unexpected response from SquashMerge: {:?}", other),
    }
}
