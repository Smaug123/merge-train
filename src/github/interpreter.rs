//! GitHub effect interpreter using octocrab.
//!
//! This module implements the `GitHubInterpreter` trait, executing GitHub effects
//! against the real GitHub API via octocrab.
//!
//! Key implementation details:
//! - Uses GraphQL for `mergeStateStatus` queries (REST doesn't expose this)
//! - SHA guard on squash-merge via the `sha` parameter
//! - Retry logic with exponential backoff for transient errors
//! - Proper categorization of errors (transient vs permanent)

use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};

use crate::effects::{
    BranchProtectionData, CommentData, GitHubEffect, GitHubInterpreter, GitHubResponse, PrData,
    Reaction, RepoSettingsData, RulesetData,
};
use crate::types::{CommentId, MergeStateStatus, PrNumber, PrState, Sha};

use super::client::OctocrabClient;
use super::error::GitHubApiError;
use super::retry::{RetryConfig, RetryPolicy, retry_with_backoff};

// ─── GraphQL Types ────────────────────────────────────────────────────────────

/// GraphQL query for PR merge state status.
const MERGE_STATE_QUERY: &str = r#"
query($owner: String!, $repo: String!, $number: Int!) {
    repository(owner: $owner, name: $repo) {
        pullRequest(number: $number) {
            mergeStateStatus
            isDraft
        }
    }
}
"#;

/// Response from the merge state GraphQL query.
#[derive(Debug, Deserialize)]
struct MergeStateQueryResponse {
    repository: Option<MergeStateRepository>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MergeStateRepository {
    pull_request: Option<MergeStatePr>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MergeStatePr {
    merge_state_status: String,
    is_draft: bool,
}

// ─── Interpreter Implementation ───────────────────────────────────────────────

impl GitHubInterpreter for OctocrabClient {
    type Error = GitHubApiError;

    async fn interpret(&self, effect: GitHubEffect) -> Result<GitHubResponse, Self::Error> {
        interpret_github_effect(
            self,
            effect,
            RetryConfig::DEFAULT,
            RetryPolicy::RetryTransient,
        )
        .await
    }
}

/// Interprets a GitHub effect, executing it against the GitHub API.
///
/// This is the main entry point for effect interpretation. It handles retry
/// logic and proper error categorization.
///
/// # Arguments
///
/// * `client` - The octocrab client scoped to a repository
/// * `effect` - The effect to execute
/// * `retry_config` - Configuration for retry behavior
/// * `retry_policy` - Whether to retry transient errors
pub async fn interpret_github_effect(
    client: &OctocrabClient,
    effect: GitHubEffect,
    retry_config: RetryConfig,
    retry_policy: RetryPolicy,
) -> Result<GitHubResponse, GitHubApiError> {
    let result = retry_with_backoff(retry_config, retry_policy, || {
        execute_effect(client, effect.clone())
    })
    .await;

    result.into_result()
}

/// Executes a single effect without retry logic.
///
/// This function is called by `retry_with_backoff` and handles the actual
/// API calls.
async fn execute_effect(
    client: &OctocrabClient,
    effect: GitHubEffect,
) -> Result<GitHubResponse, GitHubApiError> {
    match effect {
        GitHubEffect::GetPr { pr } => get_pr(client, pr).await,
        GitHubEffect::ListOpenPrs => list_open_prs(client).await,
        GitHubEffect::ListRecentlyMergedPrs { since_days } => {
            list_recently_merged_prs(client, since_days).await
        }
        GitHubEffect::GetMergeState { pr } => get_merge_state(client, pr).await,
        GitHubEffect::SquashMerge { pr, expected_sha } => {
            squash_merge(client, pr, expected_sha).await
        }
        GitHubEffect::RetargetPr { pr, new_base } => retarget_pr(client, pr, new_base).await,
        GitHubEffect::PostComment { pr, body } => post_comment(client, pr, body).await,
        GitHubEffect::UpdateComment { comment_id, body } => {
            update_comment(client, comment_id, body).await
        }
        GitHubEffect::AddReaction {
            comment_id,
            reaction,
        } => add_reaction(client, comment_id, reaction).await,
        GitHubEffect::ListComments { pr } => list_comments(client, pr).await,
        GitHubEffect::GetBranchProtection { branch } => get_branch_protection(client, branch).await,
        GitHubEffect::GetRulesets => get_rulesets(client).await,
        GitHubEffect::GetRepoSettings => get_repo_settings(client).await,
    }
}

// ─── PR Operations ────────────────────────────────────────────────────────────

async fn get_pr(client: &OctocrabClient, pr: PrNumber) -> Result<GitHubResponse, GitHubApiError> {
    let result = client
        .inner()
        .pulls(client.owner(), client.repo_name())
        .get(pr.0)
        .await;

    match result {
        Ok(pull) => {
            let state = match pull.merged_at {
                Some(_) => {
                    // PR is merged - get merge commit SHA
                    let sha = pull.merge_commit_sha.as_ref().ok_or_else(|| {
                        GitHubApiError::permanent_without_source(format!(
                            "PR {} is merged but has no merge_commit_sha",
                            pr
                        ))
                    })?;
                    PrState::Merged {
                        merge_commit_sha: Sha::parse(sha).map_err(|e| {
                            GitHubApiError::permanent_without_source(format!(
                                "Invalid merge commit SHA: {}",
                                e
                            ))
                        })?,
                    }
                }
                None => {
                    if pull.state == Some(octocrab::models::IssueState::Closed) {
                        PrState::Closed
                    } else {
                        PrState::Open
                    }
                }
            };

            let head_sha = Sha::parse(&pull.head.sha).map_err(|e| {
                GitHubApiError::permanent_without_source(format!("Invalid head SHA: {}", e))
            })?;

            Ok(GitHubResponse::Pr(PrData {
                number: pr,
                head_sha,
                head_ref: pull.head.ref_field,
                base_ref: pull.base.ref_field,
                state,
                is_draft: pull.draft.unwrap_or(false),
            }))
        }
        Err(e) => Err(GitHubApiError::from_octocrab(e)),
    }
}

async fn list_open_prs(client: &OctocrabClient) -> Result<GitHubResponse, GitHubApiError> {
    let mut page = 1u32;
    let mut all_prs = Vec::new();

    loop {
        let result = client
            .inner()
            .pulls(client.owner(), client.repo_name())
            .list()
            .state(octocrab::params::State::Open)
            .per_page(100)
            .page(page)
            .send()
            .await;

        match result {
            Ok(page_result) => {
                let items = page_result.items;
                let is_last_page = items.len() < 100;

                for pull in items {
                    let head_sha = match Sha::parse(&pull.head.sha) {
                        Ok(sha) => sha,
                        Err(e) => {
                            tracing::warn!(pr = pull.number, error = %e, "Skipping PR with invalid SHA");
                            continue;
                        }
                    };

                    all_prs.push(PrData {
                        number: PrNumber(pull.number),
                        head_sha,
                        head_ref: pull.head.ref_field,
                        base_ref: pull.base.ref_field,
                        state: PrState::Open,
                        is_draft: pull.draft.unwrap_or(false),
                    });
                }

                if is_last_page {
                    break;
                }
                page += 1;
            }
            Err(e) => return Err(GitHubApiError::from_octocrab(e)),
        }
    }

    Ok(GitHubResponse::PrList(all_prs))
}

async fn list_recently_merged_prs(
    client: &OctocrabClient,
    since_days: u32,
) -> Result<GitHubResponse, GitHubApiError> {
    // GitHub's PR list API doesn't have a "merged since" filter, so we need to
    // list closed PRs and filter client-side. This is inefficient for repos with
    // many closed PRs, but works for the expected use case (recent merges).
    let since = Utc::now() - ChronoDuration::days(i64::from(since_days));
    let mut page = 1u32;
    let mut all_prs = Vec::new();

    loop {
        let result = client
            .inner()
            .pulls(client.owner(), client.repo_name())
            .list()
            .state(octocrab::params::State::Closed)
            .sort(octocrab::params::pulls::Sort::Updated)
            .direction(octocrab::params::Direction::Descending)
            .per_page(100)
            .page(page)
            .send()
            .await;

        match result {
            Ok(page_result) => {
                let items = page_result.items;
                let is_last_page = items.len() < 100;
                let mut found_old_pr = false;

                for pull in items {
                    // Check if this PR is too old
                    if let Some(updated_at) = pull.updated_at
                        && updated_at < since
                    {
                        found_old_pr = true;
                        continue;
                    }

                    // Only include merged PRs
                    let _merged_at = match pull.merged_at {
                        Some(t) if t >= since => t,
                        Some(_) => {
                            found_old_pr = true;
                            continue;
                        }
                        None => continue, // Closed but not merged
                    };

                    let merge_commit_sha = match &pull.merge_commit_sha {
                        Some(sha) => match Sha::parse(sha) {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::warn!(pr = pull.number, error = %e, "Skipping merged PR with invalid SHA");
                                continue;
                            }
                        },
                        None => {
                            tracing::warn!(
                                pr = pull.number,
                                "Skipping merged PR without merge_commit_sha"
                            );
                            continue;
                        }
                    };

                    let head_sha = match Sha::parse(&pull.head.sha) {
                        Ok(sha) => sha,
                        Err(e) => {
                            tracing::warn!(pr = pull.number, error = %e, "Skipping PR with invalid head SHA");
                            continue;
                        }
                    };

                    all_prs.push(PrData {
                        number: PrNumber(pull.number),
                        head_sha,
                        head_ref: pull.head.ref_field,
                        base_ref: pull.base.ref_field,
                        state: PrState::Merged { merge_commit_sha },
                        is_draft: pull.draft.unwrap_or(false),
                    });
                }

                // Stop if we've hit old PRs or last page
                if found_old_pr || is_last_page {
                    break;
                }
                page += 1;
            }
            Err(e) => return Err(GitHubApiError::from_octocrab(e)),
        }
    }

    Ok(GitHubResponse::PrList(all_prs))
}

// ─── Merge State (GraphQL) ────────────────────────────────────────────────────

async fn get_merge_state(
    client: &OctocrabClient,
    pr: PrNumber,
) -> Result<GitHubResponse, GitHubApiError> {
    #[derive(Serialize)]
    struct Variables<'a> {
        owner: &'a str,
        repo: &'a str,
        number: i64,
    }

    let variables = Variables {
        owner: client.owner(),
        repo: client.repo_name(),
        number: pr.0 as i64,
    };

    let result: Result<MergeStateQueryResponse, _> = client
        .inner()
        .graphql(&serde_json::json!({
            "query": MERGE_STATE_QUERY,
            "variables": variables,
        }))
        .await;

    match result {
        Ok(response) => {
            let pr_data = response
                .repository
                .and_then(|r| r.pull_request)
                .ok_or_else(|| {
                    GitHubApiError::permanent_without_source(format!("PR {} not found", pr))
                })?;

            // If PR is a draft, return Draft status regardless of merge_state_status
            // (GitHub may report CLEAN for a draft PR if CI passes, but it's not mergeable)
            if pr_data.is_draft {
                return Ok(GitHubResponse::MergeState(MergeStateStatus::Draft));
            }

            let status = parse_merge_state_status(&pr_data.merge_state_status);
            Ok(GitHubResponse::MergeState(status))
        }
        Err(e) => Err(GitHubApiError::from_octocrab(e)),
    }
}

/// Parses the mergeStateStatus string from GraphQL into our enum.
fn parse_merge_state_status(status: &str) -> MergeStateStatus {
    match status.to_uppercase().as_str() {
        "CLEAN" => MergeStateStatus::Clean,
        "UNSTABLE" => MergeStateStatus::Unstable,
        "BLOCKED" => MergeStateStatus::Blocked,
        "BEHIND" => MergeStateStatus::Behind,
        "DIRTY" => MergeStateStatus::Dirty,
        "UNKNOWN" => MergeStateStatus::Unknown,
        "DRAFT" => MergeStateStatus::Draft,
        "HAS_HOOKS" => MergeStateStatus::HasHooks,
        other => {
            tracing::warn!(
                status = other,
                "Unknown mergeStateStatus, treating as Unknown"
            );
            MergeStateStatus::Unknown
        }
    }
}

// ─── Squash Merge ─────────────────────────────────────────────────────────────

async fn squash_merge(
    client: &OctocrabClient,
    pr: PrNumber,
    expected_sha: Sha,
) -> Result<GitHubResponse, GitHubApiError> {
    // Use the REST API directly since octocrab's merge method doesn't support
    // all the parameters we need (specifically the SHA guard).
    let url = format!(
        "/repos/{}/{}/pulls/{}/merge",
        client.owner(),
        client.repo_name(),
        pr.0
    );

    #[derive(Serialize)]
    struct MergeRequest<'a> {
        merge_method: &'static str,
        sha: &'a str,
    }

    let request = MergeRequest {
        merge_method: "squash",
        sha: expected_sha.as_str(),
    };

    let result: Result<MergeResponse, _> = client.inner().put(&url, Some(&request)).await;

    match result {
        Ok(response) => {
            if response.merged {
                let sha = Sha::parse(&response.sha).map_err(|e| {
                    GitHubApiError::permanent_without_source(format!(
                        "Invalid merge commit SHA in response: {}",
                        e
                    ))
                })?;
                Ok(GitHubResponse::Merged { sha })
            } else {
                Err(GitHubApiError::permanent_without_source(format!(
                    "Merge request returned merged=false: {}",
                    response.message.as_deref().unwrap_or("unknown reason")
                )))
            }
        }
        Err(e) => {
            // Check if this is a SHA mismatch (409 Conflict)
            let err_str = e.to_string();
            if err_str.contains("409")
                || err_str.to_lowercase().contains("head branch was modified")
                || err_str.to_lowercase().contains("sha")
            {
                Err(GitHubApiError::sha_mismatch(pr, &expected_sha, e))
            } else {
                Err(GitHubApiError::from_octocrab(e))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct MergeResponse {
    sha: String,
    merged: bool,
    message: Option<String>,
}

// ─── PR Retargeting ───────────────────────────────────────────────────────────

async fn retarget_pr(
    client: &OctocrabClient,
    pr: PrNumber,
    new_base: String,
) -> Result<GitHubResponse, GitHubApiError> {
    let result = client
        .inner()
        .pulls(client.owner(), client.repo_name())
        .update(pr.0)
        .base(new_base)
        .send()
        .await;

    match result {
        Ok(_) => Ok(GitHubResponse::Retargeted),
        Err(e) => Err(GitHubApiError::from_octocrab(e)),
    }
}

// ─── Comments ─────────────────────────────────────────────────────────────────

async fn post_comment(
    client: &OctocrabClient,
    pr: PrNumber,
    body: String,
) -> Result<GitHubResponse, GitHubApiError> {
    let result = client
        .inner()
        .issues(client.owner(), client.repo_name())
        .create_comment(pr.0, body)
        .await;

    match result {
        Ok(comment) => Ok(GitHubResponse::CommentPosted {
            id: CommentId(comment.id.into_inner()),
        }),
        Err(e) => Err(GitHubApiError::from_octocrab(e)),
    }
}

async fn update_comment(
    client: &OctocrabClient,
    comment_id: CommentId,
    body: String,
) -> Result<GitHubResponse, GitHubApiError> {
    let url = format!(
        "/repos/{}/{}/issues/comments/{}",
        client.owner(),
        client.repo_name(),
        comment_id.0
    );

    #[derive(Serialize)]
    struct UpdateRequest {
        body: String,
    }

    let result: Result<serde_json::Value, _> = client
        .inner()
        .patch(&url, Some(&UpdateRequest { body }))
        .await;

    match result {
        Ok(_) => Ok(GitHubResponse::CommentUpdated),
        Err(e) => Err(GitHubApiError::from_octocrab(e)),
    }
}

async fn add_reaction(
    client: &OctocrabClient,
    comment_id: CommentId,
    reaction: Reaction,
) -> Result<GitHubResponse, GitHubApiError> {
    let url = format!(
        "/repos/{}/{}/issues/comments/{}/reactions",
        client.owner(),
        client.repo_name(),
        comment_id.0
    );

    #[derive(Serialize)]
    struct ReactionRequest {
        content: &'static str,
    }

    let result: Result<serde_json::Value, _> = client
        .inner()
        .post(
            &url,
            Some(&ReactionRequest {
                content: reaction.as_api_str(),
            }),
        )
        .await;

    match result {
        Ok(_) => Ok(GitHubResponse::ReactionAdded),
        Err(e) => Err(GitHubApiError::from_octocrab(e)),
    }
}

async fn list_comments(
    client: &OctocrabClient,
    pr: PrNumber,
) -> Result<GitHubResponse, GitHubApiError> {
    let mut page = 1u32;
    let mut all_comments = Vec::new();

    loop {
        let result = client
            .inner()
            .issues(client.owner(), client.repo_name())
            .list_comments(pr.0)
            .per_page(100)
            .page(page)
            .send()
            .await;

        match result {
            Ok(page_result) => {
                let items = page_result.items;
                let is_last_page = items.len() < 100;

                for comment in items {
                    all_comments.push(CommentData {
                        id: CommentId(comment.id.into_inner()),
                        author_id: comment.user.id.into_inner(),
                        body: comment.body.unwrap_or_default(),
                    });
                }

                if is_last_page {
                    break;
                }
                page += 1;
            }
            Err(e) => return Err(GitHubApiError::from_octocrab(e)),
        }
    }

    Ok(GitHubResponse::Comments(all_comments))
}

// ─── Repository Settings ──────────────────────────────────────────────────────

async fn get_branch_protection(
    client: &OctocrabClient,
    branch: String,
) -> Result<GitHubResponse, GitHubApiError> {
    let url = format!(
        "/repos/{}/{}/branches/{}/protection",
        client.owner(),
        client.repo_name(),
        branch
    );

    let result: Result<BranchProtectionResponse, _> = client.inner().get(&url, None::<&()>).await;

    match result {
        Ok(protection) => {
            let dismiss_stale_reviews = protection
                .required_pull_request_reviews
                .map(|r| r.dismiss_stale_reviews)
                .unwrap_or(false);

            let required_status_checks = protection
                .required_status_checks
                .map(|r| r.contexts)
                .unwrap_or_default();

            Ok(GitHubResponse::BranchProtection(BranchProtectionData {
                dismiss_stale_reviews,
                required_status_checks,
            }))
        }
        Err(e) => {
            // 404 means no branch protection - return defaults
            let err_str = e.to_string();
            if err_str.contains("404") || err_str.to_lowercase().contains("not found") {
                Ok(GitHubResponse::BranchProtection(BranchProtectionData {
                    dismiss_stale_reviews: false,
                    required_status_checks: vec![],
                }))
            } else {
                Err(GitHubApiError::from_octocrab(e))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct BranchProtectionResponse {
    required_pull_request_reviews: Option<RequiredPullRequestReviews>,
    required_status_checks: Option<RequiredStatusChecks>,
}

#[derive(Debug, Deserialize)]
struct RequiredPullRequestReviews {
    dismiss_stale_reviews: bool,
}

#[derive(Debug, Deserialize)]
struct RequiredStatusChecks {
    contexts: Vec<String>,
}

async fn get_rulesets(client: &OctocrabClient) -> Result<GitHubResponse, GitHubApiError> {
    // includes_parents=true is required to include rulesets inherited from parent
    // organizations, which may also enforce "dismiss stale reviews on push"
    let url = format!(
        "/repos/{}/{}/rulesets?includes_parents=true",
        client.owner(),
        client.repo_name()
    );

    let result: Result<Vec<RulesetResponse>, _> = client.inner().get(&url, None::<&()>).await;

    match result {
        Ok(rulesets) => {
            let data: Vec<_> = rulesets
                .into_iter()
                .map(|r| {
                    // Check if any rule in this ruleset dismisses stale reviews
                    let dismiss_stale = r.rules.iter().any(|rule| {
                        rule.r#type == "pull_request"
                            && rule
                                .parameters
                                .as_ref()
                                .map(|p| p.dismiss_stale_reviews_on_push)
                                .unwrap_or(false)
                    });

                    RulesetData {
                        name: r.name,
                        dismiss_stale_reviews_on_push: dismiss_stale,
                    }
                })
                .collect();

            Ok(GitHubResponse::Rulesets(data))
        }
        Err(e) => {
            // 404 might mean rulesets aren't available (older API or not enabled)
            let err_str = e.to_string();
            if err_str.contains("404") || err_str.to_lowercase().contains("not found") {
                Ok(GitHubResponse::Rulesets(vec![]))
            } else {
                Err(GitHubApiError::from_octocrab(e))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct RulesetResponse {
    name: String,
    rules: Vec<RulesetRule>,
}

#[derive(Debug, Deserialize)]
struct RulesetRule {
    r#type: String,
    parameters: Option<RulesetRuleParameters>,
}

#[derive(Debug, Deserialize)]
struct RulesetRuleParameters {
    #[serde(default)]
    dismiss_stale_reviews_on_push: bool,
}

async fn get_repo_settings(client: &OctocrabClient) -> Result<GitHubResponse, GitHubApiError> {
    let result = client
        .inner()
        .repos(client.owner(), client.repo_name())
        .get()
        .await;

    match result {
        Ok(repo) => Ok(GitHubResponse::RepoSettings(RepoSettingsData {
            default_branch: repo.default_branch.unwrap_or_else(|| "main".to_string()),
            allow_squash_merge: repo.allow_squash_merge.unwrap_or(true),
            allow_merge_commit: repo.allow_merge_commit.unwrap_or(true),
            allow_rebase_merge: repo.allow_rebase_merge.unwrap_or(true),
        })),
        Err(e) => Err(GitHubApiError::from_octocrab(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_merge_state_status_all_values() {
        assert_eq!(parse_merge_state_status("CLEAN"), MergeStateStatus::Clean);
        assert_eq!(parse_merge_state_status("clean"), MergeStateStatus::Clean);
        assert_eq!(
            parse_merge_state_status("UNSTABLE"),
            MergeStateStatus::Unstable
        );
        assert_eq!(
            parse_merge_state_status("BLOCKED"),
            MergeStateStatus::Blocked
        );
        assert_eq!(parse_merge_state_status("BEHIND"), MergeStateStatus::Behind);
        assert_eq!(parse_merge_state_status("DIRTY"), MergeStateStatus::Dirty);
        assert_eq!(
            parse_merge_state_status("UNKNOWN"),
            MergeStateStatus::Unknown
        );
        assert_eq!(parse_merge_state_status("DRAFT"), MergeStateStatus::Draft);
        assert_eq!(
            parse_merge_state_status("HAS_HOOKS"),
            MergeStateStatus::HasHooks
        );
    }

    #[test]
    fn parse_merge_state_status_unknown_fallback() {
        assert_eq!(
            parse_merge_state_status("INVALID"),
            MergeStateStatus::Unknown
        );
        assert_eq!(parse_merge_state_status(""), MergeStateStatus::Unknown);
    }
}
