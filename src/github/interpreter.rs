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
                    //
                    // IMPORTANT: GitHub's API has eventual consistency. After a PR is merged,
                    // merge_commit_sha may not be populated immediately. Per DESIGN.md, we
                    // treat this as a transient error to allow retry with backoff.
                    let sha = pull.merge_commit_sha.as_ref().ok_or_else(|| {
                        GitHubApiError::transient_without_source(format!(
                            "PR {} is merged but merge_commit_sha not yet available (eventual consistency)",
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

    // Safety limit to prevent runaway pagination on repos with many closed PRs
    const MAX_PAGES: u32 = 10;

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

                for pull in items {
                    // Only include merged PRs with merged_at within our window
                    match pull.merged_at {
                        Some(t) if t >= since => {} // Within window, continue processing
                        Some(_) => continue,        // Merged but too old
                        None => continue,           // Closed but not merged
                    };

                    // IMPORTANT: GitHub's API has eventual consistency. After a PR is merged,
                    // merge_commit_sha may not be populated immediately. Per DESIGN.md, we
                    // treat this as a transient error to allow retry with backoff.
                    let merge_commit_sha = match &pull.merge_commit_sha {
                        Some(sha) => match Sha::parse(sha) {
                            Ok(s) => s,
                            Err(e) => {
                                tracing::warn!(pr = pull.number, error = %e, "Skipping merged PR with invalid SHA");
                                continue;
                            }
                        },
                        None => {
                            return Err(GitHubApiError::transient_without_source(format!(
                                "PR {} is merged but merge_commit_sha not yet available (eventual consistency)",
                                pull.number
                            )));
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

                // Stop if:
                // 1. This is the last page (< 100 items)
                // 2. We've hit the safety limit
                //
                // IMPORTANT: We do NOT stop based on whether this page had recent merges.
                // Results are sorted by `updated` (not `merged_at`), so a page could contain
                // only old PRs with recent comments while recent merges appear on later pages.
                // We must paginate through all pages (up to MAX_PAGES) to find all recent merges.
                if is_last_page {
                    // Naturally exhausted all pages - results are complete
                    return Ok(GitHubResponse::RecentlyMergedPrList {
                        prs: all_prs,
                        may_be_incomplete: false,
                    });
                }
                if page >= MAX_PAGES {
                    // Hit the safety limit before exhausting pages - results may be incomplete
                    tracing::warn!(
                        pages = page,
                        prs_found = all_prs.len(),
                        "Hit pagination limit for recently merged PRs; results may be incomplete"
                    );
                    return Ok(GitHubResponse::RecentlyMergedPrList {
                        prs: all_prs,
                        may_be_incomplete: true,
                    });
                }
                page += 1;
            }
            Err(e) => return Err(GitHubApiError::from_octocrab(e)),
        }
    }
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

            let status = resolve_merge_state(&pr_data.merge_state_status, pr_data.is_draft);
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

/// Resolves the final merge state, applying draft override logic.
///
/// GitHub may report CLEAN for a draft PR if CI passes, but draft PRs are not
/// actually mergeable. This function ensures that `is_draft == true` always
/// results in `MergeStateStatus::Draft`, regardless of the reported status.
///
/// This is a pure function extracted for testability.
pub fn resolve_merge_state(merge_state_status: &str, is_draft: bool) -> MergeStateStatus {
    if is_draft {
        MergeStateStatus::Draft
    } else {
        parse_merge_state_status(merge_state_status)
    }
}

// ─── Squash Merge ─────────────────────────────────────────────────────────────

/// Checks if an error message indicates a SHA mismatch on merge.
///
/// GitHub returns HTTP 409 for multiple reasons:
/// - SHA mismatch: "Head branch was modified. Review and try the merge again."
/// - Merge conflicts: "Merge conflict" or similar
///
/// We only treat it as a SHA mismatch (which triggers re-evaluation) if the
/// message specifically indicates the head branch changed. Other 409 errors
/// (like merge conflicts) are permanent failures.
///
/// This is a pure function extracted for testability.
pub fn is_sha_mismatch_error(err_str: &str) -> bool {
    err_str.to_lowercase().contains("head branch was modified")
}

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
            // Check if this is a SHA mismatch (409 Conflict with specific message)
            let err_str = e.to_string();
            if is_sha_mismatch_error(&err_str) {
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

/// Checks if an error message indicates we should fall back to "unknown" status.
///
/// For branch protection and ruleset queries, 404/403 errors can mean:
/// - No protection/rulesets configured
/// - Insufficient permissions to view settings
/// - Branch/resource doesn't exist
///
/// We cannot distinguish these cases via the API, so we return an "unknown"
/// status and let callers implement "warn and proceed" per DESIGN.md rather
/// than silently treating these as "no protection" or failing hard.
///
/// This is a pure function extracted for testability.
pub fn should_fallback_to_unknown(err_str: &str) -> bool {
    let err_lower = err_str.to_lowercase();
    let is_not_found = err_str.contains("404") || err_lower.contains("not found");
    let is_forbidden = err_str.contains("403") || err_lower.contains("forbidden");
    is_not_found || is_forbidden
}

async fn get_branch_protection(
    client: &OctocrabClient,
    branch: String,
) -> Result<GitHubResponse, GitHubApiError> {
    // URL-encode the branch name to handle special characters like '/'
    // e.g., "feature/foo" -> "feature%2Ffoo"
    let encoded_branch = urlencoding::encode(&branch);
    let url = format!(
        "/repos/{}/{}/branches/{}/protection",
        client.owner(),
        client.repo_name(),
        encoded_branch
    );

    let result: Result<BranchProtectionResponse, _> = client.inner().get(&url, None::<&()>).await;

    match result {
        Ok(protection) => {
            let dismiss_stale_reviews = protection
                .required_pull_request_reviews
                .map(|r| r.dismiss_stale_reviews)
                .unwrap_or(false);

            // Combine legacy `contexts` with modern `checks` array.
            // The modern format has: {"checks": [{"context": "ci/build", "app_id": 123}]}
            // We extract just the context names for compatibility.
            let mut required_status_checks = Vec::new();
            if let Some(ref checks_config) = protection.required_status_checks {
                // Add legacy contexts
                required_status_checks.extend(checks_config.contexts.iter().cloned());

                // Add modern checks (extract context names)
                for check in &checks_config.checks {
                    // Avoid duplicates if the same context appears in both
                    if !required_status_checks.contains(&check.context) {
                        required_status_checks.push(check.context.clone());
                    }
                }
            }

            Ok(GitHubResponse::BranchProtection(BranchProtectionData {
                dismiss_stale_reviews,
                required_status_checks,
            }))
        }
        Err(e) => {
            let err_str = e.to_string();
            if should_fallback_to_unknown(&err_str) {
                tracing::warn!(
                    branch = %branch,
                    error = %err_str,
                    "Branch protection query failed - could be no protection, \
                     insufficient permissions, or missing branch"
                );
                Ok(GitHubResponse::BranchProtectionUnknown)
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
    /// Legacy status check contexts (array of strings)
    #[serde(default)]
    contexts: Vec<String>,
    /// Modern status checks with app_id (GitHub Apps)
    #[serde(default)]
    checks: Vec<RequiredStatusCheck>,
}

/// A modern required status check entry with optional app_id
#[derive(Debug, Deserialize)]
struct RequiredStatusCheck {
    context: String,
    #[serde(default)]
    #[allow(dead_code)]
    app_id: Option<i64>,
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

                    // Extract target branch patterns and exclude patterns from conditions
                    let (target_branches, exclude_patterns) = r
                        .conditions
                        .and_then(|c| c.ref_name)
                        .map(|rn| (rn.include, rn.exclude))
                        .unwrap_or_default();

                    RulesetData {
                        name: r.name,
                        dismiss_stale_reviews_on_push: dismiss_stale,
                        target_branches,
                        exclude_patterns,
                    }
                })
                .collect();

            Ok(GitHubResponse::Rulesets(data))
        }
        Err(e) => {
            let err_str = e.to_string();
            if should_fallback_to_unknown(&err_str) {
                tracing::warn!(
                    error = %err_str,
                    "Rulesets query failed - could be no rulesets, \
                     insufficient permissions, or unsupported API"
                );
                Ok(GitHubResponse::RulesetsUnknown)
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
    #[serde(default)]
    conditions: Option<RulesetConditions>,
}

#[derive(Debug, Deserialize)]
struct RulesetConditions {
    #[serde(default)]
    ref_name: Option<RulesetRefNameCondition>,
}

#[derive(Debug, Deserialize)]
struct RulesetRefNameCondition {
    /// Branch patterns to include (e.g., "refs/heads/main", "refs/heads/*")
    #[serde(default)]
    include: Vec<String>,
    /// Branch patterns to exclude from the ruleset.
    /// A branch matching any exclude pattern is not targeted, even if it
    /// matches an include pattern.
    #[serde(default)]
    exclude: Vec<String>,
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
        Ok(repo) => {
            // Per DESIGN.md, the squash-only preflight check is a "hard requirement"
            // with "no warn-and-proceed". If we can't determine the merge method
            // settings, we must fail rather than default to permissive values that
            // could incorrectly pass the preflight check.
            let allow_squash_merge = repo.allow_squash_merge.ok_or_else(|| {
                GitHubApiError::permanent_without_source(
                    "Repository settings missing 'allow_squash_merge' field - \
                     cannot verify squash-only configuration",
                )
            })?;
            let allow_merge_commit = repo.allow_merge_commit.ok_or_else(|| {
                GitHubApiError::permanent_without_source(
                    "Repository settings missing 'allow_merge_commit' field - \
                     cannot verify squash-only configuration",
                )
            })?;
            let allow_rebase_merge = repo.allow_rebase_merge.ok_or_else(|| {
                GitHubApiError::permanent_without_source(
                    "Repository settings missing 'allow_rebase_merge' field - \
                     cannot verify squash-only configuration",
                )
            })?;

            Ok(GitHubResponse::RepoSettings(RepoSettingsData {
                default_branch: repo.default_branch.unwrap_or_else(|| "main".to_string()),
                allow_squash_merge,
                allow_merge_commit,
                allow_rebase_merge,
            }))
        }
        Err(e) => Err(GitHubApiError::from_octocrab(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ─── Unit Tests ───────────────────────────────────────────────────────────

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

    // ─── resolve_merge_state tests ────────────────────────────────────────────

    #[test]
    fn resolve_merge_state_draft_override() {
        // Draft PRs should always return Draft, regardless of the merge state status
        assert_eq!(resolve_merge_state("CLEAN", true), MergeStateStatus::Draft);
        assert_eq!(
            resolve_merge_state("BLOCKED", true),
            MergeStateStatus::Draft
        );
        assert_eq!(resolve_merge_state("BEHIND", true), MergeStateStatus::Draft);
    }

    #[test]
    fn resolve_merge_state_non_draft_passes_through() {
        // Non-draft PRs should return the parsed merge state status
        assert_eq!(resolve_merge_state("CLEAN", false), MergeStateStatus::Clean);
        assert_eq!(
            resolve_merge_state("BLOCKED", false),
            MergeStateStatus::Blocked
        );
        assert_eq!(
            resolve_merge_state("BEHIND", false),
            MergeStateStatus::Behind
        );
    }

    // ─── is_sha_mismatch_error tests ──────────────────────────────────────────

    #[test]
    fn is_sha_mismatch_error_detects_github_message() {
        assert!(is_sha_mismatch_error(
            "Head branch was modified. Review and try the merge again."
        ));
        assert!(is_sha_mismatch_error(
            "409 Conflict: Head branch was modified"
        ));
        // Case insensitive
        assert!(is_sha_mismatch_error("HEAD BRANCH WAS MODIFIED"));
    }

    #[test]
    fn is_sha_mismatch_error_rejects_other_409s() {
        // Merge conflicts are 409 but not SHA mismatch
        assert!(!is_sha_mismatch_error("Merge conflict"));
        assert!(!is_sha_mismatch_error("409 Conflict: cannot merge"));
        assert!(!is_sha_mismatch_error("Pull request is not mergeable"));
    }

    // ─── should_fallback_to_unknown tests ─────────────────────────────────────

    #[test]
    fn should_fallback_to_unknown_detects_not_found() {
        assert!(should_fallback_to_unknown("404 Not Found"));
        assert!(should_fallback_to_unknown("Resource not found"));
        assert!(should_fallback_to_unknown("Branch not found"));
    }

    #[test]
    fn should_fallback_to_unknown_detects_forbidden() {
        assert!(should_fallback_to_unknown("403 Forbidden"));
        assert!(should_fallback_to_unknown("Access forbidden"));
        assert!(should_fallback_to_unknown("Permission denied 403"));
    }

    #[test]
    fn should_fallback_to_unknown_rejects_other_errors() {
        assert!(!should_fallback_to_unknown("500 Internal Server Error"));
        assert!(!should_fallback_to_unknown("Rate limit exceeded"));
        assert!(!should_fallback_to_unknown("Timeout"));
        assert!(!should_fallback_to_unknown("Network error"));
    }

    // ─── Property Tests ───────────────────────────────────────────────────────

    proptest! {
        /// Property: is_draft == true implies Draft status, regardless of merge state string
        #[test]
        fn prop_draft_always_overrides(status in ".*") {
            prop_assert_eq!(
                resolve_merge_state(&status, true),
                MergeStateStatus::Draft,
                "Draft PRs must always return Draft status"
            );
        }

        /// Property: is_draft == false with known status returns that status
        #[test]
        fn prop_non_draft_with_known_status_returns_parsed(
            status in prop_oneof![
                Just("CLEAN"),
                Just("UNSTABLE"),
                Just("BLOCKED"),
                Just("BEHIND"),
                Just("DIRTY"),
                Just("DRAFT"),
                Just("HAS_HOOKS"),
            ]
        ) {
            let result = resolve_merge_state(status, false);
            // Should return the corresponding status, not Unknown
            prop_assert_ne!(
                result,
                MergeStateStatus::Unknown,
                "Known statuses should parse correctly"
            );
        }

        /// Property: is_draft == false with unknown status returns Unknown
        #[test]
        fn prop_non_draft_with_unknown_status_returns_unknown(
            status in "[A-Z]{1,20}"
                .prop_filter("must not be a known status", |s| {
                    !matches!(
                        s.as_str(),
                        "CLEAN" | "UNSTABLE" | "BLOCKED" | "BEHIND" | "DIRTY" | "UNKNOWN" | "DRAFT" | "HAS_HOOKS"
                    )
                })
        ) {
            prop_assert_eq!(
                resolve_merge_state(&status, false),
                MergeStateStatus::Unknown,
                "Unknown statuses should return Unknown"
            );
        }

        /// Property: "head branch was modified" (case insensitive) implies SHA mismatch
        #[test]
        fn prop_sha_mismatch_detected_with_marker(
            prefix in ".*",
            suffix in ".*",
        ) {
            let err_str = format!("{}head branch was modified{}", prefix, suffix);
            prop_assert!(
                is_sha_mismatch_error(&err_str),
                "Error containing 'head branch was modified' should be SHA mismatch"
            );
        }

        /// Property: strings without "head branch was modified" are not SHA mismatch
        #[test]
        fn prop_no_sha_mismatch_without_marker(
            err_str in ".*"
                .prop_filter("must not contain the marker", |s| {
                    !s.to_lowercase().contains("head branch was modified")
                })
        ) {
            prop_assert!(
                !is_sha_mismatch_error(&err_str),
                "Error without 'head branch was modified' should not be SHA mismatch"
            );
        }

        /// Property: "404" or "not found" implies fallback to unknown
        #[test]
        fn prop_404_triggers_fallback(prefix in ".*", suffix in ".*") {
            let err_str = format!("{}404{}", prefix, suffix);
            prop_assert!(
                should_fallback_to_unknown(&err_str),
                "Error containing '404' should trigger fallback"
            );
        }

        /// Property: "403" or "forbidden" implies fallback to unknown
        #[test]
        fn prop_403_triggers_fallback(prefix in ".*", suffix in ".*") {
            let err_str = format!("{}403{}", prefix, suffix);
            prop_assert!(
                should_fallback_to_unknown(&err_str),
                "Error containing '403' should trigger fallback"
            );
        }

        /// Property: "not found" (case insensitive) implies fallback to unknown
        #[test]
        fn prop_not_found_triggers_fallback(prefix in ".*", suffix in ".*") {
            let err_str = format!("{}not found{}", prefix, suffix);
            prop_assert!(
                should_fallback_to_unknown(&err_str),
                "Error containing 'not found' should trigger fallback"
            );
        }

        /// Property: "forbidden" (case insensitive) implies fallback to unknown
        #[test]
        fn prop_forbidden_triggers_fallback(prefix in ".*", suffix in ".*") {
            let err_str = format!("{}forbidden{}", prefix, suffix);
            prop_assert!(
                should_fallback_to_unknown(&err_str),
                "Error containing 'forbidden' should trigger fallback"
            );
        }

        /// Property: strings without 404/403/not found/forbidden don't trigger fallback
        #[test]
        fn prop_no_fallback_without_markers(
            err_str in "[a-z0-9 ]{1,100}"
                .prop_filter("must not contain markers", |s| {
                    let lower = s.to_lowercase();
                    !s.contains("404")
                        && !s.contains("403")
                        && !lower.contains("not found")
                        && !lower.contains("forbidden")
                })
        ) {
            prop_assert!(
                !should_fallback_to_unknown(&err_str),
                "Error without markers should not trigger fallback"
            );
        }
    }
}
