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

use std::future::Future;

use super::client::OctocrabClient;
use super::error::{GitHubApiError, is_rate_limit_error, is_transient_message};
use super::retry::{RetryConfig, RetryPolicy, RetryResult, retry_with_backoff};

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
    data: Option<MergeStateData>,
    errors: Option<Vec<GraphQLError>>,
}

#[derive(Debug, Deserialize)]
struct MergeStateData {
    repository: Option<MergeStateRepository>,
}

/// GraphQL error from GitHub API.
#[derive(Debug, Deserialize)]
struct GraphQLError {
    message: String,
    /// GitHub's machine-readable error type, e.g. `RATE_LIMITED`.
    #[serde(default)]
    r#type: Option<String>,
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
/// * `retry_policy` - Whether to retry transient errors (idempotent effects only)
pub async fn interpret_github_effect(
    client: &OctocrabClient,
    effect: GitHubEffect,
    retry_config: RetryConfig,
    retry_policy: RetryPolicy,
) -> Result<GitHubResponse, GitHubApiError> {
    // A non-idempotent effect must not be retried: the server may have
    // processed a request whose response was lost, so a retry would duplicate
    // the side effect (PostComment) or misreport an applied one as a failure
    // (SquashMerge against an already-merged PR).
    let policy = if effect.is_idempotent() {
        retry_policy
    } else {
        RetryPolicy::NoRetry
    };

    let result = retry_with_backoff(retry_config, policy, || {
        execute_effect(client, effect.clone())
    })
    .await;

    if let RetryResult::ExhaustedRetries {
        attempts,
        last_error,
    } = &result
    {
        tracing::warn!(
            attempts,
            error = %last_error,
            "GitHub effect failed with a transient error after all attempts"
        );
    }

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

// ─── Pagination ───────────────────────────────────────────────────────────────

/// Fetches all pages of a listing endpoint, collecting every item.
///
/// `fetch_page` is called with successive 1-based page numbers. The `Link`
/// header (surfaced as `Page::next`) decides whether another page exists; the
/// number of items on a page does not.
async fn collect_all_pages<T, F, Fut>(mut fetch_page: F) -> Result<Vec<T>, GitHubApiError>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<octocrab::Page<T>, octocrab::Error>>,
{
    let mut page_number = 1u32;
    let mut items = Vec::new();

    loop {
        let mut page = fetch_page(page_number)
            .await
            .map_err(GitHubApiError::from_octocrab)?;
        let has_next = page.next.is_some();
        items.append(&mut page.items);

        if !has_next {
            return Ok(items);
        }
        page_number += 1;
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
    let pulls = collect_all_pages(|page_number| async move {
        client
            .inner()
            .pulls(client.owner(), client.repo_name())
            .list()
            .state(octocrab::params::State::Open)
            .per_page(100)
            .page(page_number)
            .send()
            .await
    })
    .await?;

    let mut all_prs = Vec::new();
    for pull in pulls {
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
    let mut page_number = 1u32;
    let mut all_prs = Vec::new();

    // Safety limit to prevent runaway pagination on repos with many closed PRs
    const MAX_PAGES: u32 = 10;

    loop {
        let page = client
            .inner()
            .pulls(client.owner(), client.repo_name())
            .list()
            .state(octocrab::params::State::Closed)
            .sort(octocrab::params::pulls::Sort::Updated)
            .direction(octocrab::params::Direction::Descending)
            .per_page(100)
            .page(page_number)
            .send()
            .await
            .map_err(GitHubApiError::from_octocrab)?;

        let has_next = page.next.is_some();
        let items = page.items;

        // Results are sorted by `updated` descending and a merge updates the
        // PR, so updated_at >= merged_at. If every item on this page was
        // updated before the window opened, no later item can have been
        // merged within it: the scan is provably complete.
        let page_proves_completion = !items.is_empty()
            && items
                .iter()
                .all(|pull| pull.updated_at.is_some_and(|t| t < since));

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

        if !has_next || page_proves_completion {
            return Ok(GitHubResponse::RecentlyMergedPrList {
                prs: all_prs,
                may_be_incomplete: false,
            });
        }
        if page_number >= MAX_PAGES {
            // Hit the safety limit before exhausting pages - results may be incomplete
            tracing::warn!(
                pages = page_number,
                prs_found = all_prs.len(),
                "Hit pagination limit for recently merged PRs; results may be incomplete"
            );
            return Ok(GitHubResponse::RecentlyMergedPrList {
                prs: all_prs,
                may_be_incomplete: true,
            });
        }
        page_number += 1;
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
            // Format GraphQL errors for use in error messages
            let error_hint = response
                .errors
                .as_ref()
                .filter(|e| !e.is_empty())
                .map(|errors| {
                    let msgs: Vec<_> = errors.iter().map(|e| e.message.as_str()).collect();
                    msgs.join("; ")
                });

            // Log GraphQL errors if present
            if let Some(ref errors_str) = error_hint {
                tracing::warn!(
                    pr = %pr,
                    errors = %errors_str,
                    "GraphQL errors in merge state query"
                );
            }

            // If we have no data at all, report the GraphQL errors
            if response.data.is_none() {
                // GitHub marks GraphQL rate limiting with `"type": "RATE_LIMITED"`;
                // the accompanying message is not guaranteed to mention rate limits.
                let rate_limited = response.errors.as_ref().is_some_and(|errors| {
                    errors
                        .iter()
                        .any(|e| e.r#type.as_deref() == Some("RATE_LIMITED"))
                });

                let error_msg = error_hint.unwrap_or_else(|| "no data returned".to_string());
                let full_msg = format!(
                    "GraphQL error querying PR {} merge state: {}",
                    pr, error_msg
                );

                if rate_limited
                    || is_rate_limit_error(&error_msg)
                    || is_transient_message(&error_msg)
                {
                    return Err(GitHubApiError::transient_without_source(full_msg));
                }

                return Err(GitHubApiError::permanent_without_source(full_msg));
            }

            let pr_data = response
                .data
                .and_then(|d| d.repository)
                .and_then(|r| r.pull_request)
                .ok_or_else(|| {
                    let suffix = error_hint
                        .map(|e| format!(" (GraphQL errors: {})", e))
                        .unwrap_or_default();
                    GitHubApiError::permanent_without_source(format!(
                        "PR {} not found{}",
                        pr, suffix
                    ))
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
            let err_str = GitHubApiError::extract_message(&e);
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
    let comments = collect_all_pages(|page_number| async move {
        client
            .inner()
            .issues(client.owner(), client.repo_name())
            .list_comments(pr.0)
            .per_page(100)
            .page(page_number)
            .send()
            .await
    })
    .await?;

    let all_comments = comments
        .into_iter()
        .map(|comment| CommentData {
            id: CommentId(comment.id.into_inner()),
            author_id: comment.user.id.into_inner(),
            body: comment.body.unwrap_or_default(),
        })
        .collect();

    Ok(GitHubResponse::Comments(all_comments))
}

// ─── Repository Settings ──────────────────────────────────────────────────────

/// Checks if an error indicates we should fall back to "unknown" status.
///
/// For branch protection and ruleset queries, 404/403 responses can mean:
/// - No protection/rulesets configured
/// - Insufficient permissions to view settings
/// - Branch/resource doesn't exist
///
/// We cannot distinguish these cases via the API, so we return an "unknown"
/// status and let callers implement "warn and proceed" per DESIGN.md rather
/// than silently treating these as "no protection" or failing hard.
///
/// This is a pure function extracted for testability.
pub fn should_fallback_to_unknown(err: &octocrab::Error) -> bool {
    match err {
        octocrab::Error::GitHub { source, .. } => {
            matches!(source.status_code.as_u16(), 403 | 404)
        }
        _ => false,
    }
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
            if should_fallback_to_unknown(&e) {
                tracing::warn!(
                    branch = %branch,
                    error = %GitHubApiError::extract_message(&e),
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
            if should_fallback_to_unknown(&e) {
                tracing::warn!(
                    error = %GitHubApiError::extract_message(&e),
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

            // A guessed default branch would silently misdirect retargeting
            // and recovery scans; fail like the merge-method fields above.
            let default_branch = repo.default_branch.ok_or_else(|| {
                GitHubApiError::permanent_without_source(
                    "Repository settings missing 'default_branch' field - \
                     cannot determine the cascade target branch",
                )
            })?;

            Ok(GitHubResponse::RepoSettings(RepoSettingsData {
                default_branch,
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
    use crate::github::error::test_support::github_error;
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

    #[tokio::test]
    async fn should_fallback_to_unknown_on_403_and_404_status() {
        // Real GitHub bodies for these statuses need not mention the status
        // or words like "forbidden"; only the structural status matters.
        for (status, message) in [
            (403, "Resource not accessible by integration"),
            (404, "Not Found"),
            (404, "Branch not protected"),
        ] {
            let err = github_error(status, message).await;
            assert!(
                should_fallback_to_unknown(&err),
                "HTTP {} ({}) must fall back to unknown",
                status,
                message
            );
        }
    }

    #[tokio::test]
    async fn should_fallback_to_unknown_rejects_other_statuses() {
        // The 500 case mentions 404/forbidden in the body; a message-sniffing
        // implementation would wrongly swallow it.
        for (status, message) in [
            (401, "Bad credentials"),
            (422, "Validation Failed"),
            (429, "Rate limit exceeded"),
            (500, "upstream returned: 404 forbidden not found"),
            (502, "Bad Gateway"),
        ] {
            let err = github_error(status, message).await;
            assert!(
                !should_fallback_to_unknown(&err),
                "HTTP {} ({}) must not fall back to unknown",
                status,
                message
            );
        }
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

    }

    /// Property: fallback depends only on the structural status code, never
    /// on the message body.
    #[tokio::test]
    async fn prop_fallback_depends_only_on_status() {
        use proptest::strategy::ValueTree;
        use proptest::test_runner::TestRunner;

        let mut runner = TestRunner::default();
        for _ in 0..64 {
            let status = (400u16..600)
                .new_tree(&mut runner)
                .expect("status strategy")
                .current();
            let message = "[ -~]{0,80}"
                .new_tree(&mut runner)
                .expect("message strategy")
                .current();
            let err = github_error(status, &message).await;
            assert_eq!(
                should_fallback_to_unknown(&err),
                matches!(status, 403 | 404),
                "HTTP {} ({:?})",
                status,
                message
            );
        }
    }

    // ─── Mock-API Tests ───────────────────────────────────────────────────────
    //
    // These run the interpreter against a local HTTP server speaking canned
    // responses, exercising the real octocrab request/response path.

    mod mock_api {
        use std::collections::VecDeque;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        use super::super::*;
        use crate::github::GitHubErrorKind;
        use crate::types::RepoId;

        struct CannedResponse {
            status: u16,
            headers: Vec<(String, String)>,
            body: String,
        }

        fn ok_page(body: serde_json::Value, next_link: bool) -> CannedResponse {
            let headers = if next_link {
                vec![(
                    "link".to_string(),
                    r#"<https://api.github.com/x?page=2>; rel="next""#.to_string(),
                )]
            } else {
                Vec::new()
            };
            CannedResponse {
                status: 200,
                headers,
                body: body.to_string(),
            }
        }

        fn error_response(status: u16, message: &str) -> CannedResponse {
            CannedResponse {
                status,
                headers: Vec::new(),
                body: serde_json::json!({ "message": message }).to_string(),
            }
        }

        fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
            haystack.windows(needle.len()).position(|w| w == needle)
        }

        /// Serves the canned responses in order, one per request, counting
        /// requests. Requests beyond the queue get a 400 with a marker message
        /// so tests fail loudly rather than hang.
        async fn spawn_mock_server(responses: Vec<CannedResponse>) -> (String, Arc<AtomicU32>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let base_uri = format!("http://{}", listener.local_addr().expect("local addr"));
            let hits = Arc::new(AtomicU32::new(0));
            let queue = Arc::new(Mutex::new(VecDeque::from(responses)));
            let hits_for_task = hits.clone();

            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    let header_end = loop {
                        let Ok(n) = stream.read(&mut tmp).await else {
                            break None;
                        };
                        if n == 0 {
                            break None;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if let Some(pos) = find_subsequence(&buf, b"\r\n\r\n") {
                            break Some(pos + 4);
                        }
                    };
                    let Some(header_end) = header_end else {
                        continue;
                    };
                    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
                    let content_length: usize = head
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            if name.eq_ignore_ascii_case("content-length") {
                                value.trim().parse().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0);
                    while buf.len() < header_end + content_length {
                        let Ok(n) = stream.read(&mut tmp).await else {
                            break;
                        };
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }

                    hits_for_task.fetch_add(1, Ordering::SeqCst);
                    let response =
                        queue
                            .lock()
                            .expect("queue lock")
                            .pop_front()
                            .unwrap_or(CannedResponse {
                                status: 400,
                                headers: Vec::new(),
                                body: r#"{"message":"mock server: unexpected extra request"}"#
                                    .to_string(),
                            });

                    let mut out = format!(
                        "HTTP/1.1 {} Status\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
                        response.status,
                        response.body.len()
                    );
                    for (name, value) in &response.headers {
                        out.push_str(&format!("{}: {}\r\n", name, value));
                    }
                    out.push_str("\r\n");
                    out.push_str(&response.body);
                    let _ = stream.write_all(out.as_bytes()).await;
                    let _ = stream.shutdown().await;
                }
            });

            (base_uri, hits)
        }

        fn mock_client(base_uri: &str) -> OctocrabClient {
            // octocrab's HTTP-layer retry must be off so that one effect
            // attempt is exactly one request; see `OctocrabClient::new`.
            let octocrab = octocrab::Octocrab::builder()
                .personal_token("test-token")
                .base_uri(base_uri)
                .expect("valid base uri")
                .add_retry_config(octocrab::service::middleware::retry::RetryConfig::None)
                .build()
                .expect("build octocrab");
            OctocrabClient::new(octocrab, RepoId::new("owner", "repo"))
        }

        fn tiny_retry_config() -> RetryConfig {
            RetryConfig::new(2, Duration::from_millis(1), Duration::from_millis(4), 2.0)
        }

        /// Minimal PR object accepted by octocrab's `PullRequest` model.
        fn pr_json(
            number: u64,
            state: &str,
            updated_at: &str,
            merged_at: Option<String>,
        ) -> serde_json::Value {
            serde_json::json!({
                "url": format!("https://example.test/pulls/{number}"),
                "id": number,
                "number": number,
                "state": state,
                "updated_at": updated_at,
                "merged_at": merged_at,
                "merge_commit_sha": merged_at.as_ref().map(|_| format!("{:040x}", number + 1_000_000)),
                "draft": false,
                "head": { "ref": format!("feature-{number}"), "sha": format!("{:040x}", number) },
                "base": { "ref": "main", "sha": format!("{:040x}", number + 2_000_000) },
            })
        }

        fn open_pr_json(number: u64) -> serde_json::Value {
            pr_json(number, "open", "2026-06-01T00:00:00Z", None)
        }

        // ─── Error classification (Task A) ────────────────────────────────────

        #[tokio::test]
        async fn idempotent_effect_retries_transient_500() {
            let (base, hits) = spawn_mock_server(vec![
                error_response(500, "Server Error"),
                error_response(500, "Server Error"),
                error_response(500, "Server Error"),
            ])
            .await;
            let client = mock_client(&base);

            let err = interpret_github_effect(
                &client,
                GitHubEffect::GetPr { pr: PrNumber(1) },
                tiny_retry_config(),
                RetryPolicy::RetryTransient,
            )
            .await
            .expect_err("500 must be an error");

            assert_eq!(err.kind, GitHubErrorKind::Transient);
            assert_eq!(err.status_code, Some(500));
            // Initial attempt + 2 retries.
            assert_eq!(hits.load(Ordering::SeqCst), 3);
        }

        #[tokio::test]
        async fn graphql_rate_limited_type_is_transient() {
            // The RATE_LIMITED type is the robust signal; the message
            // deliberately contains no rate-limit wording.
            let (base, _hits) = spawn_mock_server(vec![CannedResponse {
                status: 200,
                headers: Vec::new(),
                body: serde_json::json!({
                    "data": null,
                    "errors": [{ "message": "throttled", "type": "RATE_LIMITED" }],
                })
                .to_string(),
            }])
            .await;
            let client = mock_client(&base);

            let err = get_merge_state(&client, PrNumber(7))
                .await
                .expect_err("rate-limited query must fail");
            assert_eq!(err.kind, GitHubErrorKind::Transient);
        }

        #[tokio::test]
        async fn branch_protection_403_falls_back_to_unknown() {
            // Real GitHub 403 bodies do not contain "403" or "forbidden".
            let (base, _hits) = spawn_mock_server(vec![error_response(
                403,
                "Resource not accessible by integration",
            )])
            .await;
            let client = mock_client(&base);

            let response = get_branch_protection(&client, "main".to_string())
                .await
                .expect("403 must fall back to unknown");
            assert!(matches!(response, GitHubResponse::BranchProtectionUnknown));
        }

        #[tokio::test]
        async fn rulesets_404_falls_back_to_unknown() {
            let (base, _hits) = spawn_mock_server(vec![error_response(404, "Not Found")]).await;
            let client = mock_client(&base);

            let response = get_rulesets(&client)
                .await
                .expect("404 must fall back to unknown");
            assert!(matches!(response, GitHubResponse::RulesetsUnknown));
        }

        #[tokio::test]
        async fn branch_protection_500_is_not_swallowed() {
            let (base, _hits) = spawn_mock_server(vec![error_response(500, "Server Error")]).await;
            let client = mock_client(&base);

            let err = get_branch_protection(&client, "main".to_string())
                .await
                .expect_err("500 must not fall back to unknown");
            assert_eq!(err.kind, GitHubErrorKind::Transient);
        }

        // ─── Per-effect retry policy (Task B) ─────────────────────────────────

        #[tokio::test]
        async fn post_comment_is_not_retried_on_transient_error() {
            // A lost response after the server processed the request would
            // mean a duplicate comment; one attempt only.
            let (base, hits) = spawn_mock_server(vec![
                error_response(500, "Server Error"),
                error_response(500, "Server Error"),
                error_response(500, "Server Error"),
            ])
            .await;
            let client = mock_client(&base);

            let err = interpret_github_effect(
                &client,
                GitHubEffect::PostComment {
                    pr: PrNumber(1),
                    body: "hello".to_string(),
                },
                tiny_retry_config(),
                RetryPolicy::RetryTransient,
            )
            .await
            .expect_err("500 must be an error");

            assert_eq!(err.kind, GitHubErrorKind::Transient);
            assert_eq!(hits.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn squash_merge_is_not_retried_on_transient_error() {
            let (base, hits) = spawn_mock_server(vec![
                error_response(502, "Bad Gateway"),
                error_response(502, "Bad Gateway"),
                error_response(502, "Bad Gateway"),
            ])
            .await;
            let client = mock_client(&base);

            let err = interpret_github_effect(
                &client,
                GitHubEffect::SquashMerge {
                    pr: PrNumber(1),
                    expected_sha: Sha::parse(format!("{:040x}", 1u64)).expect("valid sha"),
                },
                tiny_retry_config(),
                RetryPolicy::RetryTransient,
            )
            .await
            .expect_err("502 must be an error");

            assert_eq!(err.kind, GitHubErrorKind::Transient);
            assert_eq!(hits.load(Ordering::SeqCst), 1);
        }

        // ─── Pagination (Task C) ──────────────────────────────────────────────

        #[tokio::test]
        async fn list_open_prs_follows_link_header() {
            // Page 1 is short but advertises a next page; the Link header,
            // not the page size, decides whether to continue.
            let page1: Vec<_> = (1..=3).map(open_pr_json).collect();
            let page2: Vec<_> = (4..=5).map(open_pr_json).collect();
            let (base, hits) = spawn_mock_server(vec![
                ok_page(serde_json::Value::Array(page1), true),
                ok_page(serde_json::Value::Array(page2), false),
            ])
            .await;
            let client = mock_client(&base);

            let response = list_open_prs(&client).await.expect("must succeed");
            let GitHubResponse::PrList(prs) = response else {
                panic!("expected PrList");
            };
            assert_eq!(prs.len(), 5);
            assert_eq!(hits.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn list_open_prs_stops_on_absent_link_header() {
            // A full page with no Link header is the last page.
            let page1: Vec<_> = (1..=100).map(open_pr_json).collect();
            let (base, hits) =
                spawn_mock_server(vec![ok_page(serde_json::Value::Array(page1), false)]).await;
            let client = mock_client(&base);

            let response = list_open_prs(&client).await.expect("must succeed");
            let GitHubResponse::PrList(prs) = response else {
                panic!("expected PrList");
            };
            assert_eq!(prs.len(), 100);
            assert_eq!(hits.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn recently_merged_stops_when_page_predates_window() {
            // Results are sorted by `updated` descending and
            // updated_at >= merged_at, so a page whose newest-to-oldest items
            // all predate the window proves later pages cannot contain an
            // in-window merge: stop, and the result is provably complete.
            let page1: Vec<_> = (1..=100)
                .map(|n| pr_json(n, "closed", "2020-01-01T00:00:00Z", None))
                .collect();
            let (base, hits) =
                spawn_mock_server(vec![ok_page(serde_json::Value::Array(page1), true)]).await;
            let client = mock_client(&base);

            let response = list_recently_merged_prs(&client, 7)
                .await
                .expect("must succeed");
            let GitHubResponse::RecentlyMergedPrList {
                prs,
                may_be_incomplete,
            } = response
            else {
                panic!("expected RecentlyMergedPrList");
            };
            assert!(prs.is_empty());
            assert!(!may_be_incomplete);
            assert_eq!(hits.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn recently_merged_reports_incomplete_only_at_page_limit() {
            // Ten short pages of in-window merges, each advertising a next
            // page: MAX_PAGES is genuinely exhausted, so the result must be
            // flagged as possibly incomplete.
            let recent = (Utc::now() - ChronoDuration::hours(1)).to_rfc3339();
            let responses: Vec<_> = (1..=10u64)
                .map(|n| {
                    ok_page(
                        serde_json::Value::Array(vec![pr_json(
                            n,
                            "closed",
                            &recent,
                            Some(recent.clone()),
                        )]),
                        true,
                    )
                })
                .collect();
            let (base, hits) = spawn_mock_server(responses).await;
            let client = mock_client(&base);

            let response = list_recently_merged_prs(&client, 7)
                .await
                .expect("must succeed");
            let GitHubResponse::RecentlyMergedPrList {
                prs,
                may_be_incomplete,
            } = response
            else {
                panic!("expected RecentlyMergedPrList");
            };
            assert_eq!(prs.len(), 10);
            assert!(may_be_incomplete);
            assert_eq!(hits.load(Ordering::SeqCst), 10);
        }

        // ─── Repo settings (Task C) ───────────────────────────────────────────

        #[tokio::test]
        async fn repo_settings_missing_default_branch_is_an_error() {
            let (base, _hits) = spawn_mock_server(vec![CannedResponse {
                status: 200,
                headers: Vec::new(),
                body: serde_json::json!({
                    "id": 1,
                    "name": "repo",
                    "url": "https://example.test/repos/owner/repo",
                    "allow_squash_merge": true,
                    "allow_merge_commit": false,
                    "allow_rebase_merge": false,
                })
                .to_string(),
            }])
            .await;
            let client = mock_client(&base);

            let err = get_repo_settings(&client)
                .await
                .expect_err("missing default_branch must not be defaulted");
            assert_eq!(err.kind, GitHubErrorKind::Permanent);
            assert!(
                err.message.contains("default_branch"),
                "message was: {}",
                err.message
            );
        }

        #[tokio::test]
        async fn repo_settings_complete_succeeds() {
            let (base, _hits) = spawn_mock_server(vec![CannedResponse {
                status: 200,
                headers: Vec::new(),
                body: serde_json::json!({
                    "id": 1,
                    "name": "repo",
                    "url": "https://example.test/repos/owner/repo",
                    "default_branch": "trunk",
                    "allow_squash_merge": true,
                    "allow_merge_commit": false,
                    "allow_rebase_merge": false,
                })
                .to_string(),
            }])
            .await;
            let client = mock_client(&base);

            let response = get_repo_settings(&client).await.expect("must succeed");
            let GitHubResponse::RepoSettings(data) = response else {
                panic!("expected RepoSettings");
            };
            assert_eq!(data.default_branch, "trunk");
        }
    }
}
