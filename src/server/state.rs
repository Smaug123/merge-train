//! State inspection endpoint for observability.
//!
//! Returns a repository's materialized state as JSON, read from its SQLite
//! `Store` via a `query_only` connection ([`Store::read_snapshot`]). WAL lets
//! this read run concurrently with the owning worker's writer without blocking
//! it; the server never opens the worker's read-write `Store` (plan P1-H).
//!
//! # Authorization
//!
//! The listener has to be reachable from the internet for GitHub to deliver
//! webhooks to it, and this endpoint serves a private repository's branch
//! names, SHAs and train topology. So it answers only a caller presenting
//! the configured bearer token, and serves nothing at all when no token is
//! configured (see [`crate::server::auth`]).
//!
//! Authorization is decided *before* the path is validated and before the
//! store is touched, so an unauthenticated caller cannot tell an existing
//! repository from an absent one, nor reach any code behind the endpoint.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use thiserror::Error;

use super::auth::{StateAuth, parse_bearer};
use super::{AppState, InvalidPathComponent, validate_path_component};
use crate::persistence::snapshot::PersistedRepoSnapshot;
use crate::store::{Store, StoreError};

/// Errors that can occur when fetching state.
#[derive(Debug, Error)]
pub enum StateError {
    /// No token is configured, so the endpoint serves nothing. Reported as
    /// a 404: to a caller, an endpoint that is switched off is an endpoint
    /// that is not there.
    #[error("the state API is disabled; set STATE_API_TOKEN on the bot to enable it")]
    Disabled,

    /// The caller presented no `Authorization` header.
    #[error("missing Authorization header")]
    MissingAuthorization,

    /// The caller presented something that is not the configured token —
    /// a different token, or a credential this endpoint cannot read.
    ///
    /// One variant for both: telling a caller *why* their credential was
    /// refused tells them how close they are to a good one.
    #[error("invalid credential")]
    InvalidCredential,

    /// No state DB exists for the repository.
    #[error("repository state not found: {owner}/{repo}")]
    NotFound { owner: String, repo: String },

    /// Reading the repo's `Store` failed.
    #[error("store read error: {0}")]
    Store(#[from] StoreError),

    /// Invalid path component (e.g., path traversal attempt).
    #[error("{0}")]
    InvalidPath(#[from] InvalidPathComponent),
}

impl IntoResponse for StateError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            StateError::Disabled | StateError::NotFound { .. } => {
                (StatusCode::NOT_FOUND, self.to_string())
            }
            StateError::MissingAuthorization | StateError::InvalidCredential => {
                // RFC 9110 §11.6.1: a 401 names the scheme to use.
                return (
                    StatusCode::UNAUTHORIZED,
                    [(WWW_AUTHENTICATE, "Bearer")],
                    self.to_string(),
                )
                    .into_response();
            }
            StateError::Store(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
            StateError::InvalidPath(_) => (StatusCode::BAD_REQUEST, self.to_string()),
        };

        (status, message).into_response()
    }
}

/// Decides whether a caller may read state, from the configured
/// authorization and the request's headers alone.
///
/// A function over data: no IO, no state, so the whole rule is one
/// exhaustive match you can read in one go.
fn authorize(auth: &StateAuth, headers: &HeaderMap) -> Result<(), StateError> {
    let token = match auth {
        StateAuth::Disabled => return Err(StateError::Disabled),
        StateAuth::Bearer(token) => token,
    };

    let header = headers
        .get(AUTHORIZATION)
        .ok_or(StateError::MissingAuthorization)?;
    // A header value that is not visible ASCII cannot be the token: one is
    // rejected at startup unless every byte of it is (`StateToken::new`).
    let header = header.to_str().map_err(|_| StateError::InvalidCredential)?;
    let presented = parse_bearer(header).ok_or(StateError::InvalidCredential)?;

    if token.matches(presented.as_bytes()) {
        Ok(())
    } else {
        Err(StateError::InvalidCredential)
    }
}

/// State inspection handler.
///
/// Returns the current materialized state of a repository as JSON.
///
/// # Path Parameters
///
/// - `owner` - The repository owner
/// - `repo` - The repository name
///
/// # Response
///
/// - 200 OK with the repository's [`PersistedRepoSnapshot`] as JSON
/// - 401 Unauthorized if the bearer token is missing or wrong
/// - 404 Not Found if the endpoint is disabled, or no state DB exists for
///   the repository
/// - 400 Bad Request for an invalid owner/repo path component
/// - 500 Internal Server Error for a store read failure
pub async fn state_handler(
    State(app_state): State<AppState>,
    Path((owner, repo)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Json<PersistedRepoSnapshot>, StateError> {
    // Before anything else: an unauthorized caller learns nothing, not even
    // whether the repository exists.
    authorize(app_state.state_auth(), &headers)?;

    // Validate path components to prevent path traversal attacks.
    validate_path_component(&owner)?;
    validate_path_component(&repo)?;

    let db_path = app_state
        .state_dir()
        .join(&owner)
        .join(&repo)
        .join("state.db");

    // A single indexed-row read over a `query_only` connection (fast enough to
    // run inline; the heavy single-writer work stays on the worker thread).
    match Store::read_snapshot(&db_path)? {
        Some(snapshot) => Ok(Json(snapshot)),
        None => Err(StateError::NotFound { owner, repo }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use proptest::prelude::*;

    use super::super::auth::StateToken;

    /// Builds a header map carrying `value` as the `Authorization` header,
    /// or an empty one when there is nothing to carry.
    fn headers_with(value: Option<&str>) -> Option<HeaderMap> {
        let mut headers = HeaderMap::new();
        if let Some(value) = value {
            headers.insert(AUTHORIZATION, HeaderValue::from_str(value).ok()?);
        }
        Some(headers)
    }

    #[test]
    fn a_disabled_endpoint_refuses_everyone() {
        let credentials = [None, Some("Bearer anything"), Some("Basic anything")];
        for credential in credentials {
            let headers = headers_with(credential).unwrap();
            assert!(matches!(
                authorize(&StateAuth::Disabled, &headers),
                Err(StateError::Disabled)
            ));
        }
    }

    #[test]
    fn a_missing_header_is_distinguished_from_a_bad_one() {
        let auth = StateAuth::Bearer(StateToken::new("s3cret").unwrap());

        assert!(matches!(
            authorize(&auth, &headers_with(None).unwrap()),
            Err(StateError::MissingAuthorization)
        ));
        assert!(matches!(
            authorize(&auth, &headers_with(Some("Bearer wrong")).unwrap()),
            Err(StateError::InvalidCredential)
        ));
    }

    /// A header value that is not visible ASCII cannot be the token, since
    /// `StateToken::new` accepts nothing else — so this is a refusal, not a
    /// panic on `to_str`.
    #[test]
    fn a_non_ascii_header_is_refused_not_a_panic() {
        let auth = StateAuth::Bearer(StateToken::new("s3cret").unwrap());
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_bytes(b"Bearer \xff\xfe").unwrap(),
        );
        assert!(matches!(
            authorize(&auth, &headers),
            Err(StateError::InvalidCredential)
        ));
    }

    /// A configured token paired with a credential drawn from around it:
    /// the exact credential, near misses of it, and unrelated strings.
    fn credentials_around_a_token() -> impl Strategy<Value = (String, Option<String>)> {
        "[!-~]{1,32}".prop_flat_map(|configured| {
            let exact = format!("Bearer {configured}");
            let variants = vec![
                // The credential that must work, and case variants of the
                // scheme (which is case-insensitive) and of the token
                // (which is not).
                Some(exact.clone()),
                Some(format!("bearer {configured}")),
                Some(format!("BEARER {configured}")),
                Some(format!("Bearer  {configured}")),
                Some(format!("Bearer {}", configured.to_ascii_uppercase())),
                Some(format!("Bearer {}", configured.to_ascii_lowercase())),
                // Near misses: a byte added, a byte removed, whitespace.
                Some(format!("Bearer {configured}x")),
                Some(format!("Bearer x{configured}")),
                Some(format!("Bearer {}", &configured[..configured.len() - 1])),
                Some(format!("Bearer {configured} ")),
                // The right token under the wrong scheme, or none at all.
                Some(format!("Basic {configured}")),
                Some(format!("Token {configured}")),
                Some(configured.clone()),
                Some("Bearer".to_owned()),
                Some("Bearer ".to_owned()),
                Some(String::new()),
                None,
            ];
            (Just(configured), prop::sample::select(variants).boxed())
        })
    }

    proptest! {
        /// Property: with a token configured, a caller is authorized exactly
        /// when the `Authorization` header is the `Bearer` scheme carrying
        /// that token — no other header, and no other credential, gets in.
        ///
        /// The credentials are drawn *around* the configured token — the
        /// token itself, the token under another scheme, the token with a
        /// byte added or removed, the token's case flipped — because a
        /// purely random header would almost never be a near miss, and near
        /// misses are where an authorization check goes wrong.
        #[test]
        fn prop_authorized_exactly_by_the_configured_bearer_token(
            (configured, presented) in credentials_around_a_token(),
        ) {
            let auth = StateAuth::Bearer(StateToken::new(configured.clone()).unwrap());
            let Some(headers) = headers_with(presented.as_deref()) else {
                return Ok(());
            };

            let expected = presented
                .as_deref()
                .and_then(parse_bearer)
                .is_some_and(|credential| credential == configured);

            prop_assert_eq!(authorize(&auth, &headers).is_ok(), expected);
        }

        /// Property: the caller who presents the configured token is always
        /// let in, whatever the token looks like.
        #[test]
        fn prop_the_configured_token_always_authorizes(configured in "[!-~]{1,32}") {
            let auth = StateAuth::Bearer(StateToken::new(configured.clone()).unwrap());
            let headers = headers_with(Some(&format!("Bearer {configured}"))).unwrap();
            prop_assert!(authorize(&auth, &headers).is_ok());
        }

        /// Property: disabling the endpoint overrides every credential.
        #[test]
        fn prop_disabled_refuses_every_credential(presented in prop::option::of("[ -~]{0,48}")) {
            let Some(headers) = headers_with(presented.as_deref()) else {
                return Ok(());
            };
            prop_assert!(matches!(
                authorize(&StateAuth::Disabled, &headers),
                Err(StateError::Disabled)
            ));
        }
    }
}
