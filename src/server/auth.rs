//! Bearer-token authorization for the state-inspection endpoint.
//!
//! The server's listener must be reachable from the public internet for
//! GitHub to deliver webhooks to it, and `/api/v1/repos/{owner}/{repo}/state`
//! serves a private repository's branch names, SHAs and train topology. So
//! the endpoint answers only a caller presenting the configured token.
//!
//! The configuration is a DU rather than an `Option<String>`: "no token
//! configured" means the endpoint is *off*, not "the endpoint is open", and
//! making that a variant rather than a `None` keeps the handler's match
//! exhaustive — a third mode cannot be added without every caller being
//! told about it.

use std::fmt;

use thiserror::Error;

use crate::webhooks::secrets_match;

/// How the state endpoint authorizes callers.
#[derive(Clone, Debug)]
pub enum StateAuth {
    /// No token is configured: the endpoint serves nothing at all. This is
    /// the default, so an operator who has not thought about the question
    /// does not expose their repositories' state by omission.
    Disabled,

    /// Callers must present `Authorization: Bearer <token>`.
    Bearer(StateToken),
}

/// A configured state-API token.
///
/// Parsed, not validated: constructing one proves the token is non-empty
/// and consists only of bytes that can appear in an HTTP header value, so
/// a token no client could ever present is refused at startup rather than
/// silently rejecting every request.
///
/// Deliberately not `PartialEq`: [`StateToken::matches`] is the only way to
/// compare one, so a caller cannot reach for a `==` that would compare the
/// bytes in variable time.
#[derive(Clone)]
pub struct StateToken(Vec<u8>);

/// Why a configured token was refused.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum InvalidStateToken {
    /// An empty token would be presented as a bare `Authorization: Bearer`,
    /// which [`parse_bearer`] refuses — so it could never authenticate.
    #[error("the state API token is empty")]
    Empty,

    /// Only visible ASCII (0x21..=0x7E) survives a trip through an HTTP
    /// header value unquoted; anything else could never be presented.
    #[error(
        "the state API token contains a byte that cannot appear in an HTTP \
         header value (only visible ASCII is usable)"
    )]
    Unpresentable,
}

impl StateToken {
    /// Parses a configured token, rejecting one that could never be
    /// presented over HTTP.
    pub fn new(token: impl Into<Vec<u8>>) -> Result<Self, InvalidStateToken> {
        let token = token.into();
        if token.is_empty() {
            return Err(InvalidStateToken::Empty);
        }
        if !token.iter().all(|b| (0x21..=0x7e).contains(b)) {
            return Err(InvalidStateToken::Unpresentable);
        }
        Ok(StateToken(token))
    }

    /// Whether `presented` is this token, compared in constant time.
    pub fn matches(&self, presented: &[u8]) -> bool {
        secrets_match(&self.0, presented)
    }
}

/// Redacted: a token must not reach a log through a `{:?}` on the config
/// that holds it.
impl fmt::Debug for StateToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StateToken(<redacted>)")
    }
}

/// Extracts the credential from an `Authorization` header value.
///
/// Returns `None` unless the value is the `Bearer` scheme followed by a
/// non-empty credential. The scheme is matched case-insensitively (RFC 9110
/// §11.1 makes it so); the credential is not.
///
/// # Examples
///
/// ```
/// use merge_train::server::parse_bearer;
///
/// assert_eq!(parse_bearer("Bearer s3cret"), Some("s3cret"));
/// // The scheme is case-insensitive; the credential is not.
/// assert_eq!(parse_bearer("bEaReR s3cret"), Some("s3cret"));
/// // Other schemes, and a scheme with no credential, are refused.
/// assert_eq!(parse_bearer("Basic s3cret"), None);
/// assert_eq!(parse_bearer("Bearer"), None);
/// assert_eq!(parse_bearer("Bearer "), None);
/// ```
pub fn parse_bearer(header: &str) -> Option<&str> {
    let (scheme, credential) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    // RFC 9110 allows more than one space between the scheme and the
    // credential; a credential of nothing but spaces is no credential.
    let credential = credential.trim_start_matches(' ');
    if credential.is_empty() {
        return None;
    }
    Some(credential)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ─── Token parsing ───

    #[test]
    fn empty_token_is_refused() {
        assert_eq!(StateToken::new("").unwrap_err(), InvalidStateToken::Empty);
    }

    #[test]
    fn unpresentable_tokens_are_refused() {
        // Space, tab, newline, DEL and non-ASCII cannot be presented.
        for bad in ["has space", "has\ttab", "has\nnewline", "\x7f", "é"] {
            assert_eq!(
                StateToken::new(bad).unwrap_err(),
                InvalidStateToken::Unpresentable,
                "expected {bad:?} to be refused"
            );
        }
        // A NUL byte, which would otherwise truncate the header.
        assert_eq!(
            StateToken::new(vec![b'a', 0x00, b'b']).unwrap_err(),
            InvalidStateToken::Unpresentable
        );
    }

    #[test]
    fn a_usable_token_is_accepted() {
        let token = StateToken::new("s3cret-token_value.123~+/=").unwrap();
        assert!(token.matches(b"s3cret-token_value.123~+/="));
    }

    #[test]
    fn a_token_does_not_match_a_prefix_or_a_variant() {
        let token = StateToken::new("s3cret").unwrap();
        assert!(!token.matches(b"s3cre"));
        assert!(!token.matches(b"s3crets"));
        assert!(!token.matches(b"S3cret"));
        assert!(!token.matches(b""));
    }

    #[test]
    fn a_token_never_prints_itself() {
        let token = StateToken::new("s3cret").unwrap();
        assert_eq!(format!("{token:?}"), "StateToken(<redacted>)");
        assert!(!format!("{:?}", StateAuth::Bearer(token)).contains("s3cret"));
    }

    // ─── Header parsing ───

    #[test]
    fn bearer_header_variants() {
        assert_eq!(parse_bearer("Bearer abc"), Some("abc"));
        assert_eq!(parse_bearer("bearer abc"), Some("abc"));
        assert_eq!(parse_bearer("BEARER abc"), Some("abc"));
        // Extra spaces between scheme and credential are allowed.
        assert_eq!(parse_bearer("Bearer    abc"), Some("abc"));
        // Trailing content is part of the credential, so it will not match
        // a configured token that lacks it.
        assert_eq!(parse_bearer("Bearer abc def"), Some("abc def"));
    }

    #[test]
    fn non_bearer_headers_are_refused() {
        assert_eq!(parse_bearer(""), None);
        assert_eq!(parse_bearer("abc"), None);
        assert_eq!(parse_bearer("Basic abc"), None);
        assert_eq!(parse_bearer("Bearer"), None);
        assert_eq!(parse_bearer("Bearer "), None);
        assert_eq!(parse_bearer("Bearer    "), None);
        // The token alone, without the scheme, is not a Bearer credential.
        assert_eq!(parse_bearer("s3cret"), None);
    }

    proptest! {
        /// Property: a configured token authorizes exactly the caller who
        /// presents it, and nobody else.
        #[test]
        fn prop_only_the_configured_token_matches(
            configured in "[!-~]{1,64}",
            presented: Vec<u8>,
        ) {
            let token = StateToken::new(configured.clone()).unwrap();
            prop_assert_eq!(token.matches(&presented), presented == configured.as_bytes());
        }

        /// Property: any token accepted by `StateToken::new` round-trips
        /// through a `Bearer` header — construction proves presentability.
        #[test]
        fn prop_an_accepted_token_can_be_presented(configured in "[!-~]{1,64}") {
            let token = StateToken::new(configured.clone()).unwrap();
            let header = format!("Bearer {configured}");
            let presented = parse_bearer(&header).unwrap();
            prop_assert!(token.matches(presented.as_bytes()));
        }

        /// Property: `StateToken::new` accepts a string exactly when every
        /// byte is visible ASCII and there is at least one.
        #[test]
        fn prop_new_accepts_exactly_the_presentable(raw: String) {
            let usable = !raw.is_empty()
                && raw.bytes().all(|b| (0x21..=0x7e).contains(&b));
            prop_assert_eq!(StateToken::new(raw).is_ok(), usable);
        }

        /// Property: header parsing never panics, whatever the client sent.
        #[test]
        fn prop_parse_bearer_never_panics(header: String) {
            let _ = parse_bearer(&header);
        }

        /// Property: whatever `parse_bearer` returns is non-empty and is a
        /// suffix of the header it came from — it invents nothing.
        #[test]
        fn prop_parsed_credential_comes_from_the_header(header: String) {
            if let Some(credential) = parse_bearer(&header) {
                prop_assert!(!credential.is_empty());
                prop_assert!(header.ends_with(credential));
            }
        }
    }
}
