//! Newtype wrappers for domain identifiers.
//!
//! These types prevent accidental mixing of different ID types (e.g., using a CommentId
//! where a PrNumber is expected) and make the code more self-documenting.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Error returned when parsing an invalid SHA.
#[derive(Debug, Clone, Error)]
#[error("invalid SHA: expected 40 hex characters, got {len} bytes: {preview}")]
pub struct InvalidSha {
    len: usize,
    preview: String,
}

/// A pull request number within a repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrNumber(pub u64);

impl fmt::Display for PrNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.0)
    }
}

impl From<u64> for PrNumber {
    fn from(n: u64) -> Self {
        PrNumber(n)
    }
}

/// A git commit SHA (40 hex characters).
///
/// This type guarantees that the contained string is exactly 40 lowercase hex characters.
/// Construction is only possible via `Sha::parse`, which validates the input.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct Sha(String);

impl Sha {
    /// Parses a string as a SHA, validating that it is exactly 40 hex characters.
    ///
    /// Returns an error if the input is not a valid SHA.
    pub fn parse(s: impl Into<String>) -> Result<Self, InvalidSha> {
        let s = s.into();
        if s.len() == 40 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            // Normalize to lowercase for consistent comparison
            Ok(Sha(s.to_ascii_lowercase()))
        } else {
            Err(InvalidSha {
                len: s.len(),
                preview: s.chars().take(20).collect(),
            })
        }
    }

    /// Returns the SHA as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns a short (7-character) version of the SHA for display.
    pub fn short(&self) -> &str {
        &self.0[..7]
    }
}

impl fmt::Display for Sha {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for Sha {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Sha::parse(s).map_err(serde::de::Error::custom)
    }
}

/// A repository identifier (owner/repo format).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RepoId {
    pub owner: String,
    pub repo: String,
}

impl RepoId {
    pub fn new(owner: impl Into<String>, repo: impl Into<String>) -> Self {
        RepoId {
            owner: owner.into(),
            repo: repo.into(),
        }
    }
}

impl fmt::Display for RepoId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.owner, self.repo)
    }
}

/// Error returned when parsing an invalid delivery ID.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid delivery ID: {reason}")]
pub struct InvalidDeliveryId {
    reason: &'static str,
}

/// Maximum length of a delivery ID in bytes.
///
/// The delivery ID is used as a filename stem (`<id>.json.proc` is the longest
/// derived name), and most filesystems cap filenames at 255 bytes. GitHub
/// delivery IDs are 36-character UUIDs, so this limit is generous.
const MAX_DELIVERY_ID_LEN: usize = 245;

/// A GitHub webhook delivery ID (the `X-GitHub-Delivery` header value).
///
/// This type guarantees that the contained string is safe to use as a
/// filesystem path component: non-empty, bounded length, no path separators
/// or null bytes, and no leading dot. Construction is only possible via
/// [`DeliveryId::parse`], which validates the input; deserialization runs
/// the same validation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct DeliveryId(String);

impl DeliveryId {
    /// Parses a string as a delivery ID, validating path safety.
    ///
    /// Rejects:
    /// - Empty strings
    /// - Strings longer than 245 bytes (filename length safety)
    /// - Strings containing `/`, `\`, or null bytes (path traversal)
    /// - Strings starting with `.` (hidden files; also covers `.` and `..`)
    pub fn parse(s: impl Into<String>) -> Result<Self, InvalidDeliveryId> {
        let s = s.into();
        if s.is_empty() {
            return Err(InvalidDeliveryId {
                reason: "empty string",
            });
        }
        if s.len() > MAX_DELIVERY_ID_LEN {
            return Err(InvalidDeliveryId {
                reason: "longer than 245 bytes",
            });
        }
        if s.contains('/') || s.contains('\\') || s.contains('\0') {
            return Err(InvalidDeliveryId {
                reason: "contains path separator or null byte",
            });
        }
        if s.starts_with('.') {
            return Err(InvalidDeliveryId {
                reason: "starts with a dot",
            });
        }
        Ok(DeliveryId(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DeliveryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'de> Deserialize<'de> for DeliveryId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DeliveryId::parse(s).map_err(serde::de::Error::custom)
    }
}

/// A GitHub comment ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CommentId(pub u64);

impl fmt::Display for CommentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for CommentId {
    fn from(n: u64) -> Self {
        CommentId(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod pr_number {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn serde_roundtrip(n: u64) {
                let pr = PrNumber(n);
                let json = serde_json::to_string(&pr).unwrap();
                let parsed: PrNumber = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(pr, parsed);
            }

            #[test]
            fn display_format(n: u64) {
                let pr = PrNumber(n);
                prop_assert_eq!(format!("{}", pr), format!("#{}", n));
            }

            #[test]
            fn comparison_matches_underlying(a: u64, b: u64) {
                let pr_a = PrNumber(a);
                let pr_b = PrNumber(b);
                prop_assert_eq!(pr_a == pr_b, a == b);
            }
        }
    }

    mod sha {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn serde_roundtrip(s in "[0-9a-f]{40}") {
                let sha = Sha::parse(&s).unwrap();
                let json = serde_json::to_string(&sha).unwrap();
                let parsed: Sha = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(sha, parsed);
            }

            #[test]
            fn short_returns_7_chars(s in "[0-9a-f]{40}") {
                let sha = Sha::parse(&s).unwrap();
                prop_assert_eq!(sha.short().len(), 7);
                prop_assert_eq!(sha.short(), &s[..7]);
            }

            #[test]
            fn comparison_matches_underlying(a in "[0-9a-f]{40}", b in "[0-9a-f]{40}") {
                let sha_a = Sha::parse(&a).unwrap();
                let sha_b = Sha::parse(&b).unwrap();
                prop_assert_eq!(sha_a == sha_b, a == b);
            }

            #[test]
            fn parse_rejects_invalid_length(s in "[0-9a-f]{0,39}|[0-9a-f]{41,80}") {
                prop_assert!(Sha::parse(&s).is_err());
            }

            #[test]
            fn parse_rejects_non_hex(s in "[0-9a-f]{39}[g-z]") {
                prop_assert!(Sha::parse(&s).is_err());
            }

            #[test]
            fn parse_normalizes_to_lowercase(s in "[0-9A-Fa-f]{40}") {
                let sha = Sha::parse(&s).unwrap();
                prop_assert_eq!(sha.as_str(), s.to_ascii_lowercase());
            }
        }

        #[test]
        fn deserialize_rejects_invalid_sha() {
            let json = r#""not-a-valid-sha""#;
            let result: Result<Sha, _> = serde_json::from_str(json);
            assert!(result.is_err());
        }
    }

    mod repo_id {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn serde_roundtrip(
                owner in "[a-zA-Z][a-zA-Z0-9-]{0,38}",
                repo in "[a-zA-Z][a-zA-Z0-9_-]{0,99}"
            ) {
                let id = RepoId::new(&owner, &repo);
                let json = serde_json::to_string(&id).unwrap();
                let parsed: RepoId = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(id, parsed);
            }

            #[test]
            fn display_format(
                owner in "[a-zA-Z][a-zA-Z0-9-]{0,38}",
                repo in "[a-zA-Z][a-zA-Z0-9_-]{0,99}"
            ) {
                let id = RepoId::new(&owner, &repo);
                prop_assert_eq!(format!("{}", id), format!("{}/{}", owner, repo));
            }
        }
    }

    mod delivery_id {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn serde_roundtrip(s in "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}") {
                let id = DeliveryId::parse(&s).unwrap();
                let json = serde_json::to_string(&id).unwrap();
                let parsed: DeliveryId = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(id, parsed);
            }

            /// Any string containing a path separator is rejected.
            #[test]
            fn rejects_path_separators(
                prefix in "[a-zA-Z0-9-]{0,10}",
                suffix in "[a-zA-Z0-9-]{0,10}",
                separator in prop::sample::select(vec!['/', '\\']),
            ) {
                let s = format!("{}{}{}", prefix, separator, suffix);
                prop_assert!(DeliveryId::parse(&s).is_err());
            }

            /// Any string starting with a dot is rejected.
            #[test]
            fn rejects_leading_dot(suffix in "[a-zA-Z0-9-]{0,20}") {
                let s = format!(".{}", suffix);
                prop_assert!(DeliveryId::parse(&s).is_err());
            }

            /// Valid UUID-format delivery IDs are always accepted.
            #[test]
            fn accepts_uuids(s in "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}") {
                let id = DeliveryId::parse(&s).unwrap();
                prop_assert_eq!(id.as_str(), s);
            }
        }

        #[test]
        fn rejects_malformed_ids() {
            assert!(DeliveryId::parse("").is_err());
            assert!(DeliveryId::parse(".").is_err());
            assert!(DeliveryId::parse("..").is_err());
            assert!(DeliveryId::parse(".hidden").is_err());
            assert!(DeliveryId::parse("/etc/passwd").is_err());
            assert!(DeliveryId::parse("../../../etc/passwd").is_err());
            assert!(DeliveryId::parse("..\\..\\windows").is_err());
            assert!(DeliveryId::parse("id\0null").is_err());
            assert!(DeliveryId::parse("x".repeat(246)).is_err());
        }

        #[test]
        fn accepts_boundary_length() {
            assert!(DeliveryId::parse("x".repeat(245)).is_ok());
        }

        #[test]
        fn deserialize_rejects_invalid() {
            let result: Result<DeliveryId, _> = serde_json::from_str(r#""../escape""#);
            assert!(result.is_err());
        }
    }

    mod comment_id {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn serde_roundtrip(n: u64) {
                let id = CommentId(n);
                let json = serde_json::to_string(&id).unwrap();
                let parsed: CommentId = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(id, parsed);
            }

            #[test]
            fn comparison_matches_underlying(a: u64, b: u64) {
                let id_a = CommentId(a);
                let id_b = CommentId(b);
                prop_assert_eq!(id_a == id_b, a == b);
            }
        }
    }
}
