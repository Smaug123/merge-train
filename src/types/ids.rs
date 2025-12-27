//! Newtype wrappers for domain identifiers.
//!
//! These types prevent accidental mixing of different ID types (e.g., using a CommentId
//! where a PrNumber is expected) and make the code more self-documenting.

use serde::{Deserialize, Serialize};
use std::fmt;

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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sha(pub String);

impl Sha {
    /// Creates a new Sha from a string.
    ///
    /// Note: This does not validate the format. Valid SHAs are 40 hex characters.
    pub fn new(s: impl Into<String>) -> Self {
        Sha(s.into())
    }

    /// Returns the SHA as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns a short (7-character) version of the SHA for display.
    pub fn short(&self) -> &str {
        // Use get() to avoid panic if string contains non-ASCII (shouldn't happen
        // for valid SHAs, but can occur via Sha::new or Deserialize on bad input).
        self.0.get(..7).unwrap_or(&self.0)
    }
}

impl fmt::Display for Sha {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for Sha {
    fn from(s: String) -> Self {
        Sha(s)
    }
}

impl From<&str> for Sha {
    fn from(s: &str) -> Self {
        Sha(s.to_string())
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

/// A GitHub webhook delivery ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeliveryId(pub String);

impl DeliveryId {
    pub fn new(s: impl Into<String>) -> Self {
        DeliveryId(s.into())
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

impl From<String> for DeliveryId {
    fn from(s: String) -> Self {
        DeliveryId(s)
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
                let sha = Sha::new(&s);
                let json = serde_json::to_string(&sha).unwrap();
                let parsed: Sha = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(sha, parsed);
            }

            #[test]
            fn short_returns_7_chars(s in "[0-9a-f]{40}") {
                let sha = Sha::new(&s);
                prop_assert_eq!(sha.short().len(), 7);
                prop_assert_eq!(sha.short(), &s[..7]);
            }

            #[test]
            fn comparison_matches_underlying(a in "[0-9a-f]{40}", b in "[0-9a-f]{40}") {
                let sha_a = Sha::new(&a);
                let sha_b = Sha::new(&b);
                prop_assert_eq!(sha_a == sha_b, a == b);
            }
        }

        #[test]
        fn short_handles_short_input() {
            let sha = Sha::new("abc");
            assert_eq!(sha.short(), "abc");
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
                let id = DeliveryId::new(&s);
                let json = serde_json::to_string(&id).unwrap();
                let parsed: DeliveryId = serde_json::from_str(&json).unwrap();
                prop_assert_eq!(id, parsed);
            }
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
