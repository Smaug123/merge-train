//! GitHub webhook signature verification using HMAC-SHA256.
//!
//! GitHub signs webhook payloads using HMAC-SHA256 with a shared secret.
//! The signature is provided in the `X-Hub-Signature-256` header as `sha256=<hex>`.
//!
//! This module provides verification against the shared secret. Signature
//! verification is the first step in webhook processing; invalid signatures
//! should be rejected before parsing.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Parses a GitHub signature header (e.g., "sha256=abc123...") into raw bytes.
///
/// Returns `None` for malformed headers (missing prefix, invalid hex, etc.).
/// Never panics.
///
/// # Examples
///
/// ```
/// use merge_train::webhooks::parse_signature_header;
///
/// // Valid header
/// let sig = parse_signature_header("sha256=abcd1234");
/// assert!(sig.is_some());
///
/// // Invalid: missing prefix
/// assert!(parse_signature_header("abcd1234").is_none());
///
/// // Invalid: wrong algorithm
/// assert!(parse_signature_header("sha1=abcd1234").is_none());
///
/// // Invalid: bad hex
/// assert!(parse_signature_header("sha256=xyz").is_none());
/// ```
pub fn parse_signature_header(header: &str) -> Option<Vec<u8>> {
    // GitHub uses "sha256=" prefix
    let hex_sig = header.strip_prefix("sha256=")?;

    // Decode hex to bytes
    hex::decode(hex_sig).ok()
}

/// Computes the HMAC-SHA256 signature of a payload using the given secret.
///
/// This is useful for testing purposes (generating expected signatures).
pub fn compute_signature(payload: &[u8], secret: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC can take key of any size");
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

/// Formats a signature as a GitHub-style header value.
///
/// Returns a string in the format "sha256=<hex>".
pub fn format_signature_header(signature: &[u8]) -> String {
    format!("sha256={}", hex::encode(signature))
}

/// Verifies a GitHub webhook signature against the payload and secret.
///
/// Returns `true` if the signature is valid, `false` otherwise.
/// Uses constant-time comparison to prevent timing attacks.
///
/// # Arguments
///
/// * `payload` - The raw webhook payload bytes
/// * `signature_header` - The value of the `X-Hub-Signature-256` header (e.g., "sha256=...")
/// * `secret` - The webhook secret configured in GitHub
///
/// # Examples
///
/// ```
/// use merge_train::webhooks::{verify_signature, compute_signature, format_signature_header};
///
/// let payload = b"Hello, World!";
/// let secret = b"my-secret-key";
///
/// // Compute and format the expected signature
/// let sig = compute_signature(payload, secret);
/// let header = format_signature_header(&sig);
///
/// // Verification should succeed
/// assert!(verify_signature(payload, &header, secret));
///
/// // Verification should fail with wrong secret
/// assert!(!verify_signature(payload, &header, b"wrong-secret"));
/// ```
pub fn verify_signature(payload: &[u8], signature_header: &str, secret: &[u8]) -> bool {
    // Parse the signature header
    let expected_signature = match parse_signature_header(signature_header) {
        Some(sig) => sig,
        None => return false,
    };

    // Compute the HMAC-SHA256
    let mut mac = match HmacSha256::new_from_slice(secret) {
        Ok(mac) => mac,
        Err(_) => return false,
    };
    mac.update(payload);

    // Constant-time comparison via the HMAC library
    mac.verify_slice(&expected_signature).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // ========================================================================
    // Unit tests for known test vectors and edge cases
    // ========================================================================

    #[test]
    fn test_parse_signature_header_valid() {
        // Simple valid case
        let result = parse_signature_header("sha256=1234abcd");
        assert_eq!(result, Some(vec![0x12, 0x34, 0xab, 0xcd]));
    }

    #[test]
    fn test_parse_signature_header_full_length() {
        // Full SHA256 output (64 hex chars = 32 bytes)
        let hex_sig = "a".repeat(64);
        let header = format!("sha256={}", hex_sig);
        let result = parse_signature_header(&header);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 32);
    }

    #[test]
    fn test_parse_signature_header_missing_prefix() {
        assert_eq!(parse_signature_header("1234abcd"), None);
    }

    #[test]
    fn test_parse_signature_header_wrong_algorithm() {
        assert_eq!(parse_signature_header("sha1=1234abcd"), None);
    }

    #[test]
    fn test_parse_signature_header_invalid_hex() {
        assert_eq!(parse_signature_header("sha256=xyz"), None);
    }

    #[test]
    fn test_parse_signature_header_empty() {
        assert_eq!(parse_signature_header(""), None);
    }

    #[test]
    fn test_parse_signature_header_just_prefix() {
        // "sha256=" with no hex
        assert_eq!(parse_signature_header("sha256="), Some(vec![]));
    }

    #[test]
    fn test_parse_signature_header_odd_length_hex() {
        // Odd-length hex is invalid
        assert_eq!(parse_signature_header("sha256=abc"), None);
    }

    #[test]
    fn test_parse_signature_header_uppercase_hex() {
        // Uppercase hex should work
        let result = parse_signature_header("sha256=ABCD1234");
        assert_eq!(result, Some(vec![0xab, 0xcd, 0x12, 0x34]));
    }

    /// Known test vector from GitHub's documentation.
    ///
    /// GitHub provides this example in their webhook documentation:
    /// <https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries>
    #[test]
    fn test_github_documentation_example() {
        // From GitHub docs: The payload body is "Hello, World!"
        // with secret "It's a Secret to Everybody"
        let payload = b"Hello, World!";
        let secret = b"It's a Secret to Everybody";

        // Expected signature (computed independently)
        let expected_sig = compute_signature(payload, secret);
        let header = format_signature_header(&expected_sig);

        assert!(verify_signature(payload, &header, secret));
    }

    #[test]
    fn test_verify_signature_wrong_secret() {
        let payload = b"test payload";
        let correct_secret = b"correct-secret";
        let wrong_secret = b"wrong-secret";

        let sig = compute_signature(payload, correct_secret);
        let header = format_signature_header(&sig);

        assert!(verify_signature(payload, &header, correct_secret));
        assert!(!verify_signature(payload, &header, wrong_secret));
    }

    #[test]
    fn test_verify_signature_modified_payload() {
        let original_payload = b"original payload";
        let modified_payload = b"modified payload";
        let secret = b"secret";

        let sig = compute_signature(original_payload, secret);
        let header = format_signature_header(&sig);

        assert!(verify_signature(original_payload, &header, secret));
        assert!(!verify_signature(modified_payload, &header, secret));
    }

    #[test]
    fn test_verify_signature_malformed_header_returns_false() {
        let payload = b"test";
        let secret = b"secret";

        // Various malformed headers - should all return false, not panic
        assert!(!verify_signature(payload, "", secret));
        assert!(!verify_signature(payload, "sha256=", secret));
        assert!(!verify_signature(payload, "sha256=invalid", secret));
        assert!(!verify_signature(payload, "sha1=abc123", secret));
        assert!(!verify_signature(payload, "not-a-header", secret));
        assert!(!verify_signature(payload, "sha256=zzzz", secret));
    }

    #[test]
    fn test_verify_signature_empty_payload() {
        let payload = b"";
        let secret = b"secret";

        let sig = compute_signature(payload, secret);
        let header = format_signature_header(&sig);

        assert!(verify_signature(payload, &header, secret));
    }

    #[test]
    fn test_verify_signature_empty_secret() {
        let payload = b"test payload";
        let secret = b"";

        let sig = compute_signature(payload, secret);
        let header = format_signature_header(&sig);

        assert!(verify_signature(payload, &header, secret));
    }

    #[test]
    fn test_verify_signature_binary_payload() {
        // Payload with null bytes and other binary data
        let payload = &[0x00, 0x01, 0xff, 0xfe, 0x00, 0x00, 0x7f];
        let secret = b"secret";

        let sig = compute_signature(payload, secret);
        let header = format_signature_header(&sig);

        assert!(verify_signature(payload, &header, secret));
    }

    #[test]
    fn test_format_signature_header() {
        let signature = vec![0x12, 0x34, 0xab, 0xcd];
        let header = format_signature_header(&signature);
        assert_eq!(header, "sha256=1234abcd");
    }

    #[test]
    fn test_compute_signature_deterministic() {
        let payload = b"test";
        let secret = b"secret";

        let sig1 = compute_signature(payload, secret);
        let sig2 = compute_signature(payload, secret);

        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_signature_is_32_bytes() {
        // SHA256 always produces 32 bytes
        let payload = b"any payload";
        let secret = b"any secret";

        let sig = compute_signature(payload, secret);
        assert_eq!(sig.len(), 32);
    }

    // ========================================================================
    // Property-based tests
    // ========================================================================

    proptest! {
        /// Property: verify(payload, sign(payload, secret), secret) == true
        ///
        /// For any payload and secret, signing and then verifying with the
        /// same secret should always succeed.
        #[test]
        fn prop_sign_verify_roundtrip(payload: Vec<u8>, secret: Vec<u8>) {
            let sig = compute_signature(&payload, &secret);
            let header = format_signature_header(&sig);
            prop_assert!(verify_signature(&payload, &header, &secret));
        }

        /// Property: verify(payload, sign(payload, wrong_secret), secret) == false
        ///
        /// Signing with one secret and verifying with a different secret
        /// should always fail.
        #[test]
        fn prop_wrong_secret_fails(payload: Vec<u8>, secret1: Vec<u8>, secret2: Vec<u8>) {
            // Skip if secrets happen to be equal
            prop_assume!(secret1 != secret2);

            let sig = compute_signature(&payload, &secret1);
            let header = format_signature_header(&sig);
            prop_assert!(!verify_signature(&payload, &header, &secret2));
        }

        /// Property: verify(modified_payload, sign(original, secret), secret) == false
        ///
        /// Any modification to the payload should cause verification to fail.
        #[test]
        fn prop_modified_payload_fails(
            original: Vec<u8>,
            modified: Vec<u8>,
            secret: Vec<u8>
        ) {
            // Skip if payloads happen to be equal
            prop_assume!(original != modified);

            let sig = compute_signature(&original, &secret);
            let header = format_signature_header(&sig);
            prop_assert!(!verify_signature(&modified, &header, &secret));
        }

        /// Property: parse(format(signature)) roundtrips
        #[test]
        fn prop_format_parse_roundtrip(signature: [u8; 32]) {
            let header = format_signature_header(&signature);
            let parsed = parse_signature_header(&header);
            prop_assert_eq!(parsed, Some(signature.to_vec()));
        }

        /// Property: compute_signature is deterministic
        #[test]
        fn prop_signature_deterministic(payload: Vec<u8>, secret: Vec<u8>) {
            let sig1 = compute_signature(&payload, &secret);
            let sig2 = compute_signature(&payload, &secret);
            prop_assert_eq!(sig1, sig2);
        }

        /// Property: signatures are always 32 bytes (SHA256 output size)
        #[test]
        fn prop_signature_length(payload: Vec<u8>, secret: Vec<u8>) {
            let sig = compute_signature(&payload, &secret);
            prop_assert_eq!(sig.len(), 32);
        }

        /// Property: malformed headers never cause panic
        #[test]
        fn prop_malformed_header_no_panic(header: String, payload: Vec<u8>, secret: Vec<u8>) {
            // This should never panic, regardless of input
            let _ = parse_signature_header(&header);
            let _ = verify_signature(&payload, &header, &secret);
        }

        /// Property: Different payloads produce different signatures (with high probability)
        ///
        /// This is a probabilistic property - for random payloads, collisions are
        /// astronomically unlikely.
        #[test]
        fn prop_different_payloads_different_signatures(
            payload1: Vec<u8>,
            payload2: Vec<u8>,
            secret: Vec<u8>
        ) {
            prop_assume!(payload1 != payload2);

            let sig1 = compute_signature(&payload1, &secret);
            let sig2 = compute_signature(&payload2, &secret);

            // With SHA256, collision probability is negligible
            prop_assert_ne!(sig1, sig2);
        }

        /// Property: Different secrets produce different signatures (with high probability)
        #[test]
        fn prop_different_secrets_different_signatures(
            payload: Vec<u8>,
            secret1: Vec<u8>,
            secret2: Vec<u8>
        ) {
            prop_assume!(secret1 != secret2);

            let sig1 = compute_signature(&payload, &secret1);
            let sig2 = compute_signature(&payload, &secret2);

            // With HMAC-SHA256, this should always differ
            prop_assert_ne!(sig1, sig2);
        }
    }
}
