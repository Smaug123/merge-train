//! Webhook handling for GitHub events.
//!
//! This module provides:
//! - Signature verification for webhook payloads (HMAC-SHA256)
//! - Future: Event parsing and priority classification

pub mod signature;

pub use signature::{
    compute_signature, format_signature_header, parse_signature_header, verify_signature,
};
