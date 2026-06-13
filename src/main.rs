//! Merge Train Bot - Main entry point.
//!
//! This binary runs the HTTP server that accepts GitHub webhooks and
//! drives the merge train state machine.

use std::net::SocketAddr;
use std::path::PathBuf;

use merge_train::server::{AppState, build_router};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Configuration for the merge train bot, read from environment variables.
struct Config {
    /// Address to bind the HTTP server to.
    listen_addr: SocketAddr,

    /// Directory for spooling webhook deliveries.
    spool_dir: PathBuf,

    /// Directory for persisted repository state.
    state_dir: PathBuf,

    /// Secret for verifying webhook signatures. Non-empty.
    webhook_secret: Vec<u8>,
}

/// Validates the webhook secret read from the environment.
///
/// A missing or empty secret is refused: HMAC with an empty key verifies
/// signatures forged with that same empty key, so starting without a secret
/// would accept arbitrary webhooks.
fn webhook_secret_from(value: Option<String>) -> Result<Vec<u8>, &'static str> {
    match value {
        None => Err("WEBHOOK_SECRET is not set; refusing to start without signature verification"),
        Some(s) if s.is_empty() => {
            Err("WEBHOOK_SECRET is empty; refusing to start without signature verification")
        }
        Some(s) => Ok(s.into_bytes()),
    }
}

impl Config {
    /// Loads configuration from environment variables.
    ///
    /// `LISTEN_ADDR`, `SPOOL_DIR`, and `STATE_DIR` have defaults;
    /// `WEBHOOK_SECRET` is required (see [`webhook_secret_from`]).
    fn from_env() -> Result<Self, &'static str> {
        let listen_addr = std::env::var("LISTEN_ADDR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 3000)));

        let spool_dir = std::env::var("SPOOL_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/spool"));

        let state_dir = std::env::var("STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/state"));

        let webhook_secret = webhook_secret_from(std::env::var("WEBHOOK_SECRET").ok())?;

        Ok(Config {
            listen_addr,
            spool_dir,
            state_dir,
            webhook_secret,
        })
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "merge_train=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = match Config::from_env() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("fatal: {message}");
            std::process::exit(1);
        }
    };

    tracing::info!(
        spool_dir = %config.spool_dir.display(),
        state_dir = %config.state_dir.display(),
        "Starting merge train bot"
    );

    // Create application state
    let app_state = AppState::new(config.spool_dir, config.state_dir, config.webhook_secret);

    // Build router
    let app = build_router(app_state);

    tracing::info!("Listening on {}", config.listen_addr);

    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .expect("Failed to bind to address");

    axum::serve(listener, app)
        .await
        .expect("Server failed to start");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_webhook_secret_is_refused() {
        assert!(webhook_secret_from(None).is_err());
    }

    #[test]
    fn empty_webhook_secret_is_refused() {
        assert!(webhook_secret_from(Some(String::new())).is_err());
    }

    #[test]
    fn nonempty_webhook_secret_is_accepted() {
        assert_eq!(
            webhook_secret_from(Some("s3cret".to_string())).unwrap(),
            b"s3cret"
        );
    }
}
