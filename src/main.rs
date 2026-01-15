//! Merge Train Bot - Main entry point.
//!
//! This binary runs the HTTP server that accepts GitHub webhooks and
//! drives the merge train state machine.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use merge_train::server::{AppState, build_router};
use merge_train::worker::{Dispatcher, DispatcherConfig};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Configuration for the merge train bot.
///
/// In a production deployment, these would come from environment variables
/// or a configuration file. For now, we use sensible defaults.
struct Config {
    /// Address to bind the HTTP server to.
    listen_addr: SocketAddr,

    /// Directory for spooling webhook deliveries.
    spool_dir: PathBuf,

    /// Directory for persisted repository state.
    state_dir: PathBuf,

    /// Secret for verifying webhook signatures.
    webhook_secret: Vec<u8>,
}

impl Config {
    /// Loads configuration from environment variables with defaults.
    fn from_env() -> Self {
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

        let webhook_secret = std::env::var("WEBHOOK_SECRET")
            .map(|s| s.into_bytes())
            .unwrap_or_else(|_| {
                tracing::warn!(
                    "WEBHOOK_SECRET not set, using empty secret (INSECURE - for development only)"
                );
                Vec::new()
            });

        Config {
            listen_addr,
            spool_dir,
            state_dir,
            webhook_secret,
        }
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

    // Load configuration
    let config = Config::from_env();

    tracing::info!(
        spool_dir = %config.spool_dir.display(),
        state_dir = %config.state_dir.display(),
        "Starting merge train bot"
    );

    // Create shutdown token for graceful shutdown
    let shutdown = CancellationToken::new();

    // Create dispatcher for worker management
    let dispatcher_config = DispatcherConfig::new(&config.spool_dir, &config.state_dir);
    let dispatcher = Dispatcher::new_with_shutdown(dispatcher_config, shutdown.clone());
    let dispatcher = Arc::new(dispatcher);

    // Spawn dispatcher background task (handles periodic polling)
    let dispatcher_for_task = Arc::clone(&dispatcher);
    let dispatcher_task = tokio::spawn(async move {
        dispatcher_for_task.run().await;
    });

    // Create application state with dispatcher
    let app_state = AppState::new_with_dispatcher(
        config.spool_dir,
        config.state_dir,
        config.webhook_secret,
        Arc::clone(&dispatcher),
    );

    // Build router
    let app = build_router(app_state);

    tracing::info!("Listening on {}", config.listen_addr);

    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .expect("Failed to bind to address");

    // Run server with graceful shutdown
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        tracing::info!("Shutdown signal received, stopping gracefully...");
        shutdown_signal.cancel();
    });

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await
        .expect("Server failed to start");

    // Wait for dispatcher to finish
    tracing::info!("Waiting for dispatcher to shut down...");
    let _ = dispatcher_task.await;
    tracing::info!("Shutdown complete");
}
