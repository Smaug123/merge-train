//! Merge Train Bot - Main entry point.
//!
//! This binary runs the HTTP server that accepts GitHub webhooks and
//! drives the merge train state machine.

use std::net::SocketAddr;
use std::path::PathBuf;

use merge_train::git::CommitIdentity;
use merge_train::server::{AppState, build_router};
use merge_train::worker::{GitHubBackend, SharedDeps};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Configuration for the merge train bot, read from environment variables.
struct Config {
    /// Address to bind the HTTP server to.
    listen_addr: SocketAddr,

    /// Root for per-repo state DBs (`<state_dir>/<owner>/<repo>/state.db`).
    state_dir: PathBuf,

    /// Root for per-repo git clones and worktrees.
    repos_dir: PathBuf,

    /// Secret for verifying webhook signatures. Non-empty.
    webhook_secret: Vec<u8>,

    /// GitHub token (PAT or installation token). Non-empty.
    github_token: String,

    /// Overrides for the git commit identity (defaults derive from the bot's
    /// GitHub identity at startup).
    git_user_name: Option<String>,
    git_user_email: Option<String>,
    git_signing_key: Option<String>,
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

/// Validates the GitHub token read from the environment. Required: the worker
/// executes real API effects (squash merges!) — there is no unauthenticated
/// mode.
fn github_token_from(value: Option<String>) -> Result<String, &'static str> {
    match value {
        None => Err("GITHUB_TOKEN is not set; the bot cannot run without GitHub access"),
        Some(s) if s.is_empty() => Err("GITHUB_TOKEN is empty"),
        Some(s) => Ok(s),
    }
}

impl Config {
    /// Loads configuration from environment variables.
    ///
    /// `LISTEN_ADDR`, `STATE_DIR`, and `REPOS_DIR` have defaults;
    /// `WEBHOOK_SECRET` and `GITHUB_TOKEN` are required.
    fn from_env() -> Result<Self, &'static str> {
        let listen_addr = std::env::var("LISTEN_ADDR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 3000)));

        let state_dir = std::env::var("STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/state"));

        let repos_dir = std::env::var("REPOS_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/repos"));

        let webhook_secret = webhook_secret_from(std::env::var("WEBHOOK_SECRET").ok())?;
        let github_token = github_token_from(std::env::var("GITHUB_TOKEN").ok())?;

        Ok(Config {
            listen_addr,
            state_dir,
            repos_dir,
            webhook_secret,
            github_token,
            git_user_name: std::env::var("GIT_USER_NAME").ok(),
            git_user_email: std::env::var("GIT_USER_EMAIL").ok(),
            git_signing_key: std::env::var("GIT_SIGNING_KEY").ok(),
        })
    }
}

/// The bot's GitHub identity, fetched once at startup (resolved design
/// question 3: `GET /user`, fail fast — no env/config sourcing).
struct BotIdentity {
    user_id: u64,
    login: String,
}

async fn fetch_bot_identity(octocrab: &octocrab::Octocrab) -> Result<BotIdentity, String> {
    let user = octocrab
        .current()
        .user()
        .await
        .map_err(|e| format!("cannot fetch the bot's identity (GET /user): {e}"))?;
    Ok(BotIdentity {
        user_id: user.id.into_inner(),
        login: user.login,
    })
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

    let octocrab = match octocrab::Octocrab::builder()
        .personal_token(config.github_token.clone())
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            eprintln!("fatal: cannot build the GitHub client: {e}");
            std::process::exit(1);
        }
    };

    // The bot's identity gates command parsing (its mention name) and
    // status-comment recovery (its user id); refuse to start without it.
    let identity = match fetch_bot_identity(&octocrab).await {
        Ok(identity) => identity,
        Err(message) => {
            eprintln!("fatal: {message}");
            std::process::exit(1);
        }
    };

    tracing::info!(
        state_dir = %config.state_dir.display(),
        repos_dir = %config.repos_dir.display(),
        bot = %identity.login,
        bot_user_id = identity.user_id,
        "Starting merge train bot"
    );

    let commit_identity = CommitIdentity {
        name: config
            .git_user_name
            .unwrap_or_else(|| identity.login.clone()),
        email: config.git_user_email.unwrap_or_else(|| {
            format!("{}@users.noreply.github.com", identity.login)
        }),
        signing_key: config.git_signing_key,
    };

    let deps = SharedDeps {
        github: GitHubBackend::Octocrab {
            client: octocrab,
            handle: tokio::runtime::Handle::current(),
        },
        repos_dir: config.repos_dir,
        commit_identity,
        worktree_max_age: std::time::Duration::from_secs(24 * 60 * 60),
        clone_url_base: Some(format!(
            "https://x-access-token:{}@github.com",
            config.github_token
        )),
        bot_user_id: identity.user_id,
        bot_name: identity.login,
    };

    // Create application state
    let app_state = AppState::new(config.state_dir, config.webhook_secret, deps);

    // Spawn workers for repos with deliveries left queued by a previous run, so
    // an acked-but-unprocessed delivery is drained at startup rather than
    // waiting for the next webhook to that repo.
    app_state.workers().recover_existing().await;

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

    #[test]
    fn missing_github_token_is_refused() {
        assert!(github_token_from(None).is_err());
        assert!(github_token_from(Some(String::new())).is_err());
    }

    #[test]
    fn nonempty_github_token_is_accepted() {
        assert_eq!(github_token_from(Some("t".to_string())).unwrap(), "t");
    }
}
