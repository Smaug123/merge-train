//! Merge Train Bot - Main entry point.
//!
//! This binary runs the HTTP server that accepts GitHub webhooks and
//! drives the merge train state machine.

use std::env::VarError;
use std::net::SocketAddr;
use std::path::PathBuf;

use merge_train::cascade::TrainSizeCap;
use merge_train::git::CommitIdentity;
use merge_train::server::{AppState, InvalidStateToken, StateAuth, StateToken, build_router};
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

    /// How `/api/v1/repos/{owner}/{repo}/state` authorizes callers.
    /// Disabled unless `STATE_API_TOKEN` is set.
    state_auth: StateAuth,

    /// GitHub token. Non-empty. Must be a *user-scoped* token (a classic or
    /// fine-grained PAT, or a GitHub App user-to-server token): startup
    /// resolves the bot's identity via `GET /user`, which App
    /// *installation* tokens cannot call — supporting those needs an
    /// App-auth identity source and token refresh, deferred until someone
    /// deploys this as a GitHub App.
    github_token: String,

    /// How often each worker re-evaluates its active trains as a fallback
    /// for missed webhooks. Zero disables polling.
    poll_interval: std::time::Duration,

    /// The largest train `@merge-train start` will accept.
    max_train_size: TrainSizeCap,

    /// Overrides for the git commit identity (defaults derive from the bot's
    /// GitHub identity at startup).
    git_user_name: Option<String>,
    git_user_email: Option<String>,
    git_signing_key: Option<String>,
}

/// Anchors a possibly-relative path to the process's working directory at
/// startup, so paths later handed to subprocesses (git receives the repos
/// tree as path arguments while running with a different cwd) mean the same
/// place everywhere.
fn absolutize(path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .expect("cannot read the current directory at startup")
            .join(path)
    }
}

/// Validates the webhook secret read from the environment.
///
/// A missing or empty secret is refused: HMAC with an empty key verifies
/// signatures forged with that same empty key, so starting without a secret
/// would accept arbitrary webhooks.
///
/// A secret that is set but is not UTF-8 is refused as well, and says so:
/// `.ok()` would report it as "not set", sending an operator who did set it
/// to look in the wrong place. The refusal does not quote the bytes.
fn webhook_secret_from(value: Result<String, VarError>) -> Result<Vec<u8>, &'static str> {
    match value {
        Err(VarError::NotPresent) => {
            Err("WEBHOOK_SECRET is not set; refusing to start without signature verification")
        }
        Err(VarError::NotUnicode(_)) => Err(
            "WEBHOOK_SECRET is set but is not valid UTF-8; refusing to start without \
             signature verification",
        ),
        Ok(s) if s.is_empty() => {
            Err("WEBHOOK_SECRET is empty; refusing to start without signature verification")
        }
        Ok(s) => Ok(s.into_bytes()),
    }
}

/// Reads the state endpoint's authorization from the environment.
///
/// Unset means the endpoint is disabled, not open: the listener has to be
/// reachable from the internet for GitHub to deliver webhooks to it, and
/// the endpoint serves a private repository's branch names, SHAs and train
/// topology. An operator who has not configured a token gets no endpoint
/// rather than an unauthenticated one.
///
/// A token that is set but unusable is refused rather than ignored — an
/// empty `STATE_API_TOKEN` is a secret-injection that silently failed, and
/// silently disabling the endpoint would hide it.
///
/// It therefore takes the whole [`VarError`], not an `Option`: `.ok()` maps
/// [`VarError::NotUnicode`] to `None`, which would file a token the operator
/// did set — bytes that are not UTF-8 — under "unset" and silently disable
/// the endpoint, which is the silent failure this function exists to refuse.
/// The refusal does not quote the offending bytes, unlike the other
/// variables': they are a secret.
fn state_auth_from(value: Result<String, VarError>) -> Result<StateAuth, &'static str> {
    let token = match value {
        Ok(token) => token,
        Err(VarError::NotPresent) => return Ok(StateAuth::Disabled),
        Err(VarError::NotUnicode(_)) => {
            return Err(
                "STATE_API_TOKEN is not valid UTF-8, so no caller could present it; \
                 unset it to disable the state endpoint, or set it to a usable token",
            );
        }
    };
    match StateToken::new(token) {
        Ok(token) => Ok(StateAuth::Bearer(token)),
        Err(InvalidStateToken::Empty) => Err(
            "STATE_API_TOKEN is empty; unset it to disable the state endpoint, \
             or set it to a token",
        ),
        Err(InvalidStateToken::Unpresentable) => Err(
            "STATE_API_TOKEN contains a byte that cannot appear in an HTTP header \
             value (only visible ASCII is usable), so no caller could present it",
        ),
    }
}

/// The largest train `@merge-train start` will accept, from
/// `MERGE_TRAIN_MAX_STACK_SIZE`. Default [`TrainSizeCap::DEFAULT`].
///
/// Unlike [`poll_interval_from`], a malformed value is refused rather than
/// defaulted. The poll interval is a performance knob, where carrying on at
/// the default is harmless; this one decides which `@merge-train start`
/// commands the bot accepts, so an operator who mistypes it must be told
/// rather than left believing in a limit that is not in force.
///
/// It therefore takes the whole [`VarError`], not an `Option`: `.ok()` maps
/// [`VarError::NotUnicode`] to `None`, which would file a value the operator
/// did set — bytes that are not UTF-8 — under "unset" and silently pick the
/// default. That is exactly the silent defaulting this function exists to
/// refuse (Codex review, P3).
fn max_train_size_from(value: Result<String, VarError>) -> Result<TrainSizeCap, String> {
    let raw = match value {
        Ok(raw) => raw,
        Err(VarError::NotPresent) => return Ok(TrainSizeCap::DEFAULT),
        Err(VarError::NotUnicode(bytes)) => {
            return Err(format!(
                "MERGE_TRAIN_MAX_STACK_SIZE is not valid UTF-8: {bytes:?}"
            ));
        }
    };
    let requested: usize = raw
        .trim()
        .parse()
        .map_err(|_| format!("MERGE_TRAIN_MAX_STACK_SIZE is not a whole number of PRs: {raw:?}"))?;
    TrainSizeCap::new(requested).map_err(|e| format!("MERGE_TRAIN_MAX_STACK_SIZE: {e}"))
}

/// Validates the GitHub token read from the environment. Required: the worker
/// executes real API effects (squash merges!) — there is no unauthenticated
/// mode.
fn github_token_from(value: Result<String, VarError>) -> Result<String, &'static str> {
    match value {
        Err(VarError::NotPresent) => {
            Err("GITHUB_TOKEN is not set; the bot cannot run without GitHub access")
        }
        // As for the webhook secret: a token that is set but is not UTF-8 is
        // refused saying so, rather than reported as absent. The refusal does
        // not quote the bytes.
        Err(VarError::NotUnicode(_)) => Err("GITHUB_TOKEN is set but is not valid UTF-8"),
        Ok(s) if s.is_empty() => Err("GITHUB_TOKEN is empty"),
        Ok(s) => Ok(s),
    }
}

impl Config {
    /// Loads configuration from environment variables.
    ///
    /// `LISTEN_ADDR`, `STATE_DIR`, `REPOS_DIR`,
    /// `MERGE_TRAIN_POLL_INTERVAL_MINS` and `MERGE_TRAIN_MAX_STACK_SIZE`
    /// have defaults; `WEBHOOK_SECRET` and `GITHUB_TOKEN` are required.
    fn from_env() -> Result<Self, String> {
        let listen_addr = std::env::var("LISTEN_ADDR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 3000)));

        let state_dir = std::env::var("STATE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/state"));

        // Absolute, or git misresolves it: clone/worktree commands receive
        // this as a path *argument* while their cwd is already inside the
        // repos tree, and git resolves arguments against the subprocess cwd
        // (Codex M5 round 11, P1 — the relative default nested a second
        // repos tree inside the first).
        let repos_dir = absolutize(
            std::env::var("REPOS_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("./data/repos")),
        );

        let webhook_secret = webhook_secret_from(std::env::var("WEBHOOK_SECRET"))?;
        let github_token = github_token_from(std::env::var("GITHUB_TOKEN"))?;
        let state_auth = state_auth_from(std::env::var("STATE_API_TOKEN"))?;

        let poll_interval =
            poll_interval_from(std::env::var("MERGE_TRAIN_POLL_INTERVAL_MINS").ok())?;

        let max_train_size = max_train_size_from(std::env::var("MERGE_TRAIN_MAX_STACK_SIZE"))?;

        Ok(Config {
            listen_addr,
            state_dir,
            repos_dir,
            webhook_secret,
            state_auth,
            github_token,
            poll_interval,
            max_train_size,
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
    let user = octocrab.current().user().await.map_err(|e| {
        format!(
            "cannot fetch the bot's identity (GET /user): {e}. Note that \
             GITHUB_TOKEN must be a user-scoped token (PAT or user-to-server); \
             GitHub App installation tokens cannot answer GET /user."
        )
    })?;
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

    // build_octocrab disables octocrab's HTTP-layer retry — required, or
    // 5xx/transport errors replay non-idempotent effects (squash merges!)
    // underneath the interpreter's per-effect retry policy.
    let octocrab = match merge_train::github::build_octocrab(config.github_token.clone()) {
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

    // The state endpoint's mode is worth saying out loud: an operator who
    // expected to be able to read a repo's state gets a 404 when no token is
    // configured, and this line is what tells them why.
    let state_api = match config.state_auth {
        StateAuth::Disabled => "disabled (set STATE_API_TOKEN to enable)",
        StateAuth::Bearer(_) => "enabled (bearer token required)",
    };

    tracing::info!(
        state_dir = %config.state_dir.display(),
        repos_dir = %config.repos_dir.display(),
        bot = %identity.login,
        bot_user_id = identity.user_id,
        state_api,
        max_train_size = config.max_train_size.get(),
        "Starting merge train bot"
    );

    let commit_identity = CommitIdentity {
        name: config
            .git_user_name
            .unwrap_or_else(|| identity.login.clone()),
        email: config
            .git_user_email
            .unwrap_or_else(|| format!("{}@users.noreply.github.com", identity.login)),
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
        // No credentials in the URL: clones authenticate via a credential
        // helper reading GITHUB_TOKEN from the environment (see
        // worker::executor::ensure_clone), so the token never reaches git
        // command lines or error output.
        clone_url_base: Some("https://github.com".to_owned()),
        bot_user_id: identity.user_id,
        bot_name: identity.login,
        stall_retry_delay: std::time::Duration::from_secs(30),
        poll_interval: config.poll_interval,
        max_train_size: config.max_train_size,
    };

    // Create application state
    let app_state = AppState::new(
        config.state_dir,
        config.webhook_secret,
        deps,
        config.state_auth,
    );

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

/// The longest accepted poll interval: a year. Anything longer is a
/// misconfiguration, and bounding it keeps every deadline computed from it
/// (`Instant + interval`, the stagger's millisecond arithmetic) safely
/// representable (Codex polling review round 3, P3).
const MAX_POLL_INTERVAL_MINS: u64 = 366 * 24 * 60;

/// Missed-webhook polling cadence (DESIGN §Polling fallback) from
/// `MERGE_TRAIN_POLL_INTERVAL_MINS`. Default 10 minutes; `0` disables
/// polling (webhooks + restart recovery remain). A malformed value falls
/// back to the default rather than refusing to start; a value above
/// [`MAX_POLL_INTERVAL_MINS`] is refused.
fn poll_interval_from(value: Option<String>) -> Result<std::time::Duration, &'static str> {
    let mins = match value {
        None => 10,
        Some(v) => match v.parse::<u64>() {
            Ok(mins) => mins,
            // All digits but too large for `u64` is an out-of-range
            // CONFIGURATION, not a typo: it must be refused, not silently
            // defaulted (Codex polling review round 6, P3).
            Err(_) if v.chars().all(|c| c.is_ascii_digit()) && !v.is_empty() => {
                return Err("MERGE_TRAIN_POLL_INTERVAL_MINS is above the maximum of a year");
            }
            Err(_) => 10,
        },
    };
    if mins > MAX_POLL_INTERVAL_MINS {
        return Err("MERGE_TRAIN_POLL_INTERVAL_MINS is above the maximum of a year");
    }
    Ok(std::time::Duration::from_secs(mins * 60))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_interval_defaults_disables_and_refuses_overflow() {
        let mins = |v: Option<&str>| poll_interval_from(v.map(str::to_owned));
        assert_eq!(mins(None).unwrap(), std::time::Duration::from_secs(600));
        assert_eq!(
            mins(Some("garbage")).unwrap(),
            std::time::Duration::from_secs(600)
        );
        assert_eq!(mins(Some("0")).unwrap(), std::time::Duration::ZERO);
        assert_eq!(
            mins(Some("7")).unwrap(),
            std::time::Duration::from_secs(420)
        );
        assert!(mins(Some(&u64::MAX.to_string())).is_err());
        // All digits but beyond u64: out of range, not a typo.
        assert!(mins(Some("99999999999999999999999")).is_err());
        assert_eq!(
            mins(Some("12x")).unwrap(),
            std::time::Duration::from_secs(600)
        );
        assert!(mins(Some(&(MAX_POLL_INTERVAL_MINS + 1).to_string())).is_err());
        assert!(mins(Some(&MAX_POLL_INTERVAL_MINS.to_string())).is_ok());
    }

    /// Git resolves path *arguments* against the subprocess cwd, which for
    /// clone/worktree commands is already inside the repos tree — a relative
    /// `REPOS_DIR` (including the old default `./data/repos`) made
    /// `git clone <url> ./data/repos/o/r/clone` nest a second tree under the
    /// first and the worker never saw `clone_dir()` (Codex M5 round 11, P1).
    #[test]
    fn repos_dir_is_absolutized() {
        let abs = absolutize(PathBuf::from("data/repos"));
        assert!(abs.is_absolute(), "got {}", abs.display());
        assert!(abs.ends_with("data/repos"));

        let already = PathBuf::from("/var/lib/merge-train/repos");
        assert_eq!(absolutize(already.clone()), already);
    }

    /// The environment as the parser sees it: a value that is set.
    fn set(value: &str) -> Result<String, VarError> {
        Ok(value.to_owned())
    }

    #[test]
    fn an_unset_train_cap_is_the_default() {
        assert_eq!(
            max_train_size_from(Err(VarError::NotPresent)).unwrap(),
            TrainSizeCap::DEFAULT
        );
    }

    /// A value that is set but is not UTF-8 is a value the operator chose,
    /// not an absent one. `std::env::var(..).ok()` would file it under
    /// "unset" and silently pick the default, which is the silent
    /// defaulting this parser refuses (Codex review, P3).
    #[test]
    fn a_non_unicode_train_cap_is_refused_not_defaulted() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let not_utf8 = OsString::from_vec(vec![b'7', 0xff]);
        let refusal = max_train_size_from(Err(VarError::NotUnicode(not_utf8)))
            .expect_err("non-UTF-8 must be refused, not defaulted");
        assert!(
            refusal.contains("MERGE_TRAIN_MAX_STACK_SIZE"),
            "the refusal must name the variable, got: {refusal}"
        );
    }

    #[test]
    fn a_configured_train_cap_is_honoured() {
        assert_eq!(max_train_size_from(set("120")).unwrap().get(), 120);
        // Surrounding whitespace is a transcription artefact, not a typo.
        assert_eq!(max_train_size_from(set(" 7 ")).unwrap().get(), 7);
    }

    #[test]
    fn a_malformed_train_cap_is_refused_not_defaulted() {
        for bad in ["fifty", "", "12x", "-1", "1.5"] {
            assert!(
                max_train_size_from(set(bad)).is_err(),
                "expected {bad:?} to be refused"
            );
        }
    }

    #[test]
    fn an_unusable_train_cap_is_refused() {
        // Zero would refuse every train; above the ceiling the status
        // comment could not hold the train.
        assert!(max_train_size_from(set("0")).is_err());
        assert!(
            max_train_size_from(set(
                &(merge_train::status::MAX_SUPPORTED_TRAIN_SIZE + 1).to_string()
            ))
            .is_err()
        );
        assert!(
            max_train_size_from(set(
                &merge_train::status::MAX_SUPPORTED_TRAIN_SIZE.to_string()
            ))
            .is_ok()
        );
    }

    #[test]
    fn missing_webhook_secret_is_refused() {
        assert!(webhook_secret_from(Err(VarError::NotPresent)).is_err());
    }

    #[test]
    fn empty_webhook_secret_is_refused() {
        assert!(webhook_secret_from(Ok(String::new())).is_err());
    }

    #[test]
    fn nonempty_webhook_secret_is_accepted() {
        assert_eq!(webhook_secret_from(set("s3cret")).unwrap(), b"s3cret");
    }

    #[test]
    fn missing_github_token_is_refused() {
        assert!(github_token_from(Err(VarError::NotPresent)).is_err());
        assert!(github_token_from(Ok(String::new())).is_err());
    }

    #[test]
    fn an_unset_state_token_disables_the_endpoint() {
        assert!(matches!(
            state_auth_from(Err(VarError::NotPresent)),
            Ok(StateAuth::Disabled)
        ));
    }

    #[test]
    fn an_empty_state_token_is_refused_rather_than_disabling() {
        // Silently disabling would hide a secret-injection that failed.
        assert!(state_auth_from(Ok(String::new())).is_err());
    }

    /// A token that is set but is not UTF-8 is a token the operator chose,
    /// not an absent one. `std::env::var(..).ok()` would file it under
    /// "unset" and silently disable the endpoint, hiding a secret injection
    /// that failed — the very thing this parser refuses for an empty value.
    #[test]
    fn a_non_unicode_state_token_is_refused_not_treated_as_unset() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let not_utf8 = OsString::from_vec(vec![b's', 0xff]);
        let refusal = state_auth_from(Err(VarError::NotUnicode(not_utf8)))
            .expect_err("non-UTF-8 must be refused, not read as unset");
        assert!(
            refusal.contains("STATE_API_TOKEN"),
            "the refusal must name the variable, got: {refusal}"
        );
    }

    #[test]
    fn an_unpresentable_state_token_is_refused() {
        assert!(state_auth_from(set("has space")).is_err());
        assert!(state_auth_from(set("café")).is_err());
    }

    #[test]
    fn a_usable_state_token_enables_the_endpoint() {
        let auth = state_auth_from(set("s3cret")).unwrap();
        match auth {
            StateAuth::Bearer(token) => assert!(token.matches(b"s3cret")),
            StateAuth::Disabled => panic!("expected the endpoint to be enabled"),
        }
    }

    /// Both secrets are refused when set but not UTF-8, and say so rather
    /// than reporting themselves absent: the outcome was always right (the
    /// bot refuses to start either way), but an operator who did set the
    /// variable was told it was missing and sent to look in the wrong place.
    #[test]
    fn a_non_unicode_secret_is_refused_as_set_not_as_missing() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let not_utf8 = || OsString::from_vec(vec![b's', 0xff]);

        let refusal = webhook_secret_from(Err(VarError::NotUnicode(not_utf8())))
            .expect_err("a non-UTF-8 secret must be refused");
        assert!(
            refusal.contains("not valid UTF-8"),
            "the refusal must say the secret is unusable, not absent, got: {refusal}"
        );
        assert!(!refusal.contains("is not set"), "got: {refusal}");

        let refusal = github_token_from(Err(VarError::NotUnicode(not_utf8())))
            .expect_err("a non-UTF-8 token must be refused");
        assert!(
            refusal.contains("not valid UTF-8"),
            "the refusal must say the token is unusable, not absent, got: {refusal}"
        );
        assert!(!refusal.contains("is not set"), "got: {refusal}");
    }

    #[test]
    fn nonempty_github_token_is_accepted() {
        assert_eq!(github_token_from(set("t")).unwrap(), "t");
    }
}
