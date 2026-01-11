#!/usr/bin/env bash
#
# Run cargo test in a sandbox to prevent filesystem/network escape.
# The purpose is to run mutation tests safely, where mutated code must not
# be able to escape during either compilation or execution.
# Uses bubblewrap on Linux, sandbox-exec (seatbelt) on macOS.
#
# Usage:
#   ./scripts/sandboxed-test.sh [cargo test args...]
#
# Examples:
#   ./scripts/sandboxed-test.sh
#   ./scripts/sandboxed-test.sh --release
#   ./scripts/sandboxed-test.sh test_name_filter
#   ./scripts/sandboxed-test.sh --release -- --test-threads=1

set -euo pipefail

# Validate that a path is safe for interpolation into SBPL (Sandbox Profile Language).
# Paths containing quotes, newlines, or backslashes could inject arbitrary policy directives.
# We reject these rather than attempting to escape them - fail closed.
sbpl_assert_safe_path() {
    local path="$1"
    local name="$2"
    # Check for: double-quote, newline, carriage return, backslash
    # Using printf to create the pattern since $'\n' in case patterns is tricky
    if [[ "$path" == *\"* ]] || [[ "$path" == *\\* ]] || [[ "$path" == *$'\n'* ]] || [[ "$path" == *$'\r'* ]]; then
        echo "FATAL: Unsafe character in path for SBPL policy: $name" >&2
        echo "  Path contains quote, backslash, or newline which could inject policy directives." >&2
        echo "  Path: $path" >&2
        exit 1
    fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# Create a dedicated temp directory for the test run.
# IMPORTANT: Canonicalize to resolve symlinks (e.g., /var -> /private/var on macOS).
# This ensures sandbox policies use the real path.
TEST_TMPDIR_RAW="$(mktemp -d)"
TEST_TMPDIR="$(cd "$TEST_TMPDIR_RAW" && pwd -P)"
trap 'rm -rf "$TEST_TMPDIR"' EXIT

# Locate Rust toolchain directories (needed read-only in sandbox)
CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"

# On macOS, SDKROOT should be set by the Nix devshell
if [[ "$(uname -s)" == "Darwin" && -z "${SDKROOT:-}" ]]; then
    echo "Warning: SDKROOT not set. Linking may fail inside sandbox." >&2
    echo "  Run inside a Nix devshell, or set SDKROOT manually." >&2
fi

# Run tests inside Linux sandbox
run_linux() {
    if ! command -v bwrap &>/dev/null; then
        echo "Error: bubblewrap (bwrap) not found. Install it with:" >&2
        echo "  nix-shell -p bubblewrap" >&2
        echo "  # or: apt install bubblewrap" >&2
        exit 1
    fi

    local -a bwrap_args=(
        --die-with-parent
        --new-session          # Prevent TIOCSTI terminal injection
        --unshare-all          # Isolate all namespaces (net, pid, ipc, uts, cgroup, user)
        --disable-userns       # Prevent creating nested user namespaces

        # Fresh /tmp for the test
        --tmpfs /tmp

        # Read-only system paths
        --ro-bind /usr /usr
        --ro-bind /lib /lib
        --ro-bind /etc /etc
        --ro-bind /bin /bin
        --ro-bind-try /lib64 /lib64
        --ro-bind-try /nix /nix

        # Rust toolchain (read-only)
        --ro-bind-try "$CARGO_HOME" "$CARGO_HOME"
        --ro-bind-try "$RUSTUP_HOME" "$RUSTUP_HOME"

        # Read-only access to project source
        --ro-bind "$PROJECT_DIR" "$PROJECT_DIR"

        # Writable target directory (for compilation output)
        --bind "$PROJECT_DIR/target" "$PROJECT_DIR/target"

        # Writable temp directory
        --bind "$TEST_TMPDIR" "$TEST_TMPDIR"

        # /dev and /proc
        --dev /dev
        --proc /proc

        # Clear environment to prevent secret leakage, then set minimal env.
        --clearenv
        --setenv PATH "$PATH"
        --setenv HOME "$TEST_TMPDIR"
        --setenv TMPDIR "$TEST_TMPDIR"
        --setenv CARGO_HOME "$CARGO_HOME"
        --setenv RUSTUP_HOME "$RUSTUP_HOME"
        --setenv USER sandbox
        --setenv RUST_BACKTRACE 1

        --chdir "$PROJECT_DIR"
    )

    # Smoke test: verify sandbox prevents writes outside allowed directories.
    # This is not comprehensive - it only checks one path. The real security comes
    # from the bwrap configuration above; this just catches gross misconfigurations.
    local verify_file
    verify_file="$PROJECT_DIR/sandbox-verify-$$-$RANDOM"
    # shellcheck disable=SC2016 # $1 is intentionally for the inner shell
    if bwrap "${bwrap_args[@]}" /bin/sh -c 'echo test >"$1"' _ "$verify_file" 2>/dev/null; then
        echo "FATAL: Sandbox verification failed - was able to write to: $verify_file" >&2
        rm -f "$verify_file"
        exit 1
    fi
    echo "  Sandbox verification passed" >&2

    bwrap "${bwrap_args[@]}" cargo --frozen test "$@"
}

# Run tests inside macOS sandbox
run_macos() {
    # Validate paths before interpolating into SBPL policy.
    # This prevents injection attacks via crafted paths containing quotes/newlines.
    sbpl_assert_safe_path "$TEST_TMPDIR" "TEST_TMPDIR"
    sbpl_assert_safe_path "$PROJECT_DIR" "PROJECT_DIR"

    local sandbox_policy="
(version 1)

; Deny by default
(deny default)

; === Process execution ===
(allow process-exec*)
(allow process-fork)
(allow signal (target self))

; === File reads ===
; Allow reading almost everything (we only care about restricting writes and network)
(allow file-read*)

; === File writes - restricted to specific paths ===
(allow file-write*
    (subpath \"$TEST_TMPDIR\")
    (subpath \"$PROJECT_DIR/target\")
    (literal \"/dev/null\")
    (literal \"/dev/tty\")
)

; === Mach IPC (required for basic macOS functionality) ===
(allow mach-lookup)
(allow mach-register)

; === System operations ===
(allow sysctl-read)
(allow file-ioctl)
(allow process-info-codesignature)
(allow process-info-pidinfo (target self))

; === Pseudo-terminals (cargo test may use these) ===
(allow pseudo-tty)
"

    # Smoke test: verify sandbox prevents writes outside allowed directories.
    # This is not comprehensive - it only checks one path. The real security comes
    # from the seatbelt policy above; this just catches gross misconfigurations.
    local verify_file
    verify_file="$PROJECT_DIR/sandbox-verify-$$-$RANDOM"
    # shellcheck disable=SC2016 # $1 is intentionally for the inner shell
    if sandbox-exec -p "$sandbox_policy" /bin/sh -c 'echo test >"$1"' _ "$verify_file" 2>/dev/null; then
        echo "FATAL: Sandbox verification failed - was able to write to: $verify_file" >&2
        rm -f "$verify_file"
        exit 1
    fi
    echo "  Sandbox verification passed" >&2

    # Convert NIX_LDFLAGS -L paths to RUSTFLAGS -L native= paths.
    # This is needed because rustc invokes the linker directly and doesn't
    # process NIX_LDFLAGS like the Nix cc wrapper does.
    local rust_link_args=""
    if [[ -n "${NIX_LDFLAGS:-}" ]]; then
        rust_link_args=$(echo "$NIX_LDFLAGS" | tr ' ' '\n' | grep '^-L' | sort -u | sed 's/^-L/-L native=/' | tr '\n' ' ')
    fi

    # Pass through Nix toolchain environment variables explicitly.
    # These are required for the Nix clang/ld wrappers to function.
    env -i \
        PATH="$PATH" \
        HOME="$TEST_TMPDIR" \
        TMPDIR="$TEST_TMPDIR" \
        CARGO_HOME="$CARGO_HOME" \
        RUSTUP_HOME="$RUSTUP_HOME" \
        SDKROOT="${SDKROOT:-}" \
        RUSTFLAGS="$rust_link_args" \
        NIX_CC="${NIX_CC:-}" \
        NIX_BINTOOLS="${NIX_BINTOOLS:-}" \
        NIX_LDFLAGS="${NIX_LDFLAGS:-}" \
        NIX_STORE="${NIX_STORE:-}" \
        USER=sandbox \
        RUST_BACKTRACE=1 \
        sandbox-exec -p "$sandbox_policy" cargo --frozen test "$@"
}

OS="$(uname -s)"

echo "=== Running tests in sandbox ===" >&2
case "$OS" in
    Linux)
        echo "  Sandbox: bubblewrap" >&2
        ;;
    Darwin)
        echo "  Sandbox: seatbelt" >&2
        ;;
    *)
        echo "Error: Unsupported OS: $OS" >&2
        exit 1
        ;;
esac

echo "  Temp dir: $TEST_TMPDIR" >&2
echo "  Network: disabled" >&2
echo "  Filesystem: read-only except temp and target directories" >&2
echo "" >&2

case "$OS" in
    Linux)
        run_linux "$@"
        ;;
    Darwin)
        run_macos "$@"
        ;;
esac
