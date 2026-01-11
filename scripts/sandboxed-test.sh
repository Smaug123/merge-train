#!/usr/bin/env bash
#
# Compile tests normally, then run them in a sandbox to prevent filesystem/network escape.
# The purpose is to run mutation tests safely, although
# that is not yet implemented.
# Uses bubblewrap on Linux, sandbox-exec (seatbelt) on macOS.
#
# Usage:
#   ./scripts/sandboxed-test.sh [cargo test args...]
#
# Examples:
#   ./scripts/sandboxed-test.sh
#   ./scripts/sandboxed-test.sh --release
#   ./scripts/sandboxed-test.sh -- --test-threads=1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments to separate cargo build args from test args
CARGO_ARGS=()
TEST_ARGS=()
seen_separator=false

for arg in "$@"; do
    if [[ "$arg" == "--" ]]; then
        seen_separator=true
    elif $seen_separator; then
        TEST_ARGS+=("$arg")
    else
        CARGO_ARGS+=("$arg")
    fi
done

echo "=== Compiling tests (outside sandbox) ===" >&2
cargo test --no-run "${CARGO_ARGS[@]}"

# Find the test binary - cargo test --no-run prints paths like:
#   Executable unittests src/lib.rs (target/debug/deps/merge_train_bot-abc123)
# It does this to stderr.
TEST_BINARY=$(cargo test --no-run "${CARGO_ARGS[@]}" 2>&1 | grep -o 'target/[^)]*' | head -1)

if [[ -z "$TEST_BINARY" ]]; then
    echo "Error: Could not find test binary" >&2
    exit 1
fi

TEST_BINARY="$PROJECT_DIR/$TEST_BINARY"
echo "Test binary: $TEST_BINARY" >&2

# Create a dedicated temp directory for the test run
TEST_TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TEST_TMPDIR"' EXIT

echo "" >&2
echo "=== Running tests (inside sandbox) ===" >&2

run_linux() {
    if ! command -v bwrap &>/dev/null; then
        echo "Error: bubblewrap (bwrap) not found. Install it with:" >&2
        echo "  nix-shell -p bubblewrap" >&2
        echo "  # or: apt install bubblewrap" >&2
        exit 1
    fi

    local -a bwrap_args=(
        --die-with-parent
        --unshare-net
        --unshare-pid
        --unshare-ipc

        # Fresh /tmp for the test
        --tmpfs /tmp

        # Read-only system paths
        --ro-bind /usr /usr
        --ro-bind /lib /lib
        --ro-bind /etc /etc
        --ro-bind /bin /bin
        --ro-bind-try /lib64 /lib64
        --ro-bind-try /nix /nix

        # Read-only access to project (for test fixtures, etc.)
        --ro-bind "$PROJECT_DIR" "$PROJECT_DIR"

        # Writable temp directory
        --bind "$TEST_TMPDIR" "$TEST_TMPDIR"

        # /dev and /proc
        --dev /dev
        --proc /proc

        # Environment
        --setenv HOME "$TEST_TMPDIR"
        --setenv TMPDIR "$TEST_TMPDIR"
        --setenv RUST_BACKTRACE 1

        --chdir "$PROJECT_DIR"
    )

    echo "  Sandbox: bubblewrap" >&2
    echo "  Temp dir: $TEST_TMPDIR" >&2
    echo "  Network: disabled" >&2
    echo "  Filesystem: read-only except $TEST_TMPDIR" >&2
    echo "" >&2

    bwrap "${bwrap_args[@]}" "$TEST_BINARY" "${TEST_ARGS[@]}"
}

run_macos() {
    echo "  Sandbox: seatbelt" >&2
    echo "  Temp dir: $TEST_TMPDIR" >&2
    echo "  Network: disabled" >&2
    echo "  Filesystem: read-only except temp directories" >&2
    echo "" >&2

    TMPDIR="$TEST_TMPDIR" \
    HOME="$TEST_TMPDIR" \
    RUST_BACKTRACE=1 \
    sandbox-exec -p "
(version 1)

(allow default)

; DENY network access
(deny network*)

; DENY writes everywhere except temp
(deny file-write*)

; Allow writes to temp directories only
(allow file-write*
    (subpath \"$TEST_TMPDIR\")
    (subpath \"/private/tmp\")
    (subpath \"/private/var/folders\")
)

; Allow /dev/null and /dev/tty (needed by git and other tools)
(allow file-write*
    (literal \"/dev/null\")
    (literal \"/dev/tty\")
)
" \
    "$TEST_BINARY" "${TEST_ARGS[@]}"
}

case "$(uname -s)" in
    Linux)
        run_linux
        ;;
    Darwin)
        run_macos
        ;;
    *)
        echo "Error: Unsupported OS: $(uname -s)" >&2
        exit 1
        ;;
esac
