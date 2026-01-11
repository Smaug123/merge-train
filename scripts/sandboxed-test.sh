#!/usr/bin/env bash
#
# Compile tests normally, then run them in a sandbox to prevent filesystem/network escape.
# The purpose is to run mutation tests safely, although
# that is not yet implemented.
# Uses bubblewrap on Linux, sandbox-exec (seatbelt) on macOS.
#
# Usage:
#   ./scripts/sandboxed-test.sh [cargo flags...] [filter] [-- test args...]
#
# Examples:
#   ./scripts/sandboxed-test.sh
#   ./scripts/sandboxed-test.sh --release
#   ./scripts/sandboxed-test.sh test_name_filter
#   ./scripts/sandboxed-test.sh --release my_test -- --test-threads=1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Parse arguments to separate cargo flags, test filters, and test args.
# Mirrors cargo test behavior:
#   ./scripts/sandboxed-test.sh --release filter_name -- --test-threads=1
#   CARGO_ARGS=(--release)   - flags passed to cargo build
#   FILTER_ARGS=(filter_name) - test name filter passed to test binary
#   TEST_ARGS=(--test-threads=1) - args passed to test binary after --
CARGO_ARGS=()
FILTER_ARGS=()
TEST_ARGS=()
seen_separator=false

for arg in "$@"; do
    if [[ "$arg" == "--" ]]; then
        seen_separator=true
    elif $seen_separator; then
        TEST_ARGS+=("$arg")
    elif [[ "$arg" == -* ]]; then
        CARGO_ARGS+=("$arg")
    else
        FILTER_ARGS+=("$arg")
    fi
done

echo "=== Compiling tests (outside sandbox) ===" >&2

# Use --message-format=json to reliably get all test executable paths.
# This handles multiple test binaries (lib tests, integration tests, etc.)
TEST_BINARIES=()
while IFS= read -r line; do
    # Filter for "compiler-artifact" messages that are tests
    executable=$(echo "$line" | jq -r '
        select(.reason == "compiler-artifact")
        | select(.profile.test == true)
        | .executable // empty
    ' 2>/dev/null)
    if [[ -n "$executable" ]]; then
        TEST_BINARIES+=("$executable")
    fi
done < <(cargo test --no-run --message-format=json "${CARGO_ARGS[@]}")

if [[ ${#TEST_BINARIES[@]} -eq 0 ]]; then
    echo "Error: Could not find any test binaries" >&2
    exit 1
fi

echo "Found ${#TEST_BINARIES[@]} test binary(ies):" >&2
for bin in "${TEST_BINARIES[@]}"; do
    echo "  $bin" >&2
done

# Create a dedicated temp directory for the test run.
# IMPORTANT: Canonicalize with realpath to resolve symlinks (e.g., /var -> /private/var on macOS).
# This allows us to grant write permissions to only this specific directory, rather than
# broad paths like /private/var/folders which would expose other applications' temp files.
TEST_TMPDIR="$(realpath "$(mktemp -d)")"
trap 'rm -rf "$TEST_TMPDIR"' EXIT

echo "" >&2
echo "=== Running tests (inside sandbox) ===" >&2

run_linux() {
    local test_binary="$1"
    shift
    local test_args=("$@")

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

        # Clear environment to prevent secret leakage, then set minimal env.
        # PATH is preserved to allow Nix-provided tools (like git) to be found.
        --clearenv
        --setenv PATH "$PATH"
        --setenv HOME "$TEST_TMPDIR"
        --setenv TMPDIR "$TEST_TMPDIR"
        --setenv USER sandbox
        --setenv RUST_BACKTRACE 1

        --chdir "$PROJECT_DIR"
    )

    # Verify sandbox prevents writes next to the binary
    local verify_file
    verify_file="$(dirname "$test_binary")/sandbox-verify-$(uuidgen)"
    if bwrap "${bwrap_args[@]}" /bin/sh -c "echo test > '$verify_file'" 2>/dev/null; then
        echo "FATAL: Sandbox verification failed - was able to write to: $verify_file" >&2
        rm -f "$verify_file"
        exit 1
    fi

    bwrap "${bwrap_args[@]}" "$test_binary" "${test_args[@]}"
}

run_macos() {
    local test_binary="$1"
    shift
    local test_args=("$@")

    # Get the Darwin user temp directory (parent of TEST_TMPDIR).
    # xcrun (which wraps git on macOS) writes cache files here.
    local darwin_temp_dir
    darwin_temp_dir="$(dirname "$TEST_TMPDIR")"

    local sandbox_policy="
(version 1)

(allow default)

; DENY network access
(deny network*)

; DENY writes everywhere except temp
(deny file-write*)

; Allow writes to Darwin user temp directory (for xcrun cache, etc.)
(allow file-write*
    (subpath \"$darwin_temp_dir\")
)

; Allow /dev/null and /dev/tty (needed by git and other tools)
(allow file-write*
    (literal \"/dev/null\")
    (literal \"/dev/tty\")
)
"

    # Verify sandbox prevents writes next to the binary
    local verify_file
    verify_file="$(dirname "$test_binary")/sandbox-verify-$(uuidgen)"
    if sandbox-exec -p "$sandbox_policy" /bin/sh -c "echo test > '$verify_file'" 2>/dev/null; then
        echo "FATAL: Sandbox verification failed - was able to write to: $verify_file" >&2
        rm -f "$verify_file"
        exit 1
    fi

    # Clear environment to prevent secret leakage, then set minimal env.
    # PATH is preserved to allow Nix-provided tools (like git) to be found.
    env -i \
        PATH="$PATH" \
        HOME="$TEST_TMPDIR" \
        TMPDIR="$TEST_TMPDIR" \
        USER=sandbox \
        RUST_BACKTRACE=1 \
        sandbox-exec -p "$sandbox_policy" "$test_binary" "${test_args[@]}"
}

OS="$(uname -s)"

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
echo "  Filesystem: read-only except temp directories" >&2
echo "  Environment: cleared (PATH, HOME, TMPDIR, USER only)" >&2
echo "" >&2

# Run all test binaries, collecting failures
any_failed=0
for test_binary in "${TEST_BINARIES[@]}"; do
    echo "--- Running: $(basename "$test_binary") ---" >&2
    case "$OS" in
        Linux)
            run_linux "$test_binary" "${FILTER_ARGS[@]}" "${TEST_ARGS[@]}" || any_failed=1
            ;;
        Darwin)
            run_macos "$test_binary" "${FILTER_ARGS[@]}" "${TEST_ARGS[@]}" || any_failed=1
            ;;
    esac
done

echo "" >&2
if [[ $any_failed -ne 0 ]]; then
    echo "=== Some test binary(ies) failed ===" >&2
    exit 1
fi
echo "=== All ${#TEST_BINARIES[@]} test binary(ies) passed ===" >&2
