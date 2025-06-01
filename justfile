# Display available commands and their descriptions (default target)
default:
    @just --list

# Format Rust code (cargo fmt)
fmt:
    cargo +nightly fmt --all

# Format Python code (ruff)
fmt-python:
    uv run --frozen ruff format dipper-iisa-python

alias fmt-py := fmt-python

# Check Rust code formatting (cargo fmt --check)
fmt-check:
    cargo +nightly fmt --all -- --check

# Check Python code formatting (ruff format --check)
fmt-check-python:
    uv run --frozen ruff format --quiet --check dipper-iisa-python

alias fmt-check-py := fmt-check-python

# Check Rust code (cargo clippy)
check *EXTRA_FLAGS:
    cargo clippy {{EXTRA_FLAGS}} -- -D warnings --force-warn deprecated --force-warn dead-code

# Check Python code (ruff)
check-python *EXTRA_FLAGS:
    @printf "\e[1;92m[1/2]\e[0m Checking Python code (ruff)...\n"
    uv run --frozen ruff check dipper-iisa-python
    @printf "\e[1;92m[2/2]\e[0m Checking Python code (mypy)...\n"
    uv run --frozen mypy dipper-iisa-python

alias check-py := check-python

# Run Rust unit tests
test-unit *EXTRA_FLAGS:
    uv run --frozen cargo test {{EXTRA_FLAGS}} 'tests::' -- --skip 'tests::it_'

# Run Rust integration tests
test-it *EXTRA_FLAGS:
    @printf "\e[1;92m[1/2]\e[0m Running in-tree integration tests...\n"
    uv run --frozen cargo test {{EXTRA_FLAGS}} 'tests::it_'
    @printf "\e[1;92m[2/2]\e[0m Running public API integration tests...\n"
    uv run --frozen cargo test {{EXTRA_FLAGS}} --test '*'

# Run Python tests (pytest)
test-python *EXTRA_FLAGS:
    uv run --frozen pytest -v {{EXTRA_FLAGS}} dipper-iisa-python

alias test-py := test-python

# Create symbolic links for migration files
create-migrations-links:
    #!/usr/bin/env bash
    set -euo pipefail

    # The project root directory
    ROOT_DIR="$(pwd)"

    TARGET_DIR="${ROOT_DIR}/migrations"
    SRC_DIRS=(
        "${ROOT_DIR}/dipper-pgmq/migrations"
        "${ROOT_DIR}/dipper-pgregistry/migrations"
    )

    # Check if the target directory exists
    if [ ! -d "$TARGET_DIR" ]; then
        mkdir -p "$TARGET_DIR"
    fi

    # Create symbolic links from the source directories
    for SRC_DIR in "${SRC_DIRS[@]}"; do
        # If the source directory does not exist (or it's empty), skip it
        if [ ! -d "$SRC_DIR" ] || [ -z "$(ls -A "$SRC_DIR" 2>/dev/null || true)" ]; then
            continue
        fi

        # Create symbolic links relative to the target directory
        ln --symbolic --relative --force "$SRC_DIR"/* "$TARGET_DIR"/
    done

    echo "Symbolic links created in '$TARGET_DIR'"

# Install Git hooks
install-git-hooks:
    #!/usr/bin/env bash
    set -e # Exit on error

    # Check if pre-commit is installed
    if ! command -v "pre-commit" &> /dev/null; then
        >&2 echo "=============================================================="
        >&2 echo "Required command 'pre-commit' not available ❌"
        >&2 echo ""
        >&2 echo "Please install pre-commit using your preferred package manager"
        >&2 echo "  pip install pre-commit"
        >&2 echo "  pacman -S pre-commit"
        >&2 echo "  apt-get install pre-commit"
        >&2 echo "  brew install pre-commit"
        >&2 echo "=============================================================="
        exit 1
    fi

    # Install the pre-commit hooks
    pre-commit install --config .github/pre-commit-config.yaml

# Remove Git hooks
remove-git-hooks:
    #!/usr/bin/env bash
    set -e # Exit on error

    # Check if pre-commit is installed
    if ! command -v "pre-commit" &> /dev/null; then
        >&2 echo "=============================================================="
        >&2 echo "Required command 'pre-commit' not available ❌"
        >&2 echo ""
        >&2 echo "Please install pre-commit using your preferred package manager"
        >&2 echo "  pip install pre-commit"
        >&2 echo "  pacman -S pre-commit"
        >&2 echo "  apt-get install pre-commit"
        >&2 echo "  brew install pre-commit"
        >&2 echo "=============================================================="
        exit 1
    fi

    # Remove the pre-commit hooks
    pre-commit uninstall --config .github/pre-commit-config.yaml
