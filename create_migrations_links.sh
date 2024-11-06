#!/bin/env bash

# The script location
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TARGET_DIR="${ROOT_DIR}/migrations"
SRC_DIRS=(
	"${ROOT_DIR}/crates/dipper-pgmq/migrations"
	"${ROOT_DIR}/crates/dipper-registry/migrations"
)

# Check if the target directory exists
if [ ! -d "$TARGET_DIR" ]; then
	mkdir -p "$TARGET_DIR"
fi

# Create symbolic links from the source directories
for SRC_DIR in "${SRC_DIRS[@]}"; do
	if [ ! -L "$TARGET_DIR/$(basename "$SRC_DIR")" ]; then
		ln -s -f "$SRC_DIR"/* "$TARGET_DIR"/
	fi
done

echo "Symbolic links created in '$TARGET_DIR'"
