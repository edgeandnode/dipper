#!/bin/env bash

# The script location
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

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
	if [ ! -d "$SRC_DIR" ] || [ -z "$(ls -A "$SRC_DIR")" ]; then
		continue
	fi

	# Create symbolic links relative to the target directory
	ln --symbolic --relative --force "$SRC_DIR"/* "$TARGET_DIR"/
done

echo "Symbolic links created in '$TARGET_DIR'"
