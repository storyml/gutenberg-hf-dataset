#!/usr/bin/env bash
set -euo pipefail

REPO_ID="${1:?Usage: full_build.sh <hf-repo-id>}"
DATA_DIR="${2:-./data}"

echo "Starting full build..."
echo "  HF Repo: $REPO_ID"
echo "  Data Dir: $DATA_DIR"
echo ""

python -m src --full --repo-id "$REPO_ID" --data-dir "$DATA_DIR"

echo ""
echo "Full build complete!"
