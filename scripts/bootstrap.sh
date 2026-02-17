#!/bin/bash
set -euo pipefail

# bootstrap.sh - Setup Provenance Guard for a Target Repository
# Usage: ./bootstrap.sh --source-repo redis/redis --source-brand Redis --target-brand Valkey

echo "🚀 Starting Provenance Guard Bootstrap..."

# --- Defaults ---
SOURCE_REPO=""
TARGET_REPO=""
SOURCE_BRAND=""
TARGET_BRAND=""
SOURCE_BRANCH="unstable"
CUTOFF_DATE="2024-03-20T00:00:00Z" # Default to Redis license change date
COMMIT_DB_FILE="commits_bootstrap.json.gz"
DB_BRANCH="verify-provenance-db"

# --- Argument Parsing ---
while [[ $# -gt 0 ]]; do
  case $1 in
    --source-repo) SOURCE_REPO="$2"; shift 2 ;;
    --target-repo) TARGET_REPO="$2"; shift 2 ;;
    --source-brand) SOURCE_BRAND="$2"; shift 2 ;;
    --target-brand) TARGET_BRAND="$2"; shift 2 ;;
    --source-branch) SOURCE_BRANCH="$2"; shift 2 ;;
    --cutoff-date) CUTOFF_DATE="$2"; shift 2 ;;
    --db-branch) DB_BRANCH="$2"; shift 2 ;;
    *) shift ;;
  esac
done

# Try to auto-detect TARGET_REPO if not provided
if [[ -z "$TARGET_REPO" ]]; then
  if git remote get-url origin >/dev/null 2>&1; then
    TARGET_REPO=$(git remote get-url origin | sed -E "s/.*[:\/]([^/]+\/[^/]+)(\.git)?$/\1/")
    echo "🔍 Auto-detected target-repo: $TARGET_REPO"
  fi
fi

if [[ -z "$SOURCE_REPO" || -z "$SOURCE_BRAND" || -z "$TARGET_BRAND" ]]; then
  echo "Error: Missing required arguments."
  echo "Usage: ./bootstrap.sh --source-repo <owner/repo> --source-brand <name> --target-brand <name> [options]"
  exit 1
fi

# 1. Create the Orphan Branch
echo "📂 Creating orphan branch "$DB_BRANCH" in local repository..."
git checkout --orphan "$DB_BRANCH"
git rm -rf .
git commit --allow-empty -m "Initialize Provenance Database Branch"

# 2. Run Initial Indexing
echo "🔍 Running initial commit bootstrap for $SOURCE_REPO..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ACTION_ROOT="$(dirname "$SCRIPT_DIR")"
export PYTHONPATH="$PYTHONPATH:${ACTION_ROOT}"

python3 "${ACTION_ROOT}/src/bootstrap_commits.py" \
  --source-url "https://github.com/${SOURCE_REPO}.git" \
  --source-repo "$SOURCE_REPO" \
  --source-branch "$SOURCE_BRANCH" \
  --cutoff-date "$CUTOFF_DATE" \
  --out-db "$COMMIT_DB_FILE" \
  --source-brand "$SOURCE_BRAND" \
  --target-brand "$TARGET_BRAND"

# 3. Finalize Branch
git add "$COMMIT_DB_FILE"
git commit -m "Bootstrap: Initial commit fingerprints from $SOURCE_REPO"

echo "✅ Local bootstrap complete!"
echo "Next steps:"
echo "1. Push the branch: git push origin $DB_BRANCH"
echo "2. Switch back to your main branch: git checkout -"
echo "3. Add the Provenance Guard workflow to your repository."
