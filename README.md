# Verify Provenance Action

A generic GitHub Action to detect when PRs contain content highly similar to a designated source repository, helping maintain proper attribution and legal provenance.

## Overview

The **Verify Provenance** system compares PR diffs against two databases of source changes:
- **Commit DB**: Source commits since a specific cutoff date.
- **PR DB**: Source PRs (open and closed), refreshed periodically.

**Key principle**: Similarity detection is **content-based**. While matching file paths are used as an optimization, fuzzy content fingerprinting (SimHash64) detects similar code changes regardless of where they appear in the codebase. Branding terms (e.g., `Redis` to `Valkey`) and prefixes (e.g., `RM_` to `VM_`) are automatically normalized.

## Setup Guide

To enable Verify Provenance for your repository, follow these two steps:

### 1. Initial Bootstrap (One-time)

Use the provided bootstrap script to create a dedicated orphaned branch (`verify-provenance-db`) and index the initial history of your source repository.

```bash
# 1. Clone this action repository locally
git clone https://github.com/valkey-io/verify-provenance.git

# 2. Go to your TARGET repository (e.g., Valkey)
cd ~/repos/valkey

# 3. Run the bootstrap script (auto-detects target-repo)
~/repos/verify-provenance/scripts/bootstrap.sh \
  --source-repo redis/redis \
  --source-brand Redis \
  --target-brand Valkey
```

*Note: By default, the script indexes the source repo from **March 20, 2024** (Redis license change). Use `--cutoff-date` to override.*

### 2. Configure GitHub Action

Add a workflow file (e.g., `.github/workflows/provenance.yml`) to your repository:

```yaml
name: Verify Provenance

on:
  pull_request:
    types: [opened, synchronize, reopened, ready_for_review]

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0 # Required to resolve git SHAs

      - name: Run Provenance Check
        uses: valkey-io/verify-provenance@v1
        with:
          source_repo: "redis/redis"
          target_repo: "${{ github.repository }}"
          branding_pairs: "Redis:Valkey,KeyDB:Valkey"
          prefix_pairs: "RM_:VM_,REDISMODULE_:VALKEYMODULE_"
          github_token: "${{ secrets.GITHUB_TOKEN }}"
```

## Configuration

| Input | Description | Default |
|---|---|---|
| `mode` | Operation mode: `check` or `refresh` | `check` |
| `source_repo` | The upstream repository (e.g., `redis/redis`) | **Required** |
| `target_repo` | Your repository (e.g., `valkey-io/valkey`) | **Required** |
| `branding_pairs` | Comma-separated `Source:Target` brand pairs | - |
| `prefix_pairs` | Comma-separated `Source:Target` prefix pairs | - |
| `source_brand` | Legacy: Brand name in source repo | - |
| `target_brand` | Legacy: Brand name in target repo | - |
| `source_prefix` | Legacy: Prefix in source | - |
| `target_prefix` | Legacy: Prefix in target | - |
| `db_branch` | Orphan branch for databases | `verify-provenance-db` |
| `pr_db_file` | Filename of PR database | `pr_fingerprints.json.gz` |
| `commit_db_file` | Filename of commit database | `commits_bootstrap.json.gz` |
| `cutoff_date` | Cutoff date for refresh (ISO 8601) | `2024-03-20T00:00:00Z` |
| `threshold` | Similarity threshold (0.0 - 1.0) | `0.85` |

## How It Works

The system uses a **2-layer approach** to balance speed and accuracy:

1.  **Layer 1: Candidate Generation (Fast, Local)**
    - Uses SimHash64 + git patch-id against local pre-computed fingerprints.
    - Matches identical file paths for speed but falls back to overall PR content.
    - High recall: catches all potential matches.
2.  **Layer 2: Deep Validation (API-based)**
    - Fetches actual diffs from GitHub API for candidates.
    - Performs precise token-based Jaccard and subset similarity checks.
    - High precision: filters out false positives.

## Maintenance

Fingerprint databases are stored in the `verify-provenance-db` branch of your repository. It is recommended to set up a weekly scheduled workflow to run the action in `mode: refresh` and commit the updated `pr_fingerprints.json.gz` back to that branch.
