# Project Agents

This document describes the AI agents and their roles within the `verify-provenance` repository.

## Roles & Responsibilities

### Provenance Guard Specialist
- **Focus**: Analyzing code provenance, identifying similarities with external repositories, and managing PR fingerprints.
- **Context**: Python logic in `src/` (specifically `check.py` and `common.py`), bootstrap data in `.json.gz` files, and GitHub Action integration via `action.yml`.
- **Expertise**: SimHash64 fuzzy fingerprinting, git patch-id stability, token-level Jaccard similarity, and branding-aware identifier normalization.

### Test Engineer
- **Focus**: Maintaining and expanding the test suite in `tests/`.
- **Context**: Pytest and `unittest` for integration testing of the `check.py` CLI and `common.py` logic. Regression testing using `tests/golden_data.json`.

## Common Workflows

### Refreshing PRs
Updating the local database of PR fingerprints. This is done periodically to include the latest changes from the source repository.
- **Entry point**: `src/refresh_prs.py`

### Bootstrapping
Initializing the commit history for tracking. This is typically a one-time operation for a new source repository.
- **Entry point**: `src/bootstrap_commits.py`
- **Helper**: `scripts/bootstrap.sh`

### Check Execution
Running the provenance check on a Pull Request.
- **Entry point**: `src/check.py`
- **Action Mode**: `mode: check` in `action.yml`

### Regression Testing
To verify system accuracy and prevent false positives/negatives, perform backtesting on the following Valkey PR range:
- **Range**: PR 2800 to PR 3120
- **Expected Positives**: Exactly 5 matches (3080, 3085, 3088, 3095, 3102).
- **Tool**: `src/backtest.py`
- **Error Handling**: `backtest.py` must handle non-existing PRs (e.g., deleted or skipped PR numbers) gracefully by ignoring 404 errors and not reporting them as failures or errors in the final summary.

## Project Context & Architecture

### Project Overview
**Provenance Guard Action** is a GitHub Action used to detect when code changes in a Pull Request are highly similar to changes from a designated source repository (e.g., detecting Redis code in a Valkey PR). It helps maintain proper attribution and legal provenance by identifying "leaked" or unattributed code from upstream sources.

### Core Technology Stack
- **Language**: Python 3.11+
- **Key Libraries**: Standard library (`re`, `hashlib`, `json`, `gzip`, `subprocess`, `urllib`).
- **Integration**: GitHub Actions (Composite Action).
- **Algorithms**:
    - **SimHash64**: Fuzzy fingerprinting for fast candidate generation.
    - **Git Patch-ID**: Stable identity for identical patches.
    - **Jaccard Similarity**: Precise token-based comparison.
    - **Normalization**: Branding-aware identifier normalization (e.g., `redisLog` -> `serverLog`) and comment stripping.

### Architecture: The 2-Layer Approach
1.  **Layer 1 (Candidate Generation)**: Fast, local lookup comparing the PR's SimHash64 and Patch-ID against pre-computed databases (`.json.gz`).
2.  **Layer 2 (Deep Validation)**: Precise validation by fetching original diffs from the GitHub API and performing token-level Jaccard and subset similarity checks.

## Key Components

### Core Logic (`src/`)
- `check.py`: Main entry point for PR verification. Can run against a PR number or a local git diff.
- `common.py`: Shared utilities for normalization, SimHash computation, and GitHub API interactions.
- `refresh_prs.py`: Tool to update the PR database with recent activity from the source repository.
- `bootstrap_commits.py`: Tool to index the initial history of a source repository.

### Databases
- `tests/redis_commits_bootstrap.json.gz`: Fingerprints of Redis commits.
- `tests/redis_pr_fingerprints.json.gz`: Fingerprints of Redis PRs.
- These are stored in compressed JSON format and should be handled with `gzip` and `json` modules.
- **IMPORTANT**: Database fingerprint files (`.json.gz`) should **NOT** be added to the `verify-provenance` repository. They belong to the target repositories being monitored and are typically stored in an orphaned branch (e.g., `verify-provenance-db`) of those repositories.

### Scripts
- `scripts/bootstrap.sh`: Simplifies the initial setup of the action for a new repository.
- `scripts/setup-action.sh`: Internal setup script (likely used by CI/Action environment).

## Git Configuration
- **Commit Signatures**: All git commits must be signed using the following identity:
  - **Name**: Ping Xie
  - **Email**: pingxie@outlook.com
  - Ensure your local git configuration reflects this:
    ```bash
    git config user.name "Ping Xie"
    git config user.email "pingxie@outlook.com"
    ```
- **Commit Messages**:
  - **Title**: Use a short, descriptive phrase with meaningful semantics. Avoid using prefixes like "fix:" or "feat:". Do not reference project phases.
  - **Body**: For non-trivial changes, use a brief bulleted list (maximum 5 points) to capture major changes and their rationale.
  - **Content**: Focus on actual logic/architectural changes. Do not include trivial details such as the number of lines changed or the fact that tests passed.
- **Commit Workflow**:
  - Always commit changes locally once both unit tests and regression tests have passed.
  - **NEVER** push changes to a remote repository; all work should remain local unless explicitly instructed otherwise.

## Development Guidelines

### Test-Driven Development (TDD)
- Always follow a TDD approach: write unit tests to define the expected behavior before implementing the actual feature or logic.

### Building and Running
- **Environment**: Ensure Python 3.11+ is installed.
- **Running a Check**:
  ```bash
  export GITHUB_TOKEN=your_token
  python3 src/check.py <PR_NUMBER>
    --source-repo redis/redis
    --target-repo valkey-io/valkey
    --source-brand Redis --target-brand Valkey
    --pr-db tests/redis_pr_fingerprints.json.gz
    --commit-db tests/redis_commits_bootstrap.json.gz
  ```
- **Local Diff Mode**:
  ```bash
  python3 src/check.py --base-sha HEAD~1 --head-sha HEAD ... (other args)
  ```

### Testing
- **Framework**: `unittest` is used for CLI integration tests.
- **Execution**:
  ```bash
  # Run all tests
  python3 -m unittest discover tests

  # Run specific test
  python3 tests/test_check.py
  ```
- **Golden Data**: Regression tests use `tests/golden_data.json` to verify consistency of detection logic.

### Conventions
- **Normalization**: Always update `PRESERVED_KEYWORDS` in `common.py` if adding support for new languages (currently supports C, Python, Tcl).
- **Branding**: The system relies on `branding_pairs` and `prefix_pairs` to normalize identifiers. New branding terms should be added to these configurations.
- **Logging**: Use the `logger` from `common.py`. Verbose mode (`--verbose`) enables DEBUG level.

## Style Guidance
- **Trailing Spaces**: Never have any trailing spaces in any files. Ensure all text and code files are trimmed before committing.
