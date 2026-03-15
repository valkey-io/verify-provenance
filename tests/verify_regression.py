#!/usr/bin/env python3
import json
import os
import sys
import subprocess
import re
import argparse

# Resolve paths relative to the test file
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
ACTION_DIR = os.path.dirname(TEST_DIR)
GOLDEN_FILE = os.path.join(TEST_DIR, 'golden_data.json')
CHECK_SCRIPT = os.path.join(ACTION_DIR, 'src', 'check.py')
DB_PR = os.path.join(TEST_DIR, 'redis_pr_fingerprints.json.gz')
DB_COMMIT = os.path.join(TEST_DIR, 'redis_commits_bootstrap.json.gz')

def resolve_target_worktree(cli_path=None, base_dir=None):
    if cli_path:
        if os.path.isdir(cli_path):
            return os.path.abspath(cli_path)
        raise FileNotFoundError(f"Target worktree not found: {cli_path}")

    candidates = [
        os.environ.get("VERIFY_PROVENANCE_TARGET_ROOT"),
        os.path.abspath(os.path.join(base_dir or ACTION_DIR, "..", "valkey")),
    ]
    for candidate in candidates:
        if candidate and os.path.isdir(candidate):
            return os.path.abspath(candidate)
    raise FileNotFoundError(
        "Target worktree not found. Pass --target-worktree or set VERIFY_PROVENANCE_TARGET_ROOT."
    )


def run_check(target_worktree, pr_num=None, sha=None):
    # SetUp env with correct PYTHONPATH
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{ACTION_DIR}/src:" + env.get("PYTHONPATH", "")

    cmd = [
        'python3', CHECK_SCRIPT,
        '--source-repo', 'redis/redis',
        '--target-repo', 'valkey-io/valkey',
        '--source-brand', 'Redis',
        '--target-brand', 'Valkey',
        '--source-prefix', 'RM_',
        '--target-prefix', 'VM_',
        '--pr-db', DB_PR,
        '--commit-db', DB_COMMIT,
        '--verbose'
    ]
    if pr_num: cmd.insert(2, str(pr_num))
    if sha:
        cmd.extend(['--base-sha', f'{sha}^', '--head-sha', sha])

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=target_worktree, env=env)
    return result.returncode, result.stdout + result.stderr

def main():
    parser = argparse.ArgumentParser(description="Verify golden provenance regression cases")
    parser.add_argument(
        "--target-worktree",
        help="Path to the target repository worktree used for commit-based regression cases",
    )
    args = parser.parse_args()

    if not os.path.exists(GOLDEN_FILE):
        print(f"Golden file not found: {GOLDEN_FILE}")
        sys.exit(1)

    target_worktree = resolve_target_worktree(args.target_worktree, ACTION_DIR)

    with open(GOLDEN_FILE, 'r') as f:
        golden = json.load(f)

    failed = 0
    for key, data in golden.items():
        print(f"Verifying {key}...")
        if data.get('type') == 'pr':
            code, output = run_check(target_worktree, pr_num=data['number'])
        else:
            code, output = run_check(target_worktree, sha=data['sha'])

        if code != 0 and "No databases loaded." in output:
            print("  FAILED: Fingerprint databases are missing; regression cannot run.")
            failed += 1
            continue

        matches = re.findall(r'similarity: ([\d\.]+)', output)

        expected_findings = data.get('findings', [])
        expected_sims = sorted([float(f[1]['similarity']) for f in expected_findings if f[1].get('similarity%') != 'N/A'], reverse=True)
        actual_sims = sorted([float(s) for s in matches], reverse=True)

        if expected_sims and actual_sims:
            count = min(len(expected_sims), len(actual_sims))
            mismatch = False
            for i in range(count):
                delta = abs(expected_sims[i] - actual_sims[i])
                if delta > 0.01:
                    print(f"  FAILED: Similarity mismatch at index {i}. Expected {expected_sims[i]}, got {actual_sims[i]} (delta={delta:.4f})")
                    mismatch = True
                    break
            if not mismatch:
                print(f"  PASSED (matches within tolerance)")
                continue
            failed += 1
        elif not expected_sims and not actual_sims:
            print(f"  PASSED (No matches as expected)")
        else:
            print(f"  FAILED: Match presence mismatch. Expected {len(expected_sims)} matches, got {len(actual_sims)} matches")
            failed += 1

    if failed:
        print(f"\nRegression verification FAILED with {failed} errors")
        sys.exit(1)
    else:
        print("\nRegression verification PASSED")

if __name__ == "__main__":
    main()
