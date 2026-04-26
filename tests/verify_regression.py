#!/usr/bin/env python3
import argparse
from contextlib import contextmanager
import json
import os
import re
import subprocess
import sys
import tempfile

# Resolve paths relative to the test file
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
ACTION_DIR = os.path.dirname(TEST_DIR)
GOLDEN_FILE = os.path.join(TEST_DIR, 'golden_data.json')
CHECK_SCRIPT = os.path.join(ACTION_DIR, 'src', 'check.py')
DB_PR = os.path.join(TEST_DIR, 'redis_pr_fingerprints.json.gz')
DB_COMMIT = os.path.join(TEST_DIR, 'redis_commits_bootstrap.json.gz')
DEFAULT_TARGET_REPO_URL = 'https://github.com/valkey-io/valkey.git'


@contextmanager
def cloned_target_repo(repo_url=DEFAULT_TARGET_REPO_URL, target_ref=None):
    with tempfile.TemporaryDirectory(prefix='verify-provenance-valkey-', dir='/tmp') as tmp_dir:
        clone_dir = os.path.join(tmp_dir, 'repo')
        subprocess.run(['git', 'clone', '--quiet', repo_url, clone_dir], check=True)
        if target_ref:
            subprocess.run(['git', 'checkout', '--quiet', target_ref], cwd=clone_dir, check=True)
        yield clone_dir


def run_check(pr_num=None, sha=None, target_root=None):
    if not target_root:
        raise ValueError('target_root is required')

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

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=target_root, env=env)
    return result.returncode, result.stdout + result.stderr


def main():
    parser = argparse.ArgumentParser(description='Verify regression expectations against a temporary target clone')
    parser.add_argument('--target-repo-url', default=DEFAULT_TARGET_REPO_URL)
    parser.add_argument('--target-ref')
    args = parser.parse_args()

    if not os.path.exists(GOLDEN_FILE):
        print(f"Golden file not found: {GOLDEN_FILE}")
        sys.exit(1)

    with open(GOLDEN_FILE, 'r') as f:
        golden = json.load(f)

    failed = 0
    with cloned_target_repo(args.target_repo_url, args.target_ref) as target_root:
        for key, data in golden.items():
            print(f"Verifying {key}...")
            if data.get('type') == 'pr':
                code, output = run_check(pr_num=data['number'], target_root=target_root)
            else:
                code, output = run_check(sha=data['sha'], target_root=target_root)

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
