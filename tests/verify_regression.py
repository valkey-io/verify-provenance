#!1/usr/bin/env python3
import json
import os
import sys
import subprocess
import re

# Resolve paths relative to the test file
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
ACTION_DIR = os.path.dirname(TEST_DIR)
GOLDEN_FILE = os.path.join(TEST_DIR, 'golden_data.json')
CHECK_SCRIPT = os.path.join(ACTION_DIR, 'src', 'check.py')
DB_PR = os.path.join(TEST_DIR, 'redis_pr_fingerprints.json.gz')
DB_COMMIT = os.path.join(TEST_DIR, 'redis_commits_bootstrap.json.gz')

def run_check(pr_num=None, sha=None):
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

    # Run from valkey root
    valkey_root = "/home/pingxie/repos/valkey"
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=valkey_root, env=env)
    return result.returncode, result.stdout + result.stderr

def main():
    if not os.path.exists(GOLDEN_FILE):
        print(f"Golden file not found: {GOLDEN_FILE}")
        sys.exit(1)

    with open(GOLDEN_FILE, 'r') as f:
        golden = json.load(f)

    failed = 0
    for key, data in golden.items():
        print(f"Verifying {key}...")
        if data.get('type') == 'pr':
            code, output = run_check(pr_num=data['number'])
        else:
            code, output = run_check(sha=data['sha'])

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
