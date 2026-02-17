#!/usr/bin/env python3
"""
backtest.py - Backtest provenance checks on a range of PRs.
Generic version: Passes all configuration to check_src.py.
"""

import subprocess
import argparse
import os
import sys
import logging
from common import logger

def check_pr(pr_number, common_args):
    """Run provenance check on a single PR."""
    try:
        cmd = ["python3", "check.py", str(pr_number)] + common_args
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )

        output = result.stdout + result.stderr
        if "404" in output:
            return "NOT_FOUND", None

        if result.returncode == 0:
            return "PASS", None
        elif result.returncode == 1:
            details = []
            for line in output.split("\n"):
                if "matches" in line:
                    details.append(line.strip())
            if details:
                return "FAIL", "; ".join(details[:2])
            else:
                return "ERROR", output[:200]
        else:
            return "ERROR", output[:200]

    except subprocess.TimeoutExpired:
        return "TIMEOUT", None
    except Exception as e:
        return "ERROR", str(e)[:100]

def main():
    parser = argparse.ArgumentParser(description="Backtest provenance checks")
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    parser.add_argument("--source-repo", required=True)
    parser.add_argument("--target-repo", required=True)
    parser.add_argument("--source-brand", required=True)
    parser.add_argument("--target-brand", required=True)
    parser.add_argument("--source-prefix")
    parser.add_argument("--target-prefix")
    parser.add_argument("--branding-pairs")
    parser.add_argument("--prefix-pairs")
    parser.add_argument("--pr-db", required=True)
    parser.add_argument("--commit-db", required=True)
    parser.add_argument("--verbose", action="store_true")

    args, extra = parser.parse_known_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Build args to pass to check.py
    # Resolve DB paths to absolute to be safe
    pr_db_abs = os.path.abspath(args.pr_db)
    commit_db_abs = os.path.abspath(args.commit_db)

    common_args = [
        "--source-repo", args.source_repo,
        "--target-repo", args.target_repo,
        "--source-brand", args.source_brand,
        "--target-brand", args.target_brand,
        "--pr-db", pr_db_abs,
        "--commit-db", commit_db_abs
    ]
    if args.source_prefix: common_args.extend(["--source-prefix", args.source_prefix])
    if args.target_prefix: common_args.extend(["--target-prefix", args.target_prefix])
    if args.branding_pairs: common_args.extend(["--branding-pairs", args.branding_pairs])
    if args.prefix_pairs: common_args.extend(["--prefix-pairs", args.prefix_pairs])
    if args.verbose: common_args.append("--verbose")

    logger.info(f"Backtesting PRs {args.start} to {args.end}")
    logger.info("=" * 80)

    failed = []
    errors = []

    total = args.end - args.start + 1
    for i, pr_num in enumerate(range(args.start, args.end + 1), 1):
        status, detail = check_pr(pr_num, common_args)

        if i == 1 or i % 20 == 0 or i == total:
            logger.info(f"Progress: {i}/{total} ({100 * i // total}%)")

        if status == "FAIL":
            failed.append((pr_num, detail))
            logger.info(f"  ✗ PR #{pr_num}: FLAGGED - {detail}")
        elif status == "ERROR":
            errors.append((pr_num, detail))
            logger.info(f"  ⚠ PR #{pr_num}: ERROR - {detail}")

    logger.info("\n" + "=" * 80)
    logger.info("BACKTEST SUMMARY")
    logger.info(f"Total checked: {total}")
    logger.info(f"❌ Flagged:    {len(failed)}")
    logger.info(f"⚠️  Errors:     {len(errors)}")

    if failed:
        logger.info("\nFlagged PRs:")
        for pr_num, detail in failed:
            logger.info(f"  - PR #{pr_num}: {detail}")

if __name__ == "__main__":
    main()
