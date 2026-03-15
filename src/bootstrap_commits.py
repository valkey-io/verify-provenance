#!/usr/bin/env python3
"""
bootstrap_commits.py - Build commit fingerprint database from local clone
Generic version: All parameters are passed via CLI.
"""

import argparse
import json
import logging
import os
import subprocess
import tempfile
import gzip
from datetime import datetime, timezone
from common import (
    simhash64,
    normalize_diff,
    compute_patch_id,
    load_db,
    ProvenanceConfig,
    logger,
)

PROGRESS_INTERVAL = 100

def clone_and_process(args, config):
    temp_dir = tempfile.mkdtemp(prefix="repo_clone_")
    try:
        os.chmod(temp_dir, 0o700)
        logger.info(f"Cloning {args.source_url} into {temp_dir}...")
        subprocess.run(["git", "clone", "--quiet", "--no-checkout", "--single-branch", "--branch", args.source_branch, args.source_url, temp_dir], check=True, timeout=600)
        subprocess.run(["git", "config", "core.hooksPath", "/dev/null"], cwd=temp_dir, check=True)

        # Size check
        res = subprocess.run(["git", "count-objects", "-v"], cwd=temp_dir, capture_output=True, check=True)
        size_kb = 0
        for l in res.stdout.decode().split("\n"):
            if l.startswith("size-pack:"):
                size_kb = int(l.split(":")[1].strip())
                break

        if size_kb > 1048576: raise RuntimeError("Repo too large")

        subprocess.run(["git", "checkout", "--quiet", args.source_branch], cwd=temp_dir, check=True)
        logger.info(f"Enumerating commits from {args.cutoff_date}...")
        res = subprocess.run(["git", "rev-list", "--reverse", f"--since={args.cutoff_date}", "HEAD"], cwd=temp_dir, capture_output=True, check=True)
        shas = [s for s in res.stdout.decode().strip().split("\n") if s]

        data = load_db(args.out_db)
        commits = data.get("commits", {})
        for idx, sha in enumerate(shas):
            if sha in commits: continue
            patch = subprocess.run(["git", "show", "--no-color", sha], cwd=temp_dir, capture_output=True).stdout.decode("utf-8", errors="replace")
            date = subprocess.run(["git", "show", "-s", "--format=%cI", sha], cwd=temp_dir, capture_output=True).stdout.decode().strip()
            commits[sha] = {"sha": sha, "date": date, "simhash64": simhash64(normalize_diff(patch, config)), "patch_id": compute_patch_id(patch)}
            if (idx + 1) % PROGRESS_INTERVAL == 0: logger.info(f"Processed {idx + 1}/{len(shas)}")

        output = {"repo": args.source_repo, "generated_at": datetime.now(timezone.utc).isoformat(), "commits": commits}
        os.makedirs(os.path.dirname(args.out_db) or ".", exist_ok=True)
        with gzip.open(args.out_db, "wt", encoding="utf-8") as f: json.dump(output, f, indent=2)
        logger.info(f"Wrote {len(commits)} commits to {args.out_db}")
    finally:
        subprocess.run(["rm", "-rf", temp_dir])

def main():
    parser = argparse.ArgumentParser(description="Build commit fingerprint database")
    parser.add_argument("--source-url", required=True, help="Git URL of source repo")
    parser.add_argument("--source-repo", required=True, help="Source repo name (owner/repo)")
    parser.add_argument("--source-branch", default="unstable", help="Branch to index")
    parser.add_argument("--cutoff-date", required=True, help="ISO cutoff date")
    parser.add_argument("--out-db", required=True, help="Output DB path")
    parser.add_argument("--branding-pairs", help="Source:Target,...")
    parser.add_argument("--prefix-pairs", help="Source:Target,...")
    parser.add_argument("--source-brand")
    parser.add_argument("--target-brand")
    parser.add_argument("--source-prefix")
    parser.add_argument("--target-prefix")
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    bps = [tuple(p.split(":")) for p in args.branding_pairs.split(",")] if args.branding_pairs else []
    pps = [tuple(p.split(":")) for p in args.prefix_pairs.split(",")] if args.prefix_pairs else []
    config = ProvenanceConfig(
        source_repo=args.source_repo,
        target_repo="",
        branding_pairs=bps,
        prefix_pairs=pps,
        source_brand=args.source_brand,
        target_brand=args.target_brand,
        source_prefix=args.source_prefix,
        target_prefix=args.target_prefix
    )
    clone_and_process(args, config)

if __name__ == "__main__": main()
