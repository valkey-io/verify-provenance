#!/usr/bin/env python3
"""
refresh_prs.py - Update PR fingerprint database from GitHub API
Generic version: All parameters are passed via CLI.
"""

import argparse
import gzip
import json
import logging
import os
import sys
from datetime import datetime, timezone
from common import (
    simhash64,
    normalize_diff,
    compute_patch_id,
    github_request,
    fetch_pr_diff,
    normalize_timestamp,
    split_diff_by_file,
    load_db,
    compute_file_fingerprints,
    ProvenanceConfig,
    logger,
)

PER_PAGE = 100
MAX_PAGES = 100

def should_skip_pr(title, pr):
    title = title.lower()
    if "merge" in title and "into" in title: return True
    if "release" in title or title.startswith("release/"): return True
    if title in ["main", "unstable", "master"]: return True
    if pr.get("changed_files", 0) > 50: return True
    return False

def fetch_pr_list(owner, repo, state, page, per_page, token, since_created=None):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state={state}&sort=created&direction=desc&per_page={per_page}&page={page}"
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "Provenance-Guard"}
    if token: headers["Authorization"] = f"Bearer {token}"
    data, _ = github_request(url, headers)
    prs = json.loads(data.decode("utf-8"))
    if since_created is None: return prs, not prs
    since_ts = normalize_timestamp(since_created)
    recent = [p for p in prs if normalize_timestamp(p.get("created_at", "")) > since_ts]
    return recent, len(recent) < len(prs) or not prs

def refresh_prs(args, config):
    token = os.environ.get("GITHUB_TOKEN")
    db = load_db(args.out_db)
    prs = db.get("prs", {})
    since_created = max(p["created_at"] for p in prs.values()) if prs else args.cutoff_date
    since_created = normalize_timestamp(since_created)

    for state in ["open", "closed"]:
        page = 1
        while page <= MAX_PAGES:
            pr_list, stop = fetch_pr_list(args.source_owner, args.source_repo_name, state, page, PER_PAGE, token, since_created)
            if not pr_list: break
            for pr in pr_list:
                pr_num = pr["number"]
                if str(pr_num) in prs and normalize_timestamp(pr["updated_at"]) <= normalize_timestamp(prs[str(pr_num)]["updated_at"]): continue
                if should_skip_pr(pr["title"], pr): continue
                try:
                    diff_bytes, _ = fetch_pr_diff(args.source_owner, args.source_repo_name, pr_num, token)
                    diff_text = diff_bytes.decode("utf-8")
                    prs[str(pr_num)] = {
                        "number": pr_num, "state": pr["state"], "created_at": pr["created_at"], "updated_at": pr["updated_at"],
                        "simhash64": simhash64(normalize_diff(diff_text, config)), "patch_id": compute_patch_id(diff_text),
                        "files": compute_file_fingerprints(split_diff_by_file(diff_text), config)
                    }
                    if len(prs) % 10 == 0:
                        output = {"repo": f"{args.source_owner}/{args.source_repo_name}", "generated_at": datetime.now(timezone.utc).isoformat(), "prs": prs}
                        with gzip.open(args.out_db, "wt", encoding="utf-8") as f: json.dump(output, f, indent=2)
                        logger.info(f"Checkpoint: saved {len(prs)} PRs")
                except Exception as e: logger.warning(f"Failed PR #{pr_num}: {e}")
            if stop: break
            page += 1

    output = {"repo": f"{args.source_owner}/{args.source_repo_name}", "generated_at": datetime.now(timezone.utc).isoformat(), "prs": prs}
    os.makedirs(os.path.dirname(args.out_db) or ".", exist_ok=True)
    with gzip.open(args.out_db, "wt", encoding="utf-8") as f: json.dump(output, f, indent=2)

def main():
    parser = argparse.ArgumentParser(description="Refresh PR fingerprint database")
    parser.add_argument("--source-owner", required=True)
    parser.add_argument("--source-repo-name", required=True)
    parser.add_argument("--cutoff-date", required=True)
    parser.add_argument("--out-db", required=True)
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
        source_repo=f"{args.source_owner}/{args.source_repo_name}",
        target_repo="",
        branding_pairs=bps,
        prefix_pairs=pps,
        source_brand=args.source_brand,
        target_brand=args.target_brand,
        source_prefix=args.source_prefix,
        target_prefix=args.target_prefix
    )
    refresh_prs(args, config)

if __name__ == "__main__": main()
