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
    title = (title or "").lower()
    if "merge" in title and "into" in title: return True
    if "release" in title or title.startswith("release/"): return True
    if title in ["main", "unstable", "master"]: return True
    if pr.get("changed_files", 0) > 50: return True
    return False

def fetch_pr_list(owner, repo, state, page, per_page, token, since_updated=None):
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls?state={state}&sort=updated&direction=desc&per_page={per_page}&page={page}"
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "Provenance-Guard"}
    if token: headers["Authorization"] = f"Bearer {token}"
    data, _ = github_request(url, headers)
    prs = json.loads(data.decode("utf-8"))
    if since_updated is None: return prs, not prs
    since_ts = normalize_timestamp(since_updated)
    recent = [p for p in prs if normalize_timestamp(p.get("updated_at", "")) > since_ts]
    return recent, len(recent) < len(prs) or not prs


def _db_output(args, prs, failed_prs):
    output = {
        "repo": f"{args.source_owner}/{args.source_repo_name}",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "prs": prs,
    }
    if failed_prs:
        output["failed_prs"] = failed_prs
    return output


def _save_db(args, prs, failed_prs):
    with gzip.open(args.out_db, "wt", encoding="utf-8") as f:
        json.dump(_db_output(args, prs, failed_prs), f, indent=2)


def _latest_updated_at(prs, fallback):
    timestamps = [
        normalize_timestamp(p.get("updated_at"))
        for p in prs.values()
        if isinstance(p, dict) and p.get("updated_at")
    ]
    return max(timestamps) if timestamps else normalize_timestamp(fallback)


def _pr_entry(args, pr, config, token):
    diff_bytes, _ = fetch_pr_diff(args.source_owner, args.source_repo_name, pr["number"], token)
    diff_text = diff_bytes.decode("utf-8", errors="replace")
    return {
        "number": pr["number"],
        "state": pr["state"],
        "created_at": pr["created_at"],
        "updated_at": pr["updated_at"],
        "title": pr.get("title"),
        "author_login": (pr.get("user") or {}).get("login"),
        "simhash64": simhash64(normalize_diff(diff_text, config)),
        "patch_id": compute_patch_id(diff_text),
        "files": compute_file_fingerprints(split_diff_by_file(diff_text), config),
    }


def _failed_pr_record(pr, error):
    return {
        "number": pr.get("number"),
        "state": pr.get("state"),
        "title": pr.get("title"),
        "created_at": pr.get("created_at"),
        "updated_at": pr.get("updated_at"),
        "changed_files": pr.get("changed_files", 0),
        "user": pr.get("user"),
        "last_error": str(error),
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }


def _existing_entry_is_current(existing, pr):
    if not existing:
        return False
    existing_updated = normalize_timestamp(existing.get("updated_at"))
    pr_updated = normalize_timestamp(pr.get("updated_at"))
    return bool(existing_updated and pr_updated and pr_updated <= existing_updated)


def refresh_prs(args, config):
    token = os.environ.get("GITHUB_TOKEN")
    db = load_db(args.out_db)
    prs = db.get("prs", {})
    failed_prs = db.get("failed_prs", {})
    since_updated = _latest_updated_at(prs, args.cutoff_date)

    os.makedirs(os.path.dirname(args.out_db) or ".", exist_ok=True)

    def process_pr(pr):
        pr_num = pr["number"]
        try:
            prs[str(pr_num)] = _pr_entry(args, pr, config, token)
            failed_prs.pop(str(pr_num), None)
            if len(prs) % 10 == 0:
                _save_db(args, prs, failed_prs)
                logger.info(f"Checkpoint: saved {len(prs)} PRs")
        except Exception as e:
            failed_prs[str(pr_num)] = _failed_pr_record(pr, e)
            logger.warning(f"Failed PR #{pr_num}: {e}")

    for pr in list(failed_prs.values()):
        if not pr.get("number"):
            continue
        if should_skip_pr(pr.get("title", ""), pr):
            failed_prs.pop(str(pr["number"]), None)
            continue
        if _existing_entry_is_current(prs.get(str(pr["number"])), pr):
            failed_prs.pop(str(pr["number"]), None)
            continue
        process_pr(pr)

    for state in ["open", "closed"]:
        page = 1
        while page <= MAX_PAGES:
            pr_list, stop = fetch_pr_list(args.source_owner, args.source_repo_name, state, page, PER_PAGE, token, since_updated)
            if not pr_list: break
            for pr in pr_list:
                pr_num = pr["number"]
                if _existing_entry_is_current(prs.get(str(pr_num)), pr):
                    failed_prs.pop(str(pr_num), None)
                    continue
                if should_skip_pr(pr.get("title"), pr):
                    failed_prs.pop(str(pr_num), None)
                    continue
                process_pr(pr)
            if stop: break
            page += 1

    _save_db(args, prs, failed_prs)

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
