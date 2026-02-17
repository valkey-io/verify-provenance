#!/usr/bin/env python3
import argparse, email.utils, json, logging, os, re, subprocess, sys
from datetime import timezone
from common import *

def get_earliest_commit_date(diff_text):
    dates = re.findall(r"Date: (.*)", diff_text)
    if not dates: return None
    try:
        parsed = [email.utils.parsedate_to_datetime(d) for d in dates]
        return min(parsed).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except: return None

def layer1_find_candidates(fingerprint, db, db_type, config, date=None, ignore_date=False):
    candidates = []
    files = fingerprint.get("files", {})
    if not any(not is_infrastructure_file(f, config) for f in files):
        return []

    target_ts = normalize_timestamp(date) if date and not ignore_date else None
    patch_id = fingerprint.get("patch_id")

    items = db.get("prs", {}) if db_type == "pr" else db.get("commits", {})
    for key, entry in items.items():
        if target_ts:
            entry_ts = normalize_timestamp(entry.get("created_at") if db_type == "pr" else entry.get("date"))
            if entry_ts and entry_ts > target_ts:
                continue

        ref_files = entry.get("files", {})
        ref_patch_id = entry.get("patch_id")
        api_id_match = bool(patch_id and ref_patch_id and patch_id == ref_patch_id)

        if not ref_files:
            sim = compute_simhash_similarity(fingerprint["simhash64"], entry.get("simhash64", 0))
            if sim >= LAYER1_SIMHASH_BASE_THRESHOLD or (sim >= LAYER1_SIMHASH_WITH_PATCHID and api_id_match):
                candidates.append({"key": key, "entry": entry, "sim": sim, "patch_id_match": api_id_match, "matched_files": []})
            continue

        best_sim = 0.0
        matched_files = []
        any_patch_id_match = api_id_match

        for v_fn, v_fp in files.items():
            if v_fn in ref_files:
                s = compute_simhash_similarity(v_fp.get("simhash64", 0), ref_files[v_fn].get("simhash64", 0))
                fp_id_match = bool(v_fp.get("patch_id") and ref_files[v_fn].get("patch_id") and v_fp["patch_id"] == ref_files[v_fn]["patch_id"])
                if s >= LAYER1_SIMHASH_BASE_THRESHOLD or (s >= LAYER1_SIMHASH_WITH_PATCHID and fp_id_match):
                    matched_files.append((v_fn, s, fp_id_match))
                    best_sim = max(best_sim, s)
                    any_patch_id_match |= fp_id_match

        overall_sim = compute_simhash_similarity(fingerprint["simhash64"], entry.get("simhash64", 0))
        best_sim = max(best_sim, overall_sim)

        if best_sim >= LAYER1_SIMHASH_BASE_THRESHOLD or (best_sim >= LAYER1_SIMHASH_WITH_PATCHID and any_patch_id_match) or matched_files:
            candidates.append({"key": key, "entry": entry, "sim": best_sim, "patch_id_match": any_patch_id_match, "matched_files": matched_files})

    candidates.sort(key=lambda x: x["sim"], reverse=True)
    return candidates

def layer2_validate_candidate(valkey_diff_files, candidate, db_type, config, token):
    try:
        owner, repo = config.source_repo.split("/")
        if db_type == "pr":
            source_diff_raw, _ = fetch_pr_diff(owner, repo, candidate["entry"].get("number"), token)
        else:
            source_diff_raw = fetch_commit_diff(owner, repo, candidate["entry"].get("sha"), token)

        source_diff = source_diff_raw.decode("utf-8", errors="replace")
        valkey_combined = "\n".join(valkey_diff_files.values())
        return deep_compare_diffs(valkey_combined, source_diff, config)[0]
    except Exception as e:
        logger.debug("Layer 2 validation failed for %s: %s", candidate.get("key"), e)
        return None

def find_matches(fingerprint, db, threshold, max_report, db_type, config, date=None, diff_files=None, ignore_date=False):
    candidates = layer1_find_candidates(fingerprint, db, db_type, config, date, ignore_date)
    if not candidates: return []

    token = os.environ.get("GITHUB_TOKEN")
    results = []
    for cand in candidates[:max_report * 2]:
        if not diff_files:
            cand.update({"method": "simhash", "deep_sim": None})
            results.append(cand)
            continue

        deep_sim = layer2_validate_candidate(diff_files, cand, db_type, config, token)
        if (deep_sim is not None and deep_sim < threshold) or (deep_sim is None and cand["sim"] < threshold): continue

        cand.update({"deep_sim": deep_sim, "method": ("simhash+deep" if deep_sim is not None else "simhash")})
        results.append(cand)
        if len(results) >= max_report: break
    return results

def check_diff(diff_bytes, pr_db, commit_db, config, threshold=0.85, max_report=5, pr_date=None, ignore_date=False):
    diff_text = diff_bytes.decode("utf-8", errors="replace")
    if not diff_text.strip(): return False, []

    diff_text = filter_branding_changes(diff_text, config)
    earliest_date = get_earliest_commit_date(diff_text)
    effective_date = min(earliest_date, pr_date) if earliest_date and pr_date else (earliest_date or pr_date)

    norm_all = normalize_diff(diff_text, config)
    if not norm_all or len(norm_all.split()) < MIN_TOKENS: return False, []

    diff_files = split_diff_by_file(diff_text)
    if sum(count_diff_lines(f) for f in diff_files.values()) < MIN_LINES: return False, []

    is_trivial, movement_ratio, net_new, _ = detect_code_movement(diff_text)
    if is_trivial: return False, []

    fingerprint = {
        "simhash64": simhash64(norm_all),
        "patch_id": compute_patch_id(diff_text),
        "files": compute_file_fingerprints(diff_files, config)
    }

    pr_matches = find_matches(fingerprint, pr_db, threshold, max_report, "pr", config, effective_date, diff_files, ignore_date)
    commit_matches = find_matches(fingerprint, commit_db, threshold, max_report, "commit", config, effective_date, diff_files, ignore_date)

    findings = []
    for m in pr_matches:
        s = m.get("deep_sim") if m.get("deep_sim") is not None else m["sim"]
        msg = "matches {} PR #{} (similarity: {:.3f}, method: {})".format(config.source_repo, m["entry"]["number"], s, m["method"])
        findings.append((msg, {"type": "pr", "number": m["entry"]["number"]}))
    for m in commit_matches:
        s = m.get("deep_sim") if m.get("deep_sim") is not None else m["sim"]
        msg = "matches {} commit {} (similarity: {:.3f}, method: {})".format(config.source_repo, m["entry"]["sha"], s, m["method"])
        findings.append((msg, {"type": "commit", "sha": m["entry"]["sha"]}))

    return bool(findings), findings

def main():
    p = argparse.ArgumentParser(description="Check PR against fingerprints")
    p.add_argument("pr_number", nargs="?", type=int)
    p.add_argument("--source-repo", required=True)
    p.add_argument("--target-repo", required=True)
    p.add_argument("--branding-pairs", help="Source:Target,...")
    p.add_argument("--prefix-pairs", help="Source:Target,...")
    p.add_argument("--source-brand")
    p.add_argument("--target-brand")
    p.add_argument("--source-prefix")
    p.add_argument("--target-prefix")
    p.add_argument("--infrastructure-patterns")
    p.add_argument("--pr-db", required=True)
    p.add_argument("--commit-db", required=True)
    p.add_argument("--threshold", type=float, default=0.85)
    p.add_argument("--max-report", type=int, default=5)
    p.add_argument("--ignore-date", action="store_true")
    p.add_argument("--base-sha")
    p.add_argument("--head-sha")
    p.add_argument("--verbose", action="store_true")
    a = p.parse_args()

    ll = logging.DEBUG if a.verbose else logging.INFO
    logger.setLevel(ll)

    bps = [tuple(pi.split(":")) for pi in a.branding_pairs.split(",")] if a.branding_pairs else []
    pps = [tuple(pi.split(":")) for pi in a.prefix_pairs.split(",")] if a.prefix_pairs else []
    ips = a.infrastructure_patterns.split(",") if a.infrastructure_patterns else []

    config = ProvenanceConfig(
        source_repo=a.source_repo,
        target_repo=a.target_repo,
        branding_pairs=bps,
        prefix_pairs=pps,
        infrastructure_patterns=ips,
        source_brand=a.source_brand,
        target_brand=a.target_brand,
        source_prefix=a.source_prefix,
        target_prefix=a.target_prefix
    )
    pr_db, commit_db = load_db(a.pr_db), load_db(a.commit_db)

    if not pr_db and not commit_db:
        logger.error("No databases loaded.")
        sys.exit(1)

    logger.info("Loaded {} PRs and {} commits".format(len(pr_db.get('prs', {})), len(commit_db.get('commits', {}))))
    token = os.environ.get("GITHUB_TOKEN")
    t_owner, t_repo = config.target_repo.split("/")

    if a.pr_number:
        try:
            diff_bytes, pr_info = fetch_pr_diff(t_owner, t_repo, a.pr_number, token)
            found, findings = check_diff(diff_bytes, pr_db, commit_db, config, a.threshold, a.max_report, pr_info.get("created_at"), a.ignore_date)
            if found:
                for msg, _ in findings: logger.info("    - %s", msg)
                sys.exit(1)
        except Exception as e:
            logger.error(e)
            sys.exit(1)
    else:
        base = a.base_sha or os.environ.get("BASE_SHA")
        head = a.head_sha or os.environ.get("HEAD_SHA")
        if not base or not head:
            logger.error("Missing SHAs for local diff mode.")
            sys.exit(1)
        res = subprocess.run(["git", "diff", "--unified=3", f"{base}...{head}"], capture_output=True, timeout=60)
        diff_bytes = res.stdout
        found, findings = check_diff(diff_bytes, pr_db, commit_db, config, a.threshold, a.max_report, ignore_date=a.ignore_date)
        if found:
            for msg, _ in findings: logger.info("    - %s", msg)
            sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
