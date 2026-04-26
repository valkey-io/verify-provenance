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

def _db_items(db, db_type):
    return db.get("prs", {}) if db_type == "pr" else db.get("commits", {})

def _entry_timestamp(entry, db_type):
    return entry.get("created_at") if db_type == "pr" else entry.get("date")

def _entry_allowed_by_date(entry, db_type, target_ts):
    if not target_ts:
        return True
    entry_ts = normalize_timestamp(_entry_timestamp(entry, db_type))
    return not entry_ts or entry_ts <= target_ts

def _ensure_candidate(candidates, key, entry):
    if key not in candidates:
        candidates[key] = {
            "key": key,
            "entry": entry,
            "sim": 0.0,
            "patch_id_match": False,
            "matched_files": [],
            "signals": [],
        }
    return candidates[key]

def _add_signal(candidate, signal, sim=None, patch_id_match=False):
    if signal not in candidate["signals"]:
        candidate["signals"].append(signal)
    if sim is not None:
        candidate["sim"] = max(candidate["sim"], sim)
    candidate["patch_id_match"] |= patch_id_match

def _add_matched_file(candidate, target_path, source_path, sim, patch_id_match):
    for match in candidate["matched_files"]:
        if match["target"] == target_path and match["source"] == source_path:
            match["sim"] = max(match["sim"], sim)
            match["patch_id_match"] |= patch_id_match
            return
    candidate["matched_files"].append({
        "target": target_path,
        "source": source_path,
        "sim": sim,
        "same_path": target_path == source_path,
        "patch_id_match": patch_id_match,
    })

def _add_patch_id_candidates(candidates, fingerprint, db, db_type, target_ts):
    patch_id = fingerprint.get("patch_id")
    if not patch_id:
        return
    for key, entry in _db_items(db, db_type).items():
        if not _entry_allowed_by_date(entry, db_type, target_ts):
            continue
        if patch_id and entry.get("patch_id") and patch_id == entry.get("patch_id"):
            candidate = _ensure_candidate(candidates, key, entry)
            _add_signal(candidate, "patch_id", patch_id_match=True)

def _add_whole_simhash_candidates(candidates, fingerprint, db, db_type, target_ts):
    target_simhash = fingerprint.get("simhash64", 0)
    for key, entry in _db_items(db, db_type).items():
        if not _entry_allowed_by_date(entry, db_type, target_ts):
            continue
        sim = compute_simhash_similarity(target_simhash, entry.get("simhash64", 0))
        if sim >= LAYER1_SIMHASH_BASE_THRESHOLD:
            candidate = _ensure_candidate(candidates, key, entry)
            _add_signal(candidate, "whole_simhash", sim=sim)

def _add_file_pair_candidates(candidates, fingerprint, db, db_type, config, target_ts):
    files = fingerprint.get("files", {})
    for key, entry in _db_items(db, db_type).items():
        if not _entry_allowed_by_date(entry, db_type, target_ts):
            continue
        for target_path, target_fp in files.items():
            if is_infrastructure_file(target_path, config):
                continue
            for source_path, source_fp in entry.get("files", {}).items():
                if is_infrastructure_file(source_path, config):
                    continue
                sim = compute_simhash_similarity(target_fp.get("simhash64", 0), source_fp.get("simhash64", 0))
                patch_id_match = bool(
                    target_fp.get("patch_id")
                    and source_fp.get("patch_id")
                    and target_fp["patch_id"] == source_fp["patch_id"]
                )
                if not patch_id_match and sim < LAYER1_SIMHASH_BASE_THRESHOLD:
                    continue

                candidate = _ensure_candidate(candidates, key, entry)
                if patch_id_match:
                    _add_signal(candidate, "file_patch_id", sim=sim, patch_id_match=True)
                if sim >= LAYER1_SIMHASH_BASE_THRESHOLD:
                    _add_signal(candidate, "file_simhash", sim=sim)
                _add_matched_file(candidate, target_path, source_path, sim, patch_id_match)

def _candidate_sort_key(candidate):
    patch_rank = 1 if candidate.get("patch_id_match") else 0
    file_rank = 1 if candidate.get("matched_files") else 0
    return (patch_rank, file_rank, candidate.get("sim", 0.0))

def _exact_match_method(candidate):
    signals = candidate.get("signals", [])
    if "patch_id" in signals:
        return "patch_id"
    if "file_patch_id" in signals:
        return "file_patch_id"
    return None

def _layer1_method(candidate):
    signals = candidate.get("signals", [])
    if "file_simhash" in signals:
        return "file_simhash"
    if "whole_simhash" in signals:
        return "whole_simhash"
    return "simhash"

def _source_pr_policy_info(pr_info):
    if not isinstance(pr_info, dict):
        return {}
    return {
        "number": pr_info.get("number"),
        "title": pr_info.get("title"),
        "author_login": author_login_from_info(pr_info),
    }

def layer1_find_candidates(fingerprint, db, db_type, config, date=None, ignore_date=False):
    files = fingerprint.get("files", {})
    if not any(not is_infrastructure_file(f, config) for f in files):
        return []

    target_ts = normalize_timestamp(date) if date and not ignore_date else None
    candidates = {}

    _add_patch_id_candidates(candidates, fingerprint, db, db_type, target_ts)
    _add_whole_simhash_candidates(candidates, fingerprint, db, db_type, target_ts)
    _add_file_pair_candidates(candidates, fingerprint, db, db_type, config, target_ts)

    return sorted(candidates.values(), key=_candidate_sort_key, reverse=True)

def _deep_validation_result(target_diff, source_diff, config, method, matched_files=None, source_info=None):
    score, shared_tokens, _ = deep_compare_diffs(target_diff, source_diff, config)
    match = matched_files[0] if matched_files else {}
    policy = evaluate_diff_exemption(
        target_diff,
        config,
        source_diff=source_diff,
        shared_tokens=shared_tokens,
        require_meaningful_tokens=(method == "file_simhash+deep"),
        target_path=match.get("target"),
        source_path=match.get("source"),
    )
    if policy["exempt"]:
        return None
    return {
        "accepted": True,
        "score": score,
        "method": method,
        "matched_files": matched_files or [],
        "source_info": source_info,
    }

def layer2_validate_candidate(valkey_diff_files, candidate, db_type, config, token):
    try:
        owner, repo = config.source_repo.split("/")
        source_info = None
        if db_type == "pr":
            source_diff_raw, source_info = fetch_pr_diff(owner, repo, candidate["entry"].get("number"), token)
            source_info = _source_pr_policy_info(source_info)
        else:
            source_diff_raw = fetch_commit_diff(owner, repo, candidate["entry"].get("sha"), token)

        source_diff = source_diff_raw.decode("utf-8", errors="replace")
        matched_files = candidate.get("matched_files") or []
        if matched_files:
            source_diff_files = split_diff_by_file(source_diff)
            best = None
            for match in matched_files:
                target_diff = valkey_diff_files.get(match["target"])
                source_file_diff = source_diff_files.get(match["source"])
                if not target_diff or not source_file_diff:
                    continue
                result = _deep_validation_result(
                    target_diff,
                    source_file_diff,
                    config,
                    "file_simhash+deep",
                    [match],
                    source_info=source_info,
                )
                if result and (not best or result["score"] > best["score"]):
                    best = result
            if best:
                return best
            if len(valkey_diff_files) == 1 and len(source_diff_files) == 1:
                return None

        valkey_combined = "\n".join(valkey_diff_files.values())
        return _deep_validation_result(
            valkey_combined,
            source_diff,
            config,
            "whole_simhash+deep",
            source_info=source_info,
        )
    except Exception as e:
        logger.debug("Layer 2 validation failed for %s: %s", candidate.get("key"), e)
        return None

def _exact_candidate_has_reportable_diff(candidate, method, diff_files, config):
    if not diff_files:
        return True

    if method == "patch_id":
        return not evaluate_diff_exemption("\n".join(diff_files.values()), config)["exempt"]

    target_diffs = [
        diff_files[match["target"]]
        for match in candidate.get("matched_files", [])
        if match.get("patch_id_match") and match.get("target") in diff_files
    ]
    if not target_diffs:
        return True
    return any(not evaluate_diff_exemption(diff, config)["exempt"] for diff in target_diffs)

def _resolve_exact_candidate(candidate, db_type, target_author, diff_files, config):
    method = _exact_match_method(candidate)
    if not method:
        return None
    if not _exact_candidate_has_reportable_diff(candidate, method, diff_files, config):
        return {"accepted": False, "reason": "diff_exempt"}
    return {"accepted": True, "method": method, "deep_sim": 1.0}

def _source_info_for_policy(candidate, db_type, config, token):
    entry = candidate.get("entry", {}) if isinstance(candidate, dict) else {}
    if db_type != "pr":
        return entry
    if entry.get("title") and author_login_from_info(entry):
        return entry
    if not token:
        return entry
    try:
        owner, repo = config.source_repo.split("/")
        return _source_pr_policy_info(fetch_pr_info(owner, repo, entry.get("number"), token))
    except Exception as e:
        logger.debug("Source PR metadata fetch failed for %s: %s", entry.get("number"), e)
        return entry

def _false_positive_filtered(candidate, db_type, method, config, target_author, target_title, diff_files, validation=None, source_info=None):
    policy = evaluate_false_positive_filter(
        candidate=candidate,
        db_type=db_type,
        method=method,
        config=config,
        target_author=target_author,
        target_title=target_title,
        target_diff_files=diff_files,
        source_info=source_info,
        validation=validation,
    )
    if policy["filtered"]:
        logger.debug("Filtered candidate %s as %s", candidate.get("key"), policy["reason"])
        return True
    return False

def find_matches(
    fingerprint,
    db,
    threshold,
    max_report,
    db_type,
    config,
    date=None,
    diff_files=None,
    ignore_date=False,
    target_author=None,
    target_title=None,
):
    candidates = layer1_find_candidates(fingerprint, db, db_type, config, date, ignore_date)
    if not candidates: return []

    token = os.environ.get("GITHUB_TOKEN")
    results = []
    for cand in candidates[:max_report * 2]:
        exact = _resolve_exact_candidate(cand, db_type, target_author, diff_files, config)
        if exact:
            if not exact["accepted"]:
                continue
            source_info = _source_info_for_policy(cand, db_type, config, token)
            if _false_positive_filtered(
                cand,
                db_type,
                exact["method"],
                config,
                target_author,
                target_title,
                diff_files,
                source_info=source_info,
            ):
                continue
            cand.update({"method": exact["method"], "deep_sim": exact["deep_sim"]})
            results.append(cand)
            if len(results) >= max_report: break
            continue

        if not diff_files:
            cand.update({"method": _layer1_method(cand), "deep_sim": None})
            results.append(cand)
            continue

        validation = layer2_validate_candidate(diff_files, cand, db_type, config, token)
        if not validation or validation["score"] < threshold:
            continue
        if _false_positive_filtered(
            cand,
            db_type,
            validation["method"],
            config,
            target_author,
            target_title,
            diff_files,
            validation=validation,
            source_info=validation.get("source_info"),
        ):
            continue

        cand.update({
            "deep_sim": validation["score"],
            "method": validation["method"],
            "layer2": validation,
        })
        results.append(cand)
        if len(results) >= max_report: break
    return results

def check_diff(
    diff_bytes,
    pr_db,
    commit_db,
    config,
    threshold=0.85,
    max_report=5,
    pr_date=None,
    ignore_date=False,
    target_author=None,
    target_title=None,
):
    diff_text = diff_bytes.decode("utf-8", errors="replace")
    if not diff_text.strip(): return False, []

    diff_text = filter_branding_changes(diff_text, config)
    earliest_date = get_earliest_commit_date(diff_text)
    effective_date = min(earliest_date, pr_date) if earliest_date and pr_date else (earliest_date or pr_date)

    diff_files = split_diff_by_file(diff_text)
    if evaluate_diff_exemption(diff_text, config)["exempt"]: return False, []

    norm_all = normalize_diff(diff_text, config)
    fingerprint = {
        "simhash64": simhash64(norm_all),
        "patch_id": compute_patch_id(diff_text),
        "files": compute_file_fingerprints(diff_files, config)
    }

    pr_matches = find_matches(
        fingerprint,
        pr_db,
        threshold,
        max_report,
        "pr",
        config,
        effective_date,
        diff_files,
        ignore_date,
        target_author,
        target_title,
    )
    commit_matches = find_matches(
        fingerprint,
        commit_db,
        threshold,
        max_report,
        "commit",
        config,
        effective_date,
        diff_files,
        ignore_date,
    )

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
            target_author = (pr_info.get("user") or {}).get("login")
            found, findings = check_diff(
                diff_bytes,
                pr_db,
                commit_db,
                config,
                a.threshold,
                a.max_report,
                pr_info.get("created_at"),
                a.ignore_date,
                target_author,
                pr_info.get("title"),
            )
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
        if res.returncode != 0:
            err = res.stderr.decode("utf-8", errors="replace").strip()
            logger.error("git diff failed for %s...%s%s", base, head, f": {err}" if err else "")
            sys.exit(1)
        diff_bytes = res.stdout
        found, findings = check_diff(diff_bytes, pr_db, commit_db, config, a.threshold, a.max_report, ignore_date=a.ignore_date)
        if found:
            for msg, _ in findings: logger.info("    - %s", msg)
            sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
