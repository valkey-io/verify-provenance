"""
common.py - Shared utilities for Provenance Guard
"""

import hashlib
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import PurePosixPath
from config import ProvenanceConfig
from db import load_db
from git_utils import PatchIdError, compute_patch_id
from github_client import fetch_commit_diff, fetch_pr_diff, fetch_pr_info, github_request


# Configure logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)


# Provenance checking constants - 2-Layer Approach
LAYER1_SIMHASH_BASE_THRESHOLD = 0.80
LAYER2_SIMILARITY_THRESHOLD = 0.85

# Pre-filters (applied before Layer 1)
MIN_TOKENS = 5
MIN_LINES = 5
MIN_NET_NEW_LINES = 5
CODE_MOVEMENT_THRESHOLD = 0.70
LAYER2_MIN_NORMALIZED_TOKENS = 8
LAYER2_MIN_SHARED_TOKENS = 6
LAYER2_MIN_TARGET_TRIGRAM_RATIO = 0.60
LAYER2_MIN_SHARED_MEANINGFUL_TOKENS = 2
LAYER2_LOW_SCOPE_SINGLE_FILE_MAX_TOKENS = 120
LAYER2_LOW_SCOPE_SINGLE_FILE_MAX_LINES = 30
LAYER2_RELATED_PEER_MIN_SIMILARITY = 0.85
LAYER2_RELATED_PEER_MIN_TOKENS = 40
FUZZY_CROSS_PATH_DATA_EXTENSIONS = {".json", ".yaml", ".yml"}
LOW_SIGNAL_TEST_BACKPORT_MAX_TOKENS = 80
LOW_SIGNAL_TEST_BACKPORT_MAX_LINES = 30
LOW_SIGNAL_METADATA_MAX_TOKENS = 100
LOW_SIGNAL_METADATA_MAX_LINES = 40
TOP_LEVEL_LEGAL_METADATA_FILES = {
    "copying",
    "license",
    "license.md",
    "license.txt",
    "notice",
    "notice.md",
    "notice.txt",
}
LOW_SIGNAL_REPOSITORY_METADATA_FILES = {
    "codecov.yml",
    "codecov.yaml",
    "makefile",
    "cmakelists.txt",
}
DEPENDENCY_LICENSE_FILENAMES = {
    "copying",
    "copying.md",
    "copying.txt",
    "license",
    "license.md",
    "license.txt",
    "notice",
    "notice.md",
    "notice.txt",
}

# Keywords to preserve during normalization (C, Python, Tcl)
PRESERVED_KEYWORDS = {
    # C / C++
    "int", "char", "void", "long", "short", "double", "float",
    "unsigned", "signed", "const", "static", "volatile", "struct",
    "union", "enum", "typedef", "if", "else", "for", "while", "do",
    "switch", "case", "default", "break", "continue", "return",
    "goto", "sizeof", "NULL", "true", "false",
    # Python
    "def", "class", "import", "from", "try", "except", "raise",
    "finally", "with", "as", "pass", "lambda", "yield", "await",
    "async", "None", "True", "False", "is", "in", "not", "and", "or",
    # Tcl (Valkey tests)
    "proc", "set", "if", "else", "elseif", "switch", "while", "for",
    "foreach", "return", "break", "continue", "expr", "catch", "puts",
    "after", "upvar", "global", "variable", "namespace", "package",
    "source", "test", "r", "assert", "assert_equal", "assert_error",
    "assert_match",
}


def normalize_timestamp(timestamp):
    """Normalize ISO 8601 timestamp to UTC with \'Z\' suffix."""
    if not timestamp: return timestamp
    if timestamp.endswith("Z"): return timestamp
    try:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        utc_dt = dt.astimezone(timezone.utc)
        return utc_dt.isoformat().replace("+00:00", "Z")
    except ValueError as e:
        logger.error(f"Invalid timestamp format: {timestamp}")
        raise ValueError(f"Invalid timestamp format: {timestamp}") from e


def detect_code_movement(diff_text):
    """Detect if a diff is primarily code movement."""
    lines = diff_text.split("\n")
    added, removed = [], []
    for line in lines:
        if line.startswith("+") and not line.startswith("+++"):
            clean = line[1:].strip()
            if clean and not any(clean.startswith(p) for p in ["//", "/*", "#"]):
                added.append(clean)
        elif line.startswith("-") and not line.startswith("---"):
            clean = line[1:].strip()
            if clean and not any(clean.startswith(p) for p in ["//", "/*", "#"]):
                removed.append(clean)
    added_set, removed_set = set(added), set(removed)
    exact_matches = added_set & removed_set
    net_new_lines = len(added) - len(removed)
    movement_ratio = len(exact_matches) / len(added) if added else 0
    stats = {"net_new_lines": net_new_lines, "movement_ratio": movement_ratio}
    is_trivial = net_new_lines < MIN_NET_NEW_LINES or movement_ratio >= CODE_MOVEMENT_THRESHOLD
    return is_trivial, movement_ratio, net_new_lines, stats


def split_diff_by_file(diff_text):
    """Split a unified diff into a dict of {filename: diff_content}."""
    files, current_file, current_lines = {}, None, []
    for line in diff_text.splitlines():
        if line.startswith("diff --git"):
            if current_file and current_lines: files[current_file] = "\n".join(current_lines)
            match = re.search(r" b/(.*)$", line)
            current_file = match.group(1) if match else "unknown"
            current_lines = [line]
        elif current_file:
            if any(line.startswith(p) for p in ["From ", "From: ", "Date: ", "Subject: ", "Signed-off-by: ", "Co-authored-by: "]) or line == "---":
                continue
            current_lines.append(line)
    if current_file and current_lines: files[current_file] = "\n".join(current_lines)
    return files


def is_ignored_provenance_file(path):
    if not path:
        return False
    normalized = PurePosixPath(path)
    return (
        len(normalized.parts) >= 3
        and normalized.parts[0] == "deps"
        and normalized.name.lower() in DEPENDENCY_LICENSE_FILENAMES
    )


def _is_in_excluded_dir(path, config):
    if not path or not config:
        return False
    normalized = str(PurePosixPath(path))
    for excluded in getattr(config, "exclude_dirs", []):
        if normalized == excluded or normalized.startswith(excluded + "/"):
            return True
    return False


def filter_ignored_provenance_files(diff_text, config=None):
    files = split_diff_by_file(diff_text)
    if not files:
        return diff_text
    kept = [
        file_diff
        for path, file_diff in files.items()
        if not is_ignored_provenance_file(path) and not _is_in_excluded_dir(path, config)
    ]
    return "\n".join(kept)


def simhash64(text):
    """Compute 64-bit SimHash of text using overlapping trigrams."""
    if not text: return 0
    tokens = text.split()
    if not tokens: return 0
    shingles = tokens if len(tokens) < 3 else [f"{tokens[i]} {tokens[i+1]} {tokens[i+2]}" for i in range(len(tokens)-2)]
    v = [0] * 64
    for t in shingles:
        h = int.from_bytes(hashlib.blake2b(t.encode("utf-8"), digest_size=8).digest(), "big")
        for i in range(64):
            if h & (1 << i): v[i] += 1
            else: v[i] -= 1
    fingerprint = 0
    for i in range(64):
        if v[i] > 0: fingerprint |= 1 << i
    return fingerprint


def normalize_diff(diff_text, config, include_context=None):
    """Normalize unified diff for content-based fingerprinting."""
    lines = []
    diff_lines = diff_text.split("\n")
    change_count = sum(1 for l in diff_lines if l.startswith("+") or l.startswith("-"))
    if include_context is True: should_include_context = True
    elif include_context is False: should_include_context = False
    else: should_include_context = change_count > 0 and change_count <= 5

    for line in diff_lines:
        line = line.rstrip()
        if any(line.startswith(p) for p in ["diff --git", "index ", "--- ", "+++ ", "@@ "]): continue
        is_change = line.startswith("+") or line.startswith("-")
        is_context = not is_change and len(line) > 0 and not line.startswith("diff")
        if (is_context and not should_include_context) or not (is_change or is_context): continue
        if line.startswith("+++") or line.startswith("---"): continue

        if is_change:
            content = line[1:] if len(line) > 0 else ""
        else:
            content = line[1:] if len(line) > 0 else line
        content = content.strip()
        if not content: continue

        # aggressive comment stripping
        content = re.sub(r"//.*", "", content)
        content = re.sub(r"/\*.*?\*/", "", content)
        content = re.sub(r"#\s.*", "", content).strip()
        if not content or content.startswith("*"): continue

        tokens = re.findall(r'"(?:[^"\\]|\\.)*"' + '|' + r"'(?:[^'\\]|\\.)*'" + r"|[A-Za-z_][A-Za-z0-9_]*" + r"|\d+[uUlLfF]*" + r"|[^\w\s]+", content)
        normalized_tokens = []
        for t in tokens:
            if t.startswith('"') or t.startswith("'"): normalized_tokens.append("STR")
            elif re.match(r"^\d", t): normalized_tokens.append("NUM")
            elif re.match(r"^[A-Za-z_]", t):
                if t in PRESERVED_KEYWORDS: normalized_tokens.append(t)
                else: normalized_tokens.append(normalize_identifier(t, config))
            else: normalized_tokens.append("".join(t.split()))
        lines.append(" ".join(normalized_tokens))
    return "\n".join(lines)


def normalize_identifier(identifier, config):
    """Normalize an identifier by removing branding but preserving semantic meaning."""
    # Handle multiple prefix pairs
    for src_p, tgt_p in config.prefix_pairs:
        for prefix in [src_p, tgt_p]:
            if not prefix: continue
            if identifier.startswith(prefix) or identifier.startswith(prefix.lower()):
                return "M_" + identifier[len(prefix):]

    # Handle multiple brand pairs for Module types
    for src_b, tgt_b in config.branding_pairs:
        for brand in [src_b, tgt_b]:
            if not brand: continue
            if identifier.startswith(brand + "Module"):
                return "Module" + identifier[len(brand) + 6:]
            if identifier.startswith(brand.lower() + "Module"):
                return "module" + identifier[len(brand) + 6:]

    lower_id = identifier.lower()

    # Collect all terms to remove from all branding pairs
    branding_terms = set()
    for src_b, tgt_b in config.branding_pairs:
        if src_b: branding_terms.add(src_b.lower())
        if tgt_b: branding_terms.add(tgt_b.lower())
    branding_terms.add("keydb")

    for term in branding_terms:
        # Pattern 1: Prefix
        if lower_id.startswith(term):
            remainder = identifier[len(term):]
            if remainder:
                if remainder[0] == "_": remainder = remainder[1:]
                return remainder if remainder else identifier

        # Pattern 2: Separated
        if lower_id.startswith(term + "_"):
            return identifier[len(term) + 1 :]

        # Pattern 3: Infix
        for i in range(1, len(identifier) - len(term)):
            if identifier[i : i + len(term)].lower() == term:
                before_ok = (i == 0 or identifier[i - 1] == "_" or identifier[i].isupper())
                after_ok = (i + len(term) >= len(identifier) or identifier[i + len(term)] == "_" or identifier[i + len(term)].isupper())
                if before_ok and after_ok:
                    result = identifier[:i] + identifier[i + len(term) :]
                    if i < len(result) and i > 0 and result[i - 1] == "_" and result[i] == "_":
                        result = result[:i] + result[i + 1 :]
                    return result if result else identifier
    return identifier


def hamming_distance(a, b):
    xor = a ^ b
    count = 0
    while xor:
        count += xor & 1
        xor >>= 1
    return count


def compute_simhash_similarity(simhash_a, simhash_b):
    distance = hamming_distance(simhash_a, simhash_b)
    return 1.0 - (distance / 64.0)


def compute_file_fingerprints(diff_files, config):
    fingerprints = {}
    for filename, file_diff in diff_files.items():
        norm_file = normalize_diff(file_diff, config)
        if not norm_file: continue
        fp = {"simhash64": simhash64(norm_file)}
        patch_id = compute_patch_id(file_diff)
        if patch_id: fp["patch_id"] = patch_id
        fingerprints[filename] = fp
    return fingerprints


def is_infrastructure_file(filename, config):
    return any(p in filename for p in config.infrastructure_patterns)


def count_diff_lines(diff_text):
    count = 0
    for line in diff_text.split("\n"):
        if (line.startswith("+") and not line.startswith("+++")) or (line.startswith("-") and not line.startswith("---")):
            count += 1
    return count


def _token_trigrams(tokens):
    if len(tokens) < 3:
        return set()
    return set(zip(tokens, tokens[1:], tokens[2:]))


def _meaningful_tokens(tokens):
    return {
        token
        for token in tokens
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", token)
        and token not in PRESERVED_KEYWORDS
        and token not in {"STR", "NUM"}
    }


def _single_diff_exemption(diff_text, config):
    normalized = normalize_diff(diff_text, config)
    token_count = len(normalized.split())
    if token_count < MIN_TOKENS:
        return {"exempt": True, "reason": "too_few_tokens", "token_count": token_count}

    line_count = count_diff_lines(diff_text)
    if line_count < MIN_LINES:
        return {"exempt": True, "reason": "too_few_lines", "line_count": line_count}

    is_trivial, movement_ratio, net_new, stats = detect_code_movement(diff_text)
    if is_trivial:
        reason = "too_few_net_new_lines" if net_new < MIN_NET_NEW_LINES else "code_movement"
        return {
            "exempt": True,
            "reason": reason,
            "movement_ratio": movement_ratio,
            "net_new_lines": net_new,
            "stats": stats,
        }
    return {"exempt": False, "reason": None, "token_count": token_count, "line_count": line_count}


def _path_suffix(path):
    return PurePosixPath(path).suffix.lower() if path else ""


def _is_generated_command_metadata(path):
    if not path:
        return False
    normalized = PurePosixPath(path)
    return str(normalized) == "src/commands.def" or (
        str(normalized).startswith("src/commands/")
        and normalized.suffix.lower() in FUZZY_CROSS_PATH_DATA_EXTENSIONS
    )


def _normalize_login(login):
    return login.lower() if isinstance(login, str) and login else None


def author_login_from_info(info):
    if not isinstance(info, dict):
        return None
    author = info.get("author_login") or info.get("author")
    if isinstance(author, dict):
        author = author.get("login")
    if not author and isinstance(info.get("user"), dict):
        author = info["user"].get("login")
    return _normalize_login(author)


def _title_from_info(info):
    return info.get("title") if isinstance(info, dict) else None


def _target_paths_from_evidence(candidate, validation, target_diff_files):
    matches = []
    if isinstance(validation, dict):
        matches = validation.get("matched_files") or []
    if not matches and isinstance(candidate, dict):
        matches = candidate.get("matched_files") or []

    paths = {m.get("target") for m in matches if m.get("target")}
    if paths:
        return paths
    return set((target_diff_files or {}).keys())


def _all_changed_paths_are_top_level_legal_metadata(target_diff_files):
    paths = set((target_diff_files or {}).keys())
    if not paths:
        return False
    for path in paths:
        normalized = PurePosixPath(path)
        if normalized.parent != PurePosixPath("."):
            return False
        if normalized.name.lower() not in TOP_LEVEL_LEGAL_METADATA_FILES:
            return False
    return True


def _is_low_signal_repository_metadata_path(path):
    if not path:
        return False
    normalized = str(PurePosixPath(path))
    if normalized.startswith(".github/"):
        return True
    return PurePosixPath(path).name.lower() in LOW_SIGNAL_REPOSITORY_METADATA_FILES


def _diff_stats_for_paths(paths, target_diff_files, config):
    stats = []
    for path in paths:
        diff = (target_diff_files or {}).get(path, "")
        stats.append({
            "path": path,
            "line_count": count_diff_lines(diff),
            "token_count": len(normalize_diff(diff, config).split()),
        })
    return stats


def _has_only_low_signal_repository_metadata(paths, target_diff_files, config):
    if not paths or not all(_is_low_signal_repository_metadata_path(p) for p in paths):
        return False
    for stats in _diff_stats_for_paths(paths, target_diff_files, config):
        if (
            stats["line_count"] > LOW_SIGNAL_METADATA_MAX_LINES
            or stats["token_count"] > LOW_SIGNAL_METADATA_MAX_TOKENS
        ):
            return False
    return True


def _looks_like_release_aggregation_title(title):
    if not isinstance(title, str) or not title.strip():
        return False
    normalized = title.strip().lower()
    if re.match(r"^(redis|valkey)\s+\d+\.\d+(\.\d+)?(\s|$)", normalized):
        return True
    if "patch release" in normalized or normalized.startswith("release/"):
        return True
    if "release" in normalized and ("merge" in normalized or "fixes" in normalized or "rc" in normalized):
        return True
    if re.match(r"^fixes for valkey \d+\.\d+", normalized):
        return True
    return False


def _is_test_path(path):
    return str(PurePosixPath(path)).startswith("tests/")


def _is_test_like_path(path):
    if not path:
        return False
    normalized = str(PurePosixPath(path))
    name = PurePosixPath(path).name.lower()
    return (
        normalized.startswith("tests/")
        or normalized.startswith("src/unit/")
        or "/test" in normalized
        or name.startswith("test_")
        or name.startswith("test-")
    )


def _is_low_signal_release_test_backport(paths, target_diff_files, config, target_title):
    if not _looks_like_release_aggregation_title(target_title):
        return False
    if not paths or not all(_is_test_path(p) for p in paths):
        return False
    for stats in _diff_stats_for_paths(paths, target_diff_files, config):
        if (
            stats["line_count"] > LOW_SIGNAL_TEST_BACKPORT_MAX_LINES
            or stats["token_count"] > LOW_SIGNAL_TEST_BACKPORT_MAX_TOKENS
        ):
            return False
    return True


def build_layer2_file_evidence(
    *,
    target_path,
    source_path,
    target_diff,
    source_diff,
    target_diff_files,
    source_diff_files,
    config,
    score,
    shared_tokens,
    patch_id_match=False,
):
    """Build structured evidence for centralized layer2 policy decisions."""
    target_stats = _diff_stats_for_paths([target_path], {target_path: target_diff}, config)[0]
    source_stats = _diff_stats_for_paths([source_path], {source_path: source_diff}, config)[0]
    peer_scores = []

    for path, peer_target_diff in (target_diff_files or {}).items():
        if path == target_path or _is_test_like_path(path):
            continue
        peer_source_diff = (source_diff_files or {}).get(path)
        if not peer_source_diff:
            continue
        peer_score, peer_shared, _ = deep_compare_diffs(peer_target_diff, peer_source_diff, config)
        peer_scores.append({
            "target": path,
            "source": path,
            "score": peer_score,
            "shared_tokens": peer_shared,
            "target_tokens": len(normalize_diff(peer_target_diff, config).split()),
            "source_tokens": len(normalize_diff(peer_source_diff, config).split()),
        })

    return {
        "matched_file": {
            "target": target_path,
            "source": source_path,
            "score": score,
            "shared_tokens": shared_tokens,
            "patch_id_match": patch_id_match,
            "target_stats": target_stats,
            "source_stats": source_stats,
        },
        "peer_file_scores": peer_scores,
    }


def _has_related_peer_file(evidence):
    for peer in (evidence or {}).get("peer_file_scores", []):
        if (
            peer.get("score", 0.0) >= LAYER2_RELATED_PEER_MIN_SIMILARITY
            and min(peer.get("target_tokens", 0), peer.get("source_tokens", 0)) >= LAYER2_RELATED_PEER_MIN_TOKENS
        ):
            return True
    return False


def _non_test_target_paths(target_diff_files):
    return {
        path
        for path in (target_diff_files or {})
        if not _is_test_like_path(path) and not _is_low_signal_repository_metadata_path(path)
    }


def _is_low_scope_isolated_layer2_file_match(method, validation, target_diff_files, config):
    if method != "file_simhash+deep" or not isinstance(validation, dict):
        return False

    matches = validation.get("matched_files") or []
    if len(matches) != 1:
        return False

    match = matches[0]
    target_path = match.get("target")
    if not target_path or _is_test_like_path(target_path):
        return False
    if match.get("patch_id_match"):
        return False
    if validation.get("score", 0.0) < 0.95:
        return False

    if len(_non_test_target_paths(target_diff_files)) <= 1:
        return False

    evidence = validation.get("evidence") or {}
    matched_file = evidence.get("matched_file") or {}
    if matched_file.get("patch_id_match"):
        return False

    target_stats = matched_file.get("target_stats") or {}
    if not target_stats and target_path in (target_diff_files or {}):
        target_stats = _diff_stats_for_paths([target_path], target_diff_files, config)[0]
    if not target_stats:
        return False

    if (
        target_stats.get("line_count", 0) > LAYER2_LOW_SCOPE_SINGLE_FILE_MAX_LINES
        or target_stats.get("token_count", 0) > LAYER2_LOW_SCOPE_SINGLE_FILE_MAX_TOKENS
    ):
        return False

    return not _has_related_peer_file(evidence)


def _false_positive_context(
    candidate,
    db_type,
    method,
    config,
    target_author,
    target_title,
    target_diff_files,
    source_info,
    validation,
):
    entry = candidate.get("entry", {}) if isinstance(candidate, dict) else {}
    source_info = source_info or {}
    return {
        "candidate": candidate,
        "entry": entry,
        "db_type": db_type,
        "method": method,
        "config": config,
        "target_author": _normalize_login(target_author),
        "target_title": target_title,
        "target_diff_files": target_diff_files,
        "source_info": source_info,
        "source_author": author_login_from_info(source_info) or author_login_from_info(entry),
        "source_title": _title_from_info(source_info) or _title_from_info(entry),
        "validation": validation,
        "paths": _target_paths_from_evidence(candidate, validation, target_diff_files),
    }


def _same_author_pr_rule(ctx):
    if (
        ctx["db_type"] == "pr"
        and ctx["target_author"]
        and ctx["source_author"]
        and ctx["target_author"] == ctx["source_author"]
    ):
        return "same_author_pr"
    return None


def _top_level_legal_metadata_rule(ctx):
    if _all_changed_paths_are_top_level_legal_metadata(ctx["target_diff_files"]):
        return "top_level_legal_metadata_only"
    return None


def _generated_command_metadata_rule(ctx):
    paths = ctx["paths"]
    if paths and all(_is_generated_command_metadata(path) for path in paths):
        return "generated_command_metadata_only"
    return None


def _release_aggregation_rule(ctx):
    if (
        ctx["db_type"] == "pr"
        and _looks_like_release_aggregation_title(ctx["source_title"])
        and _looks_like_release_aggregation_title(ctx["target_title"])
    ):
        return "release_aggregation_candidate"
    return None


def _low_signal_repository_metadata_rule(ctx):
    if _has_only_low_signal_repository_metadata(ctx["paths"], ctx["target_diff_files"], ctx["config"]):
        return "low_signal_repository_metadata_only"
    return None


def _low_signal_release_test_backport_rule(ctx):
    if _is_low_signal_release_test_backport(
        ctx["paths"],
        ctx["target_diff_files"],
        ctx["config"],
        ctx["target_title"],
    ):
        return "low_signal_release_test_backport"
    return None


def _low_scope_isolated_layer2_file_match_rule(ctx):
    if _is_low_scope_isolated_layer2_file_match(
        ctx["method"],
        ctx["validation"],
        ctx["target_diff_files"],
        ctx["config"],
    ):
        return "low_scope_isolated_layer2_file_match"
    return None


FALSE_POSITIVE_RULES = (
    _same_author_pr_rule,
    _top_level_legal_metadata_rule,
    _generated_command_metadata_rule,
    _release_aggregation_rule,
    _low_signal_repository_metadata_rule,
    _low_signal_release_test_backport_rule,
    _low_scope_isolated_layer2_file_match_rule,
)


def evaluate_false_positive_filter(
    *,
    candidate,
    db_type,
    method,
    config,
    target_author=None,
    target_title=None,
    target_diff_files=None,
    source_info=None,
    validation=None,
):
    """Central policy dispatcher for suppressing known false positives."""
    ctx = _false_positive_context(
        candidate,
        db_type,
        method,
        config,
        target_author,
        target_title,
        target_diff_files,
        source_info,
        validation,
    )
    for rule in FALSE_POSITIVE_RULES:
        reason = rule(ctx)
        if reason:
            return {"filtered": True, "reason": reason}
    return {"filtered": False, "reason": None}


def evaluate_diff_exemption(
    diff_text,
    config,
    source_diff=None,
    shared_tokens=None,
    require_meaningful_tokens=False,
    target_path=None,
    source_path=None,
):
    target = _single_diff_exemption(diff_text, config)
    if target["exempt"]:
        return target

    if source_diff is None:
        return target

    if (
        require_meaningful_tokens
        and target_path
        and source_path
        and _is_generated_command_metadata(target_path)
        and _is_generated_command_metadata(source_path)
    ):
        return {"exempt": True, "reason": "deep_generated_command_metadata"}

    if (
        require_meaningful_tokens
        and target_path
        and source_path
        and target_path != source_path
        and _path_suffix(target_path) in FUZZY_CROSS_PATH_DATA_EXTENSIONS
        and _path_suffix(source_path) == _path_suffix(target_path)
    ):
        return {"exempt": True, "reason": "deep_cross_path_data_file"}

    source = _single_diff_exemption(source_diff, config)
    if source["exempt"]:
        return {"exempt": True, "reason": f"source_{source['reason']}", "source": source}

    target_tokens = normalize_diff(diff_text, config).split()
    source_tokens = normalize_diff(source_diff, config).split()
    if min(len(target_tokens), len(source_tokens)) < LAYER2_MIN_NORMALIZED_TOKENS:
        return {"exempt": True, "reason": "deep_too_few_tokens"}
    if shared_tokens is not None and shared_tokens < LAYER2_MIN_SHARED_TOKENS:
        return {"exempt": True, "reason": "deep_too_few_shared_tokens", "shared_tokens": shared_tokens}

    if require_meaningful_tokens:
        meaningful_overlap = _meaningful_tokens(target_tokens) & _meaningful_tokens(source_tokens)
        if len(meaningful_overlap) < LAYER2_MIN_SHARED_MEANINGFUL_TOKENS:
            return {
                "exempt": True,
                "reason": "deep_too_few_meaningful_tokens",
                "shared_meaningful_tokens": len(meaningful_overlap),
            }

    target_trigrams = _token_trigrams(target_tokens)
    source_trigrams = _token_trigrams(source_tokens)
    if not target_trigrams:
        return {"exempt": True, "reason": "deep_too_few_trigrams"}
    shared_trigrams = target_trigrams & source_trigrams
    trigram_ratio = len(shared_trigrams) / len(target_trigrams)
    if trigram_ratio < LAYER2_MIN_TARGET_TRIGRAM_RATIO:
        return {"exempt": True, "reason": "deep_too_few_shared_trigrams", "trigram_ratio": trigram_ratio}

    return {
        "exempt": False,
        "reason": None,
        "target": target,
        "source": source,
        "trigram_ratio": trigram_ratio,
        "shared_tokens": shared_tokens,
    }


def normalize_branding_terms(text, config):
    """Normalize all branding terms to BRAND for comparison."""
    patterns = []

    # Add patterns for all branding pairs
    for src_b, tgt_b in config.branding_pairs:
        if src_b:
            patterns.append((rf"\b{re.escape(src_b)}", "BRAND"))
            patterns.append((rf"\b{re.escape(src_b.lower())}", "BRAND"))
        if tgt_b:
            patterns.append((rf"\b{re.escape(tgt_b)}", "BRAND"))
            patterns.append((rf"\b{re.escape(tgt_b.lower())}", "BRAND"))

    # Add patterns for all prefix pairs
    for src_p, tgt_p in config.prefix_pairs:
        if src_p: patterns.append((rf"\b{re.escape(src_p)}", "BRAND_"))
        if tgt_p: patterns.append((rf"\b{re.escape(tgt_p)}", "BRAND_"))

    # Generic server/sentinel patterns
    patterns.extend([
        (r"\bserver([A-Z])", r"BRAND\1"), (r"\bServer([A-Z])", r"BRAND\1"),
        (r"\bsentinel([A-Z])", r"BRAND\1"), (r"\bSentinel([A-Z])", r"BRAND\1")
    ])

    result = text
    for pattern, replacement in patterns:
        result = re.sub(pattern, replacement, result)
    return result


def filter_branding_changes(diff_text, config):
    """Remove branding-only changes from a diff."""
    if not diff_text: return diff_text
    lines = diff_text.split("\n")
    filtered_lines, idx = [], 0
    while idx < len(lines):
        line = lines[idx]
        if line.startswith("-") and not line.startswith("---"):
            minus_lines, plus_lines, j = [line], [], idx + 1
            while j < len(lines) and lines[j].startswith("-") and not lines[j].startswith("---"):
                minus_lines.append(lines[j])
                j += 1
            while j < len(lines) and lines[j].startswith("+") and not lines[j].startswith("+++"):
                plus_lines.append(lines[j])
                j += 1
            if len(minus_lines) == len(plus_lines) and len(minus_lines) > 0:
                all_branding = True
                for m, p in zip(minus_lines, plus_lines):
                    if normalize_branding_terms(m[1:], config) != normalize_branding_terms(p[1:], config):
                        all_branding = False
                        break
                if all_branding:
                    idx = j
                    continue
        filtered_lines.append(line)
        idx += 1
    return "\n".join(filtered_lines)


def deep_compare_diffs(valkey_diff, redis_diff, config, matched_file=None):
    """Perform deep comparison of two diffs."""
    valkey_normalized = normalize_diff(valkey_diff, config)
    redis_normalized = normalize_diff(redis_diff, config)
    valkey_tokens = valkey_normalized.split()
    redis_tokens = redis_normalized.split()

    if not valkey_tokens or not redis_tokens:
        return 0.0, 0, max(len(valkey_tokens), len(redis_tokens))

    valkey_set, redis_set = set(valkey_tokens), set(redis_tokens)
    intersection = valkey_set & redis_set
    union = valkey_set | redis_set
    if not union: return 0.0, 0, 0

    jaccard = len(intersection) / len(union)
    subset_ratio = len(intersection) / len(valkey_set) if valkey_set else 0.0

    max_len = max(len(valkey_tokens), len(redis_tokens))
    matching_count = sum(1 for v, r in zip(valkey_tokens, redis_tokens) if v == r)
    sequence_sim = matching_count / max_len if max_len > 0 else 0.0

    weighted_sim = 0.6 * jaccard + 0.4 * sequence_sim
    final_similarity = max(weighted_sim, subset_ratio)

    return final_similarity, len(intersection), len(union)
