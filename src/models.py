"""Typed data contracts for provenance matching."""

from typing import Any, Dict, List, Optional, TypedDict


class FileFingerprint(TypedDict, total=False):
    simhash64: int
    patch_id: str


class Fingerprint(TypedDict, total=False):
    simhash64: int
    patch_id: Optional[str]
    files: Dict[str, FileFingerprint]


class FileMatch(TypedDict, total=False):
    target: str
    source: str
    sim: float
    same_path: bool
    patch_id_match: bool


class Candidate(TypedDict, total=False):
    key: str
    entry: Dict[str, Any]
    sim: float
    patch_id_match: bool
    matched_files: List[FileMatch]
    signals: List[str]
    deep_sim: Optional[float]
    method: str
    layer2: Dict[str, Any]


class Layer2Validation(TypedDict, total=False):
    accepted: bool
    score: float
    method: str
    matched_files: List[FileMatch]
    source_info: Dict[str, Any]
    evidence: Optional[Dict[str, Any]]
