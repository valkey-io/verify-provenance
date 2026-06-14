"""Configuration parsing helpers for provenance tools."""

from pathlib import PurePosixPath


class ProvenanceConfig:
    """Repository-specific provenance configuration."""

    def __init__(
        self,
        source_repo=None,
        target_repo=None,
        normalization_pairs=None,
        infrastructure_patterns=None,
        exclude_dirs=None,
        **kwargs,
    ):
        self.source_repo = source_repo
        self.target_repo = target_repo
        self.normalization_pairs = list(normalization_pairs) if normalization_pairs else []

        self.infrastructure_patterns = infrastructure_patterns or []
        self.exclude_dirs = [
            str(PurePosixPath(path.strip().strip("/")))
            for path in (exclude_dirs or [])
            if path and path.strip().strip("/")
        ]

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


def parse_pair_list(raw):
    if not raw:
        return []
    pairs = []
    for part in raw.split(","):
        if not part:
            continue
        source, separator, target = part.partition(":")
        if not separator:
            raise ValueError(f"Invalid pair '{part}', expected Source:Target")
        pairs.append((source, target))
    return pairs


def parse_csv_list(raw):
    if not raw:
        return []
    return [part for part in raw.split(",") if part]


def config_from_args(args, *, source_repo=None, target_repo=None):
    return ProvenanceConfig(
        source_repo=source_repo if source_repo is not None else getattr(args, "source_repo", None),
        target_repo=target_repo if target_repo is not None else getattr(args, "target_repo", None),
        normalization_pairs=parse_pair_list(getattr(args, "normalization_pairs", None)),
        infrastructure_patterns=parse_csv_list(getattr(args, "infrastructure_patterns", None)),
        exclude_dirs=parse_csv_list(getattr(args, "exclude_dirs", None)),
    )
