"""Configuration parsing helpers for provenance tools."""

from pathlib import PurePosixPath


class ProvenanceConfig:
    """Repository-specific provenance configuration."""

    def __init__(
        self,
        source_repo=None,
        target_repo=None,
        branding_pairs=None,
        prefix_pairs=None,
        infrastructure_patterns=None,
        exclude_dirs=None,
        **kwargs,
    ):
        self.source_repo = source_repo
        self.target_repo = target_repo
        self.branding_pairs = list(branding_pairs) if branding_pairs else []
        self.prefix_pairs = list(prefix_pairs) if prefix_pairs else []

        # Handle backward-compatible single-pair arguments.
        self.source_brand = kwargs.get("source_brand")
        self.target_brand = kwargs.get("target_brand")
        if self.source_brand or self.target_brand:
            pair = (self.source_brand, self.target_brand)
            if pair not in self.branding_pairs:
                self.branding_pairs.append(pair)

        self.source_prefix = kwargs.get("source_prefix")
        self.target_prefix = kwargs.get("target_prefix")
        if self.source_prefix or self.target_prefix:
            pair = (self.source_prefix, self.target_prefix)
            if pair not in self.prefix_pairs:
                self.prefix_pairs.append(pair)

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
        branding_pairs=parse_pair_list(getattr(args, "branding_pairs", None)),
        prefix_pairs=parse_pair_list(getattr(args, "prefix_pairs", None)),
        infrastructure_patterns=parse_csv_list(getattr(args, "infrastructure_patterns", None)),
        exclude_dirs=parse_csv_list(getattr(args, "exclude_dirs", None)),
        source_brand=getattr(args, "source_brand", None),
        target_brand=getattr(args, "target_brand", None),
        source_prefix=getattr(args, "source_prefix", None),
        target_prefix=getattr(args, "target_prefix", None),
    )
