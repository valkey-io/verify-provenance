"""Fingerprint database helpers."""

import gzip
import json
import logging
import os

logger = logging.getLogger(__name__)


class DatabaseLoadError(RuntimeError):
    pass


def load_db(path, *, strict=False):
    if not os.path.exists(path):
        return {}
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, gzip.BadGzipFile, json.JSONDecodeError) as e:
        if strict:
            raise DatabaseLoadError(f"Failed to load database {path}: {e}") from e
        logger.warning("Failed to load database %s: %s", path, e)
        return {}
