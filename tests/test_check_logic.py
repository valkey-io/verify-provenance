#!/usr/bin/env python3
"""
Unit tests for check.py matching logic.
"""

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from check import find_matches, layer1_find_candidates
from common import ProvenanceConfig


class TestCheckLogic(unittest.TestCase):
    def setUp(self):
        self.config = ProvenanceConfig(source_repo="redis/redis", target_repo="valkey-io/valkey")

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_applies_threshold_when_deep_unavailable(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"src/a.c": {"simhash64": 1}}, "patch_id": None}
        db = {"prs": {"1": {"number": 1, "simhash64": 2, "files": {}}}}
        mock_layer1.return_value = [
            {"key": "1", "entry": {"number": 1}, "sim": 0.81, "patch_id_match": False, "matched_files": []}
        ]
        mock_layer2.return_value = None

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"src/a.c": "dummy"},
        )
        self.assertEqual(results, [])

    def test_layer1_file_match_is_path_independent(self):
        fingerprint = {
            "simhash64": 0,
            "patch_id": None,
            "files": {
                "src/new_path.c": {"simhash64": (1 << 64) - 1, "patch_id": "abc"},
            },
        }
        db = {
            "prs": {
                "42": {
                    "number": 42,
                    "simhash64": (1 << 64) - 1,
                    "patch_id": None,
                    "files": {
                        "src/old_path.c": {"simhash64": (1 << 64) - 1, "patch_id": "def"},
                    },
                }
            }
        }

        candidates = layer1_find_candidates(fingerprint, db, "pr", self.config)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["entry"]["number"], 42)
        self.assertIn("file_simhash", candidates[0]["signals"])
        self.assertEqual(
            candidates[0]["matched_files"],
            [
                {
                    "target": "src/new_path.c",
                    "source": "src/old_path.c",
                    "sim": 1.0,
                    "same_path": False,
                    "patch_id_match": False,
                }
            ],
        )

    def test_layer1_patch_id_match_is_independent_of_simhash(self):
        fingerprint = {
            "simhash64": 0,
            "patch_id": "same-patch",
            "files": {"src/a.c": {"simhash64": 0}},
        }
        db = {
            "prs": {
                "42": {
                    "number": 42,
                    "simhash64": (1 << 64) - 1,
                    "patch_id": "same-patch",
                    "files": {},
                }
            }
        }

        candidates = layer1_find_candidates(fingerprint, db, "pr", self.config)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["sim"], 0.0)
        self.assertTrue(candidates[0]["patch_id_match"])
        self.assertEqual(candidates[0]["signals"], ["patch_id"])

    def test_layer1_file_patch_id_match_is_independent_of_simhash_and_path(self):
        fingerprint = {
            "simhash64": 0,
            "patch_id": None,
            "files": {"src/new.c": {"simhash64": 0, "patch_id": "same-file-patch"}},
        }
        db = {
            "prs": {
                "42": {
                    "number": 42,
                    "simhash64": (1 << 64) - 1,
                    "files": {
                        "src/old.c": {
                            "simhash64": (1 << 64) - 1,
                            "patch_id": "same-file-patch",
                        }
                    },
                }
            }
        }

        candidates = layer1_find_candidates(fingerprint, db, "pr", self.config)
        self.assertEqual(len(candidates), 1)
        self.assertTrue(candidates[0]["patch_id_match"])
        self.assertIn("file_patch_id", candidates[0]["signals"])
        self.assertEqual(candidates[0]["matched_files"][0]["target"], "src/new.c")
        self.assertEqual(candidates[0]["matched_files"][0]["source"], "src/old.c")
        self.assertTrue(candidates[0]["matched_files"][0]["patch_id_match"])

    def test_layer1_dedupes_and_accumulates_signals(self):
        fingerprint = {
            "simhash64": 123,
            "patch_id": "same-patch",
            "files": {"src/a.c": {"simhash64": 123, "patch_id": "file-patch"}},
        }
        db = {
            "prs": {
                "42": {
                    "number": 42,
                    "simhash64": 123,
                    "patch_id": "same-patch",
                    "files": {"src/b.c": {"simhash64": 123, "patch_id": "file-patch"}},
                }
            }
        }

        candidates = layer1_find_candidates(fingerprint, db, "pr", self.config)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(
            candidates[0]["signals"],
            ["patch_id", "whole_simhash", "file_patch_id", "file_simhash"],
        )

    def test_layer1_sorts_patch_id_before_higher_simhash(self):
        fingerprint = {
            "simhash64": 0,
            "patch_id": "same-patch",
            "files": {"src/a.c": {"simhash64": 0}},
        }
        db = {
            "prs": {
                "sim": {"number": 1, "simhash64": 0, "files": {}},
                "patch": {
                    "number": 2,
                    "simhash64": (1 << 64) - 1,
                    "patch_id": "same-patch",
                    "files": {},
                },
            }
        }

        candidates = layer1_find_candidates(fingerprint, db, "pr", self.config)
        self.assertEqual([c["entry"]["number"] for c in candidates], [2, 1])

    def test_layer1_applies_date_filter_to_all_signals(self):
        fingerprint = {
            "simhash64": 0,
            "patch_id": "same-patch",
            "files": {"src/a.c": {"simhash64": 0, "patch_id": "file-patch"}},
        }
        db = {
            "prs": {
                "future": {
                    "number": 42,
                    "created_at": "2026-01-02T00:00:00Z",
                    "simhash64": 0,
                    "patch_id": "same-patch",
                    "files": {"src/b.c": {"simhash64": 0, "patch_id": "file-patch"}},
                }
            }
        }

        candidates = layer1_find_candidates(
            fingerprint,
            db,
            "pr",
            self.config,
            date="2026-01-01T00:00:00Z",
        )
        self.assertEqual(candidates, [])

    def test_layer1_applies_commit_date_filter(self):
        fingerprint = {
            "simhash64": 0,
            "patch_id": "same-patch",
            "files": {"src/a.c": {"simhash64": 0}},
        }
        db = {
            "commits": {
                "future": {
                    "sha": "abc123",
                    "date": "2026-01-02T00:00:00Z",
                    "simhash64": 0,
                    "patch_id": "same-patch",
                    "files": {},
                }
            }
        }

        candidates = layer1_find_candidates(
            fingerprint,
            db,
            "commit",
            self.config,
            date="2026-01-01T00:00:00Z",
        )
        self.assertEqual(candidates, [])

        candidates = layer1_find_candidates(
            fingerprint,
            db,
            "commit",
            self.config,
            date="2026-01-01T00:00:00Z",
            ignore_date=True,
        )
        self.assertEqual(len(candidates), 1)

    def test_layer1_skips_infrastructure_source_files(self):
        config = ProvenanceConfig(
            source_repo="redis/redis",
            target_repo="valkey-io/valkey",
            infrastructure_patterns=[".github/"],
        )
        fingerprint = {
            "simhash64": (1 << 64) - 1,
            "patch_id": None,
            "files": {"src/a.c": {"simhash64": (1 << 64) - 1}},
        }
        db = {
            "prs": {
                "1": {
                    "number": 1,
                    "simhash64": 0,
                    "files": {
                        ".github/workflows/ci.yml": {"simhash64": (1 << 64) - 1},
                    },
                }
            }
        }

        self.assertEqual(layer1_find_candidates(fingerprint, db, "pr", config), [])

    def test_layer1_skips_infrastructure_only_target(self):
        config = ProvenanceConfig(
            source_repo="redis/redis",
            target_repo="valkey-io/valkey",
            infrastructure_patterns=[".github/"],
        )
        fingerprint = {
            "simhash64": 0,
            "patch_id": "same-patch",
            "files": {".github/workflows/ci.yml": {"simhash64": 0}},
        }
        db = {"prs": {"1": {"number": 1, "patch_id": "same-patch", "files": {}}}}

        self.assertEqual(layer1_find_candidates(fingerprint, db, "pr", config), [])


if __name__ == "__main__":
    unittest.main()
