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
                    "simhash64": 0,
                    "patch_id": None,
                    "files": {
                        "src/old_path.c": {"simhash64": (1 << 64) - 1, "patch_id": "abc"},
                    },
                }
            }
        }

        candidates = layer1_find_candidates(fingerprint, db, "pr", self.config)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["entry"]["number"], 42)


if __name__ == "__main__":
    unittest.main()
