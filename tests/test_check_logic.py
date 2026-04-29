#!/usr/bin/env python3
"""
Unit tests for check.py matching logic.
"""

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from check import check_diff, find_matches, layer1_find_candidates, layer2_validate_candidate
from common import ProvenanceConfig


class TestCheckLogic(unittest.TestCase):
    def setUp(self):
        self.config = ProvenanceConfig(source_repo="redis/redis", target_repo="valkey-io/valkey")

    def make_diff(self, path, added_lines):
        return "\n".join(
            [
                f"diff --git a/{path} b/{path}",
                f"--- a/{path}",
                f"+++ b/{path}",
                "@@ -0,0 +1,6 @@",
                *[f"+{line}" for line in added_lines],
            ]
        )

    @patch("check.find_matches")
    def test_check_diff_ignores_dependency_license_files_before_matching(self, mock_find_matches):
        diff = self.make_diff(
            "deps/lz4/LICENSE",
            [f"license boilerplate clause {i} with common copyright terms" for i in range(12)],
        )

        found, findings = check_diff(
            diff.encode("utf-8"),
            {"prs": {}},
            {"commits": {}},
            self.config,
        )

        self.assertFalse(found)
        self.assertEqual(findings, [])
        mock_find_matches.assert_not_called()

    @patch("check.find_matches")
    def test_check_diff_ignores_configured_excluded_directories_before_matching(self, mock_find_matches):
        config = ProvenanceConfig(
            source_repo="redis/redis",
            target_repo="valkey-io/valkey",
            exclude_dirs=["deps/"],
        )
        diff = self.make_diff(
            "deps/lz4/lz4.c",
            [
                "int lz4_copy_block(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    total += 4;",
                "    total += 5;",
                "    return total;",
                "}",
            ],
        )

        found, findings = check_diff(
            diff.encode("utf-8"),
            {"prs": {}},
            {"commits": {}},
            config,
        )

        self.assertFalse(found)
        self.assertEqual(findings, [])
        mock_find_matches.assert_not_called()

    @patch("check.find_matches")
    def test_check_diff_keeps_dependency_code_without_configured_exclusion(self, mock_find_matches):
        diff = self.make_diff(
            "deps/lz4/lz4.c",
            [
                "int lz4_copy_block(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    total += 4;",
                "    total += 5;",
                "    return total;",
                "}",
            ],
        )
        mock_find_matches.side_effect = [[], []]

        found, findings = check_diff(
            diff.encode("utf-8"),
            {"prs": {}},
            {"commits": {}},
            self.config,
        )

        self.assertFalse(found)
        self.assertEqual(findings, [])
        self.assertEqual(mock_find_matches.call_count, 2)

    @patch("check.find_matches")
    def test_check_diff_reports_matching_file_pairs(self, mock_find_matches):
        diff = self.make_diff(
            "src/compression.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    return total;",
                "}",
            ],
        )
        mock_find_matches.side_effect = [
            [
                {
                    "entry": {"number": 42},
                    "method": "file_simhash+deep",
                    "deep_sim": 0.93,
                    "sim": 0.90,
                    "layer2": {
                        "matched_files": [
                            {
                                "target": "src/compression.c",
                                "source": "src/old_compression.c",
                                "sim": 0.91,
                                "patch_id_match": False,
                            }
                        ]
                    },
                }
            ],
            [],
        ]

        found, findings = check_diff(
            diff.encode("utf-8"),
            {"prs": {"42": {}}},
            {"commits": {}},
            self.config,
        )

        self.assertTrue(found)
        self.assertIn("file pairs: src/compression.c <- src/old_compression.c", findings[0][0])
        self.assertEqual(
            findings[0][1]["file_pairs"],
            [
                {
                    "target": "src/compression.c",
                    "source": "src/old_compression.c",
                    "similarity": 0.91,
                    "patch_id_match": False,
                }
            ],
        )

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

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_rejects_fuzzy_candidate_when_deep_unavailable(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"src/a.c": {"simhash64": 1}}, "patch_id": None}
        db = {"prs": {"1": {"number": 1, "simhash64": 1, "files": {}}}}
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1},
                "sim": 0.95,
                "patch_id_match": False,
                "signals": ["whole_simhash"],
                "matched_files": [],
            }
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

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_uses_structured_layer2_result(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"src/a.c": {"simhash64": 1}}, "patch_id": None}
        db = {"prs": {"1": {"number": 1, "simhash64": 1, "files": {}}}}
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1},
                "sim": 0.91,
                "patch_id_match": False,
                "signals": ["file_simhash"],
                "matched_files": [{"target": "src/a.c", "source": "src/old.c"}],
            }
        ]
        mock_layer2.return_value = {
            "accepted": True,
            "score": 0.93,
            "method": "file_simhash+deep",
            "matched_files": [{"target": "src/a.c", "source": "src/old.c"}],
        }

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"src/a.c": "dummy"},
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["method"], "file_simhash+deep")
        self.assertEqual(results[0]["deep_sim"], 0.93)
        self.assertEqual(results[0]["layer2"]["matched_files"][0]["source"], "src/old.c")

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_filters_same_author_deep_candidate(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"src/a.c": {"simhash64": 1}}, "patch_id": None}
        db = {"prs": {"1": {"number": 1, "simhash64": 1, "files": {}}}}
        target_diff = self.make_diff(
            "src/a.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    return total;",
                "}",
            ],
        )
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1},
                "sim": 0.95,
                "patch_id_match": False,
                "signals": ["file_simhash"],
                "matched_files": [{"target": "src/a.c", "source": "src/old.c"}],
            }
        ]
        mock_layer2.return_value = {
            "accepted": True,
            "score": 0.95,
            "method": "file_simhash+deep",
            "matched_files": [{"target": "src/a.c", "source": "src/old.c"}],
            "source_info": {"user": {"login": "alice"}},
        }

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"src/a.c": target_diff},
            target_author="alice",
        )

        self.assertEqual(results, [])

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_filters_metadata_only_legal_file(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"COPYING": {"simhash64": 1}}, "patch_id": None}
        db = {"prs": {"1": {"number": 1, "simhash64": 1, "files": {}}}}
        target_diff = self.make_diff(
            "COPYING",
            [f"Copyright line {i} with license boilerplate text" for i in range(12)],
        )
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1},
                "sim": 0.95,
                "patch_id_match": False,
                "signals": ["file_simhash"],
                "matched_files": [{"target": "COPYING", "source": "COPYING"}],
            }
        ]
        mock_layer2.return_value = {
            "accepted": True,
            "score": 0.95,
            "method": "file_simhash+deep",
            "matched_files": [{"target": "COPYING", "source": "COPYING"}],
        }

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"COPYING": target_diff},
        )

        self.assertEqual(results, [])

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_filters_generated_command_metadata_whole_fallback(self, mock_layer2, mock_layer1):
        fingerprint = {
            "simhash64": 1,
            "files": {"src/commands/latency-latest.json": {"simhash64": 1}},
            "patch_id": None,
        }
        db = {"prs": {"1": {"number": 1, "simhash64": 1, "files": {}}}}
        target_diff = self.make_diff(
            "src/commands/latency-latest.json",
            [
                "{",
                '    "summary": "Return latest latency samples",',
                '    "complexity": "O(N)",',
                '    "arguments": [{"name": "event", "type": "string"}]',
                "}",
            ],
        )
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1},
                "sim": 0.95,
                "patch_id_match": False,
                "signals": ["file_simhash"],
                "matched_files": [
                    {
                        "target": "src/commands/latency-latest.json",
                        "source": "src/commands/monitor.json",
                    }
                ],
            }
        ]
        mock_layer2.return_value = {
            "accepted": True,
            "score": 1.0,
            "method": "whole_simhash+deep",
            "matched_files": [],
        }

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"src/commands/latency-latest.json": target_diff},
        )

        self.assertEqual(results, [])

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_filters_release_aggregation_candidate(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"src/acl.c": {"simhash64": 1}}, "patch_id": None}
        db = {"prs": {"1": {"number": 1, "files": {}}}}
        target_diff = self.make_diff(
            "src/acl.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    return total;",
                "}",
            ],
        )
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1, "title": "Redis 7.2.7"},
                "sim": 1.0,
                "patch_id_match": True,
                "signals": ["file_patch_id"],
                "matched_files": [
                    {
                        "target": "src/acl.c",
                        "source": "src/acl.c",
                        "sim": 1.0,
                        "same_path": True,
                        "patch_id_match": True,
                    }
                ],
            }
        ]

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"src/acl.c": target_diff},
            target_title="Valkey Patch Release 7.2.8",
        )

        mock_layer2.assert_not_called()
        self.assertEqual(results, [])

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_filters_low_signal_release_test_backport(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"tests/unit/pause.tcl": {"simhash64": 1}}, "patch_id": None}
        db = {"prs": {"1": {"number": 1, "simhash64": 1, "files": {}}}}
        target_diff = self.make_diff(
            "tests/unit/pause.tcl",
            [
                'test "pause case" {',
                "    r multi",
                "    r ping",
                "    r exec",
                "}",
                'test "pause case two" {',
                "    r multi",
                "    r ping",
                "    r exec",
                "}",
            ],
        )
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1},
                "sim": 0.95,
                "patch_id_match": False,
                "signals": ["file_simhash"],
                "matched_files": [{"target": "tests/unit/pause.tcl", "source": "tests/unit/pause.tcl"}],
            }
        ]
        mock_layer2.return_value = {
            "accepted": True,
            "score": 1.0,
            "method": "file_simhash+deep",
            "matched_files": [{"target": "tests/unit/pause.tcl", "source": "tests/unit/pause.tcl"}],
        }

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"tests/unit/pause.tcl": target_diff},
            target_title="Fixes for Valkey 8.0.3",
        )

        self.assertEqual(results, [])

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_keeps_regular_test_match(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"tests/unit/acl.tcl": {"simhash64": 1}}, "patch_id": None}
        db = {"prs": {"1": {"number": 1, "simhash64": 1, "files": {}}}}
        target_diff = self.make_diff(
            "tests/unit/acl.tcl",
            [
                "test {acl comments are ignored} {",
                "    r acl setuser default on",
                "    r acl load",
                "    assert_match {*OK*} [r acl save]",
                "    r acl setuser replica on",
                "    r acl load",
                "    assert_equal {user default on} [r acl list]",
                "    r acl deluser replica",
                "    assert_equal 1 [r acl load]",
                "    assert_match {*default*} [r acl list]",
                "}",
            ],
        )
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1},
                "sim": 0.95,
                "patch_id_match": False,
                "signals": ["file_simhash"],
                "matched_files": [{"target": "tests/unit/acl.tcl", "source": "tests/unit/acl.tcl"}],
            }
        ]
        mock_layer2.return_value = {
            "accepted": True,
            "score": 1.0,
            "method": "file_simhash+deep",
            "matched_files": [{"target": "tests/unit/acl.tcl", "source": "tests/unit/acl.tcl"}],
        }

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"tests/unit/acl.tcl": target_diff},
            target_title="Allow comments in ACL files",
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["method"], "file_simhash+deep")

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_accepts_whole_patch_id_without_layer2(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"src/a.c": {"simhash64": 1}}, "patch_id": "same"}
        db = {"prs": {"1": {"number": 1, "patch_id": "same", "files": {}}}}
        target_diff = self.make_diff(
            "src/a.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    return total;",
                "}",
            ],
        )
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1},
                "sim": 0.0,
                "patch_id_match": True,
                "signals": ["patch_id"],
                "matched_files": [],
            }
        ]

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"src/a.c": target_diff},
        )

        mock_layer2.assert_not_called()
        self.assertEqual(results[0]["method"], "patch_id")
        self.assertEqual(results[0]["deep_sim"], 1.0)

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_skips_same_author_patch_id(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"src/a.c": {"simhash64": 1}}, "patch_id": "same"}
        db = {"prs": {"1": {"number": 1, "patch_id": "same", "files": {}}}}
        target_diff = self.make_diff(
            "src/a.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    return total;",
                "}",
            ],
        )
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1, "author_login": "alice"},
                "sim": 0.0,
                "patch_id_match": True,
                "signals": ["patch_id"],
                "matched_files": [],
            }
        ]

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"src/a.c": target_diff},
            target_author="alice",
        )

        mock_layer2.assert_not_called()
        self.assertEqual(results, [])

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_accepts_different_author_patch_id(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"src/a.c": {"simhash64": 1}}, "patch_id": "same"}
        db = {"prs": {"1": {"number": 1, "patch_id": "same", "files": {}}}}
        target_diff = self.make_diff(
            "src/a.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    return total;",
                "}",
            ],
        )
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1, "author_login": "alice"},
                "sim": 0.0,
                "patch_id_match": True,
                "signals": ["patch_id"],
                "matched_files": [],
            }
        ]

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"src/a.c": target_diff},
            target_author="bob",
        )

        mock_layer2.assert_not_called()
        self.assertEqual(results[0]["method"], "patch_id")

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_accepts_file_patch_id_without_layer2(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"src/a.c": {"simhash64": 1}}, "patch_id": None}
        db = {"prs": {"1": {"number": 1, "files": {}}}}
        target_diff = self.make_diff(
            "src/a.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    return total;",
                "}",
            ],
        )
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1},
                "sim": 0.0,
                "patch_id_match": True,
                "signals": ["file_patch_id"],
                "matched_files": [
                    {
                        "target": "src/a.c",
                        "source": "src/old.c",
                        "sim": 0.0,
                        "same_path": False,
                        "patch_id_match": True,
                    }
                ],
            }
        ]

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"src/a.c": target_diff},
        )

        mock_layer2.assert_not_called()
        self.assertEqual(results[0]["method"], "file_patch_id")
        self.assertEqual(results[0]["deep_sim"], 1.0)

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_skips_same_author_file_patch_id(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"src/a.c": {"simhash64": 1}}, "patch_id": None}
        db = {"prs": {"1": {"number": 1, "files": {}}}}
        target_diff = self.make_diff(
            "src/a.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    return total;",
                "}",
            ],
        )
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1, "author_login": "alice"},
                "sim": 0.0,
                "patch_id_match": True,
                "signals": ["file_patch_id"],
                "matched_files": [
                    {
                        "target": "src/a.c",
                        "source": "src/old.c",
                        "sim": 0.0,
                        "same_path": False,
                        "patch_id_match": True,
                    }
                ],
            }
        ]

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"src/a.c": target_diff},
            target_author="alice",
        )

        mock_layer2.assert_not_called()
        self.assertEqual(results, [])

    @patch("check.layer1_find_candidates")
    @patch("check.layer2_validate_candidate")
    def test_find_matches_exempts_tiny_file_patch_id(self, mock_layer2, mock_layer1):
        fingerprint = {"simhash64": 1, "files": {"src/a.c": {"simhash64": 1}}, "patch_id": None}
        db = {"prs": {"1": {"number": 1, "files": {}}}}
        mock_layer1.return_value = [
            {
                "key": "1",
                "entry": {"number": 1},
                "sim": 0.0,
                "patch_id_match": True,
                "signals": ["file_patch_id"],
                "matched_files": [
                    {
                        "target": "src/a.c",
                        "source": "src/old.c",
                        "sim": 0.0,
                        "same_path": False,
                        "patch_id_match": True,
                    }
                ],
            }
        ]

        results = find_matches(
            fingerprint,
            db,
            threshold=0.90,
            max_report=5,
            db_type="pr",
            config=self.config,
            diff_files={"src/a.c": self.make_diff("src/a.c", ["return a < b ? a : b;"])},
        )

        mock_layer2.assert_not_called()
        self.assertEqual(results, [])

    @patch("check.fetch_pr_diff")
    def test_layer2_validates_matched_file_without_target_noise(self, mock_fetch):
        target_bad = self.make_diff(
            "src/bad.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    serverLog(LL_NOTICE, \"copied\");",
                "    return total;",
                "}",
            ],
        )
        target_noise = self.make_diff(
            "src/noise.c",
            [f"int unrelated_{i}(void) {{ return {i}; }}" for i in range(40)],
        )
        source = self.make_diff(
            "src/old.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    redisLog(LL_NOTICE, \"copied\");",
                "    return total;",
                "}",
            ],
        )
        mock_fetch.return_value = (source.encode("utf-8"), {"number": 42})
        candidate = {
            "key": "42",
            "entry": {"number": 42},
            "signals": ["file_simhash"],
            "matched_files": [
                {
                    "target": "src/bad.c",
                    "source": "src/old.c",
                    "sim": 0.91,
                    "same_path": False,
                    "patch_id_match": False,
                }
            ],
        }

        result = layer2_validate_candidate(
            {"src/bad.c": target_bad, "src/noise.c": target_noise},
            candidate,
            "pr",
            self.config,
            token=None,
        )

        self.assertIsNotNone(result)
        self.assertTrue(result["accepted"])
        self.assertEqual(result["method"], "file_simhash+deep")
        self.assertGreaterEqual(result["score"], 0.90)
        self.assertEqual(result["matched_files"][0]["target"], "src/bad.c")

    @patch("check.fetch_pr_diff")
    def test_layer2_rejects_tiny_generic_overlap(self, mock_fetch):
        target = self.make_diff("src/bad.c", ["return NULL;"])
        source = self.make_diff("src/old.c", ["return NULL;", "return NULL;", "return NULL;"])
        mock_fetch.return_value = (source.encode("utf-8"), {"number": 42})
        candidate = {
            "key": "42",
            "entry": {"number": 42},
            "signals": ["file_simhash"],
            "matched_files": [
                {
                    "target": "src/bad.c",
                    "source": "src/old.c",
                    "sim": 0.90,
                    "same_path": False,
                    "patch_id_match": False,
                }
            ],
        }

        result = layer2_validate_candidate(
            {"src/bad.c": target},
            candidate,
            "pr",
            self.config,
            token=None,
        )

        self.assertIsNone(result)

    @patch("check.fetch_pr_diff")
    def test_layer2_rejects_string_heavy_cross_path_file_match(self, mock_fetch):
        target = self.make_diff(
            "src/commands/new.json",
            [
                "{",
                '    "summary": "Copy a slot list",',
                '    "complexity": "O(N)",',
                '    "arguments": [{"name": "slot", "type": "integer"}]',
                "}",
            ],
        )
        source = self.make_diff(
            "src/commands/old.json",
            [
                "{",
                '    "summary": "Migrate a slot list",',
                '    "complexity": "O(N)",',
                '    "arguments": [{"name": "slot", "type": "integer"}]',
                "}",
            ],
        )
        mock_fetch.return_value = (source.encode("utf-8"), {"number": 42})
        candidate = {
            "key": "42",
            "entry": {"number": 42},
            "signals": ["file_simhash"],
            "matched_files": [
                {
                    "target": "src/commands/new.json",
                    "source": "src/commands/old.json",
                    "sim": 0.90,
                    "same_path": False,
                    "patch_id_match": False,
                }
            ],
        }

        result = layer2_validate_candidate(
            {"src/commands/new.json": target},
            candidate,
            "pr",
            self.config,
            token=None,
        )

        self.assertIsNone(result)

    @patch("check.fetch_pr_diff")
    def test_layer2_rejects_generated_command_metadata_file_match(self, mock_fetch):
        target = self.make_diff(
            "src/commands.def",
            [
                "#ifndef SKIP_CMD_HISTORY_TABLE",
                "#define HGETDEL_History NULL",
                "#endif",
                "struct COMMAND_ARG HGETDEL_Args[] = {",
                '    {MAKE_ARG("key", ARG_TYPE_KEY, 0, NULL, NULL, NULL, CMD_ARG_NONE, 0, NULL)},',
                "};",
            ],
        )
        source = self.make_diff(
            "src/commands.def",
            [
                "#ifndef SKIP_CMD_HISTORY_TABLE",
                "#define HGETDEL_History NULL",
                "#endif",
                "struct COMMAND_ARG HGETDEL_Args[] = {",
                '    {MAKE_ARG("key", ARG_TYPE_KEY, 0, NULL, NULL, NULL, CMD_ARG_NONE, 0, NULL)},',
                '    {MAKE_ARG("fields", ARG_TYPE_STRING, -1, NULL, NULL, NULL, CMD_ARG_MULTIPLE, 0, NULL)},',
                "};",
            ],
        )
        mock_fetch.return_value = (source.encode("utf-8"), {"number": 42})
        candidate = {
            "key": "42",
            "entry": {"number": 42},
            "signals": ["file_simhash"],
            "matched_files": [
                {
                    "target": "src/commands.def",
                    "source": "src/commands.def",
                    "sim": 0.90,
                    "same_path": True,
                    "patch_id_match": False,
                }
            ],
        }

        result = layer2_validate_candidate(
            {"src/commands.def": target},
            candidate,
            "pr",
            self.config,
            token=None,
        )

        self.assertIsNone(result)

    @patch("check.fetch_pr_diff")
    def test_layer2_falls_back_to_whole_diff_when_file_pair_is_exempt(self, mock_fetch):
        target_tiny = self.make_diff("src/module.c", ["int moduleApiVersion = 1;"])
        target_context = self.make_diff(
            "src/other.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    return total;",
                "}",
            ],
        )
        source_tiny = self.make_diff("src/module.c", ["int moduleApiVersion = 1;"])
        source_context = self.make_diff(
            "src/other.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    total += 3;",
                "    return total;",
                "}",
            ],
        )
        mock_fetch.return_value = ((source_tiny + "\n" + source_context).encode("utf-8"), {"number": 42})
        candidate = {
            "key": "42",
            "entry": {"number": 42},
            "signals": ["file_simhash"],
            "matched_files": [
                {
                    "target": "src/module.c",
                    "source": "src/module.c",
                    "sim": 0.91,
                    "same_path": True,
                    "patch_id_match": False,
                }
            ],
        }

        result = layer2_validate_candidate(
            {"src/module.c": target_tiny, "src/other.c": target_context},
            candidate,
            "pr",
            self.config,
            token=None,
        )

        self.assertIsNotNone(result)
        self.assertEqual(result["method"], "whole_simhash+deep")

    @patch("check.fetch_pr_diff")
    def test_layer2_uses_whole_diff_fallback_without_file_evidence(self, mock_fetch):
        target = self.make_diff(
            "src/a.c",
            [
                "int copied(int input) {",
                "    int total = input + 1;",
                "    total += 2;",
                "    return total;",
                "}",
            ],
        )
        mock_fetch.return_value = (target.encode("utf-8"), {"number": 42})
        candidate = {
            "key": "42",
            "entry": {"number": 42},
            "signals": ["whole_simhash"],
            "matched_files": [],
        }

        result = layer2_validate_candidate(
            {"src/a.c": target},
            candidate,
            "pr",
            self.config,
            token=None,
        )

        self.assertIsNotNone(result)
        self.assertTrue(result["accepted"])
        self.assertEqual(result["method"], "whole_simhash+deep")

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
