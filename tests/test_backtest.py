#!/usr/bin/env python3
"""
Unit tests for backtest.py behavior.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from backtest import (
    check_pr,
    default_expected_positives,
    main,
    parse_expected_positives,
    validate_backtest_results,
)


class TestBacktest(unittest.TestCase):
    @patch("backtest.subprocess.run")
    def test_check_pr_404_is_not_found(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="HTTPError 404 Not Found")
        status, detail = check_pr(1234, ["--source-repo", "redis/redis"])
        self.assertEqual(status, "NOT_FOUND")
        self.assertIsNone(detail)

    @patch("backtest.subprocess.run")
    def test_check_pr_match_lines_report_fail(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="2026-02-17 [INFO] matches redis/redis PR #1 (similarity: 0.900, method: simhash)\n",
        )
        status, detail = check_pr(1234, ["--source-repo", "redis/redis"])
        self.assertEqual(status, "FAIL")
        self.assertIn("matches redis/redis PR #1", detail)

    def test_parse_expected_positives(self):
        self.assertEqual(parse_expected_positives("3080, 3085,3102"), {3080, 3085, 3102})
        self.assertEqual(parse_expected_positives(""), set())

    def test_default_expected_positives_for_valkey_range(self):
        args = MagicMock(start=2800, end=3120, target_repo="valkey-io/valkey")
        self.assertEqual(default_expected_positives(args), {3080, 3085, 3088, 3095, 3102})

    def test_default_expected_positives_are_range_scoped(self):
        args = MagicMock(start=3080, end=3088, target_repo="valkey-io/valkey")
        self.assertEqual(default_expected_positives(args), {3080, 3085, 3088})

    def test_validate_backtest_results_rejects_unexpected_flags(self):
        failed = [(3080, "match"), (3001, "unexpected")]
        errors = []
        ok, problems = validate_backtest_results(failed, errors, {3080})
        self.assertFalse(ok)
        self.assertTrue(any("Unexpected flagged PRs: 3001" in p for p in problems))

    def test_validate_backtest_results_rejects_missing_flags(self):
        ok, problems = validate_backtest_results([], [], {3080})
        self.assertFalse(ok)
        self.assertTrue(any("Missing expected flagged PRs: 3080" in p for p in problems))

    def test_validate_backtest_results_rejects_errors(self):
        ok, problems = validate_backtest_results([], [(2800, "401")], set())
        self.assertFalse(ok)
        self.assertTrue(any("Backtest had 1 errors/timeouts" in p for p in problems))

    def test_validate_backtest_results_accepts_expected_flags(self):
        ok, problems = validate_backtest_results([(3080, "match")], [], {3080})
        self.assertTrue(ok)
        self.assertEqual(problems, [])

    @patch("backtest.check_pr")
    def test_main_forwards_extra_check_arguments(self, mock_check_pr):
        mock_check_pr.return_value = ("PASS", None)
        argv = [
            "backtest.py",
            "--start", "1",
            "--end", "1",
            "--source-repo", "redis/redis",
            "--target-repo", "valkey-io/valkey",
            "--source-brand", "Redis",
            "--target-brand", "Valkey",
            "--pr-db", "tests/redis_pr_fingerprints.json.gz",
            "--commit-db", "tests/redis_commits_bootstrap.json.gz",
            "--exclude-dirs", "deps/",
        ]

        with patch.object(sys, "argv", argv):
            main()

        common_args = mock_check_pr.call_args.args[1]
        self.assertIn("--exclude-dirs", common_args)
        self.assertIn("deps/", common_args)

    @patch("backtest.check_pr")
    def test_main_strips_separator_before_forwarding_extra_check_arguments(self, mock_check_pr):
        mock_check_pr.return_value = ("PASS", None)
        argv = [
            "backtest.py",
            "--start", "1",
            "--end", "1",
            "--source-repo", "redis/redis",
            "--target-repo", "valkey-io/valkey",
            "--source-brand", "Redis",
            "--target-brand", "Valkey",
            "--pr-db", "tests/redis_pr_fingerprints.json.gz",
            "--commit-db", "tests/redis_commits_bootstrap.json.gz",
            "--",
            "--exclude-dirs", "deps/",
        ]

        with patch.object(sys, "argv", argv):
            main()

        common_args = mock_check_pr.call_args.args[1]
        self.assertNotIn("--", common_args)
        self.assertIn("--exclude-dirs", common_args)
        self.assertIn("deps/", common_args)


if __name__ == "__main__":
    unittest.main()
