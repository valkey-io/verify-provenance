#!/usr/bin/env python3
"""
Unit tests for backtest.py behavior.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from backtest import check_pr


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


if __name__ == "__main__":
    unittest.main()
