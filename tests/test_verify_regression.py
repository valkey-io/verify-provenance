#!/usr/bin/env python3
"""
Unit tests for verify_regression.py helpers.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import verify_regression


class TestVerifyRegression(unittest.TestCase):
    @patch("verify_regression.subprocess.run")
    def test_cloned_target_repo_uses_tmp_and_cleans_up(self, mock_run):
        with verify_regression.cloned_target_repo("https://example.com/valkey.git") as target_root:
            tmp_root = os.path.dirname(target_root)
            self.assertTrue(tmp_root.startswith("/tmp/verify-provenance-valkey-"))
            self.assertEqual(os.path.basename(target_root), "repo")
            self.assertTrue(os.path.isdir(tmp_root))

        self.assertFalse(os.path.exists(tmp_root))
        mock_run.assert_called_once_with(
            ["git", "clone", "--quiet", "https://example.com/valkey.git", target_root],
            check=True,
        )

    @patch("verify_regression.subprocess.run")
    def test_cloned_target_repo_checks_out_target_ref(self, mock_run):
        with verify_regression.cloned_target_repo("https://example.com/valkey.git", "unstable") as target_root:
            pass

        self.assertEqual(
            mock_run.call_args_list[1].args[0],
            ["git", "checkout", "--quiet", "unstable"],
        )
        self.assertEqual(mock_run.call_args_list[1].kwargs["cwd"], target_root)

    def test_run_check_requires_target_root(self):
        with self.assertRaises(ValueError):
            verify_regression.run_check(pr_num=1)

    @patch("verify_regression.subprocess.run")
    def test_run_check_uses_supplied_target_root(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="out", stderr="err")

        code, output = verify_regression.run_check(pr_num=1, target_root="/tmp/target")

        self.assertEqual(code, 0)
        self.assertEqual(output, "outerr")
        self.assertEqual(mock_run.call_args.kwargs["cwd"], "/tmp/target")

if __name__ == "__main__":
    unittest.main()
