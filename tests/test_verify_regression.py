#!/usr/bin/env python3
"""
Unit tests for verify_regression.py helpers.
"""

import os
import sys
import tempfile
import unittest
from unittest.mock import patch

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TEST_DIR)

from verify_regression import resolve_target_worktree


class TestVerifyRegression(unittest.TestCase):
    def test_resolve_target_worktree_prefers_cli_argument(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.assertEqual(resolve_target_worktree(tmp_dir, None), tmp_dir)

    def test_resolve_target_worktree_uses_environment_override(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict(os.environ, {"VERIFY_PROVENANCE_TARGET_ROOT": tmp_dir}, clear=False):
                self.assertEqual(resolve_target_worktree(None, None), tmp_dir)

    def test_resolve_target_worktree_rejects_missing_path(self):
        missing = os.path.join(TEST_DIR, "does-not-exist")
        with self.assertRaises(FileNotFoundError):
            resolve_target_worktree(missing, None)


if __name__ == "__main__":
    unittest.main()
