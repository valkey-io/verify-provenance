#!/usr/bin/env python3
"""
test_refresh_prs.py - Unit tests for refresh_prs.py
"""

import unittest
from unittest.mock import patch, MagicMock
import json
import io
import os
import sys

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from refresh_prs import fetch_pr_list, refresh_prs, should_skip_pr

class TestRefreshPrs(unittest.TestCase):
    @patch("refresh_prs.github_request")
    def test_fetch_pr_list_no_cutoff(self, mock_request):
        """Verify fetching all PRs when no date cutoff is specified."""
        prs_data = [{"number": 100, "updated_at": "2024-01-20T10:00:00Z"}]
        mock_request.return_value = (json.dumps(prs_data).encode("utf-8"), 200)
        result, stop = fetch_pr_list("owner", "repo", "open", 1, 100, "token", None)
        self.assertEqual(len(result), 1)
        self.assertFalse(stop)

    @patch.dict(os.environ, {"GITHUB_TOKEN": "test_token"})
    @patch("refresh_prs.github_request")
    @patch("refresh_prs.fetch_pr_diff")
    @patch("gzip.open")
    @patch("os.path.exists", return_value=False)
    @patch("os.makedirs")
    def test_refresh_prs_integration(self, mock_makedirs, mock_exists, mock_gzip, mock_fetch_diff, mock_request):
        """Integration test for the PR database refresh cycle."""
        mock_request.return_value = (b"[]", 200)
        mock_gzip.return_value.__enter__.return_value = io.StringIO()
        args = MagicMock()
        args.source_owner = "redis"; args.source_repo_name = "redis"; args.cutoff_date = "2024-03-20T00:00:00Z"
        args.out_db = "test.json.gz"; args.source_brand = "Redis"; args.target_brand = "Valkey"
        config = MagicMock()
        refresh_prs(args, config)
        self.assertTrue(mock_gzip.called)

    def test_should_skip_pr_cases(self):
        """Test the logic for skipping non-feature PRs based on title or size."""
        self.assertTrue(should_skip_pr("Merge unstable into 8.0", {}))
        self.assertTrue(should_skip_pr("Release 8.0.1", {}))
        self.assertTrue(should_skip_pr("unstable", {}))
        self.assertFalse(should_skip_pr("Fix crash in networking", {}))
        self.assertTrue(should_skip_pr("Massive refactor", {"changed_files": 100}))

    @patch("refresh_prs.github_request")
    def test_fetch_pr_list_with_cutoff(self, mock_request):
        """Verify refresh filtering/stop logic uses updated_at timestamps."""
        prs_data = [
            {"number": 100, "updated_at": "2024-01-20T10:00:00Z"},
            {"number": 99, "updated_at": "2024-01-10T10:00:00Z"}
        ]
        mock_request.return_value = (json.dumps(prs_data).encode("utf-8"), 200)
        result, stop = fetch_pr_list("owner", "repo", "open", 1, 100, "token", "2024-01-15T00:00:00Z")
        self.assertEqual(len(result), 1)
        self.assertTrue(stop)

    def test_api_url_construction(self):
        """Ensure correct construction of GitHub API URLs with parameters."""
        with patch("refresh_prs.github_request") as mock_req:
            mock_req.return_value = (b"[]", 200)
            fetch_pr_list("myorg", "myrepo", "closed", 5, 25, "tok", None)
            url = mock_req.call_args[0][0]
            self.assertIn("repos/myorg/myrepo/pulls", url)
            self.assertIn("state=closed", url)
            self.assertIn("sort=updated", url)
            self.assertIn("per_page=25", url)
            self.assertIn("page=5", url)

if __name__ == "__main__":
    unittest.main()
