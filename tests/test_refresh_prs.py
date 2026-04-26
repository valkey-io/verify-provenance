#!/usr/bin/env python3
"""
test_refresh_prs.py - Unit tests for refresh_prs.py
"""

import unittest
from unittest.mock import patch, MagicMock
import gzip
import json
import io
import os
import sys
import tempfile

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from refresh_prs import fetch_pr_list, refresh_prs, should_skip_pr
from common import ProvenanceConfig

class TestRefreshPrs(unittest.TestCase):
    def make_pr(self, number=1, title="Fix copied logic", updated_at="2024-01-02T00:00:00Z"):
        return {
            "number": number,
            "state": "closed",
            "title": title,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": updated_at,
            "changed_files": 1,
            "user": {"login": "alice"},
        }

    def make_diff(self, path="src/a.c", extra=b""):
        text = "\n".join(
            [
                f"diff --git a/{path} b/{path}",
                f"--- a/{path}",
                f"+++ b/{path}",
                "@@ -0,0 +1,6 @@",
                "+int f(int input) {",
                "+    int total = input + 1;",
                "+    total += 2;",
                "+    return total;",
                "+}",
            ]
        ).encode("utf-8")
        return text + extra

    @patch("refresh_prs.github_request")
    def test_fetch_pr_list_no_cutoff(self, mock_request):
        """Verify fetching all PRs when no date cutoff is specified."""
        prs_data = [{"number": 100, "created_at": "2024-01-10T10:00:00Z", "updated_at": "2024-01-20T10:00:00Z"}]
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

    @patch.dict(os.environ, {"GITHUB_TOKEN": "test_token"})
    @patch("refresh_prs.fetch_pr_list")
    @patch("refresh_prs.fetch_pr_diff")
    def test_refresh_prs_records_policy_metadata(self, mock_fetch_diff, mock_fetch_list):
        """PR fingerprints should preserve source metadata for policy checks."""
        pr = self.make_pr(title="Fix copied logic")
        mock_fetch_list.side_effect = [([pr], True), ([], True)]
        mock_fetch_diff.return_value = (self.make_diff(), {})

        with tempfile.TemporaryDirectory() as tmp_dir:
            args = MagicMock()
            args.source_owner = "redis"
            args.source_repo_name = "redis"
            args.cutoff_date = "2024-01-01T00:00:00Z"
            args.out_db = os.path.join(tmp_dir, "prs.json.gz")

            refresh_prs(args, ProvenanceConfig(source_brand="Redis", target_brand="Valkey"))

            with gzip.open(args.out_db, "rt", encoding="utf-8") as f:
                data = json.load(f)
        self.assertEqual(data["prs"]["1"]["author_login"], "alice")
        self.assertEqual(data["prs"]["1"]["title"], "Fix copied logic")

    @patch.dict(os.environ, {"GITHUB_TOKEN": "test_token"})
    @patch("refresh_prs.fetch_pr_list")
    @patch("refresh_prs.fetch_pr_diff")
    def test_refresh_prs_decodes_invalid_utf8_with_replacement(self, mock_fetch_diff, mock_fetch_list):
        """Refresh should not drop PRs solely because a diff contains invalid UTF-8."""
        pr = self.make_pr()
        mock_fetch_list.side_effect = [([pr], True), ([], True)]
        mock_fetch_diff.return_value = (self.make_diff(extra=b"\xff"), {})

        with tempfile.TemporaryDirectory() as tmp_dir:
            args = MagicMock()
            args.source_owner = "redis"
            args.source_repo_name = "redis"
            args.cutoff_date = "2024-01-01T00:00:00Z"
            args.out_db = os.path.join(tmp_dir, "prs.json.gz")

            refresh_prs(args, ProvenanceConfig(source_brand="Redis", target_brand="Valkey"))

            with gzip.open(args.out_db, "rt", encoding="utf-8") as f:
                data = json.load(f)
        self.assertIn("1", data["prs"])

    @patch.dict(os.environ, {"GITHUB_TOKEN": "test_token"})
    @patch("refresh_prs.logger.warning")
    @patch("refresh_prs.os.makedirs")
    @patch("refresh_prs.gzip.open")
    @patch("refresh_prs.fetch_pr_diff")
    @patch("refresh_prs.fetch_pr_list")
    def test_refresh_prs_creates_output_dir_before_checkpoint(
        self,
        mock_fetch_list,
        mock_fetch_diff,
        mock_gzip_open,
        mock_makedirs,
        mock_warning,
    ):
        """Checkpoint writes should not fail for a new nested output directory."""
        prs = [self.make_pr(number=i, updated_at=f"2024-01-{i:02d}T00:00:00Z") for i in range(1, 11)]
        mock_fetch_list.side_effect = [(prs, True), ([], True)]
        mock_fetch_diff.return_value = (self.make_diff(), {})
        directory_created = {"value": False}

        def mark_directory_created(path, exist_ok=False):
            directory_created["value"] = True

        def open_after_directory_exists(*args, **kwargs):
            if not directory_created["value"]:
                raise FileNotFoundError("checkpoint directory is missing")
            return io.StringIO()

        mock_makedirs.side_effect = mark_directory_created
        mock_gzip_open.side_effect = open_after_directory_exists

        args = MagicMock()
        args.source_owner = "redis"
        args.source_repo_name = "redis"
        args.cutoff_date = "2024-01-01T00:00:00Z"
        args.out_db = "nested/prs.json.gz"

        refresh_prs(args, ProvenanceConfig(source_brand="Redis", target_brand="Valkey"))

        mock_warning.assert_not_called()

    @patch.dict(os.environ, {"GITHUB_TOKEN": "test_token"})
    @patch("refresh_prs.fetch_pr_list")
    @patch("refresh_prs.fetch_pr_diff")
    def test_refresh_prs_records_failed_prs_for_retry(self, mock_fetch_diff, mock_fetch_list):
        """A transient per-PR failure should be persisted for a later retry."""
        pr = self.make_pr(number=1)
        mock_fetch_list.side_effect = [([pr], True), ([], True)]
        mock_fetch_diff.side_effect = RuntimeError("temporary failure")

        with tempfile.TemporaryDirectory() as tmp_dir:
            args = MagicMock()
            args.source_owner = "redis"
            args.source_repo_name = "redis"
            args.cutoff_date = "2024-01-01T00:00:00Z"
            args.out_db = os.path.join(tmp_dir, "prs.json.gz")

            refresh_prs(args, ProvenanceConfig(source_brand="Redis", target_brand="Valkey"))

            with gzip.open(args.out_db, "rt", encoding="utf-8") as f:
                data = json.load(f)
        self.assertIn("1", data["failed_prs"])
        self.assertEqual(data["failed_prs"]["1"]["title"], "Fix copied logic")
        self.assertEqual(data["prs"], {})

    @patch.dict(os.environ, {"GITHUB_TOKEN": "test_token"})
    @patch("refresh_prs.fetch_pr_list")
    @patch("refresh_prs.fetch_pr_diff")
    def test_refresh_prs_retries_persisted_failed_prs(self, mock_fetch_diff, mock_fetch_list):
        """Previously failed PRs should be retried even when newer successful entries set the watermark."""
        failed = self.make_pr(number=42, updated_at="2024-01-02T00:00:00Z")
        existing = self.make_pr(number=100, updated_at="2024-02-01T00:00:00Z")
        existing_entry = {
            "number": 100,
            "state": existing["state"],
            "created_at": existing["created_at"],
            "updated_at": existing["updated_at"],
            "title": existing["title"],
            "author_login": "alice",
            "simhash64": 0,
            "patch_id": None,
            "files": {},
        }
        mock_fetch_list.side_effect = [([], True), ([], True)]
        mock_fetch_diff.return_value = (self.make_diff(), {})

        with tempfile.TemporaryDirectory() as tmp_dir:
            args = MagicMock()
            args.source_owner = "redis"
            args.source_repo_name = "redis"
            args.cutoff_date = "2024-01-01T00:00:00Z"
            args.out_db = os.path.join(tmp_dir, "prs.json.gz")
            with gzip.open(args.out_db, "wt", encoding="utf-8") as f:
                json.dump(
                    {
                        "repo": "redis/redis",
                        "generated_at": "2024-02-01T00:00:00Z",
                        "prs": {"100": existing_entry},
                        "failed_prs": {"42": failed},
                    },
                    f,
                )

            refresh_prs(args, ProvenanceConfig(source_brand="Redis", target_brand="Valkey"))

            with gzip.open(args.out_db, "rt", encoding="utf-8") as f:
                data = json.load(f)

        self.assertIn("42", data["prs"])
        self.assertNotIn("42", data.get("failed_prs", {}))

    def test_should_skip_pr_cases(self):
        """Test the logic for skipping non-feature PRs based on title or size."""
        self.assertTrue(should_skip_pr("Merge unstable into 8.0", {}))
        self.assertTrue(should_skip_pr("Release 8.0.1", {}))
        self.assertTrue(should_skip_pr("unstable", {}))
        self.assertFalse(should_skip_pr("Fix crash in networking", {}))
        self.assertFalse(should_skip_pr(None, {}))
        self.assertTrue(should_skip_pr("Massive refactor", {"changed_files": 100}))

    @patch("refresh_prs.github_request")
    def test_fetch_pr_list_with_cutoff(self, mock_request):
        """Verify refresh filtering/stop logic uses updated_at timestamps."""
        prs_data = [
            {"number": 100, "created_at": "2023-12-01T10:00:00Z", "updated_at": "2024-01-20T10:00:00Z"},
            {"number": 99, "created_at": "2024-01-10T10:00:00Z", "updated_at": "2024-01-10T10:00:00Z"}
        ]
        mock_request.return_value = (json.dumps(prs_data).encode("utf-8"), 200)
        result, stop = fetch_pr_list("owner", "repo", "open", 1, 100, "token", "2024-01-15T00:00:00Z")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["number"], 100)
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
