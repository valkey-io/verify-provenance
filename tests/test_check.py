#!1/usr/bin/env python3
"""
test_check_src.py - Integration tests for check.py CLI
"""

import unittest
import sys
import os
import subprocess
import tempfile
import shutil
import gzip
import json

# Resolve paths relative to the test file
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PROV_DIR = os.path.dirname(TEST_DIR)
SCRIPT_PATH = os.path.join(PROV_DIR, "src", "check.py")

class TestCheckCLI(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.pr_db = os.path.join(self.tmp_dir, "pr.json.gz")
        self.commit_db = os.path.join(self.tmp_dir, "commit.json.gz")
        with gzip.open(self.pr_db, "wt", encoding="utf-8") as f:
            json.dump({"repo": "redis/redis", "generated_at": "2026-01-01T00:00:00Z", "prs": {}}, f)
        with gzip.open(self.commit_db, "wt", encoding="utf-8") as f:
            json.dump({"repo": "redis/redis", "generated_at": "2026-01-01T00:00:00Z", "commits": {}}, f)

        self.common_args = [
            sys.executable, SCRIPT_PATH,
            "--source-repo", "redis/redis",
            "--target-repo", "valkey-io/valkey",
            "--source-brand", "Redis",
            "--target-brand", "Valkey",
            "--pr-db", self.pr_db,
            "--commit-db", self.commit_db
        ]

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    def test_error_handling_no_databases(self):
        """Verify exit code 1 when databases are missing."""
        result = subprocess.run( [
            sys.executable, SCRIPT_PATH,
            "--source-repo", "a/b", "--target-repo", "c/d",
            "--source-brand", "A", "--target-brand", "B",
            "--pr-db", "/nonexistent/pr.json.gz",
            "--commit-db", "/nonexistent/commit.json.gz",
            "12345"],
            capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("no databases loaded", result.stderr.lower())

    def test_local_diff_mode_no_match(self):
        """Verify successful exit (code 0) for a local diff with no matching content."""
        # Create a temporary git repo
        tmp_repo = tempfile.mkdtemp()
        try:
            subprocess.run(["git", "init"], cwd=tmp_repo, capture_output=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_repo)
            subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_repo)
            with open(os.path.join(tmp_repo, "file.txt"), "w") as f: f.write("initial")
            subprocess.run(["git", "add", "file.txt"], cwd=tmp_repo)
            subprocess.run(["git", "commit", "-m", "initial"], cwd=tmp_repo)

            with open(os.path.join(tmp_repo, "file.txt"), "w") as f: f.write("ThisIsAVeryUniqueLine12345\nAnotherUniqueToken")
            subprocess.run(["git", "add", "file.txt"], cwd=tmp_repo)
            subprocess.run(["git", "commit", "-m", "change"], cwd=tmp_repo)

            result = subprocess.run(
                self.common_args + ["--base-sha", "HEAD~1", "--head-sha", "HEAD"],
                capture_output=True, text=True, cwd=tmp_repo
            )
            self.assertEqual(result.returncode, 0)
        finally:
            shutil.rmtree(tmp_repo)

    def test_invalid_pr_number_error(self):
        result = subprocess.run(
            self.common_args + ["not-a-number"],
            capture_output=True, text=True
        )
        self.assertNotEqual(result.returncode, 0)

    def test_missing_required_args(self):
        result = subprocess.run(
            [sys.executable, SCRIPT_PATH, "--pr-db", "db.gz"],
            capture_output=True, text=True
        )
        self.assertNotEqual(result.returncode, 0)

    @unittest.skipIf(
        not os.environ.get("GITHUB_TOKEN") or os.environ.get("RUN_LIVE_API_TESTS") != "1",
        "Requires GITHUB_TOKEN and RUN_LIVE_API_TESTS=1",
    )
    def test_valid_pr_fetch_real_api(self):
        result = subprocess.run(
            self.common_args + ["3111"],
            capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 0)

    def test_help_message(self):
        result = subprocess.run(
            [sys.executable, SCRIPT_PATH, "--help"],
            capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("usage:", result.stdout.lower())

    def test_verbose_logging_activation(self):
        tmp_repo = tempfile.mkdtemp()
        try:
            subprocess.run(["git", "init"], cwd=tmp_repo, capture_output=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_repo)
            subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_repo)
            with open(os.path.join(tmp_repo, "file.txt"), "w") as f: f.write("initial")
            subprocess.run(["git", "add", "file.txt"], cwd=tmp_repo)
            subprocess.run(["git", "commit", "-m", "initial"], cwd=tmp_repo)
            result = subprocess.run(
                self.common_args + ["--base-sha", "HEAD", "--head-sha", "HEAD", "--verbose"],
                capture_output=True, text=True, cwd=tmp_repo
            )
            self.assertIn("Loaded", result.stderr)
        finally:
            shutil.rmtree(tmp_repo)


    def test_multi_pair_arg_parsing(self):
        """Verify comma-separated pair arguments are accepted during execution."""
        tmp_repo = tempfile.mkdtemp()
        try:
            subprocess.run(["git", "init"], cwd=tmp_repo, capture_output=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_repo)
            subprocess.run(["git", "config", "user.name", "Test"], cwd=tmp_repo)
            with open(os.path.join(tmp_repo, "file.txt"), "w") as f: f.write("initial")
            subprocess.run(["git", "add", "file.txt"], cwd=tmp_repo)
            subprocess.run(["git", "commit", "-m", "initial"], cwd=tmp_repo)
            result = subprocess.run(
                self.common_args + [
                    "--base-sha", "HEAD", "--head-sha", "HEAD",
                    "--branding-pairs", "Redis:Valkey,KeyDB:Valkey",
                    "--prefix-pairs", "RM_:VM_,REDISMODULE_:VALKEYMODULE_",
                ],
                capture_output=True, text=True, cwd=tmp_repo
            )
            self.assertEqual(result.returncode, 0)
        finally:
            shutil.rmtree(tmp_repo)

if __name__ == "__main__":
    unittest.main()
