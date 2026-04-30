"""Source repository access abstractions."""

from typing import Protocol, Tuple

from github_client import fetch_commit_diff, fetch_pr_diff, fetch_pr_info


class SourceProvider(Protocol):
    def fetch_pr_diff(self, owner: str, repo: str, pr_number: int) -> Tuple[bytes, dict]:
        ...

    def fetch_commit_diff(self, owner: str, repo: str, sha: str) -> bytes:
        ...

    def fetch_pr_info(self, owner: str, repo: str, pr_number: int) -> dict:
        ...


class GitHubSourceProvider:
    """GitHub-backed source provider used by production checks."""

    def __init__(self, token=None):
        self.token = token

    def fetch_pr_diff(self, owner, repo, pr_number):
        return fetch_pr_diff(owner, repo, pr_number, self.token)

    def fetch_commit_diff(self, owner, repo, sha):
        return fetch_commit_diff(owner, repo, sha, self.token)

    def fetch_pr_info(self, owner, repo, pr_number):
        return fetch_pr_info(owner, repo, pr_number, self.token)
