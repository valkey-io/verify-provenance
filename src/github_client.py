"""GitHub API helpers."""

import json
import logging
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


def github_request(url, headers, retry=3):
    """Make GitHub API request with retry and rate limit handling."""
    for attempt in range(retry):
        try:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=30) as response:
                return response.read(), response.status
        except HTTPError as e:
            if e.code == 403:
                reset_time = e.headers.get("X-RateLimit-Reset")
                if reset_time:
                    raw_wait = max(int(reset_time) - int(time.time()), 0) + 1
                    wait = min(raw_wait, 300)
                    logger.warning("Rate limited. Waiting %ss", wait)
                    if raw_wait > 600:
                        raise RuntimeError(f"Rate limit reset time too far in future: {raw_wait}s") from e
                    time.sleep(wait)
                    continue
                raise
            if e.code >= 500 and attempt < retry - 1:
                time.sleep(2**attempt)
                continue
            raise
        except URLError:
            if attempt < retry - 1:
                time.sleep(2**attempt)
                continue
            raise
    raise RuntimeError(f"Failed to fetch {url} after {retry} attempts")


def _github_headers(accept, token):
    headers = {
        "Accept": accept,
        "User-Agent": "Provenance-Guard",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def fetch_pr_info(owner, repo, pr_number, token):
    """Fetch PR metadata from GitHub API."""
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}"
    data, _ = github_request(url, _github_headers("application/vnd.github+json", token))
    return json.loads(data.decode("utf-8", errors="replace"))


def fetch_pr_diff(owner, repo, pr_number, token):
    """Fetch PR diff using HEAD commit."""
    pr_info = fetch_pr_info(owner, repo, pr_number, token)
    base_sha = pr_info["base"]["sha"]
    head_sha = pr_info["head"]["sha"]
    url = f"https://api.github.com/repos/{owner}/{repo}/compare/{base_sha}...{head_sha}"
    data, _ = github_request(url, _github_headers("application/vnd.github.v3.diff", token))
    return data, pr_info


def fetch_commit_diff(owner, repo, sha, token):
    """Fetch commit diff from GitHub API."""
    url = f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}"
    data, _ = github_request(url, _github_headers("application/vnd.github.v3.diff", token))
    return data
