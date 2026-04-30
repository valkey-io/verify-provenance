"""Git command helpers."""

import subprocess


class PatchIdError(RuntimeError):
    pass


def compute_patch_id(diff_text):
    """Compute git patch-id for a diff."""
    diff_bytes = diff_text.encode("utf-8") if isinstance(diff_text, str) else diff_text
    try:
        result = subprocess.run(
            ["git", "patch-id", "--stable"],
            input=diff_bytes,
            capture_output=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        raise PatchIdError(f"git patch-id failed: {e}") from e

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        raise PatchIdError(f"git patch-id exited {result.returncode}: {stderr}")
    if not result.stdout:
        return None
    return result.stdout.decode("utf-8").split()[0]
