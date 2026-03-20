import logging
import os
import subprocess

from ScannerMinute.definitions import PROJECT_ROOT_DIR
from ScannerMinute.src.aws_utils import run_command_on_instance

GIT_TOKEN_PATH = os.path.join(PROJECT_ROOT_DIR, "api_keys", "git_token.txt")


def get_remote_url():
    """Return the remote URL of the current git repository (origin)."""
    result = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def get_git_credentials():
    """
    Read git user and token from api_keys/git_token.txt.
    File format:
        line 1: username
        line 2: token

    Returns (user, token) tuple, or (None, None) if not found.
    """
    try:
        with open(GIT_TOKEN_PATH, "r") as f:
            lines = [line.strip() for line in f.readlines()]
        if len(lines) < 2 or not lines[0] or not lines[1]:
            logging.warning(f"[get_git_credentials] {GIT_TOKEN_PATH} must have user on line 1 and token on line 2")
            return None, None
        user, token = lines[0], lines[1]
        logging.info(f"[get_git_credentials] user={user}, token={token[:4]}...{token[-4:]}")
        return user, token
    except FileNotFoundError:
        logging.warning(f"[get_git_credentials] File not found: {GIT_TOKEN_PATH}")
        return None, None
    except Exception as e:
        logging.warning(f"[get_git_credentials] Failed to read {GIT_TOKEN_PATH}: {e}")
        return None, None


def ensure_projects_dir(instance_id, region="us-east-1"):
    """Create /home/ubuntu/projects directory on the EC2 instance if it doesn't exist."""
    return run_command_on_instance(
        instance_id=instance_id,
        command="mkdir -p /home/ubuntu/projects && chown -R ubuntu:ubuntu /home/ubuntu/projects",
        region=region,
    )


def git_clone_repo(instance_id, region="us-east-1", branch=None):
    """
    Clone the current repository into /home/ubuntu/projects/ on the EC2 instance.

    Uses the local remote URL. If a token is found in the URL, it is
    embedded in the clone URL for authentication. Skips cloning if the
    repo directory already exists on the instance.

    Parameters:
        instance_id: str — EC2 instance ID
        region: str — AWS region
        branch: str — optional branch to checkout after cloning
    """
    remote_url = get_remote_url()
    user, token = get_git_credentials()

    # Inject credentials into the clone URL
    if user and token and "@" not in remote_url:
        remote_url = remote_url.replace("https://", f"https://{user}:{token}@")

    # Derive repo directory name from URL (e.g. "scanner_minute")
    repo_name = remote_url.rstrip("/").rsplit("/", 1)[-1].removesuffix(".git")

    # Ensure /home/ubuntu/projects exists, then clone if not already present
    cmd = (
        f"mkdir -p /home/ubuntu/projects && cd /home/ubuntu/projects && "
        f"if [ -d {repo_name} ]; then "
        f"  echo 'Repo already cloned, pulling latest...' && "
        f"  cd {repo_name} && git pull; "
        f"else "
        f"  git clone {remote_url}; "
        f"fi"
    )

    cmd += f" && chown -R ubuntu:ubuntu /home/ubuntu/projects/{repo_name}"

    if branch:
        cmd += f" && cd /home/ubuntu/projects/{repo_name} && git checkout {branch}"

    logging.info(f"[git_clone_repo] Cloning {repo_name} on {instance_id}")
    return run_command_on_instance(
        instance_id=instance_id,
        command=cmd,
        region=region,
        timeout_seconds=120,
    )
