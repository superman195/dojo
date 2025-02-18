import os
import subprocess

from git import Repo

from dojo.utils.config import get_config, source_dotenv

source_dotenv()


def get_latest_git_tag(repo_path="."):
    repo = Repo(repo_path)
    tags = sorted(repo.tags, key=lambda t: t.commit.committed_date)
    return str(tags[-1]).lstrip("v") if tags else None


def get_latest_remote_tag(repo_path="."):
    repo = Repo(repo_path)
    remote_tags = repo.git.ls_remote("--tags", "--sort=-v:refname", "origin").split(
        "\n"
    )
    if remote_tags and remote_tags[0]:
        return remote_tags[0].split("refs/tags/")[-1]
    return None


def get_commit_hash():
    try:
        # Get the latest git commit hash
        latest_commit_hash = (
            subprocess.check_output(["git", "rev-parse", "HEAD"])
            .strip()
            .decode("utf-8")
        )
        return latest_commit_hash
    except subprocess.CalledProcessError as e:
        print(f"Error getting the latest Git commit hash: {e}")
        raise RuntimeError("Failed to get latest Git commit hash")


def get_spec_version():
    latest_tag = get_latest_git_tag()
    if latest_tag is None:
        raise ValueError("No Git tag found")
    version_split = latest_tag.split(".")
    return (
        (1000 * int(version_split[0]))
        + (10 * int(version_split[1]))
        + (1 * int(version_split[2]))
    )


VALIDATOR_MIN_STAKE = int(os.getenv("VALIDATOR_MIN_STAKE", "20000"))
TASK_DEADLINE = 6 * 60 * 60

# Define the time intervals for various tasks.
VALIDATOR_RUN = 900
VALIDATOR_HEARTBEAT = 150
VALIDATOR_UPDATE_SCORE = 3600
VALIDATOR_STATUS = 60
MINER_STATUS = 60
DOJO_TASK_MONITORING = 300
assert VALIDATOR_UPDATE_SCORE < TASK_DEADLINE

if get_config().fast_mode:
    print("Running in fast mode for testing purposes...")
    VALIDATOR_MIN_STAKE = int(os.getenv("VALIDATOR_MIN_STAKE", "20000"))
    TASK_DEADLINE = 180
    VALIDATOR_RUN = 60
    VALIDATOR_HEARTBEAT = 15
    VALIDATOR_UPDATE_SCORE = 120
    VALIDATOR_STATUS = 1200
    MINER_STATUS = 1200
    DOJO_TASK_MONITORING = 15


def get_dojo_api_base_url() -> str:
    base_url = os.getenv("DOJO_API_BASE_URL")
    if base_url is None:
        raise ValueError("DOJO_API_BASE_URL is not set in the environment.")

    return base_url
