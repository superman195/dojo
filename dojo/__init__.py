import os
import subprocess

from dojo.utils.config import get_config, source_dotenv

source_dotenv()


def get_latest_git_tag():
    try:
        # Get the latest git tag
        latest_tag = (
            subprocess.check_output(["git", "describe", "--tags", "--abbrev=0"])
            .strip()
            .decode("utf-8")
        )
        return latest_tag.lstrip("v")
    except subprocess.CalledProcessError as e:
        print(f"Error getting the latest Git tag: {e}")
        raise RuntimeError("Failed to get latest Git tag")


# Define the version of the template module.
__version__ = get_latest_git_tag()
version_split = __version__.split(".")
__spec_version__ = (
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
