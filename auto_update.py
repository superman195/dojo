import argparse
import subprocess
import sys
import time
from datetime import datetime

from bittensor.utils.btlogging import logging as logger

from commons.utils import datetime_to_iso8601_str
from dojo import __version__
from dojo.utils.config import source_dotenv

source_dotenv()

RED = "\033[91m"
GREEN = "\033[92m"
RESET = "\033[0m"

CHECK_INTERVAL = 1800  # 30 minutes

# Define the base URL for the images
BASE_IMAGE_URL = "ghcr.io/tensorplex-labs/"
BRANCH = "main"

CONFIG = {
    "validator": {
        "images": ["dojo-synthetic-api", "dojo"],
        "docker_compose_down": "docker compose --env-file .env.validator -f docker-compose.validator.yaml down",
        "docker_compose_up": "docker compose --env-file .env.validator -f docker-compose.validator.yaml up --build -d validator",
    },
    "miner-decentralised": {
        "images": ["dojo-worker-api", "dojo-ui", "dojo"],
        "docker_compose_down": "docker compose --env-file .env.miner -f docker-compose.miner.yaml down",
        "docker_compose_up": "docker compose --env-file .env.miner -f docker-compose.miner.yaml up --build -d miner-decentralised",
    },
    "miner-centralised": {
        "services": [
            "miner-centralised",
        ],
        "images": ["dojo"],
        "docker_compose_down": "docker compose --env-file .env.miner -f docker-compose.miner.yaml down",
        "docker_compose_up": "docker compose --env-file .env.miner -f docker-compose.miner.yaml up --build -d miner-centralised",
    },
}


def get_latest_remote_tag():
    """Fetch and return the latest tag from the remote repository."""
    try:
        result = subprocess.run(
            [
                "git",
                "ls-remote",
                "--tags",
                "https://github.com/tensorplex-labs/dojo.git",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        # Parse the result to get the latest tag
        tags = [line.split("/")[-1] for line in result.stdout.strip().split("\n")]
        latest_tag = sorted(
            tags, key=lambda s: list(map(int, s.strip("v").split(".")))
        )[-1]
        # strip "v"
        latest_tag = latest_tag[1:]
        logger.debug(f"Latest tag from remote: {latest_tag}")
        return latest_tag
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to fetch latest git tag: {e}")
        return None


def get_image_digest(image_name):
    """Get the image digest for the specified Docker image."""
    try:
        digest = (
            subprocess.run(
                [
                    "docker",
                    "inspect",
                    "--format='{{index .RepoDigests 0}}'",
                    image_name,
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            .stdout.strip()
            .replace("'", "")
        )
        return digest
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get image digest for {image_name}: {e}")
        return None


def check_for_update(image_url):
    """Check if there is an update available for the Docker image."""
    logger.info(f"Checking for updates for {image_url}...")
    local_digest = get_image_digest(image_url)

    if not local_digest:
        return False

    logger.debug(f"Local digest: {local_digest}")

    # Pull the remote image
    pull_docker_image(image_url)

    remote_digest = get_image_digest(image_url)

    if not remote_digest:
        return False

    logger.debug(f"Remote digest: {remote_digest}")

    if local_digest != remote_digest:
        logger.info(f"Update available for {image_url}.")
        return True
    else:
        logger.info(f"No update available for {image_url}.")
        return False


def pull_docker_image(image_url):
    """Pull the latest Docker image."""
    logger.info(f"Pulling the latest Docker image for {image_url}.")
    try:
        subprocess.run(["docker", "pull", "--quiet", image_url], check=True)
        logger.info(f"Successfully pulled {image_url}.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to pull Docker image {image_url}: {e}")
        return False
    return True


def pull_docker_images(list_of_images: list[str]):
    for image_name in list_of_images:
        image_url = (
            f"{BASE_IMAGE_URL}{image_name}:tensorplex-prod"
            if image_name == "dojo-ui"
            else f"{BASE_IMAGE_URL}{image_name}:{BRANCH}"
        )
        pull_docker_image(image_url)


def check_for_image_updates(images):
    """
    Check for updates for a list of Docker images.

    This function iterates over a list of image names, constructs the full image URL,
    and checks if there is an update available for each image by comparing the local
    and remote image digests. If any image has an update, pull the images quietly, and returns True.

    Args:
        images (list[str]): A list of Docker image names to check for updates.

    Returns:
        bool: True if any of the images have updates available, False otherwise.
    """
    logger.info(f"Checking images: {images}")
    has_update = False
    for image_name in images:
        image_url = (
            f"{BASE_IMAGE_URL}{image_name}:tensorplex-prod"
            if image_name == "dojo-ui"
            else f"{BASE_IMAGE_URL}{image_name}:{BRANCH}"
        )
        result = check_for_update(image_url)
        if result:
            has_update = True
    return has_update


def stash_changes(service_name: str):
    result = subprocess.run(
        ["git", "status", "--porcelain"], capture_output=True, text=True
    )
    if result.stdout.strip():
        logger.info("Stashing any local changes.")
        current_time_utc = datetime_to_iso8601_str(datetime.now())
        stash_entry_message = f"dojo auto-updater: {service_name} {current_time_utc}"
        subprocess.run(["git", "stash", "push", "-m", stash_entry_message], check=True)
    else:
        logger.info("No changes to stash.")


def pull_latest_changes():
    logger.info("Pulling latest changes from the main branch.")
    subprocess.run(["git", "pull", "origin", "main"], check=True)
    subprocess.run(["git", "fetch", "--tags"], check=True)


def pop_stash():
    logger.info("Popping stashed changes.")
    subprocess.run(["git", "stash", "pop"], check=True)


def restart_docker(service_name):
    logger.info(f"Restarting Docker services for: {service_name}.")
    # Get the service data e.g. miner or validator
    service_data = CONFIG.get(service_name, {})

    docker_compose_down: list[str] = service_data.get("docker_compose_down", "").split()
    docker_compose_up: list[str] = service_data.get("docker_compose_up", "").split()

    # Stop the services in a single command
    subprocess.run(docker_compose_down, check=True)

    # Start the services in a single command
    subprocess.run(docker_compose_up, check=True)


def get_current_version():
    version = __version__
    logger.debug(f"Current version: {version}")
    return version


def update_repo(service_name: str):
    logger.info("Updating the repository..")
    stash_changes(service_name)
    pull_latest_changes()
    pop_stash()


def get_current_branch():
    """Get the current Git branch name."""
    try:
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get current branch: {e}")
        sys.exit(1)


def main(service_name):
    # Check if the current branch is 'main'
    current_branch = get_current_branch()
    if current_branch != "main":
        logger.error(f"Current branch is '{current_branch}'. Please switch to 'main'.")
        sys.exit(1)

    logger.info("Starting the main loop.")
    config = CONFIG[service_name]

    pull_docker_images(config["images"])
    restart_docker(service_name)

    try:
        # Start the periodic check loop
        while True:
            logger.info("Checking for updates...")
            current_dojo_version = get_current_version()
            new_dojo_version = get_latest_remote_tag()

            logger.info(f"Current version: {current_dojo_version}")
            logger.info(f"Latest version: {new_dojo_version}")

            has_image_updates = check_for_image_updates(config["images"])

            # Check if either the version has changed or there are image updates
            if current_dojo_version != new_dojo_version or has_image_updates:
                if current_dojo_version != new_dojo_version:
                    logger.info(
                        f"Repository has changed. {RED}{current_dojo_version}{RESET} -> {GREEN}{new_dojo_version}{RESET}"
                    )
                    update_repo(service_name)

                # Restart Docker if there are any updates
                restart_docker(service_name)
            logger.info(f"Sleeping for {CHECK_INTERVAL} seconds.")
            time.sleep(CHECK_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Graceful shutdown initiated.")
        # Perform any cleanup here if necessary
        subprocess.run(config["docker_compose_down"].split(), check=True)
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the auto-update script.")
    parser.add_argument(
        "--service",
        choices=["miner-decentralised", "miner-centralised", "validator"],
        help="Specify the service to run (miner or validator).",
    )

    args, _ = parser.parse_known_args()

    logger.info(f"Starting the {args.service} process.")
    main(args.service)
