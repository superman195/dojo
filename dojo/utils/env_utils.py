from bittensor.utils.btlogging import logging as logger
from dotenv import find_dotenv, load_dotenv

from dojo.settings import get_config


def source_dotenv():
    """Source env file if provided"""
    config = get_config()
    if config.env_file:
        load_dotenv(find_dotenv(config.env_file), override=True)
        logger.info(f"Sourcing env vars from {config.env_file}")
        return

    load_dotenv()
