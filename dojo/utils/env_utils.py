from bittensor.utils.btlogging import logging as logger
from dotenv import find_dotenv, load_dotenv


def source_dotenv(env_file=None):
    """
    Source env file if provided

    Args:
        env_file: Optional path to env file. If None, will try to use .env
    """
    if env_file:
        load_dotenv(find_dotenv(env_file), override=True)
        logger.info(f"Sourcing env vars from {env_file}")
        return

    load_dotenv()
