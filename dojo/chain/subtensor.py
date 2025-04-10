import bittensor
from async_lru import alru_cache
from tenacity import (
    AsyncRetrying,
    RetryError,
    stop_after_attempt,
    wait_fixed,
)

from commons.logging import logging as logger
from commons.objects import ObjectManager


@alru_cache(maxsize=1)
async def get_async_subtensor(max_retries: int = 5) -> bittensor.AsyncSubtensor | None:
    """Connect to the async subtensor instance, including retries to handle possible ConnectionError that may occur."""
    try:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(max_attempt_number=max_retries), wait=wait_fixed(2)
        ):
            with attempt:
                config = ObjectManager.get_config()
                async_subtensor = bittensor.AsyncSubtensor(config=config)
                await async_subtensor.initialize()
                logger.success("Successfully connected to subtensor.")
                return async_subtensor
    except RetryError:
        logger.error(f"Failed to connect to subtensor after {max_retries} attempts.")
        return None
