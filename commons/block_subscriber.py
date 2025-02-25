import asyncio
from datetime import datetime
from typing import Any, Awaitable, Callable

from bittensor.core.async_subtensor import AsyncSubstrateInterface
from bittensor.core.subtensor import SubstrateRequestException
from loguru import logger

from commons.objects import ObjectManager

BLOCK_TIME = 12


class SubscriptionWatchdog:
    def __init__(self, max_block_interval: float):
        self.last_block_time = datetime.now()
        self.max_block_interval = max_block_interval
        self.is_healthy = True

    def update(self):
        """Updates the last block time and marks the subscription as healthy."""
        self.last_block_time = datetime.now()
        self.is_healthy = True

    def check_health(self) -> bool:
        """Checks if the subscription is healthy by comparing the time since the last block to the max block interval."""
        time_since_last_block = (datetime.now() - self.last_block_time).total_seconds()
        self.is_healthy = time_since_last_block <= self.max_block_interval
        return self.is_healthy


async def monitor_subscription(
    watchdog: SubscriptionWatchdog, max_block_interval: float
):
    """Monitors the health of the block subscription by checking the time since the last block.

    Runs continuously in the background, checking every 10 seconds if a new block has been
    received within the maximum allowed interval. If no blocks are received for longer than
    the max interval, raises a ConnectionError.

    Raises:
        ConnectionError: When no new blocks have been received for longer than max_block_interval seconds,
                       indicating the subscription has likely failed.
    """
    while True:
        await asyncio.sleep(5 * BLOCK_TIME)
        time_since_last = (datetime.now() - watchdog.last_block_time).total_seconds()

        if not watchdog.check_health():
            logger.warning(
                f"No blocks received for {time_since_last:.1f} seconds! (max allowed: {max_block_interval})"
            )
            # Create a specific exception type for this case
            raise ConnectionError(
                f"Subscription watchdog timeout - no blocks for {time_since_last:.1f} seconds"
            )
        else:
            logger.debug(
                f"Subscription is healthy - last block {time_since_last:.1f} seconds ago"
            )


async def start_block_subscriber(
    callbacks: list[Callable[..., Awaitable[Any]]],
    url: str = ObjectManager.get_config().subtensor.chain_endpoint,  # type: ignore
    retry_delay: float = 5.0,
    max_block_interval: float = 2 * BLOCK_TIME,
    max_retries: int | None = None,
):
    """Starts a block subscriber that monitors the health of the block subscription.

    Args:
        callback (Callable[..., Awaitable[Any]]): The callback function to call when a block is received.
        url (str, optional): The URL of the substrate node. Defaults to ObjectManager.get_config().subtensor.chain_endpoint.
        retry_delay (float, optional): The delay between retries. Defaults to 5.0.
        max_retries (int | None, optional): The maximum number of retries. Defaults to None.
        max_block_interval (float, optional): The maximum interval between blocks. Defaults to 2*BLOCK_TIME.

    Raises:
        ConnectionError: When no new blocks have been received for longer than max_block_interval seconds,
                           indicating the subscription has likely failed.
    """
    watchdog = SubscriptionWatchdog(max_block_interval)

    retry_count = 0

    async def wrapped_callback(*args, **kwargs):
        """Wraps the original callback function to provide additional functionality.

        Updates the watchdog timer and resets retry count on successful block processing.
        Forwards all arguments to the original callback function.

        Args:
            *args: Variable positional arguments to pass to the callback
            **kwargs: Variable keyword arguments to pass to the callback
        """
        nonlocal retry_count
        retry_count = 0
        watchdog.update()

        # execute all callbacks
        for callback in callbacks:
            await callback(*args, **kwargs)

    while True:
        try:
            # Connect to the substrate node
            async with AsyncSubstrateInterface(url=url) as substrate:
                monitor_task = asyncio.create_task(
                    monitor_subscription(watchdog, max_block_interval)
                )
                try:
                    logger.info("Subscribing to block headers...")
                    # Create the subscription task
                    subscription_task = asyncio.create_task(
                        substrate.subscribe_block_headers(
                            subscription_handler=wrapped_callback, finalized_only=True
                        )
                    )

                    # Wait for either task to complete (or fail)
                    done, pending = await asyncio.wait(
                        [monitor_task, subscription_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    # Cancel the remaining task
                    for task in pending:
                        task.cancel()

                    # Check if monitor_task raised an exception
                    if monitor_task in done:
                        monitor_task.result()  # This will raise the exception if there was one

                except ConnectionError:
                    logger.error("Watchdog detected subscription failure")
                    raise
                except Exception as subscription_error:
                    logger.error(f"Subscription failed: {subscription_error}")
                    raise
                finally:
                    # Clean up tasks
                    for task in [monitor_task]:
                        if not task.done():
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass

        except KeyboardInterrupt:
            logger.info("\nSubscription ended by user")
            raise

        except (SubstrateRequestException, Exception) as e:
            logger.error(f"Error occurred: {e}")

            retry_count += 1
            if max_retries is not None and retry_count >= max_retries:
                logger.error(
                    f"Max retries ({max_retries}) reached. Stopping subscription."
                )
                raise

            # Calculate exponential delay with base delay and retry count
            current_delay = retry_delay * (2 ** (retry_count - 1))

            logger.error(f"Error occurred: {e}")
            logger.info(
                f"Attempting to resubscribe in {current_delay} seconds... (attempt {retry_count})"
            )
            await asyncio.sleep(current_delay)
            continue


async def your_callback(block: dict):
    logger.success(f"Received block: {block}")


async def main():
    try:
        # Will raise an exception if no blocks received for 60 seconds
        await start_block_subscriber(
            [your_callback],
            max_block_interval=12,
            retry_delay=5.0,
        )
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Subscription failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
