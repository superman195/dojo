import asyncio
import json
from collections.abc import Awaitable
from datetime import datetime
from typing import Any, Callable

import websockets
from bittensor.utils.btlogging import logging as logger

from commons.objects import ObjectManager
from dojo.chain import parse_block_headers

BLOCK_TIME = 12
WS_OPEN_TIMEOUT = 30
WS_CLOSE_TIMEOUT = 30
WS_RECV_TIMEOUT = 30


class SubscriptionWatchdog:
    def __init__(self, max_interval_sec: float):
        self.last_block_time: datetime = datetime.now()
        self.max_interval_sec: float = float(max_interval_sec)
        self.is_healthy: bool = True

    def update(self):
        """Updates the last block time and marks the subscription as healthy."""
        self.last_block_time = datetime.now()
        self.is_healthy = True

    def check_health(self) -> bool:
        """Checks if the subscription is healthy by comparing the time since the last block to the max block interval."""
        time_since_last_block = (datetime.now() - self.last_block_time).total_seconds()
        self.is_healthy = time_since_last_block <= self.max_interval_sec
        return self.is_healthy


async def monitor_subscription(watchdog: SubscriptionWatchdog, max_interval_sec: float):
    """Monitors the health of the block subscription by checking the time since the last block.

    Runs continuously in the background, checking every N seconds if a new block has been
    received within the maximum allowed interval. If no blocks are received for longer than
    the max interval, raises a ConnectionError.

    Raises:
        ConnectionError: When no new blocks have been received for longer than max_interval_sec seconds,
                       indicating the subscription has likely failed.
    """
    while True:
        await asyncio.sleep(5 * BLOCK_TIME)
        time_since_last = (datetime.now() - watchdog.last_block_time).total_seconds()

        if not watchdog.check_health():
            logger.warning(
                f"No blocks received for {time_since_last:.1f} seconds! (max allowed: {max_interval_sec})"
            )
            # Create a specific exception type for this case
            raise ConnectionError(
                f"Subscription watchdog timeout - no blocks for {time_since_last:.1f} seconds"
            )
        else:
            logger.info(
                f"Subscription is healthy - last block {time_since_last:.1f} seconds ago"
            )


async def start_block_subscriber(
    callbacks: list[Callable[..., Awaitable[Any] | Any]],  # pyright: ignore[reportExplicitAny]
    max_interval_sec: float,
    url: str = ObjectManager.get_config().subtensor.chain_endpoint,
    retry_delay: float = 5.0,
    max_retries: int | None = None,
):
    """Starts a block subscriber that monitors the health of the block subscription.

    Args:
        callbacks (list[Callable[..., Awaitable[Any] | Any]]): The callback functions to call when a block is received, callback functions may be asynchronous or synchronous.
        max_interval_sec (float): The maximum expected interval between websocket messages.
        url (str, optional): The URL of the substrate node. Defaults to ObjectManager.get_config().subtensor.chain_endpoint.
        retry_delay (float, optional): The delay between retries. Defaults to 5.0.
        max_retries (int | None, optional): The maximum number of retries. Defaults to None.

    Raises:
        ConnectionError: When no new blocks have been received for longer than max_interval_sec seconds,
                           indicating the subscription has likely failed.
    """
    watchdog = SubscriptionWatchdog(max_interval_sec)
    retry_count = 0

    async def process_block(block_header):
        """Process a block and execute all callbacks."""
        nonlocal retry_count
        retry_count = 0
        watchdog.update()

        # execute all callbacks
        for callback in callbacks:
            logger.debug(f"Executing callback: {callback.__name__}")
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(block_header)
                else:
                    callback(block_header)
            except Exception as e:
                logger.error(f"Error in callback: {e}")

    async def subscribe_to_blocks():
        logger.info(f"Connecting to WebSocket at {url}")
        try:
            async with websockets.connect(
                url, close_timeout=WS_CLOSE_TIMEOUT, open_timeout=WS_OPEN_TIMEOUT
            ) as websocket:
                # Subscribe to finalized blocks
                subscription_request = {
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "chain_subscribeFinalizedHeads",
                    "params": [],
                }
                await websocket.send(json.dumps(subscription_request))

                # Get subscription ID from response (with timeout)
                try:
                    response = await asyncio.wait_for(
                        websocket.recv(), timeout=WS_RECV_TIMEOUT
                    )
                    response_data = json.loads(response)

                    if "error" in response_data:
                        raise Exception(f"Subscription error: {response_data['error']}")

                    subscription_id = response_data.get("result")
                    if subscription_id is None:
                        raise Exception(f"No subscription ID returned: {response_data}")

                    logger.info(
                        f"Subscribed to finalized heads with ID: {subscription_id}"
                    )
                except asyncio.TimeoutError:
                    raise TimeoutError(
                        "Timed out waiting for subscription confirmation"
                    )

                # Process incoming blocks - run indefinitely
                while True:
                    try:
                        # Add a timeout to recv() to prevent hanging indefinitely
                        response = await asyncio.wait_for(
                            websocket.recv(), timeout=max_interval_sec
                        )
                        data = json.loads(response)

                        if (
                            "params" in data
                            and "subscription" in data["params"]
                            and data["params"]["subscription"] == subscription_id
                            and "result" in data["params"]
                        ):
                            block_header = data["params"]["result"]
                            logger.trace(f"Got data: {data}")
                            await process_block(block_header)

                        elif "error" in data:
                            logger.warning(
                                f"Received error from subscription: {data['error']}"
                            )

                    except asyncio.TimeoutError:
                        logger.warning(
                            f"No message received for {max_interval_sec} seconds, checking connection..."
                        )

                        # Send a ping to verify the connection is still alive
                        pong_waiter = await websocket.ping()
                        try:
                            await asyncio.wait_for(pong_waiter, timeout=WS_RECV_TIMEOUT)
                            logger.info("Connection is still alive")
                        except asyncio.TimeoutError:
                            logger.error("WebSocket ping timed out")
                            raise ConnectionError("WebSocket ping timed out")

        except asyncio.CancelledError:
            logger.error("Subscription task cancelled...")
            raise
        except TimeoutError as e:
            logger.error(f"WebSocket timeout: {e}")
            raise
        except Exception as e:
            logger.error(f"WebSocket encountered fatal error: {e}")
            raise

    while True:
        try:
            # Create the subscription task
            logger.info("Starting new WebSocket subscription...")
            monitor_task = asyncio.create_task(
                monitor_subscription(watchdog, max_interval_sec)
            )
            subscription_task = asyncio.create_task(subscribe_to_blocks())

            # Wait for either task to complete (or fail)
            done, pending = await asyncio.wait(
                [monitor_task, subscription_task],
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Cancel the remaining task
            for task in pending:
                task.cancel()

            # Check which task completed and handle its result
            for task in done:
                try:
                    task.result()  # This will raise the exception if there was one
                except asyncio.CancelledError:
                    logger.info("Task was cancelled")
                except Exception as e:
                    # Re-raise the exception to be caught by the outer try/except
                    raise e

        except KeyboardInterrupt:
            logger.info("\nSubscription ended by user")
            raise

        except ConnectionError:
            logger.error("Watchdog detected subscription failure")
            retry_count += 1

        except Exception as e:
            logger.error(f"Error occurred: {e}")
            retry_count += 1

        # Handle retries
        if max_retries is not None and retry_count >= max_retries:
            logger.error(f"Max retries ({max_retries}) reached. Stopping subscription.")
            raise

        # Calculate exponential delay with base delay and retry count
        current_delay = retry_delay * (2 ** (retry_count - 1))

        logger.info(
            f"Attempting to resubscribe in {current_delay} seconds... (attempt {retry_count})"
        )
        await asyncio.sleep(current_delay)


async def your_callback(block: dict):
    logger.info("Calling asynchronous callback function")
    logger.info(f"Received block headers {block}")
    block_header = parse_block_headers(block)
    logger.info(f"Parsed block number: {block_header.number.to_int()}")


def sync_callback(block: dict):
    logger.info("Calling synchronous callback function")
    logger.info(f"Received block headers {block}")


async def main():
    try:
        # Will raise an exception if no blocks received for 60 seconds
        await start_block_subscriber(
            [your_callback, sync_callback],
            max_interval_sec=60,
            retry_delay=5.0,
        )
    except KeyboardInterrupt:
        logger.info("\nShutting down...")
    except Exception as e:
        logger.error(f"Subscription failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
