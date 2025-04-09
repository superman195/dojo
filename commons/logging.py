"""
Custom logging handler for forwarding logs to the validator API
"""

import asyncio
import logging
from datetime import datetime

import aiohttp
import bittensor as bt
from bittensor.utils.btlogging import logging as logger


class ValidatorAPILogHandler(logging.Handler):
    def __init__(
        self,
        api_url: str,
        hotkey: str,
        wallet: bt.wallet,
        batch_size: int = 100,
        flush_interval: float = 1.0,
    ):
        super().__init__()

        # Ensure api_url is properly formatted
        if api_url:
            # Make sure the URL has a scheme
            if not api_url.startswith(("http://", "https://")):
                api_url = f"https://{api_url}"

            # Remove trailing slash if present
            if api_url.endswith("/"):
                api_url = api_url[:-1]

            logger.info(f"Initializing validator API logger with URL: {api_url}")
        else:
            logger.warning("API URL is not set, log forwarding to API will be disabled")

        self.api_url = api_url
        self.hotkey = hotkey
        self.wallet = wallet  # Store wallet for signing messages
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.log_queue = asyncio.Queue()
        self._shutdown = False
        self._flush_task = None

    def sign_message(self, message: str) -> str:
        """Sign a message using the wallet's hotkey"""
        signature = self.wallet.hotkey.sign(message).hex()
        if not signature.startswith("0x"):
            signature = f"0x{signature}"
        return signature

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to the queue"""
        try:
            # Get module path from record
            module_path = f"{record.module}:{record.funcName}:{record.lineno}"

            # Format time
            timestamp = datetime.fromtimestamp(record.created).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )[:-3]

            # Get the original message (potentially with ANSI colors)
            message = record.getMessage()

            # Format message to match sample logs but preserve any existing colors
            formatted_message = (
                f"{timestamp} | {record.levelname:^15} | {module_path} | {message}"
            )

            # Convert the log record to a dict with only essential fields
            log_entry = {
                "timestamp": datetime.fromtimestamp(record.created).isoformat(),
                "level": record.levelname,
                "message": formatted_message,
            }

            # Put the log entry in the queue
            asyncio.create_task(self.log_queue.put(log_entry))

        except Exception as e:
            logger.error(f"Error in ValidatorAPILogHandler.emit: {e}")

    async def _flush_logs(self):
        """Periodically flush logs to the API"""
        while not self._shutdown:
            try:
                # Wait for flush interval or batch size
                logs = []
                try:
                    while len(logs) < self.batch_size:
                        log = await asyncio.wait_for(
                            self.log_queue.get(), timeout=self.flush_interval
                        )
                        logs.append(log)
                except asyncio.TimeoutError:
                    pass

                if logs and self.api_url:
                    # Create a deterministic message for this batch
                    current_time = datetime.now().isoformat()
                    message = (
                        f"Log batch containing {len(logs)} entries at {current_time}"
                    )
                    # Sign the message
                    signature = self.sign_message(message)

                    # Prepare the batch
                    batch = {
                        "hotkey": self.hotkey,
                        "signature": signature,
                        "message": message,
                        "logs": logs,
                    }

                    # Construct the complete URL
                    api_endpoint = f"{self.api_url}/api/v1/validator/logging"

                    # Send to API
                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            api_endpoint,
                            json=batch,
                            headers={"Content-Type": "application/json"},
                        ) as response:
                            if response.status != 200:
                                logger.error(
                                    f"Failed to send logs to API: {await response.text()}"
                                )

            except Exception as e:
                logger.error(f"Error in _flush_logs: {e}")

    def start(self):
        """Start the flush task"""
        if not self._flush_task:
            self._flush_task = asyncio.create_task(self._flush_logs())

    async def stop(self):
        """Stop the handler and flush remaining logs"""
        self._shutdown = True
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None

        # Flush any remaining logs
        logs = []
        while not self.log_queue.empty():
            try:
                log = self.log_queue.get_nowait()
                logs.append(log)
            except asyncio.QueueEmpty:
                break

        if logs and self.api_url:
            # Create a final message for this batch
            current_time = datetime.now().isoformat()
            message = (
                f"Final log batch containing {len(logs)} entries at {current_time}"
            )
            # Sign the message
            signature = self.sign_message(message)

            batch = {
                "hotkey": self.hotkey,
                "signature": signature,
                "message": message,
                "logs": logs,
            }

            # Construct the complete URL
            api_endpoint = f"{self.api_url}/api/v1/validator/logging"

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    api_endpoint,
                    json=batch,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    if response.status != 200:
                        logger.error(
                            f"Failed to send final logs to API: {await response.text()}"
                        )
