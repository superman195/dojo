import os
from typing import List

import requests
from bittensor.utils.btlogging import logging as logger

from ..core.models import LogEntry


class LogStorage:
    """Storage class for logging data to external systems"""

    def __init__(self, config=None):
        self.config = config
        self.loki_url = os.getenv("DOJO_LOKI_URL")
        if not self.loki_url:
            logger.warning("DOJO_LOKI_URL environment variable not set")

    async def send_to_loki(self, logs: List[LogEntry], validator_hotkey: str) -> bool:
        if not self.loki_url:
            logger.error("Loki URL not configured")
            return False

        # Prepare the Loki payload
        streams = []
        for log in logs:
            # Convert timestamp to nanoseconds (Loki requirement)
            timestamp_ns = int(log.timestamp.timestamp() * 1e9)

            # Prepare labels - add an ansi_color_enabled label to signal to Loki that this contains color codes
            labels = {
                "validator": validator_hotkey,
                "level": log.level.lower(),
                "service_name": "unknown_service",
                "ansi_color_enabled": "true",
            }
            if hasattr(log, "labels") and log.labels:
                labels.update(log.labels)

            # Just use the pre-formatted message directly as a string
            # This ensures the format in Loki matches what we want
            stream = {"stream": labels, "values": [[str(timestamp_ns), log.message]]}
            streams.append(stream)

        payload = {"streams": streams}

        try:
            response = requests.post(
                f"{self.loki_url}/loki/api/v1/push",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            logger.info(
                f"Successfully sent {len(logs)} logs to Loki for validator {validator_hotkey}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to send logs to Loki: {str(e)}")
            return False
