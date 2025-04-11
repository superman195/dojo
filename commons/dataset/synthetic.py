import os
import traceback

import aiohttp
from bittensor.utils.btlogging import logging as logger
from tenacity import (
    AsyncRetrying,
    RetryError,
    before_sleep_log,
    stop_after_attempt,
    wait_exponential,
)

from commons.exceptions import (
    FatalSyntheticGenerationError,
    SyntheticGenerationError,
)
from dojo.protocol import SyntheticQA

SYNTHETIC_API_BASE_URL = os.getenv("SYNTHETIC_API_URL")


def _map_synthetic_response(response: dict) -> SyntheticQA:
    # Create a new dictionary to store the mapped fields
    mapped_data = {
        "prompt": response["prompt"],
        "ground_truth": response["ground_truth"],
        "metadata": response["metadata"],
    }

    responses = list(
        map(
            lambda resp: {
                "model": resp["model"],
                "completion": resp["completion"],
                "completion_id": resp["cid"],
            },
            response["responses"],
        )
    )

    mapped_data["responses"] = responses

    return SyntheticQA.model_validate(mapped_data)


class SyntheticAPI:
    _session: aiohttp.ClientSession | None = None

    @classmethod
    async def init_session(cls):
        if cls._session is None:
            cls._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=120)
            )
        return

    @classmethod
    async def close_session(cls):
        if cls._session is not None:
            await cls._session.close()
            cls._session = None
        logger.debug("Ensured SyntheticAPI session is closed.")

    @classmethod
    async def get_qa(cls) -> SyntheticQA | None:
        await cls.init_session()

        path = f"{SYNTHETIC_API_BASE_URL}/api/synthetic-gen"
        logger.debug(f"Generating synthetic QA from {path}.")

        MAX_RETRIES = 6

        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(MAX_RETRIES),
                wait=wait_exponential(multiplier=1, max=30),
                before_sleep=before_sleep_log(
                    logger._logger, log_level=10, exc_info=True
                ),
            ):
                with attempt:
                    async with cls._session.get(path) as response:
                        response.raise_for_status()
                        response_json = await response.json()
                        if response_json["success"] is False:
                            raise SyntheticGenerationError(
                                message=response_json.get(
                                    "error", "No error details provided"
                                ),
                            )
                        if "body" not in response_json:
                            raise SyntheticGenerationError(
                                "Invalid response from the server. "
                                "No body found in the response."
                            )

                        synthetic_qa = _map_synthetic_response(response_json["body"])
                        logger.success("Synthetic QA generated and parsed successfully")
                        return synthetic_qa
        except RetryError:
            logger.error(
                f"Failed to generate synthetic QA after {MAX_RETRIES} retries."
            )
            traceback.print_exc()
            raise FatalSyntheticGenerationError(
                "QA generation failed after all retry attempts"
            )

        except Exception:
            raise
