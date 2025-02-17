import asyncio
import json
import os
import traceback
from pathlib import Path
from typing import List

import aiohttp
import bittensor as bt
from bittensor.utils.btlogging import logging as logger
from tenacity import RetryError

import dojo
from commons.dataset.synthetic import SyntheticAPI
from commons.orm import ORM
from commons.utils import get_epoch_time, get_new_uuid, set_expire_time
from dojo.protocol import (
    DendriteQueryResponse,
    FeedbackRequest,
    MultiScoreCriteria,
    SyntheticQA,
    TaskType,
)
from dojo.utils.config import get_config
from neurons.validator import Validator


class ValidatorSim(Validator):
    def __init__(self):
        if get_config().simulation_validator_synthetic_dataset:
            self.synthetic_dataset_path = Path(
                get_config().simulation_validator_synthetic_dataset
            )
            self.last_modified_time = os.path.getmtime(self.synthetic_dataset_path)
            self.synthetic_data = self._load_synthetic_dataset()
            self.current_synthetic_index = 0
            logger.info("Validator is using synthetic dataset")

        self.forward_ground_truth = (
            get_config().simulation_validator_forward_ground_truth
        )

        super().__init__()
        logger.info("Starting Validator Simulator")

    def _load_synthetic_dataset(self):
        try:
            with open(self.synthetic_dataset_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(
                f"Failed to load synthetic dataset from {self.synthetic_dataset_path}: {e}"
            )
            return []

    def _hot_reload(self):
        try:
            current_mtime = os.path.getmtime(self.synthetic_dataset_path)
            if current_mtime > self.last_modified_time:
                logger.info("Dataset file has been modified, reloading...")
                self.synthetic_data = self._load_synthetic_dataset()
                self.current_synthetic_index = 0
                logger.success("Dataset reloaded successfully")
        except Exception as e:
            logger.error(f"Error checking/reloading dataset: {e}")

    async def _get_next_synthetic_data(self) -> SyntheticQA:
        self._hot_reload()

        if not self.synthetic_data:
            raise ValueError("No synthetic data available")

        synthetic_qa = self.synthetic_data[self.current_synthetic_index]
        self.current_synthetic_index = (self.current_synthetic_index + 1) % len(
            self.synthetic_data
        )
        return SyntheticQA.model_validate(synthetic_qa)

    async def send_request(
        self,
        synapse: FeedbackRequest | None = None,
        external_user: bool = False,
    ):
        start = get_epoch_time()
        # typically the request may come from an external source however,
        # initially will seed it with some data for miners to get started
        if len(self._active_miner_uids) == 0:
            logger.info("No active miners to send request to... skipping")
            return

        request_id = get_new_uuid()
        # sel_miner_uids = await self.get_miner_uids(external_user, request_id)
        sel_miner_uids = sorted(list(self._active_miner_uids))

        axons = [
            self.metagraph.axons[uid]
            for uid in sel_miner_uids
            if self.metagraph.axons[uid].hotkey.casefold()
            != self.wallet.hotkey.ss58_address.casefold()
        ]
        if not len(axons):
            logger.warning("🤷 No axons to query ... skipping")
            return

        obfuscated_model_to_model = {}

        if synapse is None:
            if self.synthetic_dataset_path:
                data = await self._get_next_synthetic_data()
            else:
                try:
                    data = await SyntheticAPI.get_qa()
                except RetryError as e:
                    logger.error(
                        f"Exhausted all retry attempts for synthetic data generation: {e}"
                    )
                    return
                except ValueError as e:
                    logger.error(f"Invalid response from synthetic data API: {e}")
                    return
                except aiohttp.ClientError as e:
                    logger.error(f"Network error when calling synthetic data API: {e}")
                    return
                except Exception as e:
                    logger.error(
                        f"Unexpected error during synthetic data generation: {e}"
                    )
                    logger.debug(f"Traceback: {traceback.format_exc()}")
                    return

            if not data:
                logger.error("No data returned from synthetic data API")
                return

            obfuscated_model_to_model = self.obfuscate_model_names(data.responses)
            expire_at = set_expire_time(dojo.TASK_DEADLINE)
            synapse = FeedbackRequest(
                request_id=request_id,
                task_type=str(TaskType.CODE_GENERATION),
                criteria_types=[
                    MultiScoreCriteria(
                        options=list(obfuscated_model_to_model.keys()),
                        min=1.0,
                        max=100.0,
                    ),
                ],
                prompt=data.prompt,
                completion_responses=data.responses,
                expire_at=expire_at,
                ground_truth=data.ground_truth
                if self.forward_ground_truth
                else {},  # Added ground truth!!!!!
            )
        elif external_user:
            obfuscated_model_to_model = self.obfuscate_model_names(
                synapse.completion_responses
            )

        logger.info(
            f"⬆️ Sending feedback request for request id: {synapse.request_id}, miners uids:{sel_miner_uids} with expire_at: {synapse.expire_at}"
        )

        miner_responses: List[FeedbackRequest] = await self._send_shuffled_requests(
            self.dendrite, axons, synapse
        )

        valid_miner_responses: List[FeedbackRequest] = []
        try:
            for miner_response in miner_responses:
                miner_hotkey = (
                    miner_response.axon.hotkey if miner_response.axon else "??"
                )
                # map obfuscated model names back to the original model names
                real_model_ids = []

                for i, completion in enumerate(miner_response.completion_responses):
                    found_model_id = obfuscated_model_to_model.get(
                        completion.model, None
                    )
                    real_model_ids.append(found_model_id)
                    if found_model_id:
                        miner_response.completion_responses[i].model = found_model_id
                        synapse.completion_responses[i].model = found_model_id

                if any(c is None for c in real_model_ids):
                    logger.warning("Failed to map obfuscated model to original model")
                    continue

                if miner_response.dojo_task_id is None:
                    logger.debug(f"Miner {miner_hotkey} must provide the dojo task id")
                    continue

                # update the miner response with the real model ids
                valid_miner_responses.append(miner_response)
        except Exception as e:
            logger.error(f"Failed to map obfuscated model to original model: {e}")
            pass

        logger.info(f"⬇️ Received {len(valid_miner_responses)} valid responses")
        if valid_miner_responses is None or len(valid_miner_responses) == 0:
            logger.info("No valid miner responses to process... skipping")
            return

        # include the ground_truth to keep in data manager
        synapse.ground_truth = data.ground_truth
        synapse.dendrite.hotkey = self.wallet.hotkey.ss58_address
        response_data = DendriteQueryResponse(
            request=synapse,
            miner_responses=valid_miner_responses,
        )

        logger.debug("Attempting to saving dendrite response")
        vali_request_model = await ORM.save_task(
            validator_request=synapse,
            miner_responses=valid_miner_responses,
            ground_truth=data.ground_truth,
        )

        if vali_request_model is None:
            logger.error("Failed to save dendrite response")
            return

        # saving response
        logger.success(
            f"Saved dendrite response for request id: {response_data.request.request_id}"
        )
        logger.info(
            f"Sending request to miners & processing took {get_epoch_time() - start}"
        )
        return

    @staticmethod
    async def _send_shuffled_requests(
        dendrite: bt.dendrite, axons: List[bt.AxonInfo], synapse: FeedbackRequest
    ) -> list[FeedbackRequest]:
        """Send the same request to all miners without shuffling the order.
        WARNING: This should only be used for testing/debugging as it could allow miners to game the system.

        Args:
            dendrite (bt.dendrite): Communication channel to send requests
            axons (List[bt.AxonInfo]): List of miner endpoints
            synapse (FeedbackRequest): The feedback request to send

        Returns:
            list[FeedbackRequest]: List of miner responses
        """
        all_responses = []
        batch_size = 10

        for i in range(0, len(axons), batch_size):
            batch_axons = axons[i : i + batch_size]
            tasks = []

            for axon in batch_axons:
                tasks.append(
                    dendrite.forward(
                        axons=[axon],
                        synapse=synapse,
                        deserialize=False,
                        timeout=60,
                    )
                )

            batch_responses = await asyncio.gather(*tasks)
            flat_batch_responses = [
                response for sublist in batch_responses for response in sublist
            ]
            all_responses.extend(flat_batch_responses)

            logger.info(
                f"Processed batch {i // batch_size + 1} of {(len(axons) - 1) // batch_size + 1}"
            )

        return all_responses
