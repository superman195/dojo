import asyncio
from typing import Dict, List, TypeAlias

# import aiohttp
import bittensor as bt
from bittensor.utils.btlogging import logging as logger

# from commons.orm import ORM
from commons.utils import ttl_get_block
from dojo.protocol import TaskSynapseObject
from neurons.validator import Validator

ObfuscatedModelMap: TypeAlias = Dict[str, str]


class ValidatorSim(Validator):
    def __init__(self):
        # self._last_block = None
        # self._block_check_attempts = 0
        # self.MAX_BLOCK_CHECK_ATTEMPTS = 3
        self._connection_lock = asyncio.Lock()

        super().__init__()
        logger.info("Starting Validator Simulator")

    async def _try_reconnect_subtensor(self):
        self._block_check_attempts += 1
        if self._block_check_attempts >= self.MAX_BLOCK_CHECK_ATTEMPTS:
            logger.error(
                f"Failed to reconnect after {self.MAX_BLOCK_CHECK_ATTEMPTS} attempts"
            )
            return False

        try:
            logger.info(
                f"Attempting to reconnect to subtensor (attempt {self._block_check_attempts}/{self.MAX_BLOCK_CHECK_ATTEMPTS})..."
            )
            if hasattr(self.subtensor.substrate, "websocket"):
                self.subtensor.substrate.websocket.close()

            self.subtensor = bt.subtensor(self.subtensor.config)
            await asyncio.sleep(1)
            return True
        except Exception as e:
            logger.error(f"Failed to reconnect to subtensor: {e}")
            return await self._try_reconnect_subtensor()

    async def _ensure_subtensor_connection(self):
        async with self._connection_lock:
            try:
                self.subtensor.get_current_block()
                self._block_check_attempts = 0
                return True
            except (BrokenPipeError, ConnectionError):
                logger.warning("Connection lost, attempting immediate reconnection")
                return await self._try_reconnect_subtensor()
            except Exception as e:
                logger.error(f"Unexpected error checking connection: {e}")
                return False

    @property
    def block(self):
        try:
            if not asyncio.get_event_loop().run_until_complete(
                self._ensure_subtensor_connection()
            ):
                logger.warning(
                    "Subtensor connection failed - returning last known block"
                )
                return self._last_block if self._last_block is not None else 0

            self._last_block = ttl_get_block(self.subtensor)
            self._block_check_attempts = 0
            return self._last_block
        except Exception as e:
            logger.error(f"Error getting block number: {e}")
            return self._last_block if self._last_block is not None else 0

    async def sync(self):
        has_connection = await self._ensure_subtensor_connection()
        if not has_connection:
            logger.warning("Subtensor connection failed - continuing with partial sync")

        await super().sync()

    # async def _generate_synthetic_request(
    #     self,
    # ) -> tuple[TaskSynapseObject | None, dict[str, int] | None, ObfuscatedModelMap]:
    #     """
    #     Generate a synthetic request for code generation tasks.

    #     Returns:
    #         tuple[TaskSynapseObject | None, dict[str, int] | ObfuscatedModelMap]: Tuple containing the generated task synapse object
    #         and ground truth, or None if generation fails
    #     """
    #     task_id = get_new_uuid()
    #     try:
    #         data: SyntheticQA | None = await SyntheticAPI.get_qa()
    #         if not data or not data.responses:
    #             logger.error("Invalid or empty data returned from synthetic data API")
    #             return None, None, {}

    #         # Create criteria for each completion response
    #         criteria: List[CriteriaType] = [
    #             ScoreCriteria(
    #                 min=1.0,
    #                 max=100.0,
    #             )
    #         ]

    #         # Set criteria_types for each completion response
    #         for response in data.responses:
    #             response.criteria_types = criteria

    #         obfuscated_model_to_model = self.obfuscate_model_names(data.responses)
    #         synapse = TaskSynapseObject(
    #             task_id=task_id,
    #             prompt=data.prompt,
    #             task_type=str(TaskTypeEnum.CODE_GENERATION),
    #             expire_at=set_expire_time(dojo.TASK_DEADLINE),
    #             completion_responses=data.responses,
    #         )

    #         return synapse, data.ground_truth, obfuscated_model_to_model

    #     except (RetryError, ValueError, aiohttp.ClientError) as e:
    #         logger.error(
    #             f"Failed to generate synthetic request: {type(e).__name__}: {str(e)}"
    #         )
    #     except Exception as e:
    #         logger.error(f"Unexpected error during synthetic data generation: {e}")
    #         logger.debug(f"Traceback: {traceback.format_exc()}")

    #     return None, None, {}

    # async def send_request(
    #     self,
    #     synapse: TaskSynapseObject | None = None,
    #     ground_truth: dict[str, int] | None = None,
    #     obfuscated_model_to_model: ObfuscatedModelMap = {},
    # ):
    #     if not synapse:
    #         logger.warning("No synapse provided... skipping")
    #         return

    #     if not self._active_miner_uids:
    #         logger.info("No active miners to send request to... skipping")
    #         return

    #     if not synapse.completion_responses:
    #         logger.warning("No completion responses to send... skipping")
    #         return

    #     start = get_epoch_time()
    #     sel_miner_uids = await self.get_miner_uids()

    #     axons = [
    #         axon
    #         for uid in sel_miner_uids
    #         if (axon := self.metagraph.axons[uid]).hotkey.casefold()
    #         != self.vali_hotkey.casefold()
    #     ]

    #     if not axons:
    #         logger.warning("🤷 No axons to query ... skipping")
    #         return

    #     logger.info(
    #         f"⬆️ Sending task request for task id: {synapse.task_id}, miners uids:{sel_miner_uids} with expire_at: {synapse.expire_at}"
    #     )

    #     miner_responses: List[TaskSynapseObject] = await self._send_shuffled_requests(
    #         self.dendrite, axons, synapse
    #     )

    #     valid_miner_responses: List[TaskSynapseObject] = []
    #     for response in miner_responses:
    #         try:
    #             if not response.dojo_task_id:
    #                 continue

    #             # map obfuscated model names back to the original model names
    #             real_model_ids = []
    #             if response.completion_responses:
    #                 for i, completion in enumerate(response.completion_responses):
    #                     found_model_id = obfuscated_model_to_model.get(
    #                         completion.model, None
    #                     )
    #                     real_model_ids.append(found_model_id)
    #                     if found_model_id:
    #                         response.completion_responses[i].model = found_model_id
    #                         synapse.completion_responses[i].model = found_model_id

    #             if any(c is None for c in real_model_ids):
    #                 logger.warning("Failed to map obfuscated model to original model")
    #                 continue

    #             response.miner_hotkey = response.axon.hotkey if response.axon else None
    #             # Get coldkey from metagraph using hotkey index
    #             if response.axon and response.axon.hotkey:
    #                 try:
    #                     hotkey_index = self.metagraph.hotkeys.index(
    #                         response.axon.hotkey
    #                     )
    #                     response.miner_coldkey = self.metagraph.coldkeys[hotkey_index]
    #                 except ValueError:
    #                     response.miner_coldkey = None
    #             else:
    #                 response.miner_coldkey = None
    #             valid_miner_responses.append(response)

    #         except Exception as e:
    #             logger.error(f"Error processing miner response: {e}")
    #             continue

    #     logger.info(f"⬇️ Received {len(valid_miner_responses)} valid responses")
    #     if not valid_miner_responses:
    #         logger.info("No valid miner responses to process... skipping")
    #         return

    #     # include the ground_truth to keep in data manager
    #     synapse.ground_truth = ground_truth
    #     synapse.dendrite.hotkey = self.vali_hotkey

    #     logger.debug("Attempting to saving dendrite response")
    #     if not await ORM.save_task(
    #         validator_task=synapse,
    #         miner_responses=valid_miner_responses,
    #         ground_truth=ground_truth or {},
    #     ):
    #         logger.error("Failed to save dendrite response")
    #         return

    #     logger.success(f"Saved dendrite response for task id: {synapse.task_id}")
    #     logger.info(
    #         f"Sending request to miners & processing took {get_epoch_time() - start}"
    #     )
    #     return

    @staticmethod
    async def _send_shuffled_requests(
        dendrite: bt.dendrite, axons: List[bt.AxonInfo], synapse: TaskSynapseObject
    ) -> list[TaskSynapseObject]:
        """Send the same request to all miners without shuffling the order.
        WARNING: This should only be used for testing/debugging as it could allow miners to game the system.

        Args:
            dendrite (bt.dendrite): Communication channel to send requests
            axons (List[bt.AxonInfo]): List of miner endpoints
            synapse (FeedbackRequest): The feedback request to send

        Returns:
            list[TaskSynapseObject]: List of miner responses
        """
        all_responses = []
        batch_size = 10

        if not synapse.completion_responses:
            logger.warning("No completion responses to send... skipping")
            return all_responses

        for i in range(0, len(axons), batch_size):
            batch_axons = axons[i : i + batch_size]
            tasks = []

            for axon in batch_axons:
                tasks.append(
                    dendrite.forward(
                        axons=[axon],
                        synapse=synapse,
                        deserialize=False,
                        timeout=12,
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
