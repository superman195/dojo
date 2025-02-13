import asyncio
import copy
import threading
import time
import traceback
from datetime import datetime
from typing import Dict, Tuple

import bittensor as bt
from bittensor.utils.btlogging import logging as logger

from commons.human_feedback.dojo import DojoAPI
from commons.utils import get_epoch_time
from dojo import MINER_STATUS, VALIDATOR_MIN_STAKE
from dojo.base.miner import BaseMinerNeuron
from dojo.protocol import (
    Heartbeat,
    ScoringResult,
    TaskResult,
    TaskResultRequest,
    TaskSynapseObject,
)
from dojo.utils.config import get_config
from dojo.utils.uids import is_miner


class Miner(BaseMinerNeuron):
    _should_exit = False

    def __init__(self):
        super().__init__()
        # Dendrite lets us send messages to other nodes (axons) in the network.
        self.dendrite = bt.dendrite(wallet=self.wallet)

        # Attach determiners which functions are called when servicing a request.
        logger.info("Attaching forward function to miner axon.")
        self.axon.attach(
            forward_fn=self.forward_task_request,
            blacklist_fn=self.blacklist_task_request,
            priority_fn=self.priority_ranking,
        ).attach(forward_fn=self.forward_score_result).attach(
            forward_fn=self.ack_heartbeat
        )

        # Attach a handler for TaskResultRequest to return task results
        self.axon.attach(forward_fn=self.forward_task_result_request)

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: threading.Thread | None = None
        self.lock = asyncio.Lock()
        # log all incoming requests
        self.hotkey_to_request: Dict[str, TaskSynapseObject] = {}

    async def ack_heartbeat(self, synapse: Heartbeat) -> Heartbeat:
        caller_hotkey = (
            synapse.dendrite.hotkey if synapse.dendrite else "unknown hotkey"
        )
        logger.debug(f"⬇️ Received heartbeat synapse from {caller_hotkey}")
        if not synapse:
            logger.error("Invalid synapse object")
            return synapse

        synapse.ack = True
        logger.debug(f"⬆️ Respondng to heartbeat synapse: {synapse}")
        return synapse

    async def forward_score_result(self, synapse: ScoringResult) -> ScoringResult:
        logger.info("Received scoring result from validators")
        try:
            # Validate that synapse is not None and has the required fields
            if not synapse or not synapse.hotkey_to_completion_responses:
                logger.error(
                    "Invalid synapse object or missing hotkey_to_completion_responses attribute."
                )
                return synapse

            miner_completion_responses = synapse.hotkey_to_completion_responses.get(
                self.wallet.hotkey.ss58_address, None
            )
            if miner_completion_responses is None:
                logger.error(
                    f"Miner hotkey {self.wallet.hotkey.ss58_address} not found in scoring result but yet was sent the result"
                )
                return synapse

            # Log shared scores once (from first completion that has them)
            shared_scores_logged = False

            # Log scores for each completion response
            for idx, completion in enumerate(miner_completion_responses):
                for criteria in completion.criteria_types:
                    if hasattr(criteria, "scores") and criteria.scores:
                        scores = criteria.scores
                        # Log shared scores only once
                        if not shared_scores_logged:
                            logger.info(
                                f"Task {synapse.task_id} shared scores:"
                                f"\n\tGround Truth Score: {scores.ground_truth_score}"
                                f"\n\tCosine Similarity: {scores.cosine_similarity_score}"
                                f"\n\tNormalised Cosine Similarity: {scores.normalised_cosine_similarity_score}"
                                f"\n\tCubic Reward Score: {scores.cubic_reward_score}"
                            )
                            shared_scores_logged = True

                        # Log individual scores for each completion
                        logger.info(
                            f"Completion {idx + 1} scores:"
                            f"\n\tRaw Score: {scores.raw_score}"
                            f"\n\tNormalised Score: {scores.normalised_score}"
                        )

        except KeyError as e:
            logger.error(f"KeyError in forward_result: {e}")
        except Exception as e:
            logger.error(f"Error in forward_result: {e}")

        return synapse

    async def forward_task_request(
        self, synapse: TaskSynapseObject
    ) -> TaskSynapseObject:
        # Validate that synapse, dendrite, dendrite.hotkey, and response are not None
        if not synapse or not synapse.completion_responses:
            logger.error("Invalid synapse: missing synapse or completion_responses")
            return synapse

        if not synapse.dendrite or not synapse.dendrite.hotkey:
            logger.error("Invalid synapse: missing dendrite information")
            return synapse

        try:
            logger.info(
                f"Miner received task id: {synapse.task_id} from {synapse.dendrite.hotkey}, with expire_at: {synapse.expire_at}"
            )

            self.hotkey_to_request[synapse.dendrite.hotkey] = synapse

            # Create task and store ID
            if task_ids := await DojoAPI.create_task(synapse):
                synapse.dojo_task_id = task_ids[0]
                # Clear completion field in completion_responses to optimize network traffic
                for response in synapse.completion_responses:
                    response.completion = None
            else:
                logger.error("Failed to create task: no task IDs returned")

        except Exception as e:
            logger.error(
                f"Error processing request id: {getattr(synapse, 'task_id', 'unknown')}: {str(e)}"
            )
            logger.debug(f"Detailed error: {traceback.format_exc()}")

        return synapse

    async def forward_task_result_request(
        self, synapse: TaskResultRequest
    ) -> TaskResultRequest:
        """Handle a TaskResultRequest from a validator, fetching the task result from the DojoAPI."""
        if not synapse or not synapse.dojo_task_id:
            logger.error("Invalid TaskResultRequest: missing dojo_task_id")
            return synapse

        try:
            logger.info(
                f"Received TaskResultRequest for dojo task id: {synapse.dojo_task_id}"
            )

            # Fetch task results from DojoAPI using task_id
            task_results = await DojoAPI.get_task_results_by_dojo_task_id(
                synapse.dojo_task_id
            )

            transformed_results = []
            if task_results:
                transformed_results = [
                    {**result, "dojo_task_id": result.pop("task_id", None)}
                    for result in (r.copy() for r in task_results)
                ]

                # Convert transformed results to TaskResult objects
                synapse.task_results = [
                    TaskResult(**result) for result in transformed_results
                ]
            else:
                logger.debug(
                    f"No task result found for dojo task id: {synapse.dojo_task_id}"
                )

        except Exception as e:
            logger.error(f"Error handling TaskResultRequest: {e}")
            traceback.print_exc()

        return synapse

    async def blacklist_task_request(
        self, synapse: TaskSynapseObject
    ) -> Tuple[bool, str]:
        logger.info("checking blacklist function")

        caller_hotkey = synapse.dendrite.hotkey
        if caller_hotkey is None or caller_hotkey not in self.metagraph.hotkeys:
            # Ignore requests from unrecognized entities.
            logger.warning(
                f"Blacklisting unrecognized hotkey {synapse.dendrite.hotkey}"
            )
            return True, "Unrecognized hotkey"

        logger.debug(f"Got request from {caller_hotkey}")

        caller_uid = self.metagraph.hotkeys.index(caller_hotkey)
        validator_neuron: bt.NeuronInfo = self.metagraph.neurons[caller_uid]

        if get_config().ignore_min_stake:
            message = f"""Ignoring min stake stake required: {VALIDATOR_MIN_STAKE} \
                for {caller_hotkey}, YOU SHOULD NOT SEE THIS when you are running a miner on mainnet"""
            logger.warning(message)
            return (
                False,
                f"Ignored minimum validator stake requirement of {VALIDATOR_MIN_STAKE}",
            )

        if is_miner(self.metagraph, caller_uid):
            return True, "Not a validator"

        if validator_neuron.stake.tao < float(VALIDATOR_MIN_STAKE):
            logger.warning(
                f"Blacklisting hotkey: {caller_hotkey} with insufficient stake, minimum stake required: {VALIDATOR_MIN_STAKE}, current stake: {validator_neuron.stake.tao}"
            )
            return True, "Insufficient validator stake"

        return False, "Valid request received from validator"

    async def priority_ranking(self, synapse: TaskSynapseObject) -> float:
        """
        The priority function determines the order in which requests are handled. Higher-priority
        requests are processed before others. Miners may receive messages from multiple entities at
        once. This function determines which request should be processed first.
        Higher values indicate that the request should be processed first.
        Lower values indicate that the request should be processed later.
        """
        current_timestamp = datetime.fromtimestamp(get_epoch_time())
        dt = current_timestamp - datetime.fromtimestamp(synapse.epoch_timestamp)
        priority = float(dt.total_seconds())
        logger.debug(f"Prioritizing {synapse.dendrite.hotkey} with value: {priority}")
        return priority

    def resync_metagraph(self):
        # Copies state of metagraph before syncing.
        previous_metagraph = copy.deepcopy(self.metagraph)

        # Sync the metagraph.
        self.metagraph.sync(subtensor=self.subtensor)

        # Check if the metagraph axon info has changed.
        if previous_metagraph.axons == self.metagraph.axons:
            return

        logger.info("Metagraph updated")

    async def log_miner_status(self):
        while not self._should_exit:
            logger.info(f"Miner running... block:{str(self.block)} time: {time.time()}")
            await asyncio.sleep(MINER_STATUS)
