import asyncio
import copy
import time
import traceback
from datetime import datetime
from typing import Dict, Tuple

import bittensor
from bittensor.core.metagraph import AsyncMetagraph
from bittensor.utils.btlogging import logging as logger

from commons.human_feedback.dojo import DojoAPI
from commons.objects import ObjectManager
from commons.utils import aget_effective_stake, aobject, get_epoch_time, serve_axon
from dojo import MINER_STATUS, VALIDATOR_MIN_STAKE
from dojo.protocol import (
    Heartbeat,
    ScoringResult,
    TaskResult,
    TaskResultRequest,
    TaskSynapseObject,
)
from dojo.utils.config import get_config


class Miner(aobject):
    _should_exit: bool = False
    root_metagraph: AsyncMetagraph
    subnet_metagraph: AsyncMetagraph

    async def __init__(self):
        self.config = ObjectManager.get_config()
        logger.info(self.config)

        logger.info("Setting up bittensor objects....")
        self.wallet = bittensor.wallet(config=self.config)
        logger.info(f"Wallet: {self.wallet}")
        # The axon handles request processing, allowing validators to send this miner requests.
        self.axon = bittensor.axon(wallet=self.wallet, port=self.config.axon.port)
        logger.info(f"Axon: {self.axon}")

        await self.init_metagraphs()

        # Each miner gets a unique identity (UID) in the network for differentiation.
        self.uid = self.subnet_metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
        logger.info(
            f"Running neuron on subnet: {self.config.netuid} with uid {self.uid}"
        )

        # Attach determiners which functions are called when servicing a request.

        # Note: The synapse parameter in blacklist functions is a different instance from the one in forward functions.
        # The blacklist synapse comes from the request headers and is used for initial validation,
        # while the forward synapse contains the full request body.

        logger.info("Attaching forward function to miner axon.")
        self.axon.attach(
            forward_fn=self.forward_task_request,
            blacklist_fn=self.blacklist_task_request,
            priority_fn=self.priority_ranking,
        ).attach(
            forward_fn=self.forward_score_result,
            blacklist_fn=self.blacklist_score_result_request,
        ).attach(
            forward_fn=self.ack_heartbeat,
            blacklist_fn=self.blacklist_heartbeat_request,
        )

        # Attach a handler for TaskResultRequest to return task results
        self.axon.attach(
            forward_fn=self.forward_task_result_request,
            blacklist_fn=self.blacklist_task_result_request,
        )

        # Instantiate runners
        self.should_exit: bool = False
        # log all incoming requests
        self.hotkey_to_request: Dict[str, TaskSynapseObject] = {}

    async def init_metagraphs(self):
        logger.info("Performing async init for miner")
        config = ObjectManager.get_config()
        async with bittensor.AsyncSubtensor(config=config) as subtensor:
            self.block = await subtensor.get_current_block()
            # The metagraph holds the state of the network, letting us know about other validators and miners.
            self.subnet_metagraph = await subtensor.metagraph(
                config.netuid,  # type: ignore
                block=self.block,
            )
            self.root_metagraph = await subtensor.metagraph(0, block=self.block)
            self.subtensor = subtensor
            # Check if the miner is registered on the Bittensor network before proceeding further.
            await self.check_registered()
            logger.info(f"Subtensor initialized, {self.subtensor}")
            logger.info(f"Root metagraph initialized, {self.root_metagraph}")
            logger.info(f"Subnet metagraph initialized, {self.subnet_metagraph}")

    async def run(self):
        """
        Initiates and manages the main loop for the miner on the Bittensor network. The main loop handles graceful shutdown on keyboard interrupts and logs unforeseen errors.

        This function performs the following primary tasks:
        1. Check for registration on the Bittensor network.
        2. Starts the miner's axon, making it active on the network.
        3. Periodically resynchronizes with the chain; updating the metagraph with the latest network state and setting weights.

        The miner continues its operations until `should_exit` is set to True or an external interruption occurs.
        During each epoch of its operation, the miner waits for new blocks on the Bittensor network, updates its
        knowledge of the network (metagraph), and sets its weights. This process ensures the miner remains active
        and up-to-date with the network's latest state.

        Note:
            - The function leverages the global configurations set during the initialization of the miner.
            - The miner's axon serves as its interface to the Bittensor network, handling incoming and outgoing requests.

        Raises:
            KeyboardInterrupt: If the miner is stopped by a manual interruption.
            Exception: For unforeseen errors during the miner's operation, which are logged for diagnosis.
        """

        # manually always register and always sync metagraph when application starts
        await self.resync_metagraph()
        await self.sync()

        # Serve passes the axon information to the network + netuid we are hosting on.
        # This will auto-update if the axon port of external ip have changed.
        logger.info(f"Serving miner axon {self.axon} with netuid: {self.config.netuid}")
        serve_success = await serve_axon(self.subtensor, self.axon, self.config)
        if serve_success:
            logger.success("Successfully served axon for miner!")
        else:
            logger.error("Failed to serve axon for miner, exiting.")
            exit()

        # Start  starts the miner's axon, making it active on the network.
        self.axon.start()

        logger.info(f"Miner starting at block: {str(self.block)}")

        # This loop maintains the miner's operations until intentionally stopped.
        try:
            while True:
                # Check if we should exit.
                if self.should_exit:
                    break

                # Sync metagraph and potentially set weights.
                await self.sync()
                await asyncio.sleep(12)

        # If someone intentionally stops the miner, it'll safely terminate operations.
        except KeyboardInterrupt:
            logger.success("Miner killed by keyboard interrupt.")
            self._cleanup()
            exit()

        # In case of unforeseen errors, the miner will log the error and continue operations.
        except Exception:
            logger.error(traceback.format_exc())
        finally:
            self._cleanup()

    def _cleanup(self):
        self.axon.stop()

    async def ack_heartbeat(self, synapse: Heartbeat) -> Heartbeat:
        caller_hotkey = (
            synapse.dendrite.hotkey if synapse.dendrite else "unknown hotkey"
        )
        logger.info(f"⬇️ Received heartbeat synapse from {caller_hotkey}")
        if not synapse:
            logger.error("Invalid synapse object")
            return synapse

        synapse.ack = True
        logger.info(f"⬆️ Respondng to heartbeat synapse: {synapse}")
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
        return await self._blacklist_function(
            synapse, "validator", "Valid task request received from validator"
        )

    async def blacklist_task_result_request(
        self, synapse: TaskResultRequest
    ) -> Tuple[bool, str]:
        return await self._blacklist_function(
            synapse, "task result", "Valid task result request from validator"
        )

    async def blacklist_heartbeat_request(self, synapse: Heartbeat) -> Tuple[bool, str]:
        return await self._blacklist_function(
            synapse, "heartbeat", "Valid heartbeat request from validator"
        )

    async def blacklist_score_result_request(
        self, synapse: ScoringResult
    ) -> Tuple[bool, str]:
        return await self._blacklist_function(
            synapse, "scoring result", "Valid scoring result request from validator"
        )

    def extract_synapse_info(self, synapse: bittensor.Synapse) -> str:
        caller_hotkey = synapse.dendrite.hotkey
        ip_addr = synapse.dendrite.ip or "Unknown IP"
        return f"Hotkey: {caller_hotkey}, IP: {ip_addr}"

    async def _blacklist_function(
        self, synapse, request_tag: str, valid_msg: str
    ) -> Tuple[bool, str]:
        """
        Common blacklist logic for any forward function to validate an incoming synapse.
        Note: Validate network-level security concerns using the header-based synapse, not the request body data

        Parameters:
            synapse: The incoming synapse object (Heartbeat, ScoringResult, etc.)
            request_tag: A tag used for logging (e.g., "heartbeat", "scoring result").
            valid_msg: The success message if the synapse is allowed.

        Returns:
            Tuple[bool, str]: (blacklisted: bool, message: str)
        """
        dendrite = synapse.dendrite
        ip_addr = getattr(dendrite, "ip", "Unknown IP")
        caller_hotkey = getattr(dendrite, "hotkey", None)

        logger.info(
            f"Incoming {request_tag} request from IP: {ip_addr} with hotkey: {caller_hotkey}"
        )

        if not caller_hotkey or caller_hotkey not in self.subnet_metagraph.hotkeys:
            logger.warning(f"Blacklisting unrecognized hotkey {caller_hotkey}")
            return True, "Unrecognized hotkey"

        logger.debug(f"Got {request_tag} request from {caller_hotkey}")

        if get_config().ignore_min_stake:
            message = (
                f"Ignoring min stake required: {VALIDATOR_MIN_STAKE} for {caller_hotkey}, "
                "YOU SHOULD NOT SEE THIS when you are running a miner on mainnet"
            )
            logger.warning(message)
            return (
                False,
                f"Ignored minimum validator stake requirement of {VALIDATOR_MIN_STAKE}",
            )

        effective_stake = await aget_effective_stake(
            caller_hotkey, self.root_metagraph, self.subnet_metagraph
        )
        if effective_stake < float(VALIDATOR_MIN_STAKE):
            message = f"Blacklisting hotkey: {caller_hotkey} with insufficient stake, minimum effective stake required: {VALIDATOR_MIN_STAKE}, current effective stake: {effective_stake}"
            logger.warning(message)
            return True, message

        return False, valid_msg

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

    async def resync_metagraph(self):
        # Copies state of metagraph before syncing.
        previous_metagraph = copy.deepcopy(self.subnet_metagraph)

        # Sync the metagraph.
        await self.subnet_metagraph.sync(subtensor=self.subtensor)
        await self.root_metagraph.sync(subtensor=self.subtensor)

        # Check if the metagraph axon info has changed.
        if previous_metagraph.axons == self.subnet_metagraph.axons:
            return

        logger.info("Metagraph updated")

    async def log_miner_status(self):
        while not self._should_exit:
            logger.info(f"Miner running... block:{str(self.block)} time: {time.time()}")
            await asyncio.sleep(MINER_STATUS)

    async def sync(self):
        """
        1. check if registered on subnet
        2. check if should sync metagraph
        3. check if should set weights
        """
        await self.check_registered()

        if self.should_sync_metagraph():
            await self.resync_metagraph()

    async def check_registered(self, subtensor: bittensor.AsyncSubtensor | None = None):
        # Checkfor registration.
        if subtensor is None:
            subtensor = self.subtensor

        if not await subtensor.is_hotkey_registered(
            netuid=self.config.netuid,
            hotkey_ss58=self.wallet.hotkey.ss58_address,
            block=self.block,
        ):
            logger.error(
                f"Wallet: {self.wallet} is not registered on netuid {self.config.netuid}."
                f" Please register the hotkey using `btcli s register` before trying again"
            )
            self._cleanup()
            exit(1)

    def should_sync_metagraph(self):
        """
        Check if enough epoch blocks have elapsed since the last checkpoint to sync.
        """
        # sync every 5 blocks
        return self.block % 5 == 0

    @property
    def block(self):
        return self._block

    @block.setter
    def block(self, value: int):
        self._block = value

    async def block_headers_callback(self, block: dict):
        logger.trace(f"Received block headers{block}")
        block_number = int(block.get("header", {}).get("number"))
        self.block = block_number
