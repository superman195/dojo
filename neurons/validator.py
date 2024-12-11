import asyncio
import copy
import gc
import math
import random
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from traceback import print_exception
from typing import AsyncGenerator, Dict, List

import aiohttp
import bittensor as bt
import numpy as np
import torch
import wandb
from bittensor.btlogging import logging as logger
from bittensor.utils.weight_utils import process_weights_for_netuid
from fastapi.encoders import jsonable_encoder
from tenacity import RetryError
from torch.nn import functional as F
from websocket import create_connection

import dojo
from commons.dataset.synthetic import SyntheticAPI
from commons.exceptions import (
    EmptyScores,
    InvalidMinerResponse,
    NoNewExpiredTasksYet,
    SetWeightsFailed,
)
from commons.obfuscation.obfuscation_utils import obfuscate_html_and_js
from commons.objects import ObjectManager
from commons.orm import ORM
from commons.score_storage import ScoreStorage
from commons.scoring import Scoring
from commons.utils import (
    _terminal_plot,
    datetime_as_utc,
    get_epoch_time,
    get_new_uuid,
    init_wandb,
    initialise,
    set_expire_time,
    ttl_get_block,
)
from dojo import __spec_version__
from dojo.protocol import (
    CompletionResponses,
    CriteriaTypeEnum,
    DendriteQueryResponse,
    FeedbackRequest,
    Heartbeat,
    MultiScoreCriteria,
    ScoringResult,
    TaskResult,
    TaskResultRequest,
    TaskType,
)
from dojo.utils.config import get_config
from dojo.utils.uids import MinerUidSelector, extract_miner_uids, is_miner


class Validator:
    _should_exit: bool = False
    _scores_alock = asyncio.Lock()
    _uids_alock = asyncio.Lock()
    _request_alock = asyncio.Lock()
    _threshold = 0.1
    _active_miner_uids: set[int] = set()

    subtensor: bt.subtensor
    wallet: bt.wallet
    metagraph: bt.metagraph
    spec_version: int = __spec_version__

    def __init__(self):
        self.MAX_BLOCK_CHECK_ATTEMPTS = 3
        self._last_block = None
        self._block_check_attempts = 0
        self._connection_lock = asyncio.Lock()

        self.loop = asyncio.get_event_loop()
        # TODO @dev WIP from BaseNeuron
        self.config = ObjectManager.get_config()

        # If a gpu is required, set the device to cuda:N (e.g. cuda:0)
        self.device = self.config.neuron.device

        # Log the configuration for reference.
        logger.info(self.config)

        self.wallet, self.subtensor, self.metagraph, self.axon = initialise(self.config)

        # Each miner gets a unique identity (UID) in the network for differentiation.
        self.uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
        logger.info(
            f"Running neuron on subnet: {self.config.netuid} with uid {self.uid}"
        )
        self.step = 0

        # Dendrite lets us send messages to other nodes (axons) in the network.
        self.dendrite = bt.dendrite(wallet=self.wallet)
        logger.info(f"Dendrite: {self.dendrite}")
        # Set up initial scoring weights for validation
        self.scores: torch.Tensor = torch.zeros(
            len(self.metagraph.hotkeys), dtype=torch.float32
        )
        self.check_registered()

        init_wandb(config=self.config, my_uid=self.uid, wallet=self.wallet)

        # Run score migration before loading state
        migration_success = self.loop.run_until_complete(ScoreStorage.migrate_from_db())
        if not migration_success:
            logger.error(
                "Score migration failed - cannot continue without valid scores"
            )
            raise RuntimeError("Score migration failed - validator cannot start")

        self.executor = ThreadPoolExecutor(max_workers=2)
        self.load_state()

    async def send_scores(self, synapse: ScoringResult, hotkeys: List[str]):
        """Send consensus score back to miners who participated in the request."""
        axons = [axon for axon in self.metagraph.axons if axon.hotkey in hotkeys]
        if not axons:
            logger.warning("No axons to send consensus to... skipping")
        else:
            logger.debug(
                f"Sending back consensus to miners for request id: {synapse.request_id}"
            )

        await self.dendrite.forward(
            axons=axons, synapse=synapse, deserialize=False, timeout=30
        )

    def obfuscate_model_names(
        self, completion_responses: list[CompletionResponses]
    ) -> dict[str, str]:
        """Obfuscate model names for both external requests and synthetic requests to prevent miners from knowing the true model names."""
        obfuscated_model_to_model: dict[str, str] = {}
        for completion in completion_responses:
            if completion.completion_id is None:
                raise ValueError("completion_id is None")
            completion.model = completion.completion_id
            obfuscated_model_to_model[completion.completion_id] = (
                completion.completion_id
            )
        return obfuscated_model_to_model

    async def send_heartbeats(self):
        """Perform a health check periodically to ensure miners are reachable"""
        while True:
            await asyncio.sleep(dojo.VALIDATOR_HEARTBEAT)
            try:
                all_miner_uids = extract_miner_uids(metagraph=self.metagraph)
                logger.debug(f"Sending heartbeats to {len(all_miner_uids)} miners")
                axons: list[bt.AxonInfo] = [
                    self.metagraph.axons[uid]
                    for uid in all_miner_uids
                    if self.metagraph.axons[uid].hotkey.casefold()
                    != self.wallet.hotkey.ss58_address.casefold()
                ]

                responses: List[Heartbeat] = await self.dendrite.forward(  # type: ignore
                    axons=axons, synapse=Heartbeat(), deserialize=False, timeout=30
                )
                active_hotkeys = [r.axon.hotkey for r in responses if r.ack and r.axon]
                active_uids = [
                    uid
                    for uid, axon in enumerate(self.metagraph.axons)
                    if axon.hotkey in active_hotkeys
                ]
                async with self._uids_alock:
                    self._active_miner_uids = set(active_uids)
                logger.debug(
                    f"⬇️ Heartbeats acknowledged by active miners: {sorted(active_uids)}"
                )
            except Exception as e:
                logger.error(
                    f"Error in sending heartbeats: {e}, traceback: {traceback.format_exc()}"
                )
                pass

    @staticmethod
    async def _send_shuffled_requests(
        dendrite: bt.dendrite, axons: List[bt.AxonInfo], synapse: FeedbackRequest
    ) -> list[FeedbackRequest]:
        """Based on the initial synapse, send shuffled ordering of responses so that miners cannot guess ordering of ground truth"""
        all_responses = []
        batch_size = 10

        for i in range(0, len(axons), batch_size):
            batch_axons = axons[i : i + batch_size]
            tasks = []

            for axon in batch_axons:
                # shuffle synapse Responses
                shuffled_completions = random.sample(
                    synapse.completion_responses,
                    k=len(synapse.completion_responses),
                )

                # Apply obfuscation to each completion's files
                # TODO re-nable obfuscation
                # await Validator._obfuscate_completion_files(shuffled_completions)

                criteria_types = []
                # ensure criteria options same order as completion_responses
                for criteria in synapse.criteria_types:
                    if not isinstance(criteria, MultiScoreCriteria):
                        logger.trace(f"Skipping non multi score criteria: {criteria}")
                        continue
                    options = [completion.model for completion in shuffled_completions]
                    criteria = MultiScoreCriteria(
                        options=options,
                        min=criteria.min,
                        max=criteria.max,
                    )
                    criteria_types.append(criteria)

                shuffled_synapse = FeedbackRequest(
                    epoch_timestamp=synapse.epoch_timestamp,
                    request_id=synapse.request_id,
                    prompt=synapse.prompt,
                    completion_responses=shuffled_completions,
                    task_type=synapse.task_type,
                    criteria_types=criteria_types,
                    expire_at=synapse.expire_at,
                )

                tasks.append(
                    dendrite.forward(
                        axons=[axon],
                        synapse=shuffled_synapse,
                        deserialize=False,
                        timeout=30,
                    )
                )

            # Gather results for this batch and flatten the list
            batch_responses = await asyncio.gather(*tasks)
            flat_batch_responses = [
                response for sublist in batch_responses for response in sublist
            ]
            all_responses.extend(flat_batch_responses)

            logger.info(
                f"Processed batch {i//batch_size + 1} of {(len(axons)-1)//batch_size + 1}"
            )

        return all_responses

    @staticmethod
    async def _obfuscate_completion_files(
        completion_responses: List[CompletionResponses],
    ):
        """Obfuscate HTML files in each completion response."""
        for completion in completion_responses:
            if hasattr(completion.completion, "files"):
                for file in completion.completion.files:
                    if file.filename.lower().endswith(".html"):
                        try:
                            original_size = len(file.content)
                            logger.debug(
                                f"Original size of {file.filename}: {original_size} bytes"
                            )
                            file.content = await obfuscate_html_and_js(file.content)
                            obfuscated_size = len(file.content)
                            logger.debug(
                                f"Obfuscated size of {file.filename}: {obfuscated_size} bytes"
                            )
                        except Exception as e:
                            logger.error(f"Error obfuscating {file.filename}: {e}")

    async def get_miner_uids(self, is_external_request: bool, request_id: str):
        async with self._uids_alock:
            if is_external_request:
                sel_miner_uids = [
                    uid
                    for uid in self._active_miner_uids
                    if self.scores[uid] > self._threshold
                ]
                logger.debug(
                    f"🌍 External user request, number of miners with scores above threshold: {len(sel_miner_uids)}"
                )
            else:
                sel_miner_uids = MinerUidSelector(
                    nodes=list(self._active_miner_uids),
                ).get_target_uids(key=request_id, k=get_config().neuron.sample_size)
        return sel_miner_uids

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
        sel_miner_uids = await self.get_miner_uids(external_user, request_id)
        # TODO @dev REMOVE AFTER TESTING
        # TODO @dev REMOVE AFTER TESTING
        # TODO @dev REMOVE AFTER TESTING
        # TODO @dev REMOVE AFTER TESTING
        # TODO @dev REMOVE AFTER TESTING
        # TODO @dev REMOVE AFTER TESTING
        # TODO @dev REMOVE AFTER TESTING
        # TODO @dev REMOVE AFTER TESTING
        # TODO @dev REMOVE AFTER TESTING
        # TODO @dev REMOVE AFTER TESTING
        # TODO @dev REMOVE AFTER TESTING
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
                logger.error(f"Unexpected error during synthetic data generation: {e}")
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

    async def run(self):
        logger.info(f"Validator starting at block: {str(self.block)}")

        # This loop maintains the validator's operations until intentionally stopped.
        try:
            while True:
                try:
                    async with self._request_alock:
                        await self.send_request()

                    # # Check if we should exit.
                    if self._should_exit:
                        logger.debug("Validator should stop...")
                        break

                    # Clear the dendrite synapse history to avoid memory leak.
                    self.dendrite.synapse_history = []

                    # Sync metagraph and potentially set weights.
                    await self.sync()

                    self.step += 1
                except Exception as e:
                    traceback.print_exc()
                    logger.error(f"Error during validator run: {e}")
                    pass
                await asyncio.sleep(dojo.VALIDATOR_RUN)

        # If someone intentionally stops the validator, it'll safely terminate operations.
        except KeyboardInterrupt:
            self.axon.stop()
            logger.success("Validator killed by keyboard interrupt.")
            exit()

        # In case of unforeseen errors, the validator will log the error and continue operations.
        except Exception as err:
            logger.error("Error during validation", str(err))
            logger.debug(print_exception(type(err), err, err.__traceback__))

    async def set_weights(self):
        """
        Sets the validator weights to the metagraph hotkeys based on the scores it has received from the miners.
        The weights determine the trust and incentive level the validator assigns to miner nodes on the network.
        """

        # ensure self.scores not being written to by other coroutines
        # Check if self.scores contains any NaN values and log a warning if it does.
        if torch.isnan(self.scores).any():
            logger.warning(
                "Scores contain NaN values. This may be due to a lack of responses from miners, or a bug in your reward functions."
            )

        logger.info("Attempting to set weights")

        # ensure sum = 1
        normalized_weights = F.normalize(self.scores.cpu(), p=1, dim=0)

        safe_normalized_weights = normalized_weights
        if isinstance(normalized_weights, np.ndarray):
            safe_normalized_weights = torch.from_numpy(normalized_weights).to("cpu")
        elif isinstance(normalized_weights, torch.Tensor):
            pass

        # we don't read uids from metagraph because polling metagraph happens
        # faster than calling set_weights and self.scores is already
        # based on uids, adjusted based on metagraph during `resync_metagraph`
        uids = torch.tensor(list(range(len(safe_normalized_weights))))

        (
            final_uids,
            final_weights,
        ) = process_weights_for_netuid(  # type: ignore
            uids=uids.numpy(),
            weights=safe_normalized_weights.numpy(),
            netuid=self.config.netuid,  # type: ignore
            subtensor=self.subtensor,
            metagraph=self.metagraph,
        )

        if isinstance(final_weights, np.ndarray):
            final_weights = torch.from_numpy(final_weights).to("cpu")
        if isinstance(final_uids, np.ndarray):
            final_uids = torch.from_numpy(final_uids).to("cpu")

        logger.debug(f"weights:\n{safe_normalized_weights}")
        logger.debug(f"uids:\n{uids}")

        _terminal_plot(
            f"pre-processed weights, block: {self.block}",
            safe_normalized_weights.numpy(),
        )

        logger.debug(f"final weights:\n{final_weights}")
        logger.debug(f"final uids:\n{final_uids}")

        _terminal_plot(
            f"final weights, block: {self.block}",
            final_weights.numpy(),
        )

        # dependent on underlying `set_weights` call
        try:
            result, message = await asyncio.wait_for(
                self._set_weights(final_uids, final_weights), timeout=90
            )
            if not result:
                logger.error(f"Failed to set weights: {message}")
                return

            logger.success(f"Set weights successfully: {message}")
        except asyncio.TimeoutError:
            logger.error("Setting weights timed out after 90 seconds")
            return

        return

    async def _set_weights(self, uids: torch.Tensor, weights: torch.Tensor):
        """Wrapper function to set weights so we can ensure set weights happens
        within a timeout.

        Args:
            uids (torch.Tensor): uids to set weights for
            weights (torch.Tensor): weights to set

        Returns:
            tuple[bool, str]: Returns the result of _set_weights function
        """

        max_attempts = 5
        attempt = 0
        result = False
        while attempt < max_attempts and not result:
            try:
                logger.debug(
                    f"Set weights attempt {attempt+1}/{max_attempts} at block: {self.block},time: {time.time()}"
                )

                # Disable this for now to check validator hanging issue
                # try:
                #     await asyncio.wait_for(
                #         self._ensure_subtensor_ws_connected(), timeout=10
                #     )
                # except asyncio.TimeoutError:
                #     pass

                result, message = self.subtensor.set_weights(
                    wallet=self.wallet,
                    netuid=self.config.netuid,  # type: ignore
                    uids=uids.tolist(),
                    weights=weights.tolist(),
                    wait_for_finalization=False,
                    wait_for_inclusion=False,
                    version_key=self.spec_version,
                    max_retries=1,
                )
                if result:
                    logger.success(f"Set weights successfully: {message}")
                    return result, message

                raise SetWeightsFailed(f"Failed to set weights with message:{message}")

            except Exception:
                logger.warning(
                    f"Failed to set weights with attempt {attempt+1}/{max_attempts} due to: {message}"
                )

                if attempt == max_attempts:
                    logger.error("Max attempts reached. Could not set weights.")
                    return False, "Max attempts reached"

                await asyncio.sleep(12)
            finally:
                attempt += 1

        return False, "Max attempts reached"

    async def resync_metagraph(self):
        """Resyncs the metagraph and updates the hotkeys and moving averages based on the new metagraph."""
        # Copies state of metagraph before syncing.
        previous_metagraph = copy.deepcopy(self.metagraph)

        # Sync the metagraph.
        self.metagraph.sync(subtensor=self.subtensor)

        # Check if the metagraph axon info has changed.
        if previous_metagraph.axons == self.metagraph.axons:
            return

        logger.info(
            "Metagraph updated, re-syncing hotkeys, dendrite pool and moving averages"
        )
        # Zero out all hotkeys that have been replaced.
        for uid, hotkey in enumerate(previous_metagraph.hotkeys):
            if hotkey != self.metagraph.hotkeys[uid]:
                self.scores[uid] = 0  # hotkey has been replaced

        # Check to see if the metagraph has changed size.
        # If so, we need to add new hotkeys and moving averages.
        if len(previous_metagraph.hotkeys) < len(self.metagraph.hotkeys):
            # Update the size of the moving average scores.
            new_moving_average = torch.zeros(len(self.metagraph.hotkeys))
            min_len = min(len(previous_metagraph.hotkeys), len(self.scores))
            new_moving_average[:min_len] = self.scores[:min_len]
            async with self._scores_alock:
                self.scores = torch.clamp(new_moving_average, min=0.0)

    async def update_scores(self, hotkey_to_scores: dict[str, float]):
        """Performs exponential moving average on the scores based on the rewards received from the miners,
        after setting the self.scores variable here, `set_weights` will be called to set the weights on chain.
        """
        if not hotkey_to_scores:
            logger.warning("hotkey_to_scores is empty, skipping score update")
            return

        nan_value_indices = np.isnan(list(hotkey_to_scores.values()))
        if nan_value_indices.any():
            logger.warning(f"NaN values detected in rewards: {hotkey_to_scores}")
            return

        # Compute forward pass rewards, assumes uids are mutually exclusive.
        # scores dimensions might have been updated after resyncing... len(uids) != len(self.scores)
        rewards = torch.zeros((len(self.metagraph.hotkeys),))
        existing_scores = torch.zeros((len(self.metagraph.hotkeys),))
        for index, (key, value) in enumerate(hotkey_to_scores.items()):
            # handle nan values
            if nan_value_indices[index]:
                rewards[key] = 0.0  # type: ignore
            # search metagraph for hotkey and grab uid
            try:
                uid = self.metagraph.hotkeys.index(key)
            except ValueError:
                logger.warning("Old hotkey found from previous metagraph")
                continue

            logger.debug(f"Score for hotkey {key} is {value}")
            rewards[uid] = value

            # self.scores is a tensor already based on uids
            # use this logic to ensure
            # 1. rewards and existing_scores are the same length
            # 2. if hotkey is deregistered, the new participant will not benefit from existing scores
            if uid < len(self.scores):
                existing_scores[uid] = self.scores[uid]

        logger.debug(f"Rewards: {rewards}")
        # Update scores with rewards produced by this step.
        # shape: [ metagraph.n ]
        alpha: float = self.config.neuron.moving_average_alpha
        # don't acquire lock here because we're already acquiring it in the CALLER
        async with self._scores_alock:
            _terminal_plot(
                f"scores before update, block: {self.block}", self.scores.numpy()
            )
            assert (
                existing_scores.shape == rewards.shape
            ), "Scores and rewards must be the same length when calculating moving average"

            self.scores = alpha * rewards + (1 - alpha) * existing_scores
            self.scores = torch.clamp(self.scores, min=0.0)
            _terminal_plot(
                f"scores after update, block: {self.block}", self.scores.numpy()
            )
        logger.debug(f"Updated scores: {self.scores}")

    async def save_state(
        self,
    ):
        """Saves the state of the validator to the database."""
        if self.step == 0:
            return

        try:
            if np.count_nonzero(self.scores) == 0:
                logger.warning("Scores are all zeros, but saving anyway!")
                # raise EmptyScores("Skipping save as scores are all empty")

            # await ORM.create_or_update_validator_score(self.scores)
            await ScoreStorage.save(self.scores)
            logger.success(f"📦 Saved validator state with scores: {self.scores}")
        except EmptyScores as e:
            logger.debug(f"No need to to save validator state: {e}")
        except Exception as e:
            logger.error(f"Failed to save validator state: {e}")

    async def _load_state(self):
        try:
            scores = await ScoreStorage.load()

            if scores is None:
                num_processed_tasks = await ORM.get_num_processed_tasks()
                if num_processed_tasks > 0:
                    logger.error(
                        "Score record not found, but you have processed tasks."
                    )
                else:
                    logger.warning(
                        "Score record not found, and no tasks processed, this is okay if you're running for the first time."
                    )
                return None

            logger.success(f"Loaded validator state: {scores=}")
            async with self._scores_alock:
                # if metagraph has more hotkeys than scores, adjust length
                if len(scores) < len(self.metagraph.hotkeys):
                    logger.warning(
                        "Scores state is less than current metagraph hotkeys length, adjusting length. This should only happen when subnet is not at max UIDs yet."
                    )
                    # length adjusted scores
                    adjusted_scores = torch.zeros(len(self.metagraph.hotkeys))
                    adjusted_scores[: len(scores)] = scores
                    logger.info(
                        f"Load state: adjusted scores shape from {scores.shape} to {adjusted_scores.shape}"
                    )
                    self.scores = torch.clamp(adjusted_scores, 0.0)
                else:
                    self.scores = torch.clamp(scores, 0.0)

                _terminal_plot(
                    f"scores on load, block: {self.block}", self.scores.numpy()
                )

        except Exception as e:
            logger.error(
                f"Unexpected error occurred while loading validator state: {e}"
            )
            return None

    def load_state(self):
        """Loads the state of the validator from a file."""
        try:
            self.loop.run_until_complete(self._load_state())
        except Exception as e:
            logger.error(f"Failed to load validator state: {e}")
            pass

    async def log_validator_status(self):
        while not self._should_exit:
            logger.info(
                f"Validator running... block:{str(self.block)} time: {time.time()}"
            )
            await asyncio.sleep(dojo.VALIDATOR_STATUS)

    async def _get_task_results_from_miner(
        self, miner_hotkey: str, task_id: str
    ) -> list[TaskResult]:
        """Fetch task results from the miner's Axon using Dendrite."""
        try:
            if not self.dendrite:
                raise ValueError("Dendrite not initialized")

            # Prepare the synapse (data request) that will be sent via Dendrite
            task_synapse = TaskResultRequest(task_id=task_id)

            # Use Dendrite to communicate with the Axon
            miner_axon = self.metagraph.axons[
                self.metagraph.hotkeys.index(miner_hotkey)
            ]
            if not miner_axon:
                raise ValueError(f"Miner Axon not found for hotkey: {miner_hotkey}")

            # Send the request via Dendrite and get the response
            response: list[TaskResultRequest] = await self.dendrite.forward(  # type: ignore
                axons=[miner_axon],
                synapse=task_synapse,
                deserialize=False,
                timeout=30,
            )

            if response and response[0]:
                return response[0].task_results
            else:
                logger.debug(
                    f"No task results found from miner {miner_hotkey} for task {task_id}"
                )
                return []

        except Exception as e:
            logger.error(f"Error fetching task result from miner {miner_hotkey}: {e}")
            return []

    @staticmethod
    def _calculate_averages(
        task_results: list[TaskResult], obfuscated_to_real_model_id
    ):
        model_id_to_avg_rank = defaultdict(float)
        model_id_to_avg_score = defaultdict(float)
        num_ranks_by_workers, num_scores_by_workers = 0, 0

        for result in task_results:
            for result_data in result.result_data:
                type = result_data.type
                value = result_data.value
                if type == CriteriaTypeEnum.RANKING_CRITERIA:
                    for model_id, rank in value.items():
                        real_model_id = obfuscated_to_real_model_id.get(
                            model_id, model_id
                        )
                        model_id_to_avg_rank[real_model_id] += rank
                    num_ranks_by_workers += 1
                elif type == CriteriaTypeEnum.MULTI_SCORE:
                    for model_id, score in value.items():
                        real_model_id = obfuscated_to_real_model_id.get(
                            model_id, model_id
                        )
                        model_id_to_avg_score[real_model_id] += score
                    num_scores_by_workers += 1

        # Average the ranks and scores
        for model_id in model_id_to_avg_rank:
            model_id_to_avg_rank[model_id] /= num_ranks_by_workers
        for model_id in model_id_to_avg_score:
            model_id_to_avg_score[model_id] /= num_scores_by_workers

        return model_id_to_avg_rank, model_id_to_avg_score

    def should_sync_metagraph(self):
        """
        Check if enough epoch blocks have elapsed since the last checkpoint to sync.
        """
        return (
            self.block - self.metagraph.last_update[self.uid]
        ) > self.config.neuron.epoch_length

    def should_set_weights(self) -> bool:
        # Don't set weights on initialization.
        if self.step == 0:
            return False

        # Define appropriate logic for when set weights.
        return (
            self.block - self.metagraph.last_update[self.uid]
        ) > self.config.neuron.epoch_length

    def check_registered(self):
        # --- Check for registration.
        if not self.subtensor.is_hotkey_registered(
            netuid=self.config.netuid,
            hotkey_ss58=self.wallet.hotkey.ss58_address,
        ):
            logger.error(
                f"Wallet: {self.wallet} is not registered on netuid {self.config.netuid}."
                f" Please register the hotkey using `btcli s register` before trying again"
            )
            exit()

    async def sync(self):
        has_connection = await self._ensure_subtensor_connection()
        if not has_connection:
            logger.warning("Subtensor connection failed - continuing with partial sync")

        self.check_registered()

        if self.should_sync_metagraph():
            await self.resync_metagraph()

        if self.should_set_weights():
            await self.set_weights()

        await self.save_state()

    @property
    def block(self):
        try:
            if not self.loop.run_until_complete(self._ensure_subtensor_connection()):
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

    async def _ensure_subtensor_ws_connected(
        self, max_attempts: int = 5, sleep: int = 3
    ):
        if not self.subtensor.substrate.websocket:
            logger.warning("Substrate websocket not initialized, skipping connection")
            return False

        attempts = 0
        while (
            not self.subtensor.substrate.websocket.connected and attempts < max_attempts
        ):
            try:
                self.subtensor.substrate.websocket = create_connection(
                    url=self.subtensor.substrate.url,  # type: ignore
                    timeout=10,
                    **self.subtensor.substrate.ws_options,
                )
                if self.subtensor.substrate.websocket.connected:
                    logger.debug(
                        f"Successfully connected to substrate websocket on attempt {attempts}"
                    )
                    return True
                else:
                    await asyncio.sleep(sleep)
            finally:
                attempts += 1

        if not self.subtensor.substrate.websocket.connected:
            logger.error(
                "Failed to connect to substrate websocket after maximum attempts"
            )
            return False

        logger.debug("Substrate websocket is already connected")
        return True

    # ---------------------------------------------------------------------------- #
    #                         VALIDATOR CORE FUNCTIONS                             #
    # ---------------------------------------------------------------------------- #
    async def update_score_and_send_feedback(self):
        while True:
            await asyncio.sleep(dojo.VALIDATOR_UPDATE_SCORE)
            # for each hotkey, a list of scores from all tasks being scored
            hotkey_to_all_scores = defaultdict(list)
            try:
                validator_hotkeys: List[str] = self._get_validator_hotkeys()

                # Grab tasks that were expired TASK_DEADLINE duration ago
                expire_from = datetime_as_utc(datetime.now(timezone.utc)) - timedelta(
                    hours=2
                )
                expire_to = datetime_as_utc(datetime.now(timezone.utc))
                logger.debug(
                    f"Updating with expire_from: {expire_from} and expire_to: {expire_to}"
                )

                # Get latest task completions before scoring
                await self.update_task_completions(
                    validator_hotkeys=validator_hotkeys,
                    expire_from=expire_from,
                    expire_to=expire_to,
                )

                logger.info("📝 performing scoring ...")
                processed_request_ids = []

                batch_size = 10
                async for task_batch in self._get_task_batches(
                    validator_hotkeys, batch_size, expire_from, expire_to
                ):
                    if not task_batch:
                        continue

                    for task in task_batch:
                        processed_id, hotkey_to_score = await self._score_task(task)
                        if processed_id:
                            processed_request_ids.append(processed_id)
                        for hotkey, score in hotkey_to_score.items():
                            hotkey_to_all_scores[hotkey].append(score)

                if processed_request_ids:
                    await ORM.mark_tasks_processed_by_request_ids(processed_request_ids)

                logger.success(
                    f"📝 All tasks processed, total tasks: {len(processed_request_ids)}"
                )

                # average scores across all tasks being scored by this trigger to update_scores
                # so miners moving average decay is lower and we incentivise quality > quantity
                final_hotkey_to_score = {
                    hotkey: sum(scores) / len(scores)
                    for hotkey, scores in hotkey_to_all_scores.items()
                    if scores
                }
                logger.debug(
                    f"📝 Got hotkey to score across all tasks between expire_at from:{expire_from} and expire_at to:{expire_to}: {final_hotkey_to_score}"
                )
                await self.update_scores(hotkey_to_scores=final_hotkey_to_score)

            except Exception:
                traceback.print_exc()
                pass
            finally:
                gc.collect()

    async def update_task_completions(
        self, validator_hotkeys: List[str], expire_from: datetime, expire_to: datetime
    ) -> None:
        try:
            logger.info("Updating Dojo task completions...")
            batch_size: int = 10

            all_miner_responses = []
            all_request_ids = []

            async for task_batch in self._get_task_batches(
                validator_hotkeys, batch_size, expire_from, expire_to
            ):
                if not task_batch:
                    continue

                for task in task_batch:
                    request_id = task.request.request_id
                    miner_responses = await self._update_task(task)
                    all_miner_responses.extend(miner_responses)
                    all_request_ids.append(request_id)

                    if len(all_miner_responses) >= batch_size:
                        await self._update_miner_completions_batch(
                            all_request_ids, all_miner_responses
                        )
                        all_miner_responses = []
                        all_request_ids = []

            # Process any remaining responses
            if all_miner_responses:
                await self._update_miner_completions_batch(
                    all_request_ids, all_miner_responses
                )

        except NoNewExpiredTasksYet as e:
            logger.info(f"No new expired tasks yet: {e}")
        except Exception as e:
            logger.error(f"Error during Dojo task monitoring: {str(e)}")
            logger.debug(f"Traceback: {traceback.format_exc()}")

    # ---------------------------------------------------------------------------- #
    #                         VALIDATOR HELPER FUNCTIONS                           #
    # ---------------------------------------------------------------------------- #
    def _get_validator_hotkeys(self) -> List[str]:
        """Get the hotkeys of the validators in the metagraph.

        Returns a list of validator hotkeys.
        """
        validator_hotkeys: List[str] = [
            hotkey
            for uid, hotkey in enumerate(self.metagraph.hotkeys)
            if not is_miner(self.metagraph, uid)
        ]
        if get_config().ignore_min_stake:
            validator_hotkeys.append(self.wallet.hotkey.ss58_address)
        return validator_hotkeys

    async def _get_task_batches(
        self,
        validator_hotkeys: list[str],
        batch_size: int,
        expire_from: datetime,
        expire_to: datetime,
    ) -> AsyncGenerator[List[DendriteQueryResponse], None]:
        """Get task in batches from the database"""
        async for task_batch, has_more_batches in ORM.get_expired_tasks(
            validator_hotkeys=validator_hotkeys,
            batch_size=batch_size,
            expire_from=expire_from,
            expire_to=expire_to,
        ):
            if not has_more_batches:
                logger.success(
                    "No more unexpired tasks found for processing, exiting task monitoring."
                )
                gc.collect()
                break
            yield task_batch

    async def _update_task(self, task: DendriteQueryResponse) -> List[FeedbackRequest]:
        """
        Returns a list of updated miner responses
        """
        request_id: str = task.request.request_id
        obfuscated_to_real_model_id: Dict[str, str] = await ORM.get_real_model_ids(
            request_id
        )

        updated_miner_responses: List[FeedbackRequest] = []

        batch_size = 30
        num_batches = math.ceil(len(task.miner_responses) / batch_size)
        for i in range(0, len(task.miner_responses), batch_size):
            safe_lim = min(i + batch_size, len(task.miner_responses))
            batch = task.miner_responses[i:safe_lim]

            logger.debug(f"Processing batch {i//batch_size + 1} of {num_batches}")

            tasks = [
                self._update_miner_response(miner_response, obfuscated_to_real_model_id)
                for miner_response in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if result is None:
                    pass
                elif isinstance(result, FeedbackRequest):
                    updated_miner_responses.append(result)
                elif isinstance(result, InvalidMinerResponse):
                    logger.error(f"Invalid miner response: {result}")
                elif isinstance(result, Exception):
                    logger.error(f"Unexpected error: {result}")

        logger.success(
            f"Completed processing {len(updated_miner_responses)} miner responses in {num_batches} batches"
        )
        return updated_miner_responses

    async def _update_miner_response(
        self,
        miner_response: FeedbackRequest,
        obfuscated_to_real_model_id: Dict[str, str],
    ) -> FeedbackRequest | None:
        """
        Gets task results from a miner. Calculates the average across all task results.

        If no task results, return None. Else append it to miner completion response.
        """
        if (
            not miner_response.axon
            or not miner_response.axon.hotkey
            or not miner_response.dojo_task_id
        ):
            raise InvalidMinerResponse(
                f"""Missing hotkey, task_id, or axon:
                axon: {miner_response.axon}
                hotkey: {miner_response.axon.hotkey}
                task_id: {miner_response.dojo_task_id}"""
            )

        miner_hotkey = miner_response.axon.hotkey
        task_id = miner_response.dojo_task_id
        task_results = await self._get_task_results_from_miner(miner_hotkey, task_id)

        if not task_results:
            return None

        model_id_to_avg_rank, model_id_to_avg_score = self._calculate_averages(
            task_results, obfuscated_to_real_model_id
        )

        for completion in miner_response.completion_responses:
            model_id = completion.model
            if model_id in model_id_to_avg_rank:
                completion.rank_id = int(model_id_to_avg_rank[model_id])
            if model_id in model_id_to_avg_score:
                completion.score = model_id_to_avg_score[model_id]

        return miner_response

    async def _update_miner_completions_batch(
        self,
        request_ids: List[str],
        miner_responses: List[FeedbackRequest],
        max_retries: int = 20,
    ) -> None:
        """
        Update the miner completions in the database in batches

        If there are any failed updates, retry using failed_indices.
        """
        remaining_responses = miner_responses
        remaining_request_ids = request_ids

        for attempt in range(max_retries):
            try:
                (
                    success,
                    failed_indices,
                ) = await ORM.update_miner_completions_by_request_id(
                    remaining_responses
                )
                if success:
                    logger.success(
                        f"Successfully updated {len(remaining_responses)} miner completions for {len(remaining_request_ids)} requests"
                    )
                    return
                else:
                    if attempt == max_retries - 1:
                        logger.error(
                            f"Failed to update {len(failed_indices)} miner completions after {max_retries} attempts"
                        )
                    else:
                        logger.warning(
                            f"Retrying {len(failed_indices)} failed updates, attempt {attempt+2}/{max_retries}"
                        )
                        remaining_responses = [
                            remaining_responses[i] for i in failed_indices
                        ]
                        remaining_request_ids = [
                            remaining_request_ids[i] for i in failed_indices
                        ]
                        await asyncio.sleep(2**attempt)

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(
                        f"Error updating miner completions batch after {max_retries} attempts: {e}"
                    )
                else:
                    logger.warning(f"Error during attempt {attempt+1}, retrying: {e}")
                    await asyncio.sleep(2**attempt)

    async def _score_task(self, task: DendriteQueryResponse) -> tuple[str, dict]:
        """Process a task and calculate the scores for the miner responses"""
        if not task.miner_responses:
            logger.warning("📝 No miner responses, skipping task")
            return task.request.request_id, {}

        criteria_to_miner_score, hotkey_to_score = {}, {}
        try:
            criteria_to_miner_score, hotkey_to_score = Scoring.calculate_score(
                criteria_types=task.request.criteria_types,
                request=task.request,
                miner_responses=task.miner_responses,
            )
        except Exception as e:
            logger.error(
                f"📝 Error occurred while calculating scores: {e}. Request ID: {task.request.request_id}"
            )
            return task.request.request_id, {}

        logger.debug(f"📝 Got hotkey to score: {hotkey_to_score}")
        logger.debug(
            f"📝 Received {len(task.miner_responses)} responses from miners. "
            f"Processed {len(hotkey_to_score.keys())} responses for scoring."
        )

        if not hotkey_to_score:
            logger.info("📝 Did not manage to generate a dict of hotkey to score")
            return task.request.request_id, {}

        await self.send_scores(
            synapse=ScoringResult(
                request_id=task.request.request_id,
                hotkey_to_scores=hotkey_to_score,
            ),
            hotkeys=list(hotkey_to_score.keys()),
        )

        asyncio.create_task(
            self._log_wandb(task, criteria_to_miner_score, hotkey_to_score)
        )

        return task.request.request_id, hotkey_to_score

    async def _log_wandb(
        self,
        task: DendriteQueryResponse,
        criteria_to_miner_score: dict,
        hotkey_to_score: dict,
    ):
        """Log the task results to wandb for visualization."""
        if not criteria_to_miner_score.values() or not hotkey_to_score:
            logger.warning(
                "📝 No criteria to miner scores available. Skipping calculating averages for wandb."
            )
            return

        mean_weighted_consensus_scores = (
            torch.stack(
                [
                    miner_scores.consensus.score
                    for miner_scores in criteria_to_miner_score.values()
                ]
            )
            .mean(dim=0)
            .tolist()
        )

        mean_weighted_gt_scores = (
            torch.stack(
                [
                    miner_scores.ground_truth
                    for miner_scores in criteria_to_miner_score.values()
                ]
            )
            .mean(dim=0)
            .tolist()
        )

        logger.info(
            f"📝 Mean miner scores across different criteria: consensus shape:{mean_weighted_consensus_scores}, gt shape:{mean_weighted_gt_scores}"
        )

        score_data = {
            "scores_by_hotkey": [hotkey_to_score],
            "mean": {
                "consensus": mean_weighted_consensus_scores,
                "ground_truth": mean_weighted_gt_scores,
            },
            "hotkey_to_dojo_task_scores_and_gt": await self._get_dojo_task_scores_and_gt(
                task.miner_responses
            ),
        }

        wandb_data = jsonable_encoder(
            {
                "request_id": task.request.request_id,
                "task": task.request.task_type,
                "criteria": task.request.criteria_types,
                "prompt": task.request.prompt,
                "completions": jsonable_encoder(task.request.completion_responses),
                "num_completions": len(task.request.completion_responses),
                "scores": score_data,
                "num_responses": len(task.miner_responses),
            }
        )

        wandb.log(wandb_data, commit=True)

    async def _get_dojo_task_scores_and_gt(
        self, miner_responses: List[FeedbackRequest]
    ):
        """Get the scores and ground truth for each miner response"""
        hotkey_to_dojo_task_scores_and_gt = []
        for miner_response in miner_responses:
            if miner_response.dojo_task_id is not None:
                model_to_score_and_gt_map = (
                    await ORM.get_scores_and_ground_truth_by_dojo_task_id(
                        miner_response.dojo_task_id
                    )
                )
                hotkey_to_dojo_task_scores_and_gt.append(
                    {
                        "hotkey": miner_response.axon.hotkey,
                        "dojo_task_id": miner_response.dojo_task_id,
                        "scores_and_gt": model_to_score_and_gt_map,
                    }
                )
        return hotkey_to_dojo_task_scores_and_gt
