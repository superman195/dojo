import json
import os
import random
import traceback
from datetime import datetime, timezone

import redis
from bittensor.btlogging import logging as logger

from commons.utils import get_new_uuid
from dojo.protocol import FeedbackRequest, Result, TaskResult, TaskResultRequest
from dojo.utils.config import get_config
from neurons.miner import Miner


class MinerSim(Miner):
    def __init__(self):
        super().__init__()
        try:
            # Initialize Redis connection
            host = os.getenv("REDIS_HOST", "localhost")
            port = int(os.getenv("REDIS_PORT", 6379))
            self.redis_client = redis.Redis(
                host=host, port=port, db=0, decode_responses=True
            )
            logger.info("Redis connection established")

            self._configure_simulation()

            self.is_bad_miner = get_config().simulation_bad_miner
            logger.info(f"Miner role set to: {'bad' if self.is_bad_miner else 'good'}")

            logger.info("Starting Miner Simulator")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    def _configure_simulation(self):
        """Configure simulation parameters with environment variables or defaults."""
        self.response_behaviors = {
            "normal": float(os.getenv("SIM_NORMAL_RESP_PROB", 0.8)),
            "no_response": float(os.getenv("SIM_NO_RESP_PROB", 0.1)),
            "timeout": float(os.getenv("SIM_TIMEOUT_PROB", 0.1)),
        }

    async def forward_feedback_request(
        self, synapse: FeedbackRequest
    ) -> FeedbackRequest:
        try:
            # Validate that synapse, dendrite, dendrite.hotkey, and response are not None
            if not synapse or not synapse.dendrite or not synapse.dendrite.hotkey:
                logger.error("Invalid synapse: dendrite or dendrite.hotkey is None.")
                return synapse

            if not synapse.completion_responses:
                logger.error("Invalid synapse: response field is None.")
                return synapse

            # Empty out completion response since not needed in simulator
            new_synapse = synapse.model_copy(deep=True)
            new_synapse.completion_responses = []

            synapse.dojo_task_id = synapse.request_id
            self.hotkey_to_request[synapse.dendrite.hotkey] = synapse

            redis_key = f"feedback:{synapse.request_id}"
            self.redis_client.set(
                redis_key,
                new_synapse.model_dump_json(),
                ex=86400,  # expire after 24 hours
            )
            logger.info(f"Stored feedback request {synapse.request_id}")

            synapse.ground_truth = {}
            return synapse

        except Exception as e:
            logger.error(f"Error handling FeedbackRequest: {e}")
            traceback.print_exc()
            return synapse

    async def forward_task_result_request(
        self, synapse: TaskResultRequest
    ) -> TaskResultRequest | None:
        try:
            logger.info(f"Received TaskResultRequest for task id: {synapse.task_id}")
            if not synapse or not synapse.task_id:
                logger.error("Invalid TaskResultRequest: missing task_id")
                return None

            # Simulate different response behaviors
            # behavior = self._get_response_behavior()

            # if behavior in ['no_response', 'timeout']:
            #     logger.debug(f"Simulating {behavior} for task {synapse.task_id}")
            #     if behavior == 'timeout':
            #         await asyncio.sleep(30)
            #     return None

            redis_key = f"feedback:{synapse.task_id}"
            request_data = self.redis_client.get(redis_key)

            request_dict = json.loads(request_data) if request_data else None
            feedback_request = FeedbackRequest(**request_dict) if request_dict else None

            if not feedback_request:
                logger.debug(f"No task result found for task id: {synapse.task_id}")
                return None

            current_time = datetime.now(timezone.utc).isoformat()

            task_results = []
            for criteria_type in feedback_request.criteria_types:
                result = Result(
                    type=criteria_type.type,
                    value=self._generate_scores(feedback_request.ground_truth),
                )

                task_result = TaskResult(
                    id=get_new_uuid(),
                    status="COMPLETED",
                    created_at=current_time,
                    updated_at=current_time,
                    result_data=[result],
                    worker_id=get_new_uuid(),
                    task_id=synapse.task_id,
                )
                task_results.append(task_result)

            synapse.task_results = task_results
            logger.info(f"TaskResultRequest: {synapse}")

            self.redis_client.delete(redis_key)
            logger.debug(f"Processed task result for task {synapse.task_id}")

            return synapse

        except Exception as e:
            traceback.print_exc()
            logger.error(f"Error handling TaskResultRequest: {e}")
            return None

    def _get_response_behavior(self) -> str:
        """Determine the response behavior based on configured probabilities."""
        return random.choices(
            list(self.response_behaviors.keys()),
            weights=list(self.response_behaviors.values()),
        )[0]

    def _generate_scores(self, ground_truth: dict) -> dict:
        scores = {}
        max_rank = max(ground_truth.values())

        for k, v in ground_truth.items():
            base_weight = int(10 - (v * (10 / max_rank)))
            if self.is_bad_miner:
                deviation = random.randint(-5, 5)
            else:
                deviation = random.randint(-2, 2)
            random_score = max(0, min(9, base_weight + deviation))
            score = int((random_score / (10 - 1)) * (100 - 1) + 1)
            scores[k] = score
        return scores

    # def __del__(self):
    #     """Cleanup Redis connection on object destruction"""
    #     try:
    #         self.redis_client.close()
    #         logger.info("Redis connection closed")
    #     except:
    #         pass
