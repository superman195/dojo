import asyncio
import json
import os
import random
from typing import Dict, List

import httpx
from bittensor.btlogging import logging as logger

from commons.exceptions import CreateTaskFailed
from commons.utils import loaddotenv
from dojo import get_dojo_api_base_url
from dojo.protocol import (
    TaskSynapseObject,
)

DOJO_API_BASE_URL = get_dojo_api_base_url()
# to be able to get the curlify requests
# DEBUG = False


def _get_max_results_param() -> int:
    max_results = os.getenv("TASK_MAX_RESULTS")
    if not max_results:
        logger.warning("TASK_MAX_RESULTS is not set, defaulting to 1")
        max_results = 1
    return int(max_results)


class DojoAPI:
    MAX_RETRIES = 5
    BASE_DELAY = 1
    TIMEOUT = 15.0
    CODE_GEN_TASK_TITLE = "LLM Code Generation Task"
    _http_client = httpx.AsyncClient()

    @classmethod
    async def _get_task_by_id(cls, task_id: str):
        """Gets task by task id and checks completion status"""
        url = f"{DOJO_API_BASE_URL}/api/v1/tasks/{task_id}"
        response = await cls._http_client.get(url)
        response.raise_for_status()
        return response.json()

    @classmethod
    async def _get_task_results_by_task_id(cls, task_id: str):
        """Gets task results from task id"""
        url = f"{DOJO_API_BASE_URL}/api/v1/tasks/task-result/{task_id}"
        response = await cls._http_client.get(url)
        response.raise_for_status()
        return response.json()

    @classmethod
    async def get_task_results_by_task_id(cls, task_id: str) -> List[Dict] | None:
        """Gets task results from task id to prepare for scoring later on"""
        # task_response = await cls._get_task_by_id(task_id)
        # task_status = task_response.get("body", {}).get("status", None)
        # is_completed = task_status and task_status.lower() == "completed"
        # if is_completed is None:
        #     logger.error(f"Failed to read status field for task_id: {task_id}")
        #     return

        # if is_completed is False:
        #     return

        max_retries = 5
        base_delay = 1

        for attempt in range(max_retries):
            try:
                task_results_response = await cls._get_task_results_by_task_id(task_id)
                task_results = task_results_response.get("body", {}).get("taskResults")
                if task_results is None or not task_results:
                    return None
                return task_results
            except Exception as e:
                if attempt < max_retries - 1:
                    delay = base_delay * 2**attempt + random.uniform(0, 1)
                    logger.warning(
                        f"Error occurred while getting task results for task_id {task_id}: {e}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"Failed to get task results for task_id {task_id} after {max_retries} attempts: {e}"
                    )
                    return None

        logger.error(
            f"Failed to get task results for task_id {task_id} after {max_retries} retries"
        )
        return None

    @staticmethod
    def serialize_task_request(data: TaskSynapseObject):
        output = dict(
            task=data.task_type,
            prompt=data.prompt,
            responses=[],
            task_type=str(data.task_type).upper(),
        )
        for completion in data.completion_responses:
            completion_dict = {}
            completion_dict["model"] = completion.model
            completion_dict["completion"] = completion.completion.model_dump()
            completion_dict["criteria"] = (
                [c.model_dump() for c in completion.criteria_types]
                if completion.criteria_types
                else []
            )
            output["responses"].append(completion_dict)

        return output

    @classmethod
    async def create_task(
        cls,
        task_request: TaskSynapseObject,
    ) -> List[str]:
        response_data = {"text": "", "json": {}}

        for attempt in range(cls.MAX_RETRIES):
            try:
                # Prepare request data
                task_data = cls.serialize_task_request(task_request)
                # TODO: make task title dynamic
                form_body = {
                    "title": ("", cls.CODE_GEN_TASK_TITLE),
                    "body": ("", task_request.prompt),
                    "expireAt": ("", task_request.expire_at),
                    "taskData": ("", json.dumps([task_data])),
                    "maxResults": ("", str(_get_max_results_param())),
                }

                # Make request
                response = await cls._http_client.post(
                    f"{DOJO_API_BASE_URL}/api/v1/tasks/create-tasks",
                    files=form_body,
                    headers={"x-api-key": loaddotenv("DOJO_API_KEY")},
                    timeout=cls.TIMEOUT,
                )

                response_data["text"] = response.text
                response_data["json"] = response.json()

                if response.status_code == 200:
                    task_ids = response_data["json"]["body"]
                    logger.success(
                        f"Successfully created task with\ntask ids:{task_ids}"
                    )
                    return task_ids

                logger.error(
                    f"Error occurred when trying to create task\nErr:{response_data['json']['error']}"
                )
                response.raise_for_status()

            except Exception as e:
                if attempt < cls.MAX_RETRIES - 1:
                    delay = cls.BASE_DELAY * 2**attempt + random.uniform(0, 1)
                    logger.warning(
                        f"Error occurred: {e}. Retrying in {delay:.2f} seconds..."
                    )
                    await asyncio.sleep(delay)
                    continue

                error_msg = (
                    "HTTP error"
                    if isinstance(e, httpx.HTTPStatusError | httpx.RequestError)
                    else (
                        "JSON decode error"
                        if isinstance(e, json.JSONDecodeError)
                        else "unexpected error"
                    )
                )

                raise CreateTaskFailed(
                    f"Failed to create task due to {error_msg}: {e}, "
                    f"response_text: {response_data['text']}, "
                    f"response_json: {response_data['json']}"
                )

        raise CreateTaskFailed(f"Failed to create task after {cls.MAX_RETRIES} retries")
