import asyncio
import json

from bittensor.utils.btlogging import logging as logger

from commons.dataset.synthetic import SyntheticAPI
from commons.human_feedback.dojo import DojoAPI
from commons.utils import set_expire_time
from dojo.protocol import (
    ScoreCriteria,
    TaskSynapseObject,
    TaskTypeEnum,
)


async def main():
    data = await SyntheticAPI.get_qa()
    if data is None:
        logger.error("Failed to generate synthetic data")
        return

    # Create criteria for each completion response
    criteria = [
        ScoreCriteria(
            min=1.0,
            max=100.0,
        )
    ]
    model_names = [response.model for response in data.responses]
    if len(set(model_names)) == len(data.responses):
        logger.info("All responses have a unique model key")
        pass
    else:
        logger.warning(
            "Duplicate model names detected. Appending indices to make them unique."
        )
        for index, response in enumerate(data.responses):
            response.model = f"{response.model}_{index}"
            response.criteria_types = criteria
            if data.ground_truth and response.completion_id in data.ground_truth.keys():
                ground_truth_rank = data.ground_truth[response.completion_id]
                response.model = f"{response.model}_{ground_truth_rank}"
            else:
                response.completion_id = f"{response.completion_id}_{index}"

    expire_at = set_expire_time(8 * 3600)
    synapse = TaskSynapseObject(
        prompt=data.prompt,
        task_type=str(TaskTypeEnum.CODE_GENERATION),
        expire_at=expire_at,
        completion_responses=data.responses,
    )

    # Serialize the synapse object to JSON
    synapse_json = json.dumps(synapse.model_dump())

    # Calculate and print the size of the request in bytes
    request_size = len(synapse_json.encode("utf-8"))
    logger.info(f"Request size: {request_size} bytes")

    task_response = await DojoAPI.create_task(synapse)
    print(task_response)
    await DojoAPI._http_client.aclose()


asyncio.run(main())
