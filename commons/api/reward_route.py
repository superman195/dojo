from bittensor.utils.btlogging import logging as logger
from fastapi import APIRouter, Header, Request, responses
from fastapi.encoders import jsonable_encoder
from pydantic.error_wrappers import ValidationError

from commons.cache import RedisCache
from commons.objects import ObjectManager
from commons.utils import get_new_uuid
from dojo.protocol import FeedbackRequest

reward_router = APIRouter(prefix="/api/reward_model")


cache = RedisCache()


@reward_router.get("/token")
async def get_token(request: Request):
    uuid = get_new_uuid()
    client_host = request.client.host
    await cache.put(client_host, uuid)
    return {"token": uuid}


@reward_router.post("/")
async def reward_request_handler(
    request: Request, authorization: str | None = Header(default=None)
):
    token = authorization.split(" ")[1]
    client_host = request.client.host
    if token != await cache.get(client_host):
        return responses.JSONResponse(
            status_code=403, content={"message": "Invalid token"}
        )

    try:
        request_data = await request.json()
        request_data["task_type"] = request_data.pop("task")
        request_data["criteria_types"] = request_data.pop("criteria")

        logger.info("Received task data from external user")
        logger.debug(f"Task data: {request_data}")
        task_data = FeedbackRequest.parse_obj(request_data)
    except (KeyError, ValidationError):
        logger.error("Invalid data sent by external user")
        return responses.JSONResponse(
            status_code=400, content={"message": "Invalid request data"}
        )
    except Exception as e:
        logger.exception(f"Encountered exception: {e}")
        return responses.JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

    try:
        validator = ObjectManager.get_validator()
        response = await validator.send_request(task_data, external_user=True)
        response_json = jsonable_encoder(response)
        return responses.JSONResponse(content=response_json)
    except Exception as e:
        logger.exception(f"Encountered exception: {e}")
