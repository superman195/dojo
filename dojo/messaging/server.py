import asyncio
from typing import Any, Awaitable, Callable, List, Type

import orjson
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from loguru import logger

from dojo.messaging.types import PayloadA, T
from dojo.messaging.utils import create_response, decode_body


def register_route_handler(
    app: FastAPI,
    handler: Callable[[Request, T], Awaitable[Any]],
    model: Type[T],
    methods: List[str] = ["POST"],
):
    """Register a route with a Pydantic model to allow easily adding new endpoints"""

    async def handler_wrapper(request: Request):
        """Wrapper around the request that handles zstd decompression and payload validation"""
        try:
            body = await decode_body(request)

            print(f"Got request: body: {body=}")
            try:
                data = orjson.loads(body)
            except orjson.JSONDecodeError as e:
                return create_response(
                    success=False, error=f"Invalid JSON, exception: {str(e)}"
                )

            try:
                payload = model.model_validate(data)
            except Exception as e:
                return create_response(
                    success=False, error=f"Validation error: {str(e)}"
                )

            result = await handler(request, payload)

            # Return standardized response format
            return create_response(success=True, body=result)
        except HTTPException as e:
            logger.error(f"HTTPException: {str(e)}")
            raise e
        except Exception as e:
            logger.error(f"Error processing request due to: {str(e)}")
            return create_response(
                success=False, error=f"Internal server error: {str(e)}"
            )

    handler_wrapper.__name__ = handler.__name__
    app.add_api_route(
        path="/" + model.__name__.lstrip("/"),
        endpoint=handler_wrapper,
        methods=methods,
        operation_id=f"handle_{model.__name__}",
    )


# NOTE: example usage below
# NOTE: example usage below

app = FastAPI()


# e.g. miner here, and this gets called
async def handler_a(request: Request, payload: PayloadA) -> PayloadA:
    # Access both request info and the validated payload
    return payload


# Register routes with their respective payload models
register_route_handler(
    app=app,
    handler=handler_a,
    model=PayloadA,
)


async def main():
    print(app.routes)
    config = uvicorn.Config(
        app=app,
        host="0.0.0.0",
        port=8001,
        workers=1,
        log_level="info",
        reload=False,
    )
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
