import asyncio
import http
from typing import Any

import substrateinterface
import uvicorn
from loguru import logger
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from dojo.messaging.types import RouteType


class SignatureMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):  # pyright: ignore
        super().__init__(app)  # pyright: ignore

    async def dispatch(self, request: Request, call_next: Any):
        signature = request.headers.get("signature", "")
        hotkey = request.headers.get("hotkey", "")
        message = request.headers.get("message", "")
        if not verify_signature(hotkey, signature, message):
            return Response(content=http.HTTPStatus(403).phrase, status_code=403)

        # otherwise, proceed with the normal request
        response = await call_next(request)
        return response


def verify_signature(hotkey: str, signature: str, message: str) -> bool:
    """
    returns true if input signature was created by input hotkey for input message.
    """
    try:
        keypair = substrateinterface.Keypair(ss58_address=hotkey, ss58_format=42)
        if not keypair.verify(data=message, signature=signature):
            logger.error(f"Invalid signature for address={hotkey}")
            return False

        logger.success(f"Signature verified, signed by {hotkey}")
        return True
    except Exception as e:
        logger.error(f"Error occurred while verifying signature, exception {e}")
        return False


class Server:
    def __init__(self):
        self._routes: list[RouteType] = []
        self._middleware: list[Middleware] = [Middleware(SignatureMiddleware)]
        self.app: Starlette | None = None

    def add_route(self, route: RouteType):
        self._routes.append(route)

    def add_middleware(self, middleware: Middleware):
        self._middleware.append(middleware)

    def initialize(self) -> Starlette:
        self.app = Starlette(
            debug=True, routes=self._routes, middleware=self._middleware
        )
        return self.app


def hello():
    print("hello")


server = Server()
server.add_route(Route("/", hello))
app = server.initialize()


async def main():
    # uvicorn.run(app, host="0.0.0.0", port=8000)
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


# async def try_forward_task_request(request: Request):
#     signature = request.headers.get("signature")
#     hotkey = request.headers.get("hotkey")
#     message = request.headers.get("message")
#     if not signature:
#         raise ValueError(f"Invalid signature: {signature=}")
#     if not hotkey:
#         raise ValueError(f"Invalid hotkey: {hotkey=}")
#     if not message:
#         raise ValueError(f"Invalid message: {message=}")
#     if not verify_signature(hotkey, signature, message):
#         raise ValueError("Invalid signature")
#
#     try:
#         logger.info(f"Request client host: {request.client.host}")
#     except:
#         pass
