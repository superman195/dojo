import http
from typing import Callable

from fastapi import Header, HTTPException, Request, Response
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware

from dojo.messaging.utils import verify_signature


class SignatureMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        signature = request.headers.get("signature", "")
        hotkey = request.headers.get("hotkey", "")
        message = request.headers.get("message", "")
        if not hotkey or not signature or not message:
            message = f"{http.HTTPStatus(400).phrase}, missing \
                    headers, expected: hotkey, signature, message, \
                    got: {hotkey=}, {signature=}, {message=}"
            return Response(content=message, status_code=400)
        if not verify_signature(hotkey, signature, message):
            return Response(content=http.HTTPStatus(403).phrase, status_code=403)

        # otherwise, proceed with the normal request
        response = await call_next(request)
        return response


async def verify_signature_dependency(
    signature: str = Header(None),
    hotkey: str = Header(None),
    message: str = Header(None),
) -> bool:
    """
    FastAPI dependency for signature verification.
    Can be used with specific routes that need signature verification.
    """
    if not hotkey or not signature or not message:
        message_error = f"Missing headers, expected: hotkey, signature, message, got: {hotkey=}, {signature=}, {message=}"
        logger.error(message_error)
        raise HTTPException(status_code=400, detail=message_error)

    if not verify_signature(hotkey, signature, message):
        raise HTTPException(status_code=403, detail="Forbidden: Invalid signature")

    return True
