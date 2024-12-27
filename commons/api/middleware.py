import time
from ipaddress import ip_address, ip_network

import httpx
from bittensor.utils.btlogging import logging as logger
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

MAX_CONTENT_LENGTH = 1 * 1024 * 1024


class LimitContentLengthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        content_length = int(request.headers.get("content-length", 0))
        if content_length <= MAX_CONTENT_LENGTH:
            return await call_next(request)
        return Response(status_code=413)


class AWSIPFilterMiddleware(BaseHTTPMiddleware):
    """Middleware to ensure that only requests from AWS Servers are allowed."""

    _aws_ips_url = "https://ip-ranges.amazonaws.com/ip-ranges.json"
    _allowed_ip_ranges = []
    _last_checked: float = 0
    _allowed_networks = []
    _allowed_regions = {"us-east-1"}

    @classmethod
    async def _get_allowed_networks(cls):
        return [ip_network(ip_range) for ip_range in await cls._get_allowed_ip_ranges()]

    @classmethod
    async def _get_allowed_ip_ranges(cls):
        if (time.time() - cls._last_checked) < 300:
            return cls._allowed_ip_ranges

        async with httpx.AsyncClient() as client:
            start_time = time.time()
            response = await client.get(cls._aws_ips_url)
            cls._last_checked = time.time()
            elapsed_time = cls._last_checked - start_time
            logger.debug(
                f"Sent request to {cls._aws_ips_url}, took {elapsed_time:.2f} seconds"
            )
            data = response.json()
            cls._allowed_ip_ranges = [
                ip_range["ip_prefix"]
                for ip_range in data["prefixes"]
                if ip_range["region"] in cls._allowed_regions
            ]
        return cls._allowed_ip_ranges

    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        client_ip = ip_address(request.client.host)
        allowed_networks = [
            ip_network(ip_range) for ip_range in await self._get_allowed_networks()
        ]
        for network in allowed_networks:
            if client_ip in network:
                response = await call_next(request)
                return response
        return Response("Forbidden", status_code=403)
