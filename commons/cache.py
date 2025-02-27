import os

from redis import asyncio as aioredis
from redis.asyncio.client import Redis

from commons.api_settings import RedisSettings


def build_redis_url(config: RedisSettings | None = None) -> str:
    if config is None:
        host = os.getenv("REDIS_HOST")
        port = int(os.getenv("REDIS_PORT", 6379))
        username = os.getenv("REDIS_USERNAME")
        password = os.getenv("REDIS_PASSWORD")
        if username and password:
            return f"redis://{username}:{password}@{host}:{port}"
        elif password:
            return f"redis://:{password}@{host}:{port}"
        else:
            return f"redis://{host}:{port}"
    else:
        host = config.REDIS_HOST
        port = config.REDIS_PORT
        username = config.REDIS_USERNAME
        password = config.REDIS_PASSWORD
        if username and password:
            return f"redis://{username}:{password}@{host}:{port}"
        elif password:
            return f"redis://:{password}@{host}:{port}"
        else:
            return f"redis://{host}:{port}"


class RedisCache:
    _instance = None
    _anal_prefix_: str = "analytics"
    _upload_key_: str = "uploaded"
    redis: Redis

    def __new__(cls, config: RedisSettings | None = None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            redis_url = build_redis_url(config)
            cls._instance.redis = aioredis.from_url(url=redis_url)
        return cls._instance

    def _build_key(self, prefix: str, *parts: str) -> str:
        if len(parts) == 0:
            raise ValueError("Must specify at least one redis key")
        return f"{prefix}:{':'.join(parts)}"

    async def connect(self):
        if self.redis is None:
            redis_url = build_redis_url()
            self.redis = await aioredis.from_url(redis_url)

    async def put(self, key: str, value, expire_time: int = 60 * 60 * 24):
        """
        by default will set an expiry time of 1 day
        """
        if self.redis is None:
            await self.connect()
        await self.redis.set(key, value, ex=expire_time)

    async def get(self, key: str):
        if self.redis is None:
            await self.connect()
        value = await self.redis.get(key)
        if value:
            return value
        return None

    async def delete(self, key: str):
        if self.redis is None:
            await self.connect()
        await self.redis.delete(key)

    async def close(self):
        if self.redis:
            await self.redis.aclose()
