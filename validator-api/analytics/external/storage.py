import json
import traceback
from datetime import datetime

import aioboto3

from commons.cache import RedisCache
from commons.logging import logging as logger

from ..core.models import AnalyticsPayload


class AnalyticsStorage:
    def __init__(self, redis_cache: RedisCache, aws_config: dict):
        self.redis = redis_cache
        self.aws_config = aws_config
        self.ONE_DAY_SECONDS = 60 * 60 * 24  # 24 hours in seconds

    def format_for_athena(self, data: AnalyticsPayload) -> str:
        try:
            formatted_data = ""
            for task in data.tasks:
                unnested_obj = json.dumps(task.model_dump(), indent=2)
                formatted_data += unnested_obj + "\n"
            return formatted_data
        except Exception as e:
            logger.error(f"Error processing data for Athena format: {str(e)}")
            raise

    async def cache_task_id(self, task_id: str) -> bool:
        try:
            key = self.redis._build_key(
                self.redis._anal_prefix_, self.redis._upload_key_, task_id
            )
            await self.redis.redis.set(key, task_id, self.ONE_DAY_SECONDS)
            return True
        except Exception as e:
            logger.error(f"Error caching task ID: {str(e)}")
            return False

    async def is_task_cached(self, task_id: str) -> bool:
        try:
            key = self.redis._build_key(
                self.redis._anal_prefix_, self.redis._upload_key_, task_id
            )
            return bool(await self.redis.redis.get(key))
        except Exception as e:
            logger.error(f"Error checking cached task ID: {str(e)}")
            return False

    async def upload_to_s3(self, data: AnalyticsPayload, hotkey: str) -> bool:
        try:
            session = aioboto3.Session(region_name=self.aws_config.AWS_REGION)
            async with session.resource("s3") as s3:
                bucket = await s3.Bucket(self.aws_config.BUCKET_NAME)
                filename = f"analytics/{hotkey}_{datetime.now().strftime('%Y-%m-%d_%H-%M')}_analytics.txt"

                # Format data for Athena compatibility
                formatted_data = self.format_for_athena(data)

                await bucket.put_object(Key=filename, Body=formatted_data)
            return True
        except Exception as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    async def remove_cached_task(self, task_id: str) -> bool:
        try:
            key = self.redis._build_key(
                self.redis._anal_prefix_, self.redis._upload_key_, task_id
            )
            await self.redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Error removing cached task ID: {str(e)}")
            return False
