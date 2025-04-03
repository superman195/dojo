"""
config file for validator api service that is not included in standardvalidator config
"""

import os

from dotenv import load_dotenv
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings

load_dotenv()


class RedisSettings(BaseSettings):
    REDIS_USERNAME: str = Field(default=os.getenv("REDIS_USERNAME", ""))
    REDIS_PASSWORD: str = Field(default=os.getenv("REDIS_PASSWORD", ""))
    REDIS_HOST: str = Field(default=os.getenv("REDIS_HOST", ""))
    REDIS_PORT: int = Field(default=os.getenv("REDIS_PORT", 6379))


class AWSSettings(BaseSettings):
    # Dataset env vars
    AWS_REGION: str = Field(default=os.getenv("AWS_REGION", ""))
    BUCKET_NAME: str = Field(default=os.getenv("S3_BUCKET_NAME", ""))
    MAX_CHUNK_SIZE_MB: int = Field(default=int(os.getenv("MAX_CHUNK_SIZE_MB", 50)))

    # Analytics env vars
    AWS_ACCESS_KEY_ID: SecretStr = Field(default=os.getenv("AWS_ACCESS_KEY_ID", ""))
    AWS_SECRET_ACCESS_KEY: SecretStr = Field(
        default=os.getenv("AWS_SECRET_ACCESS_KEY", "")
    )


class ValidatorAPISettings(BaseSettings):
    redis: RedisSettings = RedisSettings()
    aws: AWSSettings = AWSSettings()


def get_settings() -> ValidatorAPISettings:
    return ValidatorAPISettings()
