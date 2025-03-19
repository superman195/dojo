import os

from pydantic import Field
from pydantic_settings import BaseSettings


# NOTE: we use this instead of the Field(..., env=...) due to some errors in resolving
# the env variables from pydantic, also due to some pyright parsing issues
class AxonSettings(BaseSettings):
    port: int = Field(default=os.getenv("AXON_PORT", 8091))


# class RedisSettings(BaseSettings):
#     host: str = Field(default=os.getenv("REDIS_HOST", "localhost"))
#     port: int = Field(default=int(os.getenv("REDIS_PORT", "6379")))
#     username: str = Field(default=os.getenv("REDIS_USERNAME", "default"))
#     password: SecretStr = Field(default=os.getenv("REDIS_PASSWORD", ""))


class ScoreSettings(BaseSettings):
    # EMA Alpha to calculate scores for synthetic tasks
    synthetic_ema_alpha: float = Field(default=0.3)
    # EMA Alpha to calculate scores for both SF_TASK and TF_TASK
    hfl_ema_alpha: float = Field(default=0.3)


class UvicornSettings(BaseSettings):
    num_workers: int = Field(default=2)
    port: int = Field(default=5003)
    host: str = Field(default="0.0.0.0")
    log_level: str = Field(default="debug")


class ChainSettings(BaseSettings):
    netuid: int = Field(default=52)
    epoch_length: int = Field(default=100)


class TorchSettings(BaseSettings):
    device: str = Field(default="cpu")


class Settings(BaseSettings):
    axon: AxonSettings = AxonSettings()
    uvicorn: UvicornSettings = UvicornSettings()
    # redis: RedisSettings = RedisSettings()
    score_settings: ScoreSettings = ScoreSettings()
    torch_settings: TorchSettings = TorchSettings()
    chain_settings: ChainSettings = ChainSettings()

    class Config:
        extra = "forbid"
        case_sensitive = True
