import os

from pydantic import BaseModel, Field


# NOTE: we use this instead of the Field(..., env=...) due to some errors in resolving
# the env variables from pydantic, also due to some pyright parsing issues
class AxonSettings(BaseModel):
    port: int = Field(default=os.getenv("AXON_PORT", 8091))


# class RedisSettings(BaseModel):
#     host: str = Field(default=os.getenv("REDIS_HOST", "localhost"))
#     port: int = Field(default=int(os.getenv("REDIS_PORT", "6379")))
#     username: str = Field(default=os.getenv("REDIS_USERNAME", "default"))
#     password: SecretStr = Field(default=os.getenv("REDIS_PASSWORD", ""))


class ScoreSettings(BaseModel):
    # EMA Alpha to calculate scores for synthetic tasks
    synthetic_ema_alpha: float = Field(default=0.3)
    # EMA Alpha to calculate scores for both SF_TASK and TF_TASK
    hfl_ema_alpha: float = Field(default=0.3)


class UvicornSettings(BaseModel):
    num_workers: int = Field(default=2)
    port: int = Field(default=5003)
    host: str = Field(default="0.0.0.0")
    log_level: str = Field(default="debug")


class ChainSettings(BaseModel):
    netuid: int = Field(default=52)
    epoch_length: int = Field(default=100)
    subtensor_network: str = Field(
        default="wss://entrypoint-finney.opentensor.ai:443",
        examples=["ws://mainnet-lite:9944", "ws://testnet-lite:9944"],
    )
    subtensor_endpoint: str = subtensor_network


class WalletSettings(BaseModel):
    coldkey: str = Field(default=os.getenv("WALLET_COLDKEY"))
    hotkey: str = Field(default=os.getenv("WALLET_HOTKEY"))


class TorchSettings(BaseModel):
    device: str = Field(default="cpu")


class Settings(BaseModel):
    axon: AxonSettings = AxonSettings()
    uvicorn: UvicornSettings = UvicornSettings()
    # redis: RedisSettings = RedisSettings()
    score: ScoreSettings = ScoreSettings()
    torch: TorchSettings = TorchSettings()
    chain: ChainSettings = ChainSettings()
    wallet: WalletSettings = WalletSettings()

    env_file: str = Field(
        default=".env", description="Path to the environment file to use."
    )
    ignore_min_stake: bool = Field(
        default=False,
        description="Whether to always include self in monitoring queries, mainly for testing",
    )

    class Config:
        extra = "forbid"
        case_sensitive = True
