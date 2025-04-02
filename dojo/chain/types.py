"""
All types related to Bittensor chain data.

Configure as needed in __init__.py for module level access.
"""

from typing import List

from pydantic import BaseModel, Field, field_validator


class HexString(str):
    def to_int(self) -> int:
        return int(self, 16)


class Digest(BaseModel):
    logs: List[str]


class BlockHeader(BaseModel):
    parentHash: str
    number: HexString = Field(description="Block number as hex string")
    stateRoot: str
    extrinsicsRoot: str
    digest: Digest

    @field_validator("number", mode="before")
    def validate_number(cls, v):
        if isinstance(v, str):
            return HexString(v)
        return v

    class Config:
        arbitrary_types_allowed = True
