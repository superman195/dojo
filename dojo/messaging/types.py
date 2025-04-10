from typing import Any, Dict, TypeVar

from pydantic import BaseModel

StdResponseBody = Dict[str, Any] | BaseModel


T = TypeVar("T", bound=BaseModel)


class StdResponse(BaseModel):
    success: bool
    body: StdResponseBody | None = None
    error: str | None = None


# TODO: remove this after testing
# Define your payload models
class PayloadA(BaseModel):
    field1: str
    field2: str
