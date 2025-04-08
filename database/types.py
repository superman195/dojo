from typing import Any

from pydantic import BaseModel

from database.prisma.enums import HFLStatusEnum


# NOTE: this exists so we can have validation
class HFLEvent(BaseModel):
    type: HFLStatusEnum
    task_id: str
    timestamp: str
    metadata: dict[str, Any]
