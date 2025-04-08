from datetime import datetime
from typing import List

from pydantic import BaseModel


class DatasetUploadResult(BaseModel):
    success: bool
    message: str
    timestamp: datetime
    filenames: List[str]
    total_size: int
