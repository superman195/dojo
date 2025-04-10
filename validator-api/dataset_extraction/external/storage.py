from typing import List, Tuple

import aioboto3
from fastapi import UploadFile

from commons.api_settings import AWSSettings
from commons.logging import logging as logger


class DatasetStorage:
    def __init__(self, aws_config: AWSSettings):
        self.aws_config = aws_config

    async def upload_dataset(
        self, files: List[UploadFile], hotkey: str
    ) -> Tuple[bool, List[str], int]:
        try:
            session = aioboto3.Session(region_name=self.aws_config.AWS_REGION)
            uploaded_files = []
            total_size = 0

            async with session.resource("s3") as s3:
                bucket = await s3.Bucket(self.aws_config.BUCKET_NAME)
                for file in files:
                    content = await file.read()
                    file_size = len(content)
                    total_size += file_size

                    filename = f"datasets/hotkey_{hotkey}_{file.filename}"
                    await bucket.put_object(
                        Key=filename,
                        Body=content,
                    )
                    uploaded_files.append(file.filename)

            return True, uploaded_files, total_size

        except Exception as e:
            logger.error(f"Error uploading dataset: {str(e)}")
            return False, [], 0
