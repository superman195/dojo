from typing import List

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from shared.auth import ValidatorAuth

from ..core.service import DatasetExtractionService
from ..external.storage import DatasetStorage

dataset_router = APIRouter()


@dataset_router.post("/api/v1/validator/upload_dataset")
async def upload_dataset(
    request: Request,
    hotkey: str = Form(...),
    signature: str = Form(...),
    message: str = Form(...),
    files: List[UploadFile] = File(...),
):
    """
    Endpoint for uploading datasets from extractors
    """
    try:
        # Validate extractor credentials
        await ValidatorAuth.validate_validator(
            request=request, hotkey=hotkey, signature=signature, message=message
        )

        # Validate file sizes
        for file in files:
            if not DatasetExtractionService.validate_file_size(
                file, request.app.state.api_config.MAX_CHUNK_SIZE_MB
            ):
                raise HTTPException(
                    status_code=413,
                    detail=f"File {file.filename} too large. Maximum size is {request.app.state.api_config.MAX_CHUNK_SIZE_MB}MB",
                )

        # Initialize storage
        storage = DatasetStorage(aws_config=request.app.state.api_config)

        # Upload files
        success, filenames, total_size = await storage.upload_dataset(files, hotkey)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to upload dataset")

        # Create response
        result = DatasetExtractionService.create_upload_result(
            success=True,
            message="Files uploaded successfully",
            filenames=filenames,
            total_size=total_size,
        )

        return JSONResponse(content=result.model_dump(), status_code=200)

    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse(
            content={"error": "Failed to process request", "details": str(e)},
            status_code=400,
        )
