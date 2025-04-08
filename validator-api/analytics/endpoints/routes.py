import time
import traceback

from bittensor.utils.btlogging import logging as logger
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from shared.auth import ValidatorAuth

from ..core.models import AnalyticsPayload
from ..core.service import AnalyticsService
from ..external.storage import AnalyticsStorage

analytics_router = APIRouter()


@analytics_router.post("/api/v1/analytics/validator/{path_hotkey}/tasks")
async def create_analytics_data(
    request: Request,
    data: AnalyticsPayload,
    path_hotkey: str,
    header_hotkey: str = Header(..., alias="X-Hotkey"),
    signature: str = Header(..., alias="X-Signature"),
    message: str = Header(..., alias="X-Message"),
):
    start_time = time.time()
    try:
        logger.info(f"Received analytics request from hotkey {header_hotkey}")

        # Verify that path hotkey matches header hotkey
        if path_hotkey != header_hotkey:
            logger.error(
                f"Path hotkey {path_hotkey} does not match header hotkey {header_hotkey}"
            )
            raise HTTPException(
                status_code=401, detail="Hotkey in path does not match hotkey in header"
            )

        # Validate validator credentials
        await ValidatorAuth.validate_validator(
            request=request, hotkey=header_hotkey, signature=signature, message=message
        )

        auth_time = time.time()
        execution_time = auth_time - start_time
        logger.info(f"Auth completed in {execution_time:.4f} seconds")

        # Initialize storage
        storage = AnalyticsStorage(
            redis_cache=request.app.state.redis, aws_config=request.app.state.api_config
        )

        # Validate analytics data
        if not AnalyticsService.validate_analytics_data(data):
            logger.error(f"Invalid analytics data from hotkey {header_hotkey}")
            raise HTTPException(status_code=400, detail="Invalid analytics data")

        # Process and upload data
        new_tasks = []
        newly_cached_tasks = []
        try:
            # Cache new tasks
            for task in data.tasks:
                if not await storage.is_task_cached(task.validator_task_id):
                    new_tasks.append(task)
                    await storage.cache_task_id(task.validator_task_id)
                    newly_cached_tasks.append(task.validator_task_id)

            if not new_tasks:
                logger.info("No processed tasks to upload. Skipping analytics upload.")
                end_time = time.time()
                execution_time = end_time - start_time
                logger.info(f"Request completed in {execution_time:.4f} seconds")
                result = AnalyticsService.create_success_response(
                    message="No processed tasks to upload. Skipping analytics upload.",
                    task_count=0,
                )
                return JSONResponse(content=result.model_dump(), status_code=200)

            # Upload to S3
            upload_success = await storage.upload_to_s3(
                AnalyticsPayload(tasks=new_tasks), header_hotkey
            )

            if not upload_success:
                logger.error(
                    f"Failed to upload analytics data for hotkey {header_hotkey}"
                )
                # Clean up cached tasks on failure
                for task_id in newly_cached_tasks:
                    await storage.remove_cached_task(task_id)
                raise HTTPException(
                    status_code=500, detail="Failed to upload analytics data"
                )

            logger.info(
                f"Successfully processed {len(new_tasks)} analytics tasks for hotkey {header_hotkey}"
            )
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(f"Request completed in {execution_time:.4f} seconds")
            result = AnalyticsService.create_success_response(
                message=f"Successfully processed {len(new_tasks)} analytics tasks",
                task_count=len(new_tasks),
            )
            return JSONResponse(content=result.model_dump(), status_code=200)

        except Exception:
            # Clean up newly cached tasks on any error
            for task_id in newly_cached_tasks:
                await storage.remove_cached_task(task_id)
            raise

    except Exception as e:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.error(f"Error processing request: {execution_time:.4f} seconds")
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())

        # Handle both HTTPException and general exceptions
        error_message = str(e.detail) if isinstance(e, HTTPException) else str(e)
        status_code = e.status_code if isinstance(e, HTTPException) else 400

        result = AnalyticsService.create_error_response(
            message="Failed to process request",
            error=error_message,
            error_details={
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
            },
        )
        return JSONResponse(content=result.model_dump(), status_code=status_code)
