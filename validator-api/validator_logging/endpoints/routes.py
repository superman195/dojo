import time
import traceback

from bittensor.utils.btlogging import logging as logger
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from ..core.models import LogBatch
from ..core.service import LoggingService
from ..external.storage import LogStorage

logging_router = APIRouter(tags=["logging"])


@logging_router.post("/api/v1/validator/logging")
async def send_logs_to_loki(request: Request, log_batch: LogBatch):
    start_time = time.time()
    try:
        logger.info(f"Received logging request from hotkey {log_batch.hotkey}")

        # Validate validator credentials
        # await ValidatorAuth.validate_validator(
        #     request=request,
        #     hotkey=log_batch.hotkey,
        #     signature=log_batch.signature,
        #     message=log_batch.message
        # )

        auth_time = time.time()
        execution_time = auth_time - start_time
        logger.info(f"Auth completed in {execution_time:.4f} seconds")

        # Initialize storage
        storage = LogStorage(config=request.app.state.api_config)

        # Validate log entries
        if not LoggingService.validate_log_entries(log_batch.logs):
            logger.error(f"Invalid log data from hotkey {log_batch.hotkey}")
            raise HTTPException(status_code=400, detail="Invalid log data")

        # Send logs to Loki
        success = await storage.send_to_loki(log_batch.logs, log_batch.hotkey)

        if success:
            logger.info(
                f"Successfully sent {len(log_batch.logs)} logs to Loki from validator {log_batch.hotkey}"
            )
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(f"Request completed in {execution_time:.4f} seconds")

            result = LoggingService.create_success_response(
                message=f"Sent {len(log_batch.logs)} logs to Loki",
                validator=log_batch.hotkey,
                log_count=len(log_batch.logs),
            )
            return JSONResponse(content=result.model_dump(), status_code=200)
        else:
            logger.error(
                f"Failed to send logs to Loki for validator {log_batch.hotkey}"
            )
            raise HTTPException(status_code=500, detail="Failed to send logs to Loki")

    except HTTPException:
        raise
    except Exception as e:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.error(f"Error processing request: {execution_time:.4f} seconds")
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())

        # Handle general exceptions
        error_message = str(e)

        result = LoggingService.create_error_response(
            message="Failed to process request",
            validator=log_batch.hotkey,
            error=error_message,
            error_details={
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
            },
        )
        return JSONResponse(content=result.model_dump(), status_code=400)
