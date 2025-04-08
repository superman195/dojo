from datetime import datetime

from bittensor.utils.btlogging import logging as logger

from .models import (
    AnalyticsErrorResponse,
    AnalyticsPayload,
    AnalyticsSuccessResponse,
    ErrorDetails,
)


class AnalyticsService:
    @staticmethod
    def validate_analytics_data(data: AnalyticsPayload) -> bool:
        if not data.tasks:
            logger.error("No analytics data to validate")
            return False

        for task in data.tasks:
            if not task.validator_hotkey or not task.validator_task_id:
                logger.error("Invalid task data: missing required fields")
                return False

        return True

    @staticmethod
    def create_success_response(
        message: str, task_count: int
    ) -> AnalyticsSuccessResponse:
        return AnalyticsSuccessResponse(
            message=message, timestamp=datetime.now(datetime.UTC), task_count=task_count
        )

    @staticmethod
    def create_error_response(
        message: str, error: str, error_details: dict
    ) -> AnalyticsErrorResponse:
        return AnalyticsErrorResponse(
            message=message,
            timestamp=datetime.now(datetime.UTC),
            error=error,
            details=ErrorDetails(**error_details),
        )
