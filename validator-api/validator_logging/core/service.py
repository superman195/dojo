from typing import Dict, List

from commons.logging import logging as logger

from .models import LogEntry, LogResponse


class LoggingService:
    """Service class for handling logging operations"""

    @staticmethod
    def validate_log_entries(logs: List[LogEntry]) -> bool:
        """
        Validate log entries

        Args:
            logs: List of log entries to validate

        Returns:
            bool: True if all logs are valid, False otherwise
        """
        # Basic validation - could be extended as needed
        if not logs or len(logs) == 0:
            logger.error("No logs provided")
            return False

        for log in logs:
            if not log.message:
                logger.error("Log entry missing message")
                return False

        return True

    @staticmethod
    def create_success_response(
        message: str, validator: str, log_count: int
    ) -> LogResponse:
        """
        Create a success response

        Args:
            message: Success message
            validator: Validator hotkey
            log_count: Number of logs processed

        Returns:
            LogResponse: Success response
        """
        return LogResponse(
            status="success", message=message, validator=validator, log_count=log_count
        )

    @staticmethod
    def create_error_response(
        message: str, validator: str, error: str = None, error_details: Dict = None
    ) -> LogResponse:
        """
        Create an error response

        Args:
            message: Error message
            validator: Validator hotkey
            error: Error string
            error_details: Additional error details

        Returns:
            LogResponse: Error response
        """
        return LogResponse(
            status="error",
            message=message,
            validator=validator,
            error_details=error_details or {"error": error} if error else None,
        )
