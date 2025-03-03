class NoNewExpiredTasksYet(Exception):
    """Exception raised when no expired tasks are found for processing."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class ExpiredFromMoreThanExpireTo(Exception):
    """Exception raised when expired_from is more than expired_to."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class UnexpiredTasksAlreadyProcessed(Exception):
    """Exception raised when all unexpired tasks have already been processed."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class InvalidValidatorRequest(Exception):
    """Exception raised when a miner response is invalid."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class InvalidMinerResponse(Exception):
    """Exception raised when a miner response is invalid."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class InvalidCompletion(Exception):
    """Exception raised when a completion response is invalid."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class InvalidTask(Exception):
    """Exception raised when a task is invalid."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class EmptyScores(Exception):
    """Exception raised when scores are invalid."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class CreateTaskFailed(Exception):
    """Exception raised when creating a task fails."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class SetWeightsFailed(Exception):
    """Exception raised when setting weights fails."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class FatalSyntheticGenerationError(Exception):
    """
    Raised when
    - synthetic QA generation still fails even after retry attempts
    - synthetic API health check still fails even after retry attempts
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class SyntheticGenerationError(Exception):
    """Raised when synthetic QA generation fails"""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
