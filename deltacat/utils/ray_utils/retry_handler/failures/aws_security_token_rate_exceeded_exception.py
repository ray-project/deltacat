from deltacat.utils.ray_utils.retry_handler.failures.retryable_error import RetryableError


class AWSSecurityTokenRateExceededException(RetryableError):
    """
    This class represents an Exception thrown that is Retryable
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
