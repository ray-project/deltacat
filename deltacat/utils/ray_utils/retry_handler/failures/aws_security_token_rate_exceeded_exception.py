from deltacat.utils.ray_utils.retry_handler.failures.retryable_error import RetryableError


class AWSSecurityTokenRateExceededException(RetryableError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
