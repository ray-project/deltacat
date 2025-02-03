from deltacat.utils.ray_utils.retry_handler.retryable_error.failures import RetryableError

class AWSSecurityTokenRateExceededException(RetryableError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)
