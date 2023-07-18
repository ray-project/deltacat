from deltacat.utils.ray_utils.retry_handler.retryable_error import RetryableError

class AWSSecurityTokenException(RetryableError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)