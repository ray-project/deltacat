from deltacat.utils.ray_utils.retry_handler.retryable_error.failures import NonRetryableError

class CairnsResourceNotFound(NonRetryableError):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)