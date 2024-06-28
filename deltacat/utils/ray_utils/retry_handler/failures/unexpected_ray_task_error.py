from deltacat.utils.ray_utils.retry_handler.failures.non_retryable_error import NonRetryableError

class UnexpectedRayTaskError(NonRetryableError):
    """
    An error class that denotes that operation cannot be completed because of Unexpected Ray task error
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)