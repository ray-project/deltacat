class RetryableError(BaseException):
    is_retryable = True


class NonRetryableError(BaseException):
    is_retryable = False


class LocalStorageError(Exception):
    msg = "A Local storage error occurred."
    DEFAULT_ERROR_MESSAGE_WITH_ERRORCODE = (
        "Error Name: {error_name}. Is Retryable Error: {is_retryable}. "
    )

    def __init__(self, **kwargs):
        msg = kwargs.get("msg", self.msg)
        self.msg = self.DEFAULT_ERROR_MESSAGE_WITH_ERRORCODE + msg
        self.error_info = self.msg.format(
            **kwargs, error_name=self.error_name, is_retryable=self.is_retryable
        )
        Exception.__init__(self, self.error_info)


class InvalidNamespaceError(LocalStorageError, NonRetryableError):
    error_name = "InvalidNamespaceError"


class LocalStorageValidationError(LocalStorageError, NonRetryableError):
    error_name = "LocalStorageValidationError"
