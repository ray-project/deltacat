from __future__ import annotations
from enum import Enum


def _pack_all_args(exception_cls, args=None, kwargs=None):
    # This is helpful for reducing Exceptions that only accept kwargs as
    # only positional arguments can be provided for __reduce__
    if args is None:
        args = ()
    if kwargs is None:
        kwargs = {}
    return exception_cls(*args, **kwargs)


class DeltaCatError(Exception):
    msg = "A DeltaCat error occurred."
    DEFAULT_ERROR_MESSAGE_WITH_ERRORCODE = (
        "Error Code: {error_code}. Is Retryable Error: {is_retryable}. "
    )

    def __init__(self, **kwargs):
        msg = kwargs.get("msg", self.msg)
        self.msg = self.DEFAULT_ERROR_MESSAGE_WITH_ERRORCODE + msg
        self.error_info = self.msg.format(
            **kwargs, error_code=self.error_code, is_retryable=self.is_retryable
        )
        Exception.__init__(self, self.error_info)
        self.kwargs = kwargs

    def __reduce__(self):
        return _pack_all_args, (self.__class__, None, self.kwargs)


class DeltaCatErrorCodes(str, Enum):

    # Dependency Error code from 10100 to 10199
    GENERAL_DEPENDENCY_ERROR = "10100"
    DEPENDENCY_RAY_ERROR = "10101"
    DEPENDENCY_RAY_WORKER_DIED_ERROR = "10102"
    DEPENDENCY_RAY_OUT_OF_MEMORY_ERROR = "10103"
    DEPENDENCY_RAY_RUNTIME_SETUP_ERROR = "10104"
    DEPENDENCY_BOTOCORE_ERROR = "10131"
    DOWNLOAD_TABLE_ERROR = "10132"
    UPLOAD_TABLE_ERROR = "10133"
    DEPENDENCY_PYARROW_ERROR = "10151"
    DEPENDENCY_PYARROW_INVALID_ERROR = "10152"
    DEPENDENCY_PYARROW_CAPACITY_ERROR = "10153"
    PYMEMCACHED_PUT_OBJECT_ERROR = "10170"
    DEPENDENCY_DAFT_ERROR = "10180"
    GENERAL_ASSERTION_ERROR = "10190"

    # Storage Error code from 10300 to 10399
    GENERAL_STORAGE_ERROR = "10300"
    STORAGE_CONCURRENT_MODIFICATION_ERROR = "10301"

    # Retryable Error code from 10500 to 10599
    GENERAL_THROTTLING_ERROR = "10500"
    RETRYABLE_UPLOAD_TABLE_ERROR = "10501"
    RETRYABLE_DOWNLOAD_TABLE_ERROR = "10502"
    RETRYABLE_TIMEOUT_ERROR = "10503"

    # Validation Error code from 10700 to 10799
    GENERAL_VALIDATION_ERROR = "10700"
    CONTENT_TYPE_VALIDATION_ERROR = "10701"


class NonRetryableError(Exception):
    is_retryable = False


class RetryableError(Exception):
    is_retryable = True


# >>> example: raise DependencyRayError(ray_task="x")
#
# >>> __main__.DependencyRayError: Error Code: 10101. Is Retryable Error: False. A dependency Ray error occurred, during ray_task: x
class DependencyRayError(DeltaCatError, NonRetryableError):
    error_code = DeltaCatErrorCodes.DEPENDENCY_RAY_ERROR.value


class DependencyDaftError(DeltaCatError, NonRetryableError):
    error_code = DeltaCatErrorCodes.DEPENDENCY_DAFT_ERROR.value


class DependencyRayWorkerDiedError(DeltaCatError, RetryableError):
    error_code = DeltaCatErrorCodes.DEPENDENCY_RAY_WORKER_DIED_ERROR.value


class DependencyRayOutOfMemoryError(DeltaCatError, RetryableError):
    error_code = DeltaCatErrorCodes.DEPENDENCY_RAY_OUT_OF_MEMORY_ERROR.value


class DependencyRayRuntimeSetupError(DeltaCatError, RetryableError):
    error_code = DeltaCatErrorCodes.DEPENDENCY_RAY_RUNTIME_SETUP_ERROR.value


class DependencyPyarrowError(DeltaCatError, NonRetryableError):
    error_code = DeltaCatErrorCodes.DEPENDENCY_PYARROW_ERROR.value


class DependencyPyarrowInvalidError(DeltaCatError, NonRetryableError):
    error_code = DeltaCatErrorCodes.DEPENDENCY_PYARROW_INVALID_ERROR.value


class DependencyPyarrowCapacityError(DeltaCatError, NonRetryableError):
    error_code = DeltaCatErrorCodes.DEPENDENCY_PYARROW_CAPACITY_ERROR.value


class PymemcachedPutObjectError(DeltaCatError, RetryableError):
    error_code = DeltaCatErrorCodes.PYMEMCACHED_PUT_OBJECT_ERROR.value


class GeneralValidationError(DeltaCatError, NonRetryableError):
    error_code = DeltaCatErrorCodes.GENERAL_VALIDATION_ERROR.value


class ContentTypeValidationError(GeneralValidationError, NonRetryableError):
    error_code = DeltaCatErrorCodes.CONTENT_TYPE_VALIDATION_ERROR.value


class GeneralStorageError(DeltaCatError, NonRetryableError):
    error_code = DeltaCatErrorCodes.GENERAL_STORAGE_ERROR.value


class DependencyBotocoreError(DeltaCatError, NonRetryableError):
    error_code = DeltaCatErrorCodes.DEPENDENCY_BOTOCORE_ERROR.value


class DownloadTableError(DeltaCatError, NonRetryableError):
    error_code = DeltaCatErrorCodes.DOWNLOAD_TABLE_ERROR.value


class UploadTableError(DeltaCatError, NonRetryableError):
    error_code = DeltaCatErrorCodes.UPLOAD_TABLE_ERROR.value


class GeneralThrottlingError(DeltaCatError, RetryableError):
    error_code = DeltaCatErrorCodes.GENERAL_THROTTLING_ERROR.value


class RetryableUploadTableError(GeneralThrottlingError, RetryableError):
    error_code = DeltaCatErrorCodes.RETRYABLE_UPLOAD_TABLE_ERROR.value


class RetryableDownloadTableError(GeneralThrottlingError, RetryableError):
    error_code = DeltaCatErrorCodes.RETRYABLE_DOWNLOAD_TABLE_ERROR.value


class RetryableTimeoutError(DeltaCatError, RetryableError):
    error_code = DeltaCatErrorCodes.RETRYABLE_TIMEOUT_ERROR.value


class GeneralAssertionError(DeltaCatError, NonRetryableError):
    error_code = DeltaCatErrorCodes.GENERAL_ASSERTION_ERROR.value
