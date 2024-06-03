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
        "Error Code: {error_name}. Is Retryable Error: {is_retryable}. "
    )

    def __init__(self, **kwargs):
        msg = kwargs.get("msg", self.msg)
        self.msg = self.DEFAULT_ERROR_MESSAGE_WITH_ERRORCODE + msg
        self.error_info = self.msg.format(
            **kwargs, error_name=self.error_name, is_retryable=self.is_retryable
        )
        Exception.__init__(self, self.error_info)
        self.kwargs = kwargs

    def __reduce__(self):
        return _pack_all_args, (self.__class__, None, self.kwargs)


class DeltaCatErrorNames(str, Enum):

    # Dependency Error
    GENERAL_DEPENDENCY_ERROR = "GeneralDependencyError"
    DEPENDENCY_RAY_ERROR = "DependencyRayError"
    DEPENDENCY_RAY_WORKER_DIED_ERROR = "DependencyRayWorkerDiedError"
    DEPENDENCY_RAY_OUT_OF_MEMORY_ERROR = "DependencyRayOOMError"
    DEPENDENCY_RAY_RUNTIME_SETUP_ERROR = "DependencyRayRuntimeSetupError"
    DEPENDENCY_BOTOCORE_ERROR = "DependencyBotocoreError"
    DOWNLOAD_TABLE_ERROR = "DownloadTableError"
    UPLOAD_TABLE_ERROR = "UploadTableError"
    DEPENDENCY_PYARROW_ERROR = "DependencyPyarrowError"
    DEPENDENCY_PYARROW_INVALID_ERROR = "DependencyPyarrowInvalidError"
    DEPENDENCY_PYARROW_CAPACITY_ERROR = "DependencyPyarrowCapacityError"
    PYMEMCACHED_PUT_OBJECT_ERROR = "PymemcachedPutObjectError"
    DEPENDENCY_DAFT_ERROR = "DependencyDaftError"
    GENERAL_ASSERTION_ERROR = "GeneralAssertionError"

    # Storage Error
    GENERAL_STORAGE_ERROR = "GeneralStorageError"
    STORAGE_CONCURRENT_MODIFICATION_ERROR = "StorageConcurrentModificationError"

    # Retryable Error
    GENERAL_THROTTLING_ERROR = "GeneralThrottlingError"
    RETRYABLE_UPLOAD_TABLE_ERROR = "RetryableUploadTableError"
    RETRYABLE_DOWNLOAD_TABLE_ERROR = "RetryableDownload"
    RETRYABLE_TIMEOUT_ERROR = "RetryableTimeoutError"
    DEPENDENCY_DAFT_TRANSIENT_ERROR = "DependencyDaftTransientError"

    # Validation Error
    GENERAL_VALIDATION_ERROR = "GeneralValidationError"
    CONTENT_TYPE_VALIDATION_ERROR = "ContentTypeValidationError"


class NonRetryableError(Exception):
    is_retryable = False


class RetryableError(Exception):
    is_retryable = True


# >>> example: raise DependencyRayError(ray_task="x")
#
# >>> __main__.DependencyRayError: Error Name: xx. Is Retryable Error: False. A dependency Ray error occurred, during ray_task: x
class DependencyRayError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_RAY_ERROR.value


class DependencyDaftError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_DAFT_ERROR.value


class DependencyRayWorkerDiedError(DeltaCatError, RetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_RAY_WORKER_DIED_ERROR.value


class DependencyRayOutOfMemoryError(DeltaCatError, RetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_RAY_OUT_OF_MEMORY_ERROR.value


class DependencyRayRuntimeSetupError(DeltaCatError, RetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_RAY_RUNTIME_SETUP_ERROR.value


class DependencyPyarrowError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_PYARROW_ERROR.value


class DependencyPyarrowInvalidError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_PYARROW_INVALID_ERROR.value


class DependencyPyarrowCapacityError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_PYARROW_CAPACITY_ERROR.value


class PymemcachedPutObjectError(DeltaCatError, RetryableError):
    error_name = DeltaCatErrorNames.PYMEMCACHED_PUT_OBJECT_ERROR.value


class GeneralValidationError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.GENERAL_VALIDATION_ERROR.value


class ContentTypeValidationError(GeneralValidationError, NonRetryableError):
    error_name = DeltaCatErrorNames.CONTENT_TYPE_VALIDATION_ERROR.value


class GeneralStorageError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.GENERAL_STORAGE_ERROR.value


class DependencyBotocoreError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_BOTOCORE_ERROR.value


class DownloadTableError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.DOWNLOAD_TABLE_ERROR.value


class UploadTableError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.UPLOAD_TABLE_ERROR.value


class GeneralThrottlingError(DeltaCatError, RetryableError):
    error_name = DeltaCatErrorNames.GENERAL_THROTTLING_ERROR.value


class RetryableUploadTableError(GeneralThrottlingError, RetryableError):
    error_name = DeltaCatErrorNames.RETRYABLE_UPLOAD_TABLE_ERROR.value


class RetryableDownloadTableError(GeneralThrottlingError, RetryableError):
    error_name = DeltaCatErrorNames.RETRYABLE_DOWNLOAD_TABLE_ERROR.value


class RetryableTimeoutError(DeltaCatError, RetryableError):
    error_name = DeltaCatErrorNames.RETRYABLE_TIMEOUT_ERROR.value


class DependencyDaftTransientError(DependencyDaftError, RetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_DAFT_TRANSIENT_ERROR.value


class GeneralAssertionError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.GENERAL_ASSERTION_ERROR.value
