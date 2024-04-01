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


class DeltaCatErrorMapping(str, Enum):

    # Dependency Error code from 10100 to 10199
    GENERAL_DEPENDENCY_ERROR = "10100"
    DEPENDENCY_RAY_ERROR = "10101"
    DEPENDENCY_RAY_WORKER_DIED_ERROR = "10102"
    DEPENDENCY_RAY_OUT_OF_MEMORY_ERROR = "10103"
    DEPENDENCY_BOTOCORE_ERROR = "10131"
    DOWNLOAD_TABLE_ERROR = "10132"
    UPLOAD_TABLE_ERROR = "10133"
    DEPENDENCY_PYARROW_ERROR = "10151"

    # Storage Error code from 10200 to 10299
    GENERAL_STORAGE_ERROR = "10200"
    STORAGE_CONCURRENT_MODIFICATION_ERROR = "10201"

    # Throttling Error code from 10300 to 10399
    GENERAL_THROTTLING_ERROR = "10300"
    UPLOAD_TABLE_THROTTLING_ERROR = "10301"
    DOWNLOAD_TABLE_THROTTLING_ERROR = "10302"

    # Validation Error code from 10400 to 10499
    GENERAL_VALIDATION_ERROR = "10400"
    CONTENT_TYPE_VALIDATION_ERROR = "10401"


class NonRetryableError(Exception):
    pass


class RetryableError(Exception):
    pass


# >>> example: raise DependencyRayError(ray_task="x")
#
# >>> __main__.DependencyRayError: Error Code: 10101. Is Retryable Error: False. A dependency Ray error occurred, during ray_task: x
class DependencyRayError(DeltaCatError):
    error_code = DeltaCatErrorMapping.DEPENDENCY_RAY_ERROR.value
    is_retryable = False


class DependencyRayWorkerDiedError(DependencyRayError):
    error_code = DeltaCatErrorMapping.DEPENDENCY_RAY_WORKER_DIED_ERROR.value
    is_retryable = False


class DependencyRayOutOfMemoryError(DependencyRayError):
    error_code = DeltaCatErrorMapping.DEPENDENCY_RAY_OUT_OF_MEMORY_ERROR.value
    is_retryable = False


class DependencyPyarrowError(DeltaCatError):
    error_code = DeltaCatErrorMapping.DEPENDENCY_PYARROW_ERROR.value
    is_retryable = False


class GeneralValidationError(DeltaCatError):
    error_code = DeltaCatErrorMapping.GENERAL_VALIDATION_ERROR.value
    is_retryable = False


class ContentTypeValidationError(GeneralValidationError):
    error_code = DeltaCatErrorMapping.CONTENT_TYPE_VALIDATION_ERROR.value
    is_retryable = False


class GeneralStorageError(DeltaCatError):
    error_code = DeltaCatErrorMapping.GENERAL_STORAGE_ERROR.value
    is_retryable = False


class StorageConcurrentModificationError(GeneralStorageError):
    error_code = DeltaCatErrorMapping.STORAGE_CONCURRENT_MODIFICATION_ERROR.value
    is_retryable = False


class DependencyBotocoreError(DeltaCatError):
    error_code = DeltaCatErrorMapping.DEPENDENCY_BOTOCORE_ERROR.value
    is_retryable = False


class DownloadTableError(DependencyBotocoreError):
    error_code = DeltaCatErrorMapping.DOWNLOAD_TABLE_ERROR.value
    is_retryable = False


class UploadTableError(DependencyBotocoreError):
    error_code = DeltaCatErrorMapping.UPLOAD_TABLE_ERROR.value
    is_retryable = False


class GeneralThrottlingError(DeltaCatError):
    error_code = DeltaCatErrorMapping.GENERAL_THROTTLING_ERROR.value
    is_retryable = True


class UploadTableThrottlingError(GeneralThrottlingError):
    error_code = DeltaCatErrorMapping.UPLOAD_TABLE_THROTTLING_ERROR.value
    is_retryable = True


class DownloadTableThrottlingError(GeneralThrottlingError):
    error_code = DeltaCatErrorMapping.DOWNLOAD_TABLE_THROTTLING_ERROR.value
    is_retryable = True
