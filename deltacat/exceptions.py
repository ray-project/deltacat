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
    msg = "An unexpected DeltaCat Error occurred."
    error_code = "10000"
    is_retryable = False

    def __init__(self, **kwargs):
        self.error_info = self.msg.format(
            **kwargs, error_code=self.error_code, is_retryable=self.is_retryable
        )
        Exception.__init__(self, self.error_info)

    def __reduce__(self):
        return _pack_all_args(
            (
                self.__class__,
                (self.error_code, self.is_retryable),
                self.kwargs,
            )
        )


class DeltaCatErrorMapping(Exception, Enum):

    # Dependency Error code from 10100 to 10199
    GeneralDependencyError = "10100"
    DependencyRayError = "10101"
    DependencyRayWorkerDiedError = "10102"
    DependencyBotocoreError = "10121"
    DownloadTableError = "10122"
    UploadTableError = "10123"

    # Storage Error code from 10200 to 10299
    GeneralStorageError = "10200"
    StorageConcurrentModificationError = "10201"

    # Throttling Error code from 10300 to 10399
    GeneralThrottlingError = "10300"
    UploadTableThrottlingError = "10301"
    DownloadTableThrottlingError = "10302"

    # Validation Error code from 10400 to 10499
    GeneralValidationError = "10400"
    ContentTypeValidationError = "10401"


# >>> example: raise DependencyRayError(ray_task="x")
#
# >>> __main__.DependencyRayError: Error Code: 10101. Is Retryable Error: False. A dependency Ray error occurred, during ray_task: x
class DependencyRayError(DeltaCatError):
    error_code = DeltaCatErrorMapping.DependencyRayError.value
    is_retryable = False
    msg = "Error Code: {error_code}. Is Retryable Error: {is_retryable}. A dependency Ray error occurred, during ray_task: {ray_task}."


class DependencyRayWorkerDiedError(DependencyRayError):
    error_code = DeltaCatErrorMapping.DependencyRayWorkerDiedError.value
    is_retryable = False
    msg = "Error Code: {error_code}. Is Retryable Error: {is_retryable}. Ray Worker Died Unexpectedly, during ray_task: {ray_task}."


class GeneralValidationError(DeltaCatError):
    error_code = DeltaCatErrorMapping.GeneralValidationError.value
    is_retryable = False
    msg = "Error Code: {error_code}. Is Retryable Error: {is_retryable}. A validation error occurred."


class ContentTypeValidationError(GeneralValidationError):
    error_code = DeltaCatErrorMapping.ContentTypeValidationError.value
    is_retryable = False
    msg = (
        "Error Code: {error_code}. Is Retryable Error: {is_retryable}. "
        "S3 file with content type: {content_type} and content encoding: {content_encoding} "
        "cannot be read into pyarrow.parquet.ParquetFile"
    )


class GeneralStorageError(DeltaCatError):
    error_code = DeltaCatErrorMapping.GeneralStorageError.value
    is_retryable = False
    msg = "Error Code: {error_code}. Is Retryable Error: {is_retryable}. A general storage error occurred."


class StorageConcurrentModificationError(GeneralStorageError):
    error_code = DeltaCatErrorMapping.StorageConcurrentModificationError.value
    is_retryable = False
    msg = "Error Code: {error_code}. Is Retryable Error: {is_retryable}. A storage concurrent modification error occurred."


class DependencyBotocoreError(DeltaCatError):
    error_code = DeltaCatErrorMapping.DependencyBotocoreError.value
    is_retryable = False
    msg = "Error Code: {error_code}. Is Retryable Error: {is_retryable}. A dependency botocore error occurred."


class DownloadTableError(DependencyBotocoreError):
    error_code = DeltaCatErrorMapping.DownloadTableError.value
    is_retryable = False
    msg = (
        "Error Code: {error_code}. Is Retryable Error: {is_retryable}. "
        "Botocore download table error occurred when downloading from {s3_url}."
    )


class UploadTableError(DependencyBotocoreError):
    error_code = DeltaCatErrorMapping.UploadTableError.value
    is_retryable = False
    msg = (
        "Error Code: {error_code}. Is Retryable Error: {is_retryable}. "
        "Botocore upload table error occurred when uploading to {s3_url}."
    )


class GeneralThrottlingError(DeltaCatError):
    error_code = DeltaCatErrorMapping.GeneralThrottlingError.value
    is_retryable = True
    msg = "Error Code: {error_code}. Is Retryable Error: {is_retryable}. A general throttling error occurred. Retried: {retry_attempts} times."


class UploadTableThrottlingError(GeneralThrottlingError):
    error_code = DeltaCatErrorMapping.UploadTableThrottlingError.value
    is_retryable = True
    msg = "Error Code: {error_code}. Is Retryable Error: {is_retryable}. A throttling error occurred during {upload_table_task}. Retried: {retry_attempts} times."


class DownloadTableThrottlingError(GeneralThrottlingError):
    error_code = DeltaCatErrorMapping.DownloadTableThrottlingError.value
    is_retryable = True
    msg = (
        "Error Code: {error_code}. Is Retryable Error: {is_retryable}. "
        "A throttling error occurred when downloading from {s3_url}."
    )
