from __future__ import annotations
from enum import Enum
from typing import Callable
import logging

import tenacity

from pyarrow.lib import ArrowException, ArrowInvalid, ArrowCapacityError

import botocore
from botocore.exceptions import BotoCoreError

import ray
from ray.exceptions import (
    RayError,
    RayTaskError,
    RuntimeEnvSetupError,
    WorkerCrashedError,
    NodeDiedError,
    OutOfMemoryError,
)

from daft.exceptions import DaftTransientError, DaftCoreException

import deltacat as dc
from deltacat import logs
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

DELTACAT_STORAGE_PARAM = "deltacat_storage"
DELTACAT_STORAGE_KWARGS_PARAM = "deltacat_storage_kwargs"


class DeltaCatErrorNames(str, Enum):

    DEPENDENCY_RAY_ERROR = "DependencyRayError"
    DEPENDENCY_RAY_WORKER_DIED_ERROR = "DependencyRayWorkerDiedError"
    DEPENDENCY_RAY_OUT_OF_MEMORY_ERROR = "DependencyRayOOMError"
    DEPENDENCY_RAY_RUNTIME_SETUP_ERROR = "DependencyRayRuntimeSetupError"
    DEPENDENCY_BOTOCORE_ERROR = "DependencyBotocoreError"
    DEPENDENCY_BOTOCORE_CONNECTION_ERROR = "DependencyBotocoreConnectionError"
    DEPENDENCY_BOTOCORE_CREDENTIAL_ERROR = "DependencyBotocoreCredentialError"
    DEPENDENCY_BOTOCORE_TIMEOUT_ERROR = "DependencyBotocoreTimeoutError"
    NON_RETRYABLE_DOWNLOAD_TABLE_ERROR = "NonRetryableDownloadTableError"
    NON_RETRYABLE_DOWNLOAD_FILE_ERROR = "NonRetryableDownloadFileError"
    NON_RETRYABLE_UPLOAD_TABLE_ERROR = "NonRetryableUploadTableError"
    NON_RETRYABLE_UPLOAD_FILE_ERROR = "NonRetryableUploadFileError"
    DEPENDENCY_PYARROW_ERROR = "DependencyPyarrowError"
    DEPENDENCY_PYARROW_INVALID_ERROR = "DependencyPyarrowInvalidError"
    DEPENDENCY_PYARROW_CAPACITY_ERROR = "DependencyPyarrowCapacityError"
    PYMEMCACHED_PUT_OBJECT_ERROR = "PymemcachedPutObjectError"
    DEPENDENCY_DAFT_ERROR = "DependencyDaftError"

    GENERAL_THROTTLING_ERROR = "GeneralThrottlingError"
    RETRYABLE_UPLOAD_TABLE_ERROR = "RetryableUploadTableError"
    RETRYABLE_UPLOAD_FILE_ERROR = "RetryableUploadFileError"
    RETRYABLE_DOWNLOAD_FILE_ERROR = "RetryableDownloadFileError"
    RETRYABLE_DOWNLOAD_TABLE_ERROR = "RetryableDownloadTableError"
    RETRYABLE_TIMEOUT_ERROR = "RetryableTimeoutError"
    DEPENDENCY_DAFT_TRANSIENT_ERROR = "DependencyDaftTransientError"

    VALIDATION_ERROR = "ValidationError"
    CONTENT_TYPE_VALIDATION_ERROR = "ContentTypeValidationError"

    DELTACAT_SYSTEM_ERROR = "DeltaCatSystemError"
    DELTACAT_TRANSIENT_ERROR = "DeltaCatTransientError"
    UNCLASSIFIED_DELTACAT_ERROR = "UnclassifiedDeltaCatError"
    UNRECOGNIZED_RAY_TASK_ERROR = "UnrecognizedRayTaskError"

    NAMESPACE_NOT_FOUND_ERROR = "NamespaceNotFoundError"
    TABLE_NOT_FOUND_ERROR = "TableNotFoundError"
    TABLE_VERSION_NOT_FOUND_ERROR = "TableVersionNotFoundError"
    STREAM_NOT_FOUND_ERROR = "StreamNotFoundError"
    DELTA_NOT_FOUND_ERROR = "DeltaNotFoundError"
    TABLE_ALREADY_EXISTS_ERROR = "TableAlreadyExistsError"
    NAMESPACE_ALREADY_EXISTS_ERROR = "NamespaceAlreadyExistsError"


class DeltaCatError(Exception):
    def __init__(self, *args, **kwargs):
        task_id, node_ip = self._get_ray_task_id_and_node_ip()
        self.task_id = task_id
        self.node_ip = node_ip
        super().__init__(*args, **kwargs)

    def _get_ray_task_id_and_node_ip(self):
        task_id = get_current_ray_task_id()
        node_ip = ray.util.get_node_ip_address()
        return task_id, node_ip


class NonRetryableError(DeltaCatError):
    is_retryable = False


class RetryableError(DeltaCatError):
    is_retryable = True


class ValidationError(NonRetryableError):
    error_name = DeltaCatErrorNames.VALIDATION_ERROR.value


class UnclassifiedDeltaCatError(NonRetryableError):
    error_name = DeltaCatErrorNames.UNCLASSIFIED_DELTACAT_ERROR.value


class DependencyRayError(NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_RAY_ERROR.value


class DeltaCatTransientError(RetryableError):
    error_name = DeltaCatErrorNames.DELTACAT_TRANSIENT_ERROR.value


class DependencyDaftError(NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_DAFT_ERROR.value


class DependencyRayWorkerDiedError(RetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_RAY_WORKER_DIED_ERROR.value


class DependencyRayOutOfMemoryError(RetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_RAY_OUT_OF_MEMORY_ERROR.value


class DependencyRayRuntimeSetupError(RetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_RAY_RUNTIME_SETUP_ERROR.value


class DependencyPyarrowError(NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_PYARROW_ERROR.value


class DependencyPyarrowInvalidError(NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_PYARROW_INVALID_ERROR.value


class DependencyPyarrowCapacityError(NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_PYARROW_CAPACITY_ERROR.value


class PymemcachedPutObjectError(RetryableError):
    error_name = DeltaCatErrorNames.PYMEMCACHED_PUT_OBJECT_ERROR.value


class ContentTypeValidationError(NonRetryableError):
    error_name = DeltaCatErrorNames.CONTENT_TYPE_VALIDATION_ERROR.value


class DependencyBotocoreError(NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_BOTOCORE_ERROR.value


class DependencyBotocoreConnectionError(DeltaCatTransientError):
    error_name = DeltaCatErrorNames.DEPENDENCY_BOTOCORE_CONNECTION_ERROR.value


class DependencyBotocoreCredentialError(DeltaCatTransientError):
    error_name = DeltaCatErrorNames.DEPENDENCY_BOTOCORE_CREDENTIAL_ERROR.value


class DependencyBotocoreTimeoutError(DeltaCatTransientError):
    error_name = DeltaCatErrorNames.DEPENDENCY_BOTOCORE_TIMEOUT_ERROR.value


class NonRetryableDownloadFileError(NonRetryableError):
    error_name = DeltaCatErrorNames.NON_RETRYABLE_DOWNLOAD_FILE_ERROR.value


class NonRetryableDownloadTableError(NonRetryableDownloadFileError):
    error_name = DeltaCatErrorNames.NON_RETRYABLE_DOWNLOAD_TABLE_ERROR.value


class NonRetryableUploadFileError(NonRetryableError):
    error_name = DeltaCatErrorNames.NON_RETRYABLE_UPLOAD_FILE_ERROR.value


class NonRetryableUploadTableError(NonRetryableUploadFileError):
    error_name = DeltaCatErrorNames.NON_RETRYABLE_UPLOAD_TABLE_ERROR.value


class GeneralThrottlingError(RetryableError):
    error_name = DeltaCatErrorNames.GENERAL_THROTTLING_ERROR.value


class RetryableUploadFileError(RetryableError):
    error_name = DeltaCatErrorNames.RETRYABLE_UPLOAD_FILE_ERROR.value


class RetryableUploadTableError(RetryableUploadFileError):
    error_name = DeltaCatErrorNames.RETRYABLE_UPLOAD_TABLE_ERROR.value


class RetryableDownloadFileError(RetryableError):
    error_name = DeltaCatErrorNames.RETRYABLE_DOWNLOAD_FILE_ERROR.value


class RetryableDownloadTableError(RetryableDownloadFileError):
    error_name = DeltaCatErrorNames.RETRYABLE_DOWNLOAD_TABLE_ERROR.value


class RetryableTimeoutError(RetryableError):
    error_name = DeltaCatErrorNames.RETRYABLE_TIMEOUT_ERROR.value


class DependencyDaftTransientError(RetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_DAFT_TRANSIENT_ERROR.value


class DeltaCatSystemError(NonRetryableError):
    error_name = DeltaCatErrorNames.DELTACAT_SYSTEM_ERROR.value


class UnrecognizedRayTaskError(NonRetryableError):
    error_name = DeltaCatErrorNames.UNRECOGNIZED_RAY_TASK_ERROR.value


class NamespaceNotFoundError(NonRetryableError):
    error_name = DeltaCatErrorNames.NAMESPACE_NOT_FOUND_ERROR.value


class TableNotFoundError(NonRetryableError):
    error_name = DeltaCatErrorNames.TABLE_NOT_FOUND_ERROR.value


class TableVersionNotFoundError(NonRetryableError):
    error_name = DeltaCatErrorNames.TABLE_VERSION_NOT_FOUND_ERROR.value


class StreamNotFoundError(NonRetryableError):
    error_name = DeltaCatErrorNames.STREAM_NOT_FOUND_ERROR.value


class DeltaNotFoundError(NonRetryableError):
    error_name = DeltaCatErrorNames.DELTA_NOT_FOUND_ERROR.value


class TableAlreadyExistsError(NonRetryableError):
    error_name = DeltaCatErrorNames.TABLE_ALREADY_EXISTS_ERROR.value


class NamespaceAlreadyExistsError(NonRetryableError):
    error_name = DeltaCatErrorNames.TABLE_ALREADY_EXISTS_ERROR.value


def categorize_errors(func: Callable):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except BaseException as e:
            deltacat_storage = None
            deltacat_storage_kwargs = {}
            all_args = args
            if kwargs:
                deltacat_storage = kwargs.get(DELTACAT_STORAGE_PARAM)
                deltacat_storage_kwargs = kwargs.get(DELTACAT_STORAGE_KWARGS_PARAM, {})
                all_args = all_args + tuple(kwargs.values())

            if not deltacat_storage and all_args:
                for arg in all_args:
                    if (
                        isinstance(arg, dict)
                        and arg.get(DELTACAT_STORAGE_PARAM) is not None
                    ):
                        deltacat_storage = arg.get(DELTACAT_STORAGE_PARAM)
                        deltacat_storage_kwargs = arg.get(
                            DELTACAT_STORAGE_KWARGS_PARAM, {}
                        )
                        break

            categorize_deltacat_exception(e, deltacat_storage, deltacat_storage_kwargs)

    return wrapper


def categorize_deltacat_exception(
    e: BaseException,
    deltacat_storage: dc.storage.interface = None,
    deltacat_storage_kwargs: dict = None,
):
    if deltacat_storage_kwargs is None:
        deltacat_storage_kwargs = {}

    if isinstance(e, DeltaCatError):
        raise e
    elif deltacat_storage and deltacat_storage.can_categorize(
        e, **deltacat_storage_kwargs
    ):
        deltacat_storage.raise_categorized_error(e, **deltacat_storage_kwargs)
    elif isinstance(e, RayError):
        _categorize_ray_error(e)
    elif isinstance(e, tenacity.RetryError):
        _categorize_tenacity_error(e)
    elif isinstance(e, ArrowException):
        _categorize_dependency_pyarrow_error(e)
    elif isinstance(e, AssertionError):
        _categorize_assertion_error(e)
    elif isinstance(e, DaftCoreException):
        _categorize_daft_error(e)
    elif isinstance(e, BotoCoreError):
        _categorize_botocore_error(e)
    else:
        _categorize_all_remaining_errors(e)

    logger.error(f"Error categorization failed for {e}.", exc_info=True)
    raise UnclassifiedDeltaCatError(
        "Error could not categorized into DeltaCat error"
    ) from e


def _categorize_ray_error(e: RayError):
    if isinstance(e, RuntimeEnvSetupError):
        raise DependencyRayRuntimeSetupError("Ray failed to setup runtime env.") from e
    elif isinstance(e, WorkerCrashedError) or isinstance(e, NodeDiedError):
        raise DependencyRayWorkerDiedError("Ray worker died unexpectedly.") from e
    elif isinstance(e, OutOfMemoryError):
        raise DependencyRayOutOfMemoryError("Ray worker Out Of Memory.") from e
    elif isinstance(e, RayTaskError):
        if e.cause is not None and isinstance(e.cause, Exception):
            categorize_deltacat_exception(e.cause)
        else:
            raise UnrecognizedRayTaskError(
                "Unrecognized underlying error detected in a Ray task."
            ) from e
    else:
        raise DependencyRayError("Dependency Ray error occurred.") from e


def _categorize_tenacity_error(e: tenacity.RetryError):
    if e.__cause__ is not None and isinstance(e.__cause__, Exception):
        categorize_deltacat_exception(e.__cause__)
    else:
        raise RetryableError("Unrecognized retryable error occurred.") from e


def _categorize_dependency_pyarrow_error(e: ArrowException):
    if isinstance(e, ArrowInvalid):
        raise DependencyPyarrowInvalidError(
            f"Pyarrow Invalid error occurred. {e}"
        ) from e
    elif isinstance(e, ArrowCapacityError):
        raise DependencyPyarrowCapacityError("Pyarrow Capacity error occurred.") from e
    else:
        raise DependencyPyarrowError("Pyarrow error occurred.") from e


def _categorize_assertion_error(e: BaseException):
    raise ValidationError(f"One of the assertions in DeltaCAT has failed. {e}") from e


def _categorize_daft_error(e: DaftCoreException):
    if isinstance(e, DaftTransientError):
        raise DependencyDaftTransientError("Daft Transient error occurred.") from e
    elif isinstance(e, DaftCoreException):
        raise DependencyDaftError("Daft error occurred.") from e


def _categorize_botocore_error(e: BotoCoreError):
    if isinstance(e, botocore.exceptions.ConnectionError) or isinstance(
        e, botocore.exceptions.HTTPClientError
    ):
        raise DependencyBotocoreConnectionError(
            "Botocore connection error occurred."
        ) from e
    elif isinstance(e, botocore.exceptions.CredentialRetrievalError) or isinstance(
        e, botocore.exceptions.NoCredentialsError
    ):
        raise DependencyBotocoreCredentialError(
            "Botocore credential retrieval failed"
        ) from e
    elif isinstance(e, botocore.exceptions.ReadTimeoutError) or isinstance(
        e, botocore.exceptions.ConnectTimeoutError
    ):
        raise DependencyBotocoreTimeoutError("Botocore connection timed out.") from e
    else:
        raise DependencyBotocoreError("Botocore error occurred.") from e


def _categorize_all_remaining_errors(e: BaseException):
    if isinstance(e, ConnectionError):
        raise DeltaCatTransientError("Connection error has occurred.") from e
    elif isinstance(e, TimeoutError):
        raise DeltaCatTransientError("Timeout error has occurred.") from e
    elif isinstance(e, OSError):
        raise DeltaCatTransientError("OSError occurred.") from e
    elif isinstance(e, SystemExit):
        raise DeltaCatSystemError("Unexpected System error occurred.") from e
