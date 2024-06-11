from __future__ import annotations
from enum import Enum
import botocore
import ray
import logging
import tenacity
from deltacat import logs
from ray.exceptions import (
    RayError,
    RayTaskError,
    RuntimeEnvSetupError,
    WorkerCrashedError,
    NodeDiedError,
    OutOfMemoryError,
)
from pyarrow.lib import ArrowException, ArrowInvalid, ArrowCapacityError
from botocore.exceptions import BotoCoreError
from typing import Callable
from deltacat.utils.ray_utils.runtime import (
    get_current_ray_task_id,
)
from daft.exceptions import DaftTransientError, DaftCoreException

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


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
    DEFAULT_ERROR_MESSAGE_WITH_ERRORCODE = "Error Name: {error_name}. Is Retryable Error: {is_retryable}. On node_ip:{node_ip} with task_id:{task_id}. "

    def __init__(self, **kwargs):
        msg = kwargs.get("msg", self.msg)
        self.msg = self.DEFAULT_ERROR_MESSAGE_WITH_ERRORCODE + msg
        task_id, node_ip = self._get_ray_task_id_and_node_ip()

        self.error_info = self.msg.format(
            **kwargs,
            error_name=self.error_name,
            is_retryable=self.is_retryable,
            task_id=task_id,
            node_ip=node_ip,
        )
        Exception.__init__(self, self.error_info)
        self.kwargs = kwargs

    def _get_ray_task_id_and_node_ip(self):
        task_id = get_current_ray_task_id()
        node_ip = ray.util.get_node_ip_address()
        return task_id, node_ip

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

    # Storage Error
    GENERAL_STORAGE_ERROR = "GeneralStorageError"
    STORAGE_CONCURRENT_MODIFICATION_ERROR = "StorageConcurrentModificationError"

    # Retryable Error
    GENERAL_THROTTLING_ERROR = "GeneralThrottlingError"
    RETRYABLE_UPLOAD_TABLE_ERROR = "RetryableUploadTableError"
    RETRYABLE_DOWNLOAD_TABLE_ERROR = "RetryableDownload"
    RETRYABLE_TIMEOUT_ERROR = "RetryableTimeoutError"
    RETRYABLE_RAY_TASK_ERROR = "RetryableRayTaskError"
    RETRYABLE_DAFT_TRANSIENT_ERROR = "RetryableDaftTransientError"

    # Validation Error
    VALIDATION_ERROR = "ValidationError"
    GENERAL_VALIDATION_ERROR = "GeneralValidationError"
    CONTENT_TYPE_VALIDATION_ERROR = "ContentTypeValidationError"

    UNEXPECTED_SYSTEM_ERROR = "UnexpectedSystemError"
    UNCLASSIFIED_DELTACAT_ERROR = "UnclassifiedDeltaCatError"


class NonRetryableError(BaseException):
    is_retryable = False


class RetryableError(BaseException):
    is_retryable = True


class ValidationError(NonRetryableError):
    error_name = DeltaCatErrorNames.VALIDATION_ERROR.value


class UnclassifiedDeltaCatError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.UNCLASSIFIED_DELTACAT_ERROR.value


# >>> example: raise DependencyRayError(ray_task="x")
#
# >>> __main__.DependencyRayError: Error Name: xx. Is Retryable Error: False. A dependency Ray error occurred, during ray_task: x
class DependencyRayError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_RAY_ERROR.value


class DependencyDaftError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.DEPENDENCY_DAFT_ERROR.value


class RetryableRayTaskError(DeltaCatError, RetryableError):
    error_name = DeltaCatErrorNames.RETRYABLE_RAY_TASK_ERROR.value


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


class NonRetryableDownloadTableError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.DOWNLOAD_TABLE_ERROR.value


class NonRetryableUploadTableError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.UPLOAD_TABLE_ERROR.value


class GeneralThrottlingError(DeltaCatError, RetryableError):
    error_name = DeltaCatErrorNames.GENERAL_THROTTLING_ERROR.value


class RetryableUploadTableError(GeneralThrottlingError, RetryableError):
    error_name = DeltaCatErrorNames.RETRYABLE_UPLOAD_TABLE_ERROR.value


class RetryableDownloadTableError(GeneralThrottlingError, RetryableError):
    error_name = DeltaCatErrorNames.RETRYABLE_DOWNLOAD_TABLE_ERROR.value


class RetryableTimeoutError(DeltaCatError, RetryableError):
    error_name = DeltaCatErrorNames.RETRYABLE_TIMEOUT_ERROR.value


class RetryableDaftTransientError(DependencyDaftError, RetryableError):
    error_name = DeltaCatErrorNames.RETRYABLE_DAFT_TRANSIENT_ERROR.value


class UnexpectedSystemError(DeltaCatError, NonRetryableError):
    error_name = DeltaCatErrorNames.UNEXPECTED_SYSTEM_ERROR.value


# List of retryable errors that can be automatically retried by Ray remote tasks
# TODO: Retry all retryable errors
# TODO: handle wrapped tanacity retry errors
RAY_TASK_RETRYABLE_ERROR_CODES = [
    RetryableRayTaskError,
    tenacity.RetryError,
]


def categorize_errors(func: Callable):
    def categorize_compaction_exception(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except BaseException as e:
            deltacat_storage = kwargs.get("deltacat_storage")
            categorize_compaction_step_exception(e, deltacat_storage)

    return categorize_compaction_exception


def categorize_compaction_step_exception(e: BaseException, deltacat_storage=None):
    if isinstance(e, DeltaCatError):
        raise e
    elif isinstance(e, RayError):
        _categorize_ray_error(e)
    elif isinstance(e, ArrowException):
        _categorize_dependency_pyarrow_error(e)
    elif isinstance(e, AssertionError):
        _categorize_assertion_error(e)
    elif isinstance(e, DaftCoreException):
        _categorize_daft_error(e)
    elif isinstance(e, BotoCoreError):
        _categorize_botocore_error(e)
    elif deltacat_storage and deltacat_storage.can_categorize(e):
        deltacat_storage.raise_categorized_error(e)
    else:
        _categorize_all_remaining_errors(e)
    logger.error(f"Error categorization failed for {e}.")
    raise e


def _categorize_all_remaining_errors(e: BaseException):
    if isinstance(e, ConnectionError) or isinstance(e, TimeoutError):
        raise RetryableRayTaskError() from e
    elif isinstance(e, SystemExit):
        raise UnexpectedSystemError(msg="Unexpected System error occurred.") from e


def _categorize_botocore_error(e: BotoCoreError):
    if isinstance(e, botocore.exceptions.ConnectionError):
        raise RetryableRayTaskError(
            msg=f"Botocore ConnectionError occurred. Retryable by Ray task.",
        ) from e
    elif isinstance(e, botocore.exceptions.HTTPClientError):
        raise RetryableRayTaskError(
            msg=f"Botocore HTTPClientError occurred. Retryable by Ray task.",
        ) from e
    else:
        raise DependencyBotocoreError(
            msg=f"Botocore Error occurred.",
        ) from e


def _categorize_ray_error(e: RayError):
    if isinstance(e, RuntimeEnvSetupError):
        raise DependencyRayRuntimeSetupError(
            msg=f"Ray failed to setup runtime env."
        ) from e
    elif isinstance(e, WorkerCrashedError) or isinstance(e, NodeDiedError):
        raise DependencyRayWorkerDiedError(
            msg=f"Ray worker died unexpectedly.",
        ) from e
    elif isinstance(e, OutOfMemoryError):
        raise DependencyRayOutOfMemoryError(
            msg=f"Ray worker Out Of Memory.",
        ) from e
    elif isinstance(e, RayTaskError):
        # TODO: Further categorize wrapped error
        raise e.cause from e
    else:
        raise DependencyRayError(
            msg=f"Dependency Ray error occurred.",
        ) from e


def _categorize_dependency_pyarrow_error(e: ArrowException):
    if isinstance(e, ArrowInvalid):
        raise DependencyPyarrowInvalidError(
            msg=f"Pyarrow Invalid error occurred.",
        ) from e
    elif isinstance(e, ArrowCapacityError):
        raise DependencyPyarrowCapacityError(
            msg=f"Pyarrow Capacity error occurred.",
        ) from e
    else:
        raise DependencyPyarrowError(
            msg=f"Pyarrow error occurred.",
        ) from e


def _categorize_daft_error(e: DaftCoreException):
    if isinstance(e, DaftTransientError):
        raise RetryableDaftTransientError(
            msg=f"DaftTransientError occurred. Retryable by Ray task.",
        ) from e
    elif isinstance(e, DaftCoreException):
        raise DependencyDaftError(
            msg=f"Daft error occurred.",
        ) from e


def _categorize_assertion_error(e: BaseException):
    raise GeneralValidationError(
        msg=f"General validation error occurred.",
    ) from e
