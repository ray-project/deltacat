import unittest
from deltacat.exceptions import categorize_errors
import ray
from deltacat.exceptions import (
    DependencyPyarrowCapacityError,
    NonRetryableDownloadTableError,
    RetryableError,
    NonRetryableError,
    DeltaCatTransientError,
    DependencyDaftTransientError,
    UnclassifiedDeltaCatError,
)
from daft.exceptions import DaftTransientError
from deltacat.tests.local_deltacat_storage.exceptions import (
    InvalidNamespaceError,
    LocalStorageValidationError,
)
from botocore.exceptions import NoCredentialsError
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from pyarrow.lib import ArrowCapacityError
import deltacat.tests.local_deltacat_storage as ds


class MockUnknownException(Exception):
    pass


@categorize_errors
def mock_raise_exception(exception_to_raise, deltacat_storage=ds):
    raise exception_to_raise


@retry(retry=retry_if_exception_type(NoCredentialsError), stop=stop_after_attempt(2))
def mock_tenacity_wrapped_method(exception_to_raise):
    mock_raise_exception(exception_to_raise)


@ray.remote
def mock_remote_task(exception_to_raise):
    mock_raise_exception(exception_to_raise)


class TestCategorizeErrors(unittest.TestCase):
    def test_pyarrow_exception_categorizer(self):
        self.assertRaises(
            DependencyPyarrowCapacityError,
            lambda: mock_raise_exception(ArrowCapacityError),
        )

    def test_storage_exception_categorizer(self):
        self.assertRaises(
            LocalStorageValidationError,
            lambda: mock_raise_exception(InvalidNamespaceError, deltacat_storage=ds),
        )

    def test_non_retryable_error(self):
        self.assertRaises(
            NonRetryableError,
            lambda: mock_raise_exception(NonRetryableDownloadTableError),
        )

    def test_retryable_error(self):
        self.assertRaises(RetryableError, lambda: mock_raise_exception(ConnectionError))

    def test_ray_task_returns_wrapped_exception(self):
        self.assertRaises(
            DeltaCatTransientError,
            lambda: ray.get(mock_remote_task.remote(ConnectionError)),
        )

    def test_daft_transient_error(self):
        self.assertRaises(
            DependencyDaftTransientError,
            lambda: ray.get(mock_remote_task.remote(DaftTransientError)),
        )

    def test_tenacity_underlying_error_returned(self):
        self.assertRaises(
            DeltaCatTransientError,
            lambda: mock_tenacity_wrapped_method(NoCredentialsError),
        )

    def test_unclassified_error_when_error_cannot_be_categorized(self):
        self.assertRaises(
            UnclassifiedDeltaCatError,
            lambda: ray.get(mock_remote_task.remote(MockUnknownException)),
        )

    def test_deltacat_exception_contains_attributes(self):

        try:
            mock_raise_exception(ConnectionError)
        except DeltaCatTransientError as e:
            self.assertTrue(hasattr(e, "is_retryable"))
            self.assertTrue(hasattr(e, "error_name"))
            assert e.error_name == "DeltaCatTransientError"
            return

        self.assertFalse(True)
