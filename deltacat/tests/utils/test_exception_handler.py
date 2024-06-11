import unittest
from deltacat.exceptions import categorize_errors

from deltacat.exceptions import (
    DependencyPyarrowCapacityError,
    RetryableDownloadTableError,
    RetryableError,
    RetryableRayTaskError,
)
from deltacat.tests.local_deltacat_storage.exceptions import (
    InvalidNamespaceError,
    LocalStorageValidationError,
)

from pyarrow.lib import ArrowCapacityError
import deltacat.tests.local_deltacat_storage as ds


class TestExceptionHandler(unittest.TestCase):
    @categorize_errors
    def mock_raise_exception(self, exception_to_raise, deltacat_storage=ds):
        raise exception_to_raise

    def test_pyarrow_exception_categorizer(self):
        try:
            self.mock_raise_exception(ArrowCapacityError)
        except BaseException as e:
            assert isinstance(e, DependencyPyarrowCapacityError)

    def test_storage_exception_categorizer(self):
        try:
            self.mock_raise_exception(InvalidNamespaceError, deltacat_storage=ds)
        except BaseException as e:
            assert isinstance(e, LocalStorageValidationError)

    def test_retryable_error(self):
        try:
            self.mock_raise_exception(RetryableDownloadTableError)
        except BaseException as e:
            assert isinstance(e, RetryableError)

    def test_retryable_ray_task_error(self):
        try:
            self.mock_raise_exception(ConnectionError)
        except BaseException as e:
            assert isinstance(e, RetryableRayTaskError)
            assert isinstance(e, RetryableError)
