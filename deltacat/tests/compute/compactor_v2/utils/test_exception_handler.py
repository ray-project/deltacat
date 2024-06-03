import unittest
from deltacat.compute.compactor_v2.utils.exception_handler import (
    handle_exception,
)

from deltacat.exceptions import (
    DependencyPyarrowCapacityError,
)
from pyarrow.lib import ArrowCapacityError


class TestExceptionHandler(unittest.TestCase):
    @handle_exception
    def mock_raise_exception(self, exception_to_raise):
        raise exception_to_raise

    def test_exception_handler(self):
        try:
            self.mock_raise_exception(ArrowCapacityError)
        except BaseException as e:
            assert isinstance(e, DependencyPyarrowCapacityError)
