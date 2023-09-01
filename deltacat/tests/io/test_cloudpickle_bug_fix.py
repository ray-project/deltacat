import unittest
from typing import Any
import pyarrow as pa
import ray


class CustomObj:
    def pickle_len(self, obj: Any):
        pass


class AnyObject:
    def __init__(self) -> None:
        pass

    def pickle_len(self, obj):
        return len(ray.cloudpickle.dumps(obj))


@ray.remote
def calculate_pickled_length(custom_obj: CustomObj):

    table = pa.table({"a": pa.array(list(range(1000000)))})

    full_len = custom_obj.pickle_len(table)
    sliced_len = custom_obj.pickle_len(table[0:1])

    return [sliced_len, full_len]


class TestCloudpickleBugFix(unittest.TestCase):
    """
    This test is specifically to validate if nothing has
    changed across Ray versions regarding the cloudpickle behavior.

    If the tables are sliced, cloudpickle used to dump entire buffer.
    However, Ray has added a custom serializer to get around this problem in
    https://github.com/ray-project/ray/pull/29993 for the issue at
    https://github.com/ray-project/ray/issues/29814.

    Note: If this test fails, it indicates that you may need to address the cloudpickle
    bug before upgrading ray version.
    """

    def test_sanity(self):
        ray.init(local_mode=True, ignore_reinit_error=True)

        result = ray.get(calculate_pickled_length.remote(AnyObject()))

        self.assertTrue(result[0] < 1000)
        self.assertTrue(result[1] >= 5000000)
