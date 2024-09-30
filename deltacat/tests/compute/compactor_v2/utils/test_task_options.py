import unittest
import ray
from deltacat.compute.compactor_v2.utils.task_options import _get_task_options


@ray.remote
def valid_func():
    return 2


@ray.remote
def throwing_func():
    raise ConnectionAbortedError()


class TestTaskOptions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True, ignore_reinit_error=True)
        super().setUpClass()

    def test_get_task_options_sanity(self):
        opts = _get_task_options(0.01, 0.01)
        result_ref = valid_func.options(**opts).remote()
        result = ray.get(result_ref)

        self.assertEqual(result, 2)

    def test_get_task_options_when_exception_is_thrown(self):
        opts = _get_task_options(0.01, 0.01)
        result_ref = throwing_func.options(**opts).remote()

        self.assertRaises(ConnectionAbortedError, lambda: ray.get(result_ref))
