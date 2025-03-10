from unittest import TestCase

import ray.util.state
from deltacat.utils.ray_utils.concurrency import invoke_parallel
import ray
import time


@ray.remote
def f(x):
    return time.sleep(x)


class TestInvokeParallel(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        ray.init(local_mode=True, ignore_reinit_error=True)

    def test_invoke_parallel_when_max_parallelism_is_list_size(self):

        items = [1 for i in range(3)]

        result = invoke_parallel(items=items, ray_task=f, max_parallelism=3)

        self.assertEqual(3, len(result))

    def test_invoke_parallel_when_max_parallelism_is_less_than_list_size(self):
        @ray.remote
        def f(x):
            return x

        items = [i for i in range(3)]

        result = invoke_parallel(items=items, ray_task=f, max_parallelism=1)

        self.assertEqual(3, len(result))

    def test_invoke_parallel_with_options_provider(self):

        items = [1 for i in range(1)]

        result = invoke_parallel(
            items=items,
            ray_task=f,
            max_parallelism=10,
            options_provider=lambda x, y: {"memory": 1},
        )

        self.assertEqual(1, len(result))
