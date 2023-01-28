import time
from typing import Any, Callable, Tuple

from deltacat.utils.ray_utils.collections import DistributedCounter


def invoke_with_perf_counter(
    counter: DistributedCounter, counter_key: Any, func: Callable, *args, **kwargs
) -> Tuple[Any, float]:

    start = time.perf_counter()
    result = func(*args, **kwargs)
    stop = time.perf_counter()
    latency = stop - start
    counter.increment.remote(counter_key, latency)
    return result, latency
