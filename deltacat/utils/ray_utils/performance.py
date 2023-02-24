import time
from deltacat.utils.ray_utils.collections import DistributedCounter
from typing import Any, Callable, Tuple


def invoke_with_perf_counter(
        counter: DistributedCounter,
        counter_key: Any,
        func: Callable,
        *args,
        **kwargs) -> Tuple[Any, float]:

    start = time.perf_counter()
    result = func(*args, **kwargs)
    stop = time.perf_counter()
    latency = stop - start
    counter.increment.remote(
        counter_key,
        latency)
    return result, latency
