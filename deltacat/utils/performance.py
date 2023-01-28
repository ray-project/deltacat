import time
from collections import Counter
from typing import Any, Callable, Tuple


def invoke_with_perf_counter(
    counter: Counter, counter_key: Any, func: Callable, *args, **kwargs
) -> Tuple[Any, float]:

    start = time.perf_counter()
    result = func(*args, **kwargs)
    stop = time.perf_counter()
    latency = stop - start
    counter[counter_key] += latency
    return result, latency


def timed_invocation(func: Callable, *args, **kwargs) -> Tuple[Any, float]:

    start = time.perf_counter()
    result = func(*args, **kwargs)
    stop = time.perf_counter()
    return result, stop - start
