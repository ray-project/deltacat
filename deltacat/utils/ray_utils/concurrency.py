from typing import Collection, Callable, List

import ray
from ray._raylet import ObjectRef


def invoke_parallel(
        items: Collection,
        ray_task: Callable,
        max_parallelism: int = 1000,
        *args,
        **kwargs) -> List[ObjectRef]:
    """
    Creates a limited number of parallel remote invocations of the given ray
    task, where each task is provided an ordered item from the input collection
    as its first argument followed by additional ordered arguments and
    keyword arguments. Synchronously waits for all remote invocations to
    complete before returning.

    Args:
        items (Collection): Collection of items to iterate over (in-order), and
        provide as the first input argument to the given ray task.
        ray_task (Callable): Ray task to invoke.
        max_parallelism (int): Maximum parallel remote invocations. Defaults to
        1000.
        *args: Ordered input arguments to the Ray task to invoke (provided
        immediately after the input item).
        **kwargs: Keyword arguments to the Ray task to invoke.
    Returns:
        pending_ids (List[ObjectRef]): List of ready Ray object references.
    """
    pending_ids = []
    for i in range(len(items)):
        if len(pending_ids) > max_parallelism:
            ray.wait(pending_ids, num_returns=i - max_parallelism)
        pending_id = ray_task.remote(items[i], *args, **kwargs)
        pending_ids.append(pending_id)
    return pending_ids
