from typing import Any, Collection, Callable, Dict, List

import ray
from ray.types import ObjectRef


def invoke_parallel(
        items: Collection,
        ray_task: Callable,
        *args,
        max_parallelism: int = 1000,
        options_provider: Callable[[int, Any], Dict[str, Any]] = None,
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
        *args: Ordered input arguments to the Ray task to invoke (provided
        immediately after the input item).
        max_parallelism (int): Maximum parallel remote invocations. Defaults to
        1000.
        options_provider (Callable): Callback that takes the current item
        index and item as input and returns `ray.remote` options as output.
        **kwargs: Keyword arguments to the Ray task to invoke.
    Returns:
        pending_ids (List[ObjectRef]): List of Ray object references.
    """
    pending_ids = []
    for i, item in enumerate(items):
        if len(pending_ids) > max_parallelism:
            ray.wait(pending_ids, num_returns=i - max_parallelism)
        opt = {}
        if options_provider:
            opt = options_provider(i, item)
        pending_id = ray_task.options(**opt).remote(item, *args, **kwargs)
        pending_ids.append(pending_id)
    return pending_ids
