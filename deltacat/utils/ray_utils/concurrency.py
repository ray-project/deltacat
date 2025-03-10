import copy
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union
from deltacat.utils.placement import PlacementGroupConfig
import ray
from ray._private.ray_constants import MIN_RESOURCE_GRANULARITY
from ray.types import ObjectRef

from deltacat.utils.ray_utils.runtime import current_node_resource_key


def invoke_parallel(
    items: Iterable,
    ray_task: Callable,
    *args,
    max_parallelism: Optional[int] = 1000,
    options_provider: Callable[[int, Any], Dict[str, Any]] = None,
    kwargs_provider: Callable[[int, Any], Dict[str, Any]] = None,
    **kwargs,
) -> List[Union[ObjectRef, Tuple[ObjectRef, ...]]]:
    """
    Creates a limited number of parallel remote invocations of the given ray
    task. By default each task is provided an ordered item from the input
    collection as its first argument followed by additional ordered arguments
    and keyword arguments. If `max_parallelism` is not None, then synchronously
    waits to reach <= `max_parallelism` in-flight remote invocations before
    returning.

    Args:
        items: Iterable of items to iterate over (in-order), and
        provide as the first input argument to the given ray task.
        ray_task: Ray task to invoke.
        *args: Ordered input arguments to the Ray task to invoke. Provided
        immediately after the input item if `kwargs_provider` is None. Provided
        as the first input arguments to the Ray task if `kwargs_provider` is
        specified.
        max_parallelism: Maximum parallel remote invocations. Defaults to
        1000. Set to `None` for unlimited max parallel remote invocations.
        options_provider: Callback that takes the current item index and item
        as input and returns `ray.remote` options as output.
        kwargs_provider: Callback that takes the current item index and item as
        input and returns a dictionary of `ray.remote` keyword arguments as
        output. Keyword arguments returned override all **kwargs with the
        same key.
        **kwargs: Keyword arguments to the Ray task to invoke.
    Returns:
        List of Ray object references returned from the submitted tasks.
    """
    if max_parallelism is not None and max_parallelism <= 0:
        raise ValueError(f"Max parallelism ({max_parallelism}) must be > 0.")
    pending_ids = []
    remaining_refs = []

    for i, item in enumerate(items):
        if max_parallelism is not None and len(remaining_refs) > max_parallelism:
            _, remaining_refs = ray.wait(
                remaining_refs, num_returns=1, fetch_local=False
            )

        opt = {}
        if options_provider:
            opt = options_provider(i, item)
        if not kwargs_provider:
            pending_id = ray_task.options(**opt).remote(item, *args, **kwargs)
        else:
            kwargs_dict = kwargs_provider(i, item)
            kwargs.update(kwargs_dict)
            pending_id = ray_task.options(**opt).remote(*args, **kwargs)

        pending_ids.append(pending_id)
        remaining_refs.append(pending_id)

    return pending_ids


def current_node_options_provider(*args, **kwargs) -> Dict[str, Any]:
    """Returns a resource dictionary that can be included with ray remote
    options to pin the task or actor on the current node via:
    `foo.options(current_node_options_provider()).remote()`"""
    return {"resources": {current_node_resource_key(): MIN_RESOURCE_GRANULARITY}}


def round_robin_options_provider(
    i: int,
    item: Any,
    resource_keys: List[str],
    *args,
    resource_amount_provider: Callable[[int], int] = lambda i: MIN_RESOURCE_GRANULARITY,
    **kwargs,
) -> Dict[str, Any]:
    """Returns a resource dictionary that can be included with ray remote
    options to round robin indexed tasks or actors across a list of resource
    keys. For example, the following code round-robins 100 tasks across all
    live cluster nodes:
    ```
    resource_keys = live_node_resource_keys()
    for i in range(100):
        opt = round_robin_options_provider(i, resource_keys=resource_keys)
        foo.options(**opt).remote()
    ```
    """
    opts = kwargs.get("pg_config")
    if opts:
        new_opts = copy.deepcopy(opts)
        bundle_key_index = i % len(
            new_opts["scheduling_strategy"].placement_group.bundle_specs
        )
        new_opts["scheduling_strategy"].placement_group_bundle_index = bundle_key_index
        return new_opts
    else:
        assert resource_keys, f"No resource keys given to round robin!"
        resource_key_index = i % len(resource_keys)
        key = resource_keys[resource_key_index]
        return {"resources": {key: resource_amount_provider(resource_key_index)}}


def task_resource_options_provider(
    i: int,
    item: Any,
    resource_amount_provider: Callable[[int, Any], Dict] = lambda x: {},
    pg_config: Optional[PlacementGroupConfig] = None,
    **kwargs,
) -> Dict:
    """
    Return options that needs to be provided to each task.
    """

    options = resource_amount_provider(i, item, **kwargs)
    if pg_config:
        options_to_append = copy.deepcopy(pg_config.opts)
        bundle_key_index = i % len(
            options_to_append["scheduling_strategy"].placement_group.bundle_specs
        )
        options_to_append[
            "scheduling_strategy"
        ].placement_group_bundle_index = bundle_key_index
        options = {**options, **options_to_append}

    return options
