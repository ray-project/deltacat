import ray
import logging
import time

from deltacat import logs

from typing import Any, Callable, Dict, List

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def node_resource_keys(
        filter_fn: Callable[[Dict[str, Any]], bool] = lambda n: True) \
        -> List[str]:
    """Get all Ray resource keys for cluster nodes that pass the given filter
    as a list of strings of the form: "node:{node_resource_name}". The returned
    keys can be used to place tasks or actors on that node via:
    `foo.options(resources={node_resource_keys()[0]: 0.01}).remote()`

    Returns all cluster node resource keys by default."""
    keys = []
    node_dict = ray.nodes()
    if node_dict:
        for node in ray.nodes():
            if filter_fn(node):
                for key in node["Resources"].keys():
                    if key.startswith("node:"):
                        keys.append(key)
    else:
        raise ValueError("No node dictionary found on current node.")
    return keys


def current_node_resource_key() -> str:
    """Get the Ray resource key for the current node as a string of the form:
    "node:{node_resource_name}". The returned key can be used to place tasks or
    actors on that node via:
    `foo.options(resources={get_current_node_resource_key(): 0.01}).remote()`
    """
    current_node_id = ray.get_runtime_context().node_id.hex()
    keys = node_resource_keys(lambda n: n["NodeID"] == current_node_id)
    assert len(keys) <= 1, \
        f"Expected <= 1 keys for the current node, but found {len(keys)}"
    return keys[0] if len(keys) == 1 else None


def is_node_alive(node: Dict[str, Any]) -> bool:
    """Takes a node from `ray.nodes()` as input. Returns True if the node is
    alive, and False otherwise."""
    return node["Alive"]


def live_node_count() -> int:
    """Returns the number of nodes in the cluster that are alive."""
    return sum(1 for n in ray.nodes() if is_node_alive(n))


def live_node_waiter(
        min_live_nodes: int,
        poll_interval_seconds: float = 0.5) -> None:
    """Waits until the given minimum number of live nodes are present in the
    cluster. Checks the current number of live nodes every
    `poll_interval_seconds`."""
    live_nodes = live_node_count()
    while live_nodes < min_live_nodes:
        live_nodes = live_node_count()
        logger.info(f"Waiting for Live Nodes: {live_nodes}/{min_live_nodes}")
        time.sleep(poll_interval_seconds)


def live_node_resource_keys() -> List[str]:
    """Get Ray resource keys for all live cluster nodes as a list of strings of
    the form: "node:{node_resource_name}". The returned keys can be used to
    place tasks or actors on that node via:
    `foo.options(resources={node_resource_keys()[0]: 0.01}).remote()`"""
    return node_resource_keys(lambda n: is_node_alive(n))


def other_live_node_resource_keys() -> List[str]:
    """Get Ray resource keys for all live cluster nodes except the current node
    as a list of strings of the form: "node:{node_resource_name}". The returned
    keys can be used to place tasks or actors on that node via:
    `foo.options(resources={node_resource_keys()[0]: 0.01}).remote()`

    For example, invoking this function from your Ray application driver on the
    head node returns the resource keys of all live worker nodes."""
    current_node_id = ray.get_runtime_context().node_id.hex()
    return node_resource_keys(
        lambda n: n["NodeID"] != current_node_id and is_node_alive(n)
    )


def other_node_resource_keys() -> List[str]:
    """Get Ray resource keys for all cluster nodes except the current node
    as a list of strings of the form: "node:{node_resource_name}". The returned
    keys can be used to place tasks or actors on that node via:
    `foo.options(resources={node_resource_keys()[0]: 0.01}).remote()`

    For example, invoking this function from your Ray application driver on the
    head node returns the resource keys of all worker nodes."""
    current_node_id = ray.get_runtime_context().node_id.hex()
    return node_resource_keys(lambda n: n["NodeID"] != current_node_id)


def cluster_cpus() -> int:
    """Returns the current cluster's total number of CPUs as an integer."""
    cpus = ray.cluster_resources().get("CPU")
    return int(cpus) if cpus is not None else 0


def available_cpus() -> int:
    """Returns the current cluster's number of available CPUs as an integer."""
    cpus = ray.available_resources()["CPU"]
    return int(cpus) if cpus is not None else 0


def log_cluster_resources() -> None:
    """Logs the cluster's total and available resources."""
    logger.info(f"Available Resources: {ray.available_resources()}")
    logger.info(f"Cluster Resources: {ray.cluster_resources()}")
    logger.info(f"Cluster Nodes: {ray.nodes()}")
