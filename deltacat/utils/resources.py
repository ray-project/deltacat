# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import ray
import sys
from typing import Dict, Any
from dataclasses import dataclass
from deltacat import logs
import logging
from resource import getrusage, RUSAGE_SELF
import platform
import psutil


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


@dataclass
class ClusterUtilization:
    def __init__(
        self, cluster_resources: Dict[str, Any], available_resources: Dict[str, Any]
    ):
        used_resources = {}

        for key in cluster_resources:
            if (
                isinstance(cluster_resources[key], float)
                or isinstance(cluster_resources[key], int)
            ) and key in available_resources:
                used_resources[key] = cluster_resources[key] - available_resources[key]

        self.total_memory_bytes = cluster_resources.get("memory")
        self.used_memory_bytes = used_resources.get("memory")
        self.total_cpu = cluster_resources.get("CPU")
        self.used_cpu = used_resources.get("CPU")
        self.total_object_store_memory_bytes = cluster_resources.get(
            "object_store_memory"
        )
        self.used_object_store_memory_bytes = used_resources.get("object_store_memory")
        self.used_memory_percent = (
            self.used_memory_bytes / self.total_memory_bytes
        ) * 100
        self.used_object_store_memory_percent = (
            self.used_object_store_memory_bytes / self.total_object_store_memory_bytes
        ) * 100
        self.used_cpu_percent = (self.used_cpu / self.total_cpu) * 100
        self.used_resources = used_resources

    @staticmethod
    def get_current_cluster_utilization() -> ClusterUtilization:
        cluster_resources = ray.cluster_resources()
        available_resources = ray.available_resources()

        return ClusterUtilization(
            cluster_resources=cluster_resources, available_resources=available_resources
        )


def get_current_node_peak_memory_usage_in_bytes():
    """
    Returns the peak memory usage of the node in bytes. This method works across
    Windows, Darwin and Linux platforms.
    """
    current_platform = platform.system()
    if current_platform != "Windows":
        usage = getrusage(RUSAGE_SELF).ru_maxrss
        if current_platform == "Linux":
            usage = usage * 1024
        return usage
    else:
        return psutil.Process().memory_info().peak_wset


def get_size_of_object_in_bytes(obj: object) -> float:
    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        return (
            size
            + sum(map(get_size_of_object_in_bytes, obj.keys()))
            + sum(map(get_size_of_object_in_bytes, obj.values()))
        )
    if isinstance(obj, (list, tuple, set, frozenset)):
        return size + sum(map(get_size_of_object_in_bytes, obj))
    return size
