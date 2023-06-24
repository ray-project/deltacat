# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import ray
from typing import Dict, Any
from dataclasses import dataclass
from deltacat import logs
import logging

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
        self.used_memory_percent = self.used_memory_bytes / self.total_memory_bytes
        self.used_object_store_memory_percent = (
            self.used_object_store_memory_bytes / self.total_object_store_memory_bytes
        )
        self.used_cpu_percent = self.used_cpu / self.total_cpu
        self.used_resources = used_resources

    @staticmethod
    def get_current_cluster_utilization() -> ClusterUtilization:
        cluster_resources = ray.cluster_resources()
        available_resources = ray.available_resources()

        return ClusterUtilization(
            cluster_resources=cluster_resources, available_resources=available_resources
        )


def log_current_cluster_utilization(log_identifier: str):
    cluster_utilization = ClusterUtilization.get_current_cluster_utilization()
    logger.info(
        f"Log ID={log_identifier} | Cluster Object store memory used: {cluster_utilization.used_object_store_memory_bytes} "
        f"which is {cluster_utilization.used_object_store_memory_percent}%"
    )
    logger.info(
        f"Log ID={log_identifier} | Total Cluster Memory used: {cluster_utilization.used_memory_bytes} which is "
        f"{cluster_utilization.used_memory_percent}%"
    )
    logger.info(
        f"Log ID={log_identifier} | Total Cluster CPU used: {cluster_utilization.used_cpu} which is "
        f"{cluster_utilization.used_cpu_percent}%"
    )
