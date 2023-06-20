import ray
from dataclasses import dataclass


@dataclass
class ClusterResourcesUsage:
    def __init__(
        self,
        total_memory_bytes: float,
        used_memory_bytes: float,
        total_cpu: float,
        used_cpu: float,
        total_object_store_memory_bytes: float,
        used_object_store_memory_bytes: float,
    ):
        self.total_memory_bytes = total_memory_bytes
        self.used_memory_bytes = used_memory_bytes
        self.total_cpu = total_cpu
        self.used_cpu = used_cpu
        self.total_object_store_memory_bytes = total_object_store_memory_bytes
        self.used_object_store_memory_bytes = used_object_store_memory_bytes


def get_current_cluster_resources_usage():

    cluster_resources = ray.cluster_resources()
    available_resources = ray.available_resources()

    return ClusterResourcesUsage(
        total_cpu=cluster_resources["CPU"],
        used_cpu=(cluster_resources["CPU"] - available_resources["CPU"]),
        total_memory_bytes=cluster_resources["memory"],
        used_memory_bytes=(cluster_resources["memory"] - available_resources["memory"]),
        total_object_store_memory_bytes=cluster_resources["object_store_memory"],
        used_object_store_memory_bytes=(
            cluster_resources["object_store_memory"]
            - available_resources["object_store_memory"]
        ),
    )
