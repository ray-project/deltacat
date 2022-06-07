from typing import Dict, Any

import math


MB_IN_BYTES = 1_000_000


class ClusterSizeSuggester:
    def __init__(self,
                 cluster_memory_bytes: float = None,
                 heap_memory_alloc_ratio: float = 0.7,  # Ray defaults
                 object_store_memory_alloc_ratio: float = 0.3,  # Ray defaults
                 instance_type: str = "r5.8xlarge"):
        """Given the total required memory constraints, constructs a helper class that
        recommends an instance type, number of instance nodes, max usable heap and object store memory,

        Args:
            cluster_memory_bytes: Total memory needed for the cluster.
                If not provided, defaults to memory size of instance type.
            heap_memory_alloc_ratio: Optional. Set to 0.7 by default.
            object_store_memory_alloc_ratio: Optional. Set to 0.3 by default.
            instance_type: Optional. Set to r5.8xlarge by default, to allow for up to 8GB of memory per vCPU.
                # TODO: suggest various r5 instance types based on memory input
        """
        self._instance_type = instance_type
        self.cluster_memory_bytes = cluster_memory_bytes if cluster_memory_bytes else self.get_node_memory_size()
        self.heap_memory_alloc_ratio = heap_memory_alloc_ratio
        self.object_store_memory_alloc_ratio = object_store_memory_alloc_ratio

    @property
    def instance_type(self):
        return self._instance_type

    def get_instance_type_specifications(self) -> Dict[str, Any]:
        """Assumes r5.8xlarge instances (for now)

        Returns: a dict of hardware details

        """
        # TODO: call ec2 describe-instance-types to extract hardware details (vCPUs, memory, network bandwidth).
        #  Current implementation assumes we only serve r5.8xlarge node types.
        if self.instance_type == "r5.8xlarge":
            return {
                # Intentionally mimic the output format of describe-instance-types API
                "VCpuInfo": {
                    "DefaultVCpus": 32,
                    "DefaultCores": 16,
                    "DefaultThreadsPerCore": 2,
                },
                "MemoryInfo": {
                    "SizeInMiB": 262144
                }
            }

    def get_num_vcpu_per_node(self):
        spec = self.get_instance_type_specifications()
        return spec["VCpuInfo"]["DefaultVCpus"]

    def get_node_memory_size(self):
        spec = self.get_instance_type_specifications()
        return spec["MemoryInfo"]["SizeInMiB"] * MB_IN_BYTES

    def get_max_memory_per_vcpu(self):
        return self.get_node_memory_size() / self.get_num_vcpu_per_node()

    def get_node_max_object_store_memory(self):
        return self.object_store_memory_alloc_ratio * self.get_node_memory_size()

    def get_node_max_heap_memory(self):
        return self.heap_memory_alloc_ratio * self.get_node_memory_size()

    def get_suggested_vcpu_count(self):
        return self.cluster_memory_bytes / self.get_max_memory_per_vcpu()

    def get_suggested_node_size(self):
        return math.ceil(self.cluster_memory_bytes / (self.get_num_vcpu_per_node() * self.get_max_memory_per_vcpu()))


class InstanceTypeSuggester:
    def __init__(self):
        raise NotImplementedError("Instance Type Suggester is not implemented.")