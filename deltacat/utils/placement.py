import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import ray
import yaml
from ray.experimental.state.api import get_node, get_placement_group
from ray.util.placement_group import placement_group, placement_group_table
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from deltacat import logs

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

# Limitation of current node group or placement group manager
# Must run on driver or head node bc state.api needs to query dashboard api server at 127.0.0.1.
# Issue: https://github.com/ray-project/ray/issues/29959


@dataclass
class PlacementGroupConfig:
    def __init__(self, opts, resource):
        self.opts = opts
        self.resource = resource


class NodeGroupManager:
    def __init__(self, path: str, gname: str):
        """Node Group Manager
        Args:
                path: cluster yaml file
                gname: node group prefix, e.g., 'partition'
        """
        # cluster init status:
        self.NODE_GROUP_PREFIX = gname
        self.cluster_config = self._read_yaml(path)
        self.init_groups = self._cluster_node_groups(self.cluster_config)
        self.init_group_res = self._parse_node_resources()

    def _cluster_node_groups(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get Worker Groups
        Args:
                config: cluster yaml data
        Returns:
                worker groups: a dict of worker node group

        """
        avail_node_types = list(config["available_node_types"].items())
        # exclude head node type
        head_node_types = [
            nt
            for nt in avail_node_types
            if "resources" in nt[1]
            and "CPU" in nt[1]["resources"]
            and nt[1]["resources"]["CPU"] == 0
        ][0]
        worker_node_types = [x for x in avail_node_types if x != head_node_types]
        # assuming homogenous cluster
        # in future, update with fleet resource
        if len(worker_node_types) > 0:
            self.INSTANCE_TYPE = worker_node_types[0][1]["node_config"]["InstanceType"]
        return worker_node_types

    def _read_yaml(self, path: str) -> Dict[str, Any]:
        with open(path, "rt") as f:
            return yaml.safe_load(f)

    def _update_groups(self) -> List[Tuple[str, float]]:
        """
                Node groups can come and go during runtime, whenever a node group is needed, we need to check the current available groups
        Returns:
                current_groups: dict of custom resource groups
        """
        # Add 1.1 second latency to avoid inconsistency issue between raylet and head
        time.sleep(1.1)
        all_available_res = ray.available_resources()
        current_groups = [
            (k, all_available_res[k])
            for k in all_available_res.keys()
            if self.NODE_GROUP_PREFIX in k
        ]
        return current_groups

    def _parse_node_resources(self) -> Dict[str, Dict[str, float]]:
        """
        Parse resources per node to get detailed resource tighted to each node group
        Returns:
                group_res: a dict of resources, e.g., {'CPU':0,'memory':0,'object_store_memory':0}
        """
        all_available_resources = (
            ray._private.state.state._available_resources_per_node()
        )
        group_keys = [x[0] for x in self.init_groups]
        group_res = {}
        for k in group_keys:
            group_res[k] = {
                "CPU": 0,
                "memory": 0,
                "object_store_memory": 0,
                "node_id": [],
            }
        for v in all_available_resources.values():
            keys = v.keys()
            r = re.compile(self.NODE_GROUP_PREFIX)
            partition = list(filter(r.match, list(keys)))
            r = re.compile("node:")
            node_id = list(filter(r.match, list(keys)))
            if len(partition) > 0:
                partition = partition[0]
            if len(node_id) > 0:
                node_id = node_id[0]
            if self.NODE_GROUP_PREFIX in partition:
                group_res[partition]["CPU"] += v["CPU"]
                group_res[partition]["memory"] += v["memory"]
                group_res[partition]["object_store_memory"] += v["object_store_memory"]
                group_res[partition]["node_id"].append(node_id)
        return group_res

    def _update_group_res(self, gname: str) -> Dict[str, Union[str, float]]:
        """
        Get the realtime resource of a node group
        Args:
                gname: name of node group
        Returns:
                group_res: dict of updated resource(cpu, memory, object store memory) for a given group
        """
        all_available_resources = (
            ray._private.state.state._available_resources_per_node()
        )
        group_res = {"CPU": 0, "memory": 0, "object_store_memory": 0, "node_id": []}
        for v in all_available_resources.values():
            keys = v.keys()
            r = re.compile("node:")
            node_id = list(filter(r.match, list(keys)))
            if len(node_id) > 0:
                node_id = node_id[0]
            if gname in v.keys():
                group_res["CPU"] += v["CPU"]
                group_res["memory"] += v["memory"]
                group_res["object_store_memory"] += v["object_store_memory"]
                group_res["node_id"].append(node_id)
        return group_res

    def get_one_group(self) -> Optional[Dict[str, Union[str, float]]]:
        """
        Pop up one node group
        Returns:
                group_res: dict of node group resource, {"group":"partition_1","CPU":2,...}
        """
        current_groups = self._update_groups()
        if len(current_groups) > 0:
            gname = current_groups[-1][0]
            group_res = self._update_group_res(gname)
            group_res["group"] = gname
            try:
                group_res["group_res"] = ray.available_resources()[gname]
            except Exception as e:
                logger.info(f"Error: {e}. There is no available resources for {gname}")
                return None
            return group_res
        else:
            return None

    def get_group_by_name(self, gname: str) -> Optional[Dict[str, Union[str, float]]]:
        """
        Get the specific node group given its pre-filled name
        Args:
                gname: name of the node group
        Returns:
                group_res: dict of node group resource

        """
        group_res = self._update_group_res(gname)
        group_res["group"] = gname
        try:
            group_res["group_res"] = ray.available_resources()[gname]
        except Exception as e:
            logger.info(f"Error: {e}. There is no available resources for {gname}")
            return None
        return group_res


class PlacementGroupManager:
    """Placement Group Manager
    Create a list of placement group with the desired number of cpus
    e.g., create a pg with 32 cpus, then this class will look for a node that has 32 cpus, and collect all
    resources, including cpu, memory, and object store;
    How to use:
            ```
                    from deltacat.utils.placement import PlacementGroupManager as pgm
                    pgm = pgm(10, 32)
                    pg_configs = pgm.pgs
                    opts = pg_configs[0][0]
                    fun.options(**opts).remote()
            ```
    Args:
            num_pgs: number of placement groups to be created
            instance_cpus: number of cpus per instance
    """

    def __init__(
        self,
        num_pgs: int,
        total_cpus_per_pg: int,
        cpu_per_bundle: int,
        strategy="SPREAD",
        capture_child_tasks=True,
    ):
        head_res_key = self.get_current_node_resource_key()
        # run the task on head and consume a fractional cpu, so that pg can be created on non-head node
        # if cpu_per_bundle is less than the cpus per node, the pg can still be created on head
        # curent assumption is that the cpu_per_bundle = cpus per node
        # TODO: figure out how to create pg on non-head explicitly
        self._pg_configs = ray.get(
            [
                _config.options(resources={head_res_key: 0.01}).remote(
                    total_cpus_per_pg, cpu_per_bundle, strategy, capture_child_tasks
                )
                for i in range(num_pgs)
            ]
        )
        # TODO: handle the cases where cpu_per_bundle is larger than max cpus per node, support it on ec2/flex/manta

    @property
    def pgs(self):
        return self._pg_configs

    def get_current_node_resource_key(self) -> str:
        # on ec2: address="172.31.34.51:6379"
        # on manta: address = "2600:1f10:4674:6815:aadb:2dc8:de61:bc8e:6379"
        current_node_name = ray.experimental.internal_kv.global_gcs_client.address[:-5]
        for node in ray.nodes():
            if node["NodeName"] == current_node_name:
                # Found the node.
                for key in node["Resources"].keys():
                    if key.startswith("node:"):
                        return key


@ray.remote(num_cpus=0.01)
def _config(
    total_cpus_per_pg: int,
    cpu_per_node: int,
    strategy="SPREAD",
    capture_child_tasks=True,
    time_out: Optional[float] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    pg_config = None
    opts = {}
    cluster_resources = {}
    num_bundles = (int)(total_cpus_per_pg / cpu_per_node)
    bundles = [{"CPU": cpu_per_node} for i in range(num_bundles)]
    pg = placement_group(bundles, strategy=strategy)
    ray.get(pg.ready(), timeout=time_out)
    if not pg:
        return None
    opts = {
        "scheduling_strategy": PlacementGroupSchedulingStrategy(
            placement_group=pg, placement_group_capture_child_tasks=capture_child_tasks
        )
    }
    pg_id = placement_group_table(pg)["placement_group_id"]
    pg_details = get_placement_group(pg_id)
    bundles = pg_details["bundles"]
    node_ids = []
    for bd in bundles:
        node_ids.append(bd["node_id"])
    # query available resources given list of node id
    all_nodes_available_res = ray._private.state.state._available_resources_per_node()
    pg_res = {"CPU": 0, "memory": 0, "object_store_memory": 0}
    for node_id in node_ids:
        if node_id in all_nodes_available_res:
            v = all_nodes_available_res[node_id]
            node_detail = get_node(node_id)
            pg_res["CPU"] += node_detail["resources_total"]["CPU"]
            pg_res["memory"] += v["memory"]
            pg_res["object_store_memory"] += v["object_store_memory"]
    cluster_resources["CPU"] = int(pg_res["CPU"])
    cluster_resources["memory"] = float(pg_res["memory"])
    cluster_resources["object_store_memory"] = float(pg_res["object_store_memory"])
    pg_config = PlacementGroupConfig(opts, cluster_resources)
    logger.info(f"pg has resources:{cluster_resources}")

    return pg_config
