import ray
import re
import time
import yaml
from typing import Optional, Union, List, Dict, Any, Callable, Tuple

class NodeGroupManager():

	def __init__(self,path: str, gname: str):
		"""Node Group Manager
		Args:
			path: cluster yaml file
			gname: node group prefix, e.g., 'partition'
		"""
		#cluster init status:
		self.NODE_GROUP_PREFIX=gname
		self.cluster_config=self._read_yaml(path)
		self.init_groups = self._cluster_node_groups(self.cluster_config)
		self.init_group_res = self._parse_node_resources()
		
	def _cluster_node_groups(self, config: Dict[str, Any]) -> Dict[str, Any]:
		"""Get Worker Groups
		Args:
			config: cluster yaml data
		Returns: 
			worker groups: a dict of worker node group

		"""
		avail_node_types =  list(config['available_node_types'].items())
		#exclude head node type
		head_node_types = [nt for nt in avail_node_types if 'resources' in nt[1] and 'CPU' in nt[1]['resources'] and nt[1]['resources']['CPU']==0][0]
		worker_node_types = [x for x in avail_node_types if x !=head_node_types]
		#assuming homogenous cluster
		#in future, update with fleet resource
		if len(worker_node_types)>0:
			self.INSTANCE_TYPE = worker_node_types[0][1]['node_config']['InstanceType']
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
		#Add 1.1 second latency to avoid inconsistency issue between raylet and head
		time.sleep(1.1)
		all_available_res = ray.available_resources()
		current_groups =[(k,all_available_res[k]) for k in all_available_res.keys() if self.NODE_GROUP_PREFIX in k]
		return current_groups

	def _parse_node_resources(self) -> Dict[str, Dict[str, float]]:
		"""
			Parse resources per node to get detailed resource tighted to each node group
			Returns:
				group_res: a dict of resources, e.g., {'CPU':0,'memory':0,'object_store_memory':0}
		"""
		all_available_resources= ray._private.state.state._available_resources_per_node()
		group_keys = [x[0] for x in self.init_groups]
		group_res={}
		for k in group_keys:
			group_res[k]={'CPU':0,'memory':0,'object_store_memory':0,'node_id':[]}
		for v in all_available_resources.values():
			keys =v.keys()
			r = re.compile(self.NODE_GROUP_PREFIX)
			partition=list(filter(r.match, list(keys)))
			r = re.compile("node:")
			node_id = list(filter(r.match, list(keys)))
			if len(partition)>0:
				partition = partition[0]
			if len(node_id)>0:
				node_id = node_id[0]
			if self.NODE_GROUP_PREFIX in partition:
				group_res[partition]['CPU']+=v['CPU']
				group_res[partition]['memory']+=v['memory']
				group_res[partition]['object_store_memory']+=v['object_store_memory']
				group_res[partition]['node_id'].append(node_id)
		return group_res

	def _update_group_res(self, gname: str) -> Dict[str, Union[str, float]]:
		"""
			Get the realtime resource of a node group
			Args:
				gname: name of node group
			Returns:
				group_res: dict of updated resource(cpu, memory, object store memory) for a given group
		"""
		all_available_resources= ray._private.state.state._available_resources_per_node()
		group_res={'CPU':0,'memory':0,'object_store_memory':0,'node_id':[]}
		for v in all_available_resources.values():
			keys =v.keys()
			r = re.compile("node:")
			node_id = list(filter(r.match, list(keys)))
			if len(node_id)>0:
				node_id = node_id[0]
			if gname in v.keys():
				group_res['CPU']+=v['CPU']
				group_res['memory']+=v['memory']
				group_res['object_store_memory']+=v['object_store_memory']
				group_res['node_id'].append(node_id)
		return group_res

	def get_one_group(self) -> Optional[Dict[str, Union[str, float]]]:
		"""
			Pop up one node group 
			Returns:
				group_res: dict of node group resource, {"group":"partition_1","CPU":2,...}
		"""
		current_groups = self._update_groups()
		if len(current_groups)>0:
			gname = current_groups[-1][0]
			group_res=self._update_group_res(gname)
			group_res['group']=gname
			try:
				group_res['group_res']=ray.available_resources()[gname]
			except Exception as e:
				print("There is no available resources for %s"%gname)
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
		group_res=self._update_group_res(gname)
		group_res['group']=gname		
		try:
			group_res['group_res']=ray.available_resources()[gname]
		except Exception as e:
			print("There is no available resources for %s"%gname)
			return None
		return group_res