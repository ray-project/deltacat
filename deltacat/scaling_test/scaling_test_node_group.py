import ray
import re
import time
import yaml
from deltacat.autoscaler.node_group import NodeGroupManager
ray.init(address="auto")

@ray.remote
def fun():
	time.sleep(10)

if __name__ == "__main__":
	nodem = NodeGroupManager(path="rootliu.yaml",gname="partition")
	print("Testing 1 resource")
	futures_test1=[]
	loop_id = 0
	while True:
		print("Test1: loop: %d"%loop_id)
		loop_id+=1
		try:
			print("Test1: partition:1:%d"%(ray.available_resources()['partition_1']))
			print("Test1: partition:2:%d"%(ray.available_resources()['partition_2']))
		except Exception as e:
			pass
		one_group = nodem.get_one_group()
		print("Test1: Getting a new node group:",one_group)
		if not one_group:
			print("Test1:no node group exists")
			break
		print("Test1: Launching a task using group %s"%one_group['group'])
		future = fun.options(resources={one_group['group']:1}).remote()
		futures_test1.append(future)
	print("Test1: Length of futures1:%d"%len(futures_test1))
	ray.get(futures_test1)
	time.sleep(2)
	print('Testing 0.5 resource')
	print("Test2: available_resources:",ray.available_resources())
	futures_test2=[]
	loop_id = 0
	while True:
		print("Test2: loop: %d"%loop_id)
		loop_id+=1
		try:
			print("Test2: partition:1:%d"%(ray.available_resources()['partition_1']))
			print("Test2: partition:2:%d"%(ray.available_resources()['partition_2']))
		except Exception as e:
			print("Test2: no partition available")
			pass
		one_group = nodem.get_one_group()
		print("Test2: Getting a new node group:",one_group)
		if not one_group:
			print("Test2: no node group exists")
			break
		print("Test2: submiting a new node request for 0.5 resource using %s"%(one_group['group']))
		future = fun.options(resources={one_group['group']:0.5}).remote()
		futures_test2.append(future)
		print("Test2: submiting a new node request for 0.5 resource using %s"%(one_group['group']))
		future = fun.options(resources={one_group['group']:0.5}).remote()
		futures_test2.append(future)
		time.sleep(2)
	print("Length of futures2:%d"%len(futures_test2))


	ray.get(futures_test2)