import ray


def get_current_node_resource_key() -> str:
    """Get the Ray resource key for the current node as a string of the form:
    "node:{node_resource_name}". The returned key can be used to place tasks or
    actors on that node via:
    `foo.options(resources={get_current_node_resource_key(): 0.01}).remote()`
    """
    current_node_id = ray.get_runtime_context().node_id.hex()
    for node in ray.nodes():
        if node["NodeID"] == current_node_id:
            # Found the node.
            for key in node["Resources"].keys():
                if key.startswith("node:"):
                    return key
    else:
        raise ValueError("No node dictionary found on current node.")
