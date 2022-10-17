import os
import errno

def replace_custom_resources(custom_resource_yaml_path, partition_values):
    with open(custom_resource_yaml_path, "r+") as file:
        single_custom_resource = file.read()
    custom_resource_str = ""
    for i in range(len(partition_values)):
        contents = single_custom_resource.replace("{{partition_hash}}", f"partition_{i}")
        custom_resource_str += f"\n{contents}"
    return custom_resource_str

def generate_scaling_test_yaml(yaml_file_path, custom_resource_yaml_path, partition_values):
    custom_resource_str = replace_custom_resources(custom_resource_yaml_path, partition_values)
    out_file_name = f"custom_resource_test.yaml"
    out_file_dir = os.path.join(os.path.dirname(yaml_file_path), "tmp")
    out_file_path = os.path.join(out_file_dir, out_file_name)
    with open(yaml_file_path, "r+") as file:
        contents = file.read().replace("{{custom_resource}}", custom_resource_str)
    try:
        os.makedirs(os.path.dirname(out_file_path))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    with open(out_file_path, "w") as output:
        output.write(contents)

generate_scaling_test_yaml("scaling_test_example.yaml", "one_custom_resource.yaml", [i for i in range(250)])