import base64
import msgpack
import json
import os
import shutil

from tempfile import mkdtemp


def _convert_bytes_to_base64_str(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, bytes):
                obj[key] = base64.b64encode(value).decode("utf-8")
            elif isinstance(value, list):
                _convert_bytes_to_base64_str(value)
            elif isinstance(value, dict):
                _convert_bytes_to_base64_str(value)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, bytes):
                obj[i] = base64.b64encode(item).decode("utf-8")
            elif isinstance(item, (dict, list)):
                _convert_bytes_to_base64_str(item)


def copy_and_convert(src_dir, dst_dir=None):
    """
    Helper function for copying a metastore recursively and converting all
    messagepack files to json. This can be used manually to more easily
    introspect metastore metadata.
    """
    if dst_dir is None:
        dst_dir = mkdtemp()
        print(f"destination is: {dst_dir}")
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)

    for item in os.listdir(src_dir):
        src_path = os.path.join(src_dir, item)
        dst_path = os.path.join(dst_dir, item)

        if os.path.isdir(src_path):
            copy_and_convert(src_path, dst_path)
        else:
            if item.endswith(".mpk"):
                with open(src_path, "rb") as f:
                    data = msgpack.unpackb(f.read(), raw=False)
                    _convert_bytes_to_base64_str(data)
                dst_path = dst_path[:-4] + ".json"
                with open(dst_path, "w") as f:
                    json.dump(data, f)
            else:
                shutil.copy2(src_path, dst_path)
