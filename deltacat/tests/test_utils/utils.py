# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
import json
from typing import Any, Dict
from boto3.resources.base import ServiceResource


def read_s3_contents(
    s3_resource: ServiceResource, bucket_name: str, key: str
) -> Dict[str, Any]:
    response = s3_resource.Object(bucket_name, key).get()
    file_content: str = response["Body"].read().decode("utf-8")
    return json.loads(file_content)
