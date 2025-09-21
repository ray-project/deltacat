# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations
import json
import os
import tempfile
from typing import Any, Dict
from boto3.resources.base import ServiceResource


def read_s3_contents(
    s3_resource: ServiceResource, bucket_name: str, key: str
) -> Dict[str, Any]:
    response = s3_resource.Object(bucket_name, key).get()
    file_content: str = response["Body"].read().decode("utf-8")
    return json.loads(file_content)


def can_chmod_to_readonly() -> bool:
    """
    Test if chmod operations can effectively create read-only directories.

    Some platforms (like certain CI environments) don't respect chmod permissions
    or run with elevated privileges that bypass read-only restrictions.

    Returns:
        bool: True if chmod can create effective read-only directories, False otherwise
    """
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            test_dir = os.path.join(temp_dir, "chmod_test")
            os.makedirs(test_dir, exist_ok=True)

            # Try to make it read-only
            os.chmod(test_dir, 0o444)

            # Test if we can still create files in the read-only directory
            test_file = os.path.join(test_dir, "test_file")
            try:
                # If this succeeds, chmod didn't work effectively
                with open(test_file, "w") as f:
                    f.write("test")
                # Restore permissions for cleanup
                os.chmod(test_dir, 0o755)
                return False  # chmod didn't effectively make it read-only
            except (OSError, PermissionError):
                # This is expected - chmod worked
                # Restore permissions for cleanup
                os.chmod(test_dir, 0o755)
                return True  # chmod worked as expected
    except (OSError, PermissionError):
        # If we can't even run chmod, assume it doesn't work
        return False
