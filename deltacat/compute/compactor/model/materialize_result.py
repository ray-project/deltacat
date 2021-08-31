from typing import Any, Dict


def of(
        delta_manifest: Dict[str, Any],
        task_index: int,
        pyarrow_write_result: Dict[str, Any]) -> Dict[str, Any]:

    return {
        "deltaManifest": delta_manifest,
        "taskIndex": task_index,
        "paWriteResult": pyarrow_write_result,
    }


def get_delta_manifest(materialize_result: Dict[str, Any]) \
        -> Dict[str, Any]:

    return materialize_result["deltaManifest"]


def get_task_index(materialize_result: Dict[str, Any]) -> int:
    return materialize_result["taskIndex"]


def get_pyarrow_write_result(materialize_result: Dict[str, Any]) \
        -> Dict[str, Any]:

    return materialize_result["paWriteResult"]
