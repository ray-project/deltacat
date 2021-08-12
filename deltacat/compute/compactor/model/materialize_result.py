from typing import Any, Dict


def of(
        delta_manifest: Dict[str, Any],
        task_index: int,
        file_count: int,
        pyarrow_bytes: int,
        file_bytes: int,
        record_count: int) -> Dict[str, Any]:

    return {
        "deltaManifest": delta_manifest,
        "taskIndex": task_index,
        "files": file_count,
        "paBytes": pyarrow_bytes,
        "fileBytes": file_bytes,
        "records": record_count,
    }


def get_delta_manifest(materialize_result: Dict[str, Any]) \
        -> Dict[str, Any]:

    return materialize_result["deltaManifest"]


def get_task_index(materialize_result: Dict[str, Any]) -> int:
    return materialize_result["taskIndex"]


def get_files(materialize_result: Dict[str, Any]) -> int:
    return materialize_result["files"]


def get_pyarrow_bytes(materialize_result: Dict[str, Any]) -> int:
    return materialize_result["paBytes"]


def get_file_bytes(materialize_result: Dict[str, Any]) -> int:
    return materialize_result["fileBytes"]


def get_records(materialize_result: Dict[str, Any]) -> int:
    return materialize_result["records"]
