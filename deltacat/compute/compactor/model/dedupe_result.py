from typing import Any, Dict


def of(
        file_count: int,
        pyarrow_bytes: int,
        file_bytes: int,
        record_count: int) -> Dict[str, int]:

    return {
        "files": file_count,
        "paBytes": pyarrow_bytes,
        "fileBytes": file_bytes,
        "records": record_count,
    }


def get_files(materialize_result: Dict[str, Any]) -> int:
    return materialize_result["files"]


def get_pyarrow_bytes(materialize_result: Dict[str, Any]) -> int:
    return materialize_result["paBytes"]


def get_file_bytes(materialize_result: Dict[str, Any]) -> int:
    return materialize_result["fileBytes"]


def get_records(materialize_result: Dict[str, Any]) -> int:
    return materialize_result["records"]
