from typing import Any, Dict, List


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


def union(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create a new Primary Key Index Write Result containing all results from
    both input Primary Key Index Write Results.
    """
    return of(
        sum([get_files(result) for result in results]),
        sum([get_pyarrow_bytes(result)] for result in results),
        sum([get_file_bytes(result)] for result in results),
        sum([get_records(result)] for result in results),
    )


def get_files(pki_write_result: Dict[str, Any]) -> int:
    return pki_write_result["files"]


def get_pyarrow_bytes(pki_write_result: Dict[str, Any]) -> int:
    return pki_write_result["paBytes"]


def get_file_bytes(pki_write_result: Dict[str, Any]) -> int:
    return pki_write_result["fileBytes"]


def get_records(pki_write_result: Dict[str, Any]) -> int:
    return pki_write_result["records"]
