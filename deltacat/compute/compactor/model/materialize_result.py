from typing import Any, Dict


def of(
        delta: Dict[str, Any],
        task_index: int,
        pyarrow_write_result: Dict[str, Any]) -> Dict[str, Any]:

    return {
        "delta": delta,
        "taskIndex": task_index,
        "paWriteResult": pyarrow_write_result,
    }


def get_delta(materialize_result: Dict[str, Any]) \
        -> Dict[str, Any]:

    return materialize_result["delta"]


def get_task_index(materialize_result: Dict[str, Any]) -> int:
    return materialize_result["taskIndex"]


def get_pyarrow_write_result(materialize_result: Dict[str, Any]) \
        -> Dict[str, Any]:

    return materialize_result["paWriteResult"]
