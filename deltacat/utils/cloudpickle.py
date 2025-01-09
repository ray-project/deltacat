import io
from collections import deque
from typing import List, Any
from ray import cloudpickle


def dump_into_chunks(obj: Any, max_size_bytes: int) -> List[bytes]:
    """
    This is an optimized version of dump which only requires additional
    BUFFER_SIZE amount of memory which can be configured.
    """
    assert max_size_bytes > 0, "max_size_bytes must be greater than zero"

    bytes_io = io.BytesIO()
    cloudpickle.dump(obj, bytes_io)
    total_size = bytes_io.tell()

    if total_size <= max_size_bytes:
        return [bytes_io.getvalue()]

    bytes_io.seek(0, 2)  # seek to end
    result = deque()
    observed_total_size = 0
    current_size = bytes_io.tell()

    while current_size > 0:
        to_read_size = min(max_size_bytes, current_size)
        bytes_io.seek(-to_read_size, 1)
        cur = bytes_io.read(to_read_size)
        observed_total_size += len(cur)
        bytes_io.seek(-to_read_size, 1)
        bytes_io.truncate()
        result.appendleft(cur)
        current_size = bytes_io.tell()

    assert (
        observed_total_size == total_size
    ), f"Not all bytes were split as {observed_total_size} != {total_size}"

    return list(result)
