import logging
from ray import cloudpickle
import time
from deltacat.io.object_store import IObjectStore
from typing import Any, List
from deltacat import logs
import os
import uuid
from builtins import open

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class FileObjectStore(IObjectStore):
    """
    An implementation of object store that uses file system.
    """

    def __init__(self, dir_path: str) -> None:
        self.dir_path = dir_path
        super().__init__()

    def put_many(self, objects: List[object], *args, **kwargs) -> List[Any]:
        result = []

        for obj in objects:
            serialized = cloudpickle.dumps(obj)
            ref = f"{self.dir_path}/{uuid.uuid4()}"
            with open(ref, "xb") as f:
                f.write(serialized)

            result.append(ref)

        return result

    def get_many(self, refs: List[Any], *args, **kwargs) -> List[object]:
        result = []
        start = time.monotonic()
        for ref in refs:
            with open(ref, "rb") as f:
                serialized = f.read()
                loaded = cloudpickle.loads(serialized)
                result.append(loaded)
            os.remove(ref)
        end = time.monotonic()

        logger.info(f"The total time taken to read all objects is: {end - start}")
        return result
