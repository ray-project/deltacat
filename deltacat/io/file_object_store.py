import logging
from ray import cloudpickle
import time
from deltacat.io.object_store import IObjectStore
from typing import List
from deltacat import logs
import os
import uuid

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class FileObjectStore(IObjectStore):
    def __init__(self, dir_path: str) -> None:
        self.dir_path = dir_path
        super().__init__()

    def put(self, obj: object) -> str:
        serialized = cloudpickle.dumps(obj)
        ref = f"{self.dir_path}/{uuid.uuid4()}"
        with open(ref, "xb") as f:
            f.write(serialized)
        return ref

    def get(self, refs: List[str]) -> List[object]:
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
