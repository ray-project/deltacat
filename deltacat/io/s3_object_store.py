import logging
from ray import cloudpickle
import time
from deltacat.io.object_store import IObjectStore
from typing import List
from deltacat import logs
import uuid
from deltacat.aws import s3u as s3_utils

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class S3ObjectStore(IObjectStore):
    def __init__(self, bucket: str) -> None:
        self.bucket = bucket
        super().__init__()

    def put(self, obj: object) -> str:
        serialized = cloudpickle.dumps(obj)
        ref = uuid.uuid4()

        s3_utils.upload(f"s3://{self.bucket}/{ref}", serialized)

        return ref

    def get(self, refs: List[str]) -> List[object]:
        result = []
        start = time.monotonic()
        for ref in refs:
            cur = s3_utils.download(f"s3://{self.bucket}/{ref}")
            serialized = cur["Body"].read()
            loaded = cloudpickle.loads(serialized)
            result.append(loaded)
        end = time.monotonic()

        logger.info(f"The total time taken to read all objects is: {end - start}")
        return result

    def flush() -> None:
        pass
