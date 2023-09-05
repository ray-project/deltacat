import logging
from ray import cloudpickle
import time
from deltacat.io.object_store import IObjectStore
from typing import Any, List
from deltacat import logs
import uuid
from deltacat.aws import s3u as s3_utils

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class S3ObjectStore(IObjectStore):
    """
    An implementation of object store that uses S3.
    """

    def __init__(self, bucket_prefix: str) -> None:
        self.bucket = bucket_prefix
        super().__init__()

    def put_many(self, objects: List[object], *args, **kwargs) -> List[Any]:
        result = []
        for obj in objects:
            serialized = cloudpickle.dumps(obj)
            ref = uuid.uuid4()

            s3_utils.upload(f"s3://{self.bucket}/{ref}", serialized)
            result.append(ref)

        return result

    def get_many(self, refs: List[Any], *args, **kwargs) -> List[object]:
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
