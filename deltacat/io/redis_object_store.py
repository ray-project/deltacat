import logging
from ray import cloudpickle
import time
from deltacat.io.object_store import IObjectStore
from typing import List
from deltacat import logs
import uuid
import socket
import redis
from collections import defaultdict


logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class RedisObjectStore(IObjectStore):
    def __init__(self) -> None:
        self.client_cache = {}
        self.current_ip = None
        self.SEPARATOR = "_"
        super().__init__()

    def put(self, obj: object) -> str:
        serialized = cloudpickle.dumps(obj)
        uid = uuid.uuid4()
        current_ip = self._get_current_ip()
        ref = f"{uid}{self.SEPARATOR}{current_ip}"

        client = self._get_client_by_ip(current_ip)
        if client.set(uid.__str__(), serialized):
            return ref
        else:
            raise AssertionError(f"Non truthy result returned from set for {ref}")

    def get(self, refs: List[str]) -> List[object]:
        """
        Note that this call does not return values in the exact
        same order as input.
        """

        result = []
        uid_per_ip = defaultdict(lambda: [])

        start = time.monotonic()
        for ref in refs:
            uid, ip = ref.split(self.SEPARATOR)
            uid_per_ip[ip].append(uid)

        for (ip, uids) in uid_per_ip.items():
            client = self._get_client_by_ip(ip)
            cache_result = client.mget(uids)
            assert len(cache_result) == len(
                uids
            ), "Not all values were returned from cache"

            total_bytes = 0

            deserialize_start = time.monotonic()
            for serialized in cache_result:
                deserialized = cloudpickle.loads(serialized)
                total_bytes += len(serialized)
                result.append(deserialized)

            deserialize_end = time.monotonic()
            logger.debug(
                f"The time taken to deserialize {total_bytes} bytes is: {deserialize_end - deserialize_start}",
            )

        end = time.monotonic()

        logger.info(f"The total time taken to read all objects is: {end - start}")

        return result

    def _get_client_by_ip(self, ip_address: str):
        if ip_address in self.client_cache:
            return self.client_cache[ip_address]

        base_client = redis.Redis(ip_address, 7777)

        self.client_cache[ip_address] = base_client
        return base_client

    def _get_current_ip(self):
        if self.current_ip is None:
            self.current_ip = socket.gethostbyname(socket.gethostname())

        return self.current_ip
