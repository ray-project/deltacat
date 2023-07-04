import logging
from ray import cloudpickle
from collections import defaultdict
import time
from deltacat.io.object_store import IObjectStore
from typing import List
from deltacat import logs
import uuid
import socket
from pymemcache.client.base import Client
from pymemcache.client.retrying import RetryingClient
from pymemcache.exceptions import MemcacheUnexpectedCloseError

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class MemcachedObjectStore(IObjectStore):
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
            raise RuntimeError("Unable to write to cache")

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
            cache_result = client.get_many(uids)
            assert len(cache_result) == len(
                uids
            ), "Not all values were returned from cache"

            values = cache_result.values()
            total_bytes = 0

            deserialize_start = time.monotonic()
            for serialized in values:
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

        base_client = Client((ip_address, 11212))
        client = RetryingClient(
            base_client,
            attempts=3,
            retry_delay=0.01,
            retry_for=[MemcacheUnexpectedCloseError],
        )

        self.client_cache[ip_address] = client
        return client

    def _get_current_ip(self):
        if self.current_ip is None:
            self.current_ip = socket.gethostbyname(socket.gethostname())

        return self.current_ip
