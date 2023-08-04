import logging
from ray import cloudpickle
from collections import defaultdict
import time
from deltacat.io.object_store import IObjectStore
from typing import Any, List
from deltacat import logs
import uuid
import socket
from pymemcache.client.base import Client
from pymemcache.client.retrying import RetryingClient
from pymemcache.exceptions import MemcacheUnexpectedCloseError
from pymemcache.client.rendezvous import RendezvousHash

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class MemcachedObjectStore(IObjectStore):
    """
    An implementation of object store that uses Memcached.
    """

    def __init__(self, storage_node_ips, port=11212) -> None:
        self.current_ip = None
        self.SEPARATOR = "_"
        self.port = port
        self.storage_node_ips = storage_node_ips
        self.hasher = None
        super().__init__()

    def initialize_hasher(self):
        if not self.hasher:
            self.hasher = RendezvousHash()
            for n in self.storage_node_ips:
                self.hasher.add_node(n)

    def put_many(self, objects: List[object], *args, **kwargs) -> List[Any]:
        input = {}
        result = []
        current_ip = self._get_current_ip()
        for obj in objects:
            serialized = cloudpickle.dumps(obj)
            uid = uuid.uuid4()
            ref = self._create_ref(uid, current_ip)
            input[uid.__str__()] = serialized
            result.append(ref)

        client = self._get_client_by_ip(current_ip)
        if client.set_many(input, noreply=False):
            raise RuntimeError("Unable to write few keys to cache")

        return result

    def put(self, obj: object, *args, **kwargs) -> Any:
        serialized = cloudpickle.dumps(obj)
        uid = uuid.uuid4()
        current_ip = self._get_current_ip()
        ref = self._create_ref(uid, current_ip)
        client = self._get_client_by_ip(current_ip)

        if client.set(uid.__str__(), serialized):
            return ref
        else:
            raise RuntimeError("Unable to write to cache")

    def get_many(self, refs: List[Any], *args, **kwargs) -> List[object]:
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
            ), f"Not all values were returned from cache as {len(cache_result)} != {len(uids)}"

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

    def get(self, ref: Any, *args, **kwargs) -> object:
        uid, ip = ref.split(self.SEPARATOR)
        client = self._get_client_by_ip(ip)
        serialized = client.get(uid)
        return cloudpickle.loads(serialized)

    def _create_ref(self, uid, ip) -> str:
        return f"{uid}{self.SEPARATOR}{ip}"

    def _get_client_by_ip(self, ip_address: str):
        self.initialize_hasher()
        storage_node_ip = self.hasher.get_node(ip_address)
        if storage_node_ip:
            base_client = Client((storage_node_ip, self.port))
            client = RetryingClient(
                base_client,
                attempts=3,
                retry_delay=0.01,
                retry_for=[MemcacheUnexpectedCloseError, ConnectionResetError],
            )
            return client
        else:
            logger.warning(f"Failed to find storage_node_ip for {ip_address}")

    def _get_current_ip(self):
        if self.current_ip is None:
            self.current_ip = socket.gethostbyname(socket.gethostname())

        return self.current_ip
