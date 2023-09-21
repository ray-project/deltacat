import logging
from ray import cloudpickle
from collections import defaultdict
import time
from deltacat.io.object_store import IObjectStore
from typing import Any, List, Optional
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

    CONNECT_TIMEOUT = 10 * 60
    FETCH_TIMEOUT = 30 * 60

    def __init__(
        self,
        storage_node_ips: Optional[List[str]] = None,
        port: Optional[int] = 11212,
        connect_timeout: float = CONNECT_TIMEOUT,
        timeout: float = FETCH_TIMEOUT,
        noreply: bool = False,
    ) -> None:
        self.client_cache = {}
        self.current_ip = None
        self.SEPARATOR = "_"
        self.port = port
        self.storage_node_ips = storage_node_ips
        self.hasher = None
        self.connect_timeout = connect_timeout
        self.timeout = timeout
        self.noreply = noreply
        logger.info(f"The storage node IPs: {self.storage_node_ips}")
        super().__init__()

    def initialize_hasher(self):
        if not self.hasher and self.storage_node_ips:
            self.hasher = RendezvousHash()
            for n in self.storage_node_ips:
                self.hasher.add_node(n)

    def put_many(self, objects: List[object], *args, **kwargs) -> List[Any]:
        input = defaultdict(dict)
        result = []
        for obj in objects:
            serialized = cloudpickle.dumps(obj)
            uid = uuid.uuid4()
            create_ref_ip = self._get_create_ref_ip(uid.__str__())
            ref = self._create_ref(uid, create_ref_ip)
            input[create_ref_ip][uid.__str__()] = serialized
            result.append(ref)
        for create_ref_ip, uid_to_object in input.items():
            client = self._get_client_by_ip(create_ref_ip)
            if client.set_many(uid_to_object, noreply=self.noreply):
                raise RuntimeError("Unable to write few keys to cache")

        return result

    def put(self, obj: object, *args, **kwargs) -> Any:
        serialized = cloudpickle.dumps(obj)
        uid = uuid.uuid4()
        create_ref_ip = self._get_create_ref_ip(uid.__str__())
        ref = self._create_ref(uid, create_ref_ip)
        client = self._get_client_by_ip(create_ref_ip)

        if client.set(uid.__str__(), serialized, noreply=self.noreply):
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

    def _get_storage_node_ip(self, key: str):
        self.initialize_hasher()
        storage_node_ip = self.hasher.get_node(key)
        return storage_node_ip

    def _get_create_ref_ip(self, uid: str):
        if self.storage_node_ips:
            create_ref_ip = self._get_storage_node_ip(uid)
        else:
            create_ref_ip = self._get_current_ip()
        return create_ref_ip

    def _get_client_by_ip(self, ip_address: str):
        if ip_address in self.client_cache:
            return self.client_cache[ip_address]

        base_client = Client(
            (ip_address, self.port),
            connect_timeout=self.connect_timeout,
            timeout=self.timeout,
            no_delay=True,
        )

        client = RetryingClient(
            base_client,
            attempts=15,
            retry_delay=1,
            retry_for=[
                MemcacheUnexpectedCloseError,
                ConnectionResetError,
                BrokenPipeError,
                TimeoutError,
            ],
        )

        self.client_cache[ip_address] = client
        return client

    def _get_current_ip(self):
        if self.current_ip is None:
            self.current_ip = socket.gethostbyname(socket.gethostname())

        return self.current_ip
