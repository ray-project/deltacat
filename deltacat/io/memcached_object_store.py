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
from deltacat.utils.cloudpickle import dump_into_chunks
from deltacat.exceptions import (
    PymemcachedPutObjectError,
)

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class MemcachedObjectStore(IObjectStore):
    """
    An implementation of object store that uses Memcached.
    """

    CONNECT_TIMEOUT = 10 * 60
    FETCH_TIMEOUT = 30 * 60
    MAX_ITEM_SIZE_BYTES = 1000000000

    def __init__(
        self,
        storage_node_ips: Optional[List[str]] = None,
        port: Optional[int] = 11212,
        connect_timeout: float = CONNECT_TIMEOUT,
        timeout: float = FETCH_TIMEOUT,
        noreply: bool = False,
        max_item_size_bytes: int = MAX_ITEM_SIZE_BYTES,
    ) -> None:
        self.client_cache = {}
        self.current_ip = None
        self.SEPARATOR = "_"
        self.port = port
        self.storage_node_ips = storage_node_ips or []
        for ip in self.storage_node_ips:
            assert (
                self.SEPARATOR not in ip
            ), f"IP address should not contain {self.SEPARATOR}"
        self.hasher = RendezvousHash(nodes=self.storage_node_ips, seed=0)
        self.connect_timeout = connect_timeout
        self.timeout = timeout
        self.noreply = noreply
        self.max_item_size_bytes = max_item_size_bytes
        logger.info(
            f"The storage node IPs: {self.storage_node_ips} with noreply: {self.noreply}"
        )
        super().__init__()

    def put_many(self, objects: List[object], *args, **kwargs) -> List[Any]:
        input = defaultdict(dict)
        result = []
        for obj in objects:
            serialized_list = dump_into_chunks(
                obj, max_size_bytes=self.max_item_size_bytes
            )
            uid = uuid.uuid4()
            create_ref_ip = self._get_create_ref_ip(uid.__str__())

            for chunk_index, chunk in enumerate(serialized_list):
                ref = self._create_ref(uid, create_ref_ip, chunk_index)
                input[create_ref_ip][ref] = chunk

            return_ref = self._create_ref(uid, create_ref_ip, len(serialized_list))
            result.append(return_ref)
        for create_ref_ip, ref_to_object in input.items():
            client = self._get_client_by_ip(create_ref_ip)
            if client.set_many(ref_to_object, noreply=self.noreply):
                raise PymemcachedPutObjectError("Unable to write a few keys to cache")

        return result

    def put(self, obj: object, *args, **kwargs) -> Any:
        serialized_list = dump_into_chunks(obj, max_size_bytes=self.max_item_size_bytes)
        uid = uuid.uuid4()
        create_ref_ip = self._get_create_ref_ip(uid.__str__())
        client = self._get_client_by_ip(create_ref_ip)

        for chunk_index, chunk in enumerate(serialized_list):
            ref = self._create_ref(uid, create_ref_ip, chunk_index)

            try:
                if not client.set(ref, chunk, noreply=self.noreply):
                    raise PymemcachedPutObjectError(f"Unable to write {ref} to cache")
            except BaseException as e:
                raise PymemcachedPutObjectError(
                    f"Received {e} while writing ref={ref} and obj size={len(chunk)}",
                )

        return self._create_ref(uid, create_ref_ip, len(serialized_list))

    def get_many(self, refs: List[Any], *args, **kwargs) -> List[object]:
        result = []
        refs_per_ip = self._get_refs_per_ip(refs)
        chunks_by_refs = defaultdict(lambda: [])

        start = time.monotonic()

        total_ref_count = 0
        for (ip, current_refs) in refs_per_ip.items():
            client = self._get_client_by_ip(ip)
            cache_result = client.get_many(current_refs)
            assert len(cache_result) == len(
                current_refs
            ), f"Not all values were returned from cache from {ip} as {len(cache_result)} != {len(current_refs)}"
            total_ref_count += len(cache_result)
            chunks_by_refs.update(cache_result)

        assert len(chunks_by_refs) == total_ref_count, (
            "Total refs retrieved from memcached doesn't "
            f"match as {len(chunks_by_refs)} != {total_ref_count}"
        )

        for ref in refs:
            uid, ip, chunk_count = ref.split(self.SEPARATOR)
            chunk_count = int(chunk_count)

            deserialize_start = time.monotonic()

            chunks = []

            for chunk_index in range(chunk_count):
                current_ref = self._create_ref(uid, ip, chunk_index)
                chunk = chunks_by_refs[current_ref]
                assert (
                    chunk
                ), f"Serialized chunks were not present for ref={current_ref}"
                chunks_by_refs.pop(current_ref)
                chunks.append(chunk)

            assert chunk_count == len(
                chunks
            ), f"The chunk count must be equal for ref={ref}"
            if chunk_count == 1:
                # avoids moving each byte
                serialized = chunks[0]
            else:
                serialized = bytearray()
                for chunk_index in range(chunk_count):
                    chunk = chunks[chunk_index]
                    serialized.extend(chunk)
                    chunks[chunk_index] = None

            deserialized = cloudpickle.loads(serialized)
            result.append(deserialized)

            deserialize_end = time.monotonic()
            logger.debug(
                f"The time taken to deserialize {len(serialized)} bytes"
                f" and {chunk_count} chunks is: {deserialize_end - deserialize_start}",
            )

        end = time.monotonic()

        logger.info(f"The total time taken to read all objects is: {end - start}")

        assert len(result) == len(
            refs
        ), f"The total number of refs must be equal as {len(result)} != {len(refs)}"
        return result

    def get(self, ref: Any, *args, **kwargs) -> object:
        uid, ip, chunk_count = ref.split(self.SEPARATOR)
        chunk_count = int(chunk_count)

        client = self._get_client_by_ip(ip)
        serialized = bytearray()

        for chunk_index in range(chunk_count):
            ref = self._create_ref(uid, ip, chunk_index)
            chunk = client.get(ref)
            if chunk is None:
                raise ValueError(
                    f"Expected uid: {uid}, chunk index: {chunk_index} from client ip: {ip}"
                    f" to be non-empty."
                )
            serialized.extend(chunk)

        return cloudpickle.loads(serialized)

    def delete_many(self, refs: List[Any], *args, **kwargs) -> bool:
        refs_per_ip = self._get_refs_per_ip(refs)
        all_deleted = True

        start = time.monotonic()

        total_refs = 0
        fully_deleted_refs = 0
        for (ip, current_refs) in refs_per_ip.items():
            client = self._get_client_by_ip(ip)
            total_refs += len(current_refs)
            try:
                # always returns true
                client.delete_many(current_refs, noreply=self.noreply)
                fully_deleted_refs += len(current_refs)
            except BaseException:
                # if an exception is raised then all, some, or none of the keys may have been deleted
                logger.warning(
                    f"Failed to fully delete {len(current_refs)} refs for ip: {ip}",
                    exc_info=True,
                )
                all_deleted = False

        end = time.monotonic()

        logger.info(
            f"From {len(refs)} objects, found {total_refs} total chunk references, of which {fully_deleted_refs} were guaranteed to be successfully deleted."
        )
        logger.info(
            f"The total time taken to attempt deleting {len(refs)} objects is: {end - start}"
        )

        # We need to clear the client cache in case of multi-round compaction because client cannot be pickled
        self.client_cache.clear()

        return all_deleted

    def clear(self) -> bool:
        start = time.monotonic()
        flushed = all(
            [
                self._get_client_by_ip(ip).flush_all(noreply=False)
                for ip in self.storage_node_ips
            ]
        )
        self.client_cache.clear()
        end = time.monotonic()

        if flushed:
            logger.info("Successfully cleared cache contents.")

        logger.info(f"The total time taken to clear the cache is: {end - start}")

        return flushed

    def close(self) -> None:
        for client in self.client_cache.values():
            client.close()

        self.client_cache.clear()
        logger.info("Successfully closed object store clients.")

    def _create_ref(self, uid, ip, chunk_index) -> str:
        return f"{uid}{self.SEPARATOR}{ip}{self.SEPARATOR}{chunk_index}"

    def _get_storage_node_ip(self, key: str):
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
                ConnectionRefusedError,
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

    def _get_refs_per_ip(self, refs: List[Any]):
        refs_per_ip = defaultdict(lambda: [])

        for ref in refs:
            uid, ip, chunk_count = ref.split(self.SEPARATOR)
            chunk_count = int(chunk_count)
            for chunk_index in range(chunk_count):
                current_ref = self._create_ref(uid, ip, chunk_index)
                refs_per_ip[ip].append(current_ref)
        return refs_per_ip
