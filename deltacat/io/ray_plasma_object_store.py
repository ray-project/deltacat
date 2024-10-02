import logging
import ray
from ray import cloudpickle
from ray._private.internal_api import free
from deltacat import logs
from deltacat.io.object_store import IObjectStore
import time
from typing import Any, List
from ray.types import ObjectRef

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class RayPlasmaObjectStore(IObjectStore):
    """
    An implementation of object store that uses Ray plasma object store.
    """

    def put_many(self, objects: List[object], *args, **kwargs) -> List[Any]:
        result = []
        for obj in objects:
            object_ref = ray.put(obj)
            pickled = cloudpickle.dumps(object_ref)
            result.append(pickled)

        return result

    def get_many(self, refs: List[Any], *args, **kwargs) -> List[object]:
        loaded_refs = [cloudpickle.loads(obj_id) for obj_id in refs]
        return ray.get(loaded_refs)

    def delete_many(self, refs: List[Any], *args, **kwargs) -> List[object]:
        object_refs = [cloudpickle.loads(obj_id) for obj_id in refs]
        all_deleted = True
        start = time.monotonic()
        try:
            # Per Ray docs, deletion success will NOT be returned. In-use objects will be deleted when ref count is 0.
            # https://github.com/ray-project/ray/blob/master/python/ray/_private/internal_api.py
            free(object_refs)
        except Exception as e:
            logger.warning(f"Failed to fully delete refs: {refs}", e)
            all_deleted = False
        end = time.monotonic()

        logger.info(
            f"The total time taken to attempt deleting {len(refs)} objects is: {end - start}"
        )

        return all_deleted

    def deserialize_references(
        self, refs: List[Any], *args, **kwargs
    ) -> List[ObjectRef]:
        return [cloudpickle.loads(obj_id) for obj_id in refs]
