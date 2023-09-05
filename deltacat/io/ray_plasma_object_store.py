import ray
from ray import cloudpickle
from deltacat.io.object_store import IObjectStore
from typing import Any, List


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
