import ray
from ray import cloudpickle
from deltacat.io.object_store import IObjectStore
from typing import List


class RayPlasmaObjectStore(IObjectStore):
    def put(self, obj: object) -> str:
        obj_ref = ray.put(obj)
        return cloudpickle.dumps(obj_ref)

    def get(self, refs: List[str]) -> List[object]:
        loaded_refs = [cloudpickle.loads(obj_id) for obj_id in refs]
        return ray.get(loaded_refs)
