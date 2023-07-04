from typing import List


class IObjectStore:
    """
    An object store interface.
    """

    def put(self, obj: object) -> str:
        ...

    """
    Put object into the object store.
    """

    def get(self, refs: List[str]) -> List[object]:
        ...

    """
    Get a list of objects from the object store.
    """
