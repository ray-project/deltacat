from typing import List, Any


class IObjectStore:
    """
    An object store interface.
    """

    def setup(self, *args, **kwargs) -> Any:
        ...

        """
        Sets up everything needed to run the object store.
        """

    def put(self, obj: object, *args, **kwargs) -> Any:
        """
        Put a single object into the object store. Depending
        on the implementation, this method can be sync or async.
        """
        return self.put_many([obj])[0]

    def put_many(self, objects: List[object], *args, **kwargs) -> List[Any]:
        ...

        """
        Put many objects into the object store. It would return an ordered list
        of object references corresponding to each object in the input.
        """

    def get(self, ref: Any, *args, **kwargs) -> object:
        """
        Get a single object from an object store.
        """
        return self.get_many([ref])[0]

    def get_many(self, refs: List[Any], *args, **kwargs) -> List[object]:
        ...

        """
        Get a list of objects from the object store. Use this method to
        avoid multiple get calls. Note that depending on implementation it may
        or may not return ordered results.
        """

    def clear(self, *args, **kwargs) -> bool:
        ...

        """
        Clears the object store and all the associated data in it.
        """
