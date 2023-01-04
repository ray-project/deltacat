# Allow self-referencing Type hints in Python 3.7.
from __future__ import annotations

import ray

from deltacat.catalog import interface as catalog_interface
from typing import Any, Dict, List, Optional


all_catalogs: Optional[Catalogs] = None


class Catalog:
    def __init__(
            self,
            impl=catalog_interface,
            *args,
            **kwargs):
        self._impl = impl
        self._impl.initialize(*args, **kwargs)

    @property
    def impl(self):
        return self._impl


@ray.remote
class Catalogs:
    def __init__(
            self,
            catalogs: Dict[str, Catalog],
            default_catalog_name: str = None,
            *args,
            **kwargs):
        if default_catalog_name and default_catalog_name not in catalogs:
            raise ValueError(
                f"Catalog {default_catalog_name} not found "
                f"in catalogs to register: {catalogs}")
        if not catalogs:
            raise ValueError(
                f"No catalogs given to register. "
                f"Please specify one or more catalogs.")
        self.catalogs: Dict[str, Catalog] = catalogs
        if default_catalog_name:
            self.default_catalog = self.catalogs[default_catalog_name]
        elif len(catalogs) == 1:
            self.default_catalog = list(self.catalogs.values())[0]
        else:
            self.default_catalog = None

    def all(self) -> Dict[str, Catalog]:
        return self.catalogs

    def names(self) -> List[str]:
        return list(self.catalogs.keys())

    def put(self, name: str, catalog: Catalog) -> None:
        self.catalogs[name] = catalog

    def get(self, name) -> Catalog:
        return self.catalogs.get(name)

    def default(self) -> Optional[Catalog]:
        return self.default_catalog


def init(
        catalogs: Dict[str, Catalog],
        default_catalog_name: str = None,
        ray_init_args: Dict[str, Any] = None,
        *args,
        **kwargs) -> None:

    if not ray.is_initialized():
        if ray_init_args:
            ray.init(**ray_init_args)
        else:
            ray.init(address="auto")

    global all_catalogs
    all_catalogs = Catalogs.remote(
        catalogs=catalogs,
        default_catalog_name=default_catalog_name)
