# Allow self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging

from typing import Any, Dict, List, Optional
from functools import partial
import ray

from deltacat import logs
from deltacat.catalog import interface as catalog_interface

all_catalogs: Optional[Catalogs] = None

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class Catalog:
    def __init__(self, impl=catalog_interface, *args, **kwargs):
        if not isinstance(self, Catalog):
            # self may contain the tuple returned from __reduce__ (ray pickle bug?)
            if callable(self[0]) and isinstance(self[1], tuple):
                logger.info(f"Invoking {self[0]} with positional args: {self[1]}")
                return self[0](*self[1])
            else:
                err_msg = f"Expected `self` to be {Catalog}, but found: {self}"
                raise RuntimeError(err_msg)
        self._impl = impl
        self._native_object = self._impl.initialize(*args, **kwargs)
        self._args = args
        self._kwargs = kwargs

    @property
    def impl(self):
        return self._impl

    @property
    def native_object(self) -> Optional[Any]:
        return self._native_object

    # support pickle, copy, deepcopy, etc.
    def __reduce__(self):
        # instantiated catalogs may fail to pickle, so exclude _native_object
        # (e.g. Iceberg catalog w/ unserializable SSLContext from boto3 client)
        return partial(self.__class__, **self._kwargs), (self._impl, *self._args)


@ray.remote
class Catalogs:
    def __init__(
        self,
        catalogs: Dict[str, Catalog],
        default_catalog_name: Optional[str] = None,
        *args,
        **kwargs,
    ):
        if default_catalog_name and default_catalog_name not in catalogs:
            raise ValueError(
                f"Catalog {default_catalog_name} not found "
                f"in catalogs to register: {catalogs}"
            )
        if not catalogs:
            raise ValueError(
                f"No catalogs given to register. "
                f"Please specify one or more catalogs."
            )
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
    default_catalog_name: Optional[str] = None,
    ray_init_args: Dict[str, Any] = None,
    *args,
    **kwargs,
) -> None:

    if not ray.is_initialized():
        if ray_init_args:
            ray.init(**ray_init_args)
        else:
            ray.init()

    # register custom serializer for catalogs since these may contain
    # unserializable objects like boto3 clients with SSLContext
    ray.util.register_serializer(
        Catalog, serializer=Catalog.__reduce__, deserializer=Catalog.__init__
    )

    global all_catalogs
    all_catalogs = Catalogs.remote(
        catalogs=catalogs, default_catalog_name=default_catalog_name
    )


def get_catalog(name: Optional[str] = None) -> Catalog:
    from deltacat.catalog.model.catalog import all_catalogs

    if not all_catalogs:
        raise ValueError(
            "No catalogs available! Call "
            "`deltacat.init(catalogs={...})` to register one or more "
            "catalogs then retry."
        )
    catalog = (
        ray.get(all_catalogs.get.remote(name))
        if name
        else ray.get(all_catalogs.default.remote())
    )
    if not catalog:
        available_catalogs = ray.get(all_catalogs.all.remote()).values()
        raise ValueError(
            f"Catalog '{name}' not found. Available catalogs: " f"{available_catalogs}."
        )
    return catalog
