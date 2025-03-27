# Allow self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
from types import ModuleType

from typing import Any, Dict, List, Optional, Union
from functools import partial
import ray

from deltacat import logs
import deltacat.catalog.main as deltacat_catalog
import deltacat.catalog.iceberg as iceberg_catalog
from deltacat.catalog import CatalogProperties
from deltacat.catalog.iceberg import IcebergCatalogConfig
from deltacat.constants import DEFAULT_CATALOG

all_catalogs: Optional[Catalogs] = None

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

class Catalog:
    def __init__(self,
                 impl: ModuleType = deltacat_catalog,
                 *args,
                 **kwargs):
        """
        Constructor for a Catalog.

        The args and kwargs here will be plumbed through to the catalog initialize function, and the results
        are stored in Catalog.inner. Any state which is required (like: metastore root URI, pyiceberg native catalog)
        MUST be returned by initialize.

        Note: all initialization configuration MUST be pickle-able. When `Catalog` is pickled, _inner is excluded
          and instead we only pass impl/args/kwargs, which

        """
        if not isinstance(self, Catalog):
            # self may contain the tuple returned from __reduce__ (ray pickle bug?)
            if callable(self[0]) and isinstance(self[1], tuple):
                logger.info(f"Invoking {self[0]} with positional args: {self[1]}")
                return self[0](*self[1])
            else:
                err_msg = f"Expected `self` to be {Catalog}, but found: {self}"
                raise RuntimeError(err_msg)

        self._impl = impl
        self._inner = self._impl.initialize(*args, **kwargs)
        self._args = args
        self._kwargs = kwargs

    @classmethod
    def iceberg(cls, config: IcebergCatalogConfig, *args, **kwargs):
        """
        Factory method to construct a catalog from Iceberg catalog params

        This method is just a wrapper around __init__ with stronger typing. You may still call __init__,
        plumbing __params__ through as kwargs
        """
        return cls(impl=iceberg_catalog, *args, **{**config, **kwargs})

    @classmethod
    def default(cls, config: CatalogProperties, *args, **kwargs):
        """
        TODO ensure this works
        """
        return cls(impl=deltacat_catalog, *args, **{**config, **kwargs})

    @property
    def impl(self):
        return self._impl

    @property
    def inner(self) -> Optional[Any]:
        return self._inner

    # support pickle, copy, deepcopy, etc.
    def __reduce__(self):
        # instantiated catalogs may fail to pickle, so exclude _inner
        # (e.g. Iceberg catalog w/ unserializable SSLContext from boto3 client)
        return partial(self.__class__, **self._kwargs), (self._impl, *self._args)

    def __str__(self):
        string_rep = f"{self.__class__.__name__}("
        if self._args:
            string_rep += f"args={self._args}, "
        if self._kwargs:
            string_rep += f"kwargs={self._kwargs}, "
        if self._inner:
            string_rep += f"inner={self._inner})"
        return string_rep

    def __repr__(self):
        return self.__str__()


@ray.remote
class Catalogs:

    def __init__(
        self,
        catalogs: Union[Catalog, Dict[str, Catalog]],
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

        # if user only provides single Catalog, override it to be a map with default key
        if isinstance(catalogs, Catalog):
            catalogs = {DEFAULT_CATALOG: catalogs}

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


def is_initialized() -> bool:
    return all_catalogs is not None


def init(
    catalogs: Union[Dict[str, Catalog], Catalog],
    default_catalog_name: Optional[str] = None,
    ray_init_args: Dict[str, Any] = None,
    *args,
    **kwargs,
) -> None:
    if is_initialized():
        logger.warning("DeltaCAT already initialized.")
        return

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


def put_catalog(name: str, impl: ModuleType = deltacat_catalog, *args, **kwargs) -> Catalog:
    from deltacat.catalog.model.catalog import all_catalogs

    new_catalog = Catalog(impl,*args, **kwargs)
    if is_initialized():
        try:
            get_catalog(name)
            raise ValueError(f"Catalog {name} already exists.")
        except ValueError:
            # TODO(pdames): Create dc.put_catalog() helper.
            ray.get(all_catalogs.put.remote(name, new_catalog))
    else:
        init({name: new_catalog})
    return new_catalog
