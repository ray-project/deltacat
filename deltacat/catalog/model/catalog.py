# Allow self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
from types import ModuleType

from typing import Any, Dict, List, Optional, Union
from functools import partial
import ray

from deltacat import logs
from deltacat.catalog.main import impl as DeltacatCatalog
from deltacat.catalog.iceberg import impl as IcebergCatalog
from deltacat.catalog import CatalogProperties
from deltacat.catalog.iceberg import IcebergCatalogConfig
from deltacat.constants import DEFAULT_CATALOG

all_catalogs: Optional[Catalogs] = None

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class Catalog:
    def __init__(self, impl: ModuleType = DeltacatCatalog, *args, **kwargs):
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
        !!! ICEBERG SUPPORT IS EXPERIMENTAL !!!

        Factory method to construct a catalog from Iceberg catalog params

        This method is just a wrapper around __init__ with stronger typing. You may still call __init__,
        plumbing __params__ through as kwargs
        """
        return cls(impl=IcebergCatalog, *args, **{"config": config, **kwargs})

    @classmethod
    def default(cls, config: CatalogProperties, *args, **kwargs):
        """
        Factory method to construct a catalog with the default implementation

        Uses CatalogProperties as configuration
        """
        return cls(impl=DeltacatCatalog, *args, **{"config": config, **kwargs})

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
    """Check if DeltaCAT is initialized with valid Ray actors"""
    global all_catalogs

    if all_catalogs is None:
        return False

    # Additional check to verify the Ray actor is still alive
    # If the ray cluster was shut down (e.g. in between test run invocations), then the global all_catalogs needs to be reset
    try:
        # Try a simple operation on the actor - this will fail if the actor is dead
        ray.get(all_catalogs.names.remote(), timeout=5)
        return True
    except (ray.exceptions.RayActorError, ray.exceptions.GetTimeoutError):
        # The actor is dead or inaccessible, so we're not properly initialized
        all_catalogs = None  # Reset the global variable since it's invalid
        return False


def init(
    catalogs: Union[Dict[str, Catalog], Catalog],
    default_catalog_name: Optional[str] = None,
    ray_init_args: Dict[str, Any] = None,
    *args,
    **kwargs,
) -> None:

    force_reinitialize = kwargs.get("force_reinitialize"), False

    if is_initialized() and not force_reinitialize:
        logger.warning("DeltaCAT already initialized.")
        return

    if force_reinitialize or not ray.is_initialized():
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
        raise KeyError(
            "No catalogs available! Call "
            "`deltacat.init(catalogs={...})` to register one or more "
            "catalogs then retry."
        )

    if name is not None:
        catalog = ray.get(all_catalogs.get.remote(name))
    else:
        default = all_catalogs.default.remote()
        if not default:
            available_catalogs = ray.get(all_catalogs.all.remote()).values()
            raise KeyError(
                f"Call to get_catalog without name failed because no default catalog is configured."
                f"Available catalogs are: {available_catalogs}"
            )
        catalog = ray.get(default)

    if not catalog:
        available_catalogs = ray.get(all_catalogs.all.remote()).values()
        raise KeyError(
            f"Catalog '{name}' not found. Available catalogs: " f"{available_catalogs}."
        )
    return catalog


def put_catalog(
    name: str,
    *,
    impl: ModuleType = DeltacatCatalog,
    catalog: Catalog = None,
    ray_init_args: Dict[str, Any] = None,
    **kwargs) -> Catalog:
    """
    Add a named catalog to the global map of named catalogs. Initializes ray if not already initialized.

    You may explicitly provide an initialized Catalog instance, like from the Catalog constructor or
    from factory methods like Catalog.default or Catalog.iceberg

    Otherwise, this function initializes the catalog by using the catalog implementation provided by `impl`.

    :param name: name of catalog
    :param catalog: catalog instance to use, if provided
    :param impl: catalog module to initialize. Only used if `catalog` param not provided
    :param ray_init_args: ray initialization args (used if ray must be initialized)
    """
    from deltacat.catalog.model.catalog import all_catalogs

    if catalog is None:
        catalog = Catalog(impl, **kwargs)
    elif catalog is not None and impl!=DeltacatCatalog:
        raise ValueError(f"PutCatalog call provided both `impl` and `catalog` parameters"
                         f"You may only provide one of these parameters")

    if is_initialized():
        try:
            get_catalog(name)
            raise ValueError(f"Catalog {name} already exists.")
        except KeyError:
            # Catalog does not already exist. Add it
            # TODO(pdames): Create dc.put_catalog() helper.
            ray.get(all_catalogs.put.remote(name, catalog))
    else:
        init({name: catalog}, ray_init_args=ray_init_args)
    return catalog
