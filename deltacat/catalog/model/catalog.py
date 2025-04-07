# Allow self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
from types import ModuleType

from typing import Any, Dict, List, Optional, Union
from functools import partial
import ray

from deltacat import logs
from deltacat.annotations import ExperimentalAPI
from deltacat.catalog.main import impl as DeltacatCatalog
from deltacat.catalog.iceberg import impl as IcebergCatalog
from deltacat.catalog import CatalogProperties
from deltacat.catalog.iceberg import IcebergCatalogConfig
from deltacat.constants import DEFAULT_CATALOG

all_catalogs: Optional[ray.actor.ActorHandle] = None

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class Catalog:
    def __init__(self, impl: ModuleType = DeltacatCatalog, *args, **kwargs):
        """
        Constructor for a Catalog.

        The args and kwargs here will be plumbed through to the catalog initialize function, and the results
        are stored in Catalog.inner. Any state which is required (like: metastore root URI, pyiceberg native catalog)
        MUST be returned by initialize.

        Note: all initialization configuration MUST be pickle-able. When `Catalog` is pickled, _inner is excluded.
        Instead, we only pass impl/args/kwargs, which are pickled and then _inner is re-constituted by calling __init__.
        See `ray.util.register_serializer` in Catalogs class.
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
    @ExperimentalAPI
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

    def put(self, name: str, catalog: Catalog, set_default: bool = False) -> None:
        self.catalogs[name] = catalog
        if set_default:
            self.default_catalog = catalog

    def get(self, name) -> Catalog:
        return self.catalogs.get(name)

    def default(self) -> Optional[Catalog]:
        return self.default_catalog


def is_initialized(*args, **kwargs) -> bool:
    """
    Check if DeltaCAT is initialized
    """
    import deltacat.catalog.model.catalog as catalog_module

    # If ray is not initialized, then Catalogs cannot be initialized
    if not ray.is_initialized():
        # Any existing actor reference stored in catalog_module must be stale - reset it
        catalog_module.all_catalogs = None
        return False

    return catalog_module.all_catalogs is not None


def init(
    catalogs: Union[Dict[str, Catalog], Catalog],
    default_catalog_name: Optional[str] = None,
    ray_init_args: Dict[str, Any] = None,
    *args,
    **kwargs,
) -> None:
    """
    Initialize DeltaCAT catalogs.

    :param catalogs: Either a single Catalog instance or a map of string to Catalog instance
    :param default_catalog_name: The Catalog to use by default. If only one Catalog is provided, it will
        be set as the default
    :param ray_init_args: kwargs to pass to ray initialization
    """
    import deltacat.catalog.model.catalog as catalog_module

    force_reinitialize = kwargs.get("force_reinitialize", False)

    if is_initialized() and not force_reinitialize:
        logger.warning("DeltaCAT already initialized.")
        return
    else:
        if ray_init_args:
            ray.init(**ray_init_args)
        else:
            ray.init()

    # register custom serializer for catalogs since these may contain
    # unserializable objects like boto3 clients with SSLContext
    ray.util.register_serializer(
        Catalog, serializer=Catalog.__reduce__, deserializer=Catalog.__init__
    )
    catalog_module.all_catalogs = Catalogs.remote(
        catalogs=catalogs, default_catalog_name=default_catalog_name
    )


def get_catalog(name: Optional[str] = None, **kwargs) -> Catalog:
    """
    Get a catalog by name, or the default catalog if no name is provided.

    Args:
        name: Name of catalog to retrieve (optional, uses default if not provided)

    Returns:
        The requested Catalog, or ValueError if it does not exist
    """
    from deltacat.catalog.model.catalog import all_catalogs

    if not all_catalogs:
        raise ValueError(
            "No catalogs available! Call "
            "`deltacat.init(catalogs={...})` to register one or more "
            "catalogs then retry."
        )

    if name is not None:
        catalog = ray.get(all_catalogs.get.remote(name))
        if not catalog:
            available_catalogs = ray.get(all_catalogs.all.remote()).values()
            raise ValueError(
                f"Catalog '{name}' not found. Available catalogs: "
                f"{available_catalogs}."
            )
        return catalog

    else:
        catalog = ray.get(all_catalogs.default.remote())
        if not catalog:
            available_catalogs = ray.get(all_catalogs.all.remote()).values()
            raise ValueError(
                f"Call to get_catalog without name set failed because there is no default Catalog set. Available catalogs: "
                f"{available_catalogs}."
            )
        return catalog


def put_catalog(
    name: str,
    catalog: Catalog = None,
    *,
    impl: ModuleType = DeltacatCatalog,
    set_as_default: bool = False,
    ray_init_args: Dict[str, Any] = None,
    **kwargs,
) -> Catalog:
    """
    Add a named catalog to the global map of named catalogs. Initializes ray if not already initialized.

    You may explicitly provide an initialized Catalog instance, like from the Catalog constructor or
    from factory methods like Catalog.default or Catalog.iceberg
    Otherwise, this function initializes the catalog by using the catalog implementation provided by `impl`.
    This function will NOT fail if the catalog already exists. It will overwrite the named catalog

    Args:
        name: name of catalog
        catalog: catalog instance to use, if provided
        impl: catalog module to initialize. Only used if `catalog` param not provided
        set_as_default: whether this catalog should be treated as the default.
            This will overwrite any existing default.
            NOTE: Catalogs with one entry will always use the single entry as the default.
        ray_init_args: ray initialization args (used only if ray not already initialized)
    """
    import deltacat.catalog.model.catalog as catalog_module

    if catalog is None:
        catalog = Catalog(impl, **kwargs)

    if is_initialized():
        ray.get(catalog_module.all_catalogs.put.remote(name, catalog, set_as_default))
    else:
        # NOTE - since we are initializing with a single catalog, it will be set to the default
        if not set_as_default:
            logger.warning(
                f"Calling put_catalog with set_as_default=False, "
                f"but still setting Catalog {catalog} as default since it is the only catalog."
            )
        init({name: catalog}, ray_init_args=ray_init_args, **kwargs)

    return catalog
