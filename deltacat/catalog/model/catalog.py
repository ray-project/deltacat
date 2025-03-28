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

# registry of namespace to ActorHandle for Catalogs class
catalog_registry: Dict[str, ray.actor.ActorHandle] = {}
_default_namespace = "DEFAULT"

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


def is_initialized(*args, **kwargs) -> bool:
    """
    Check if DeltaCAT is initialized

    Supports initializing in multiple Ray namespaces (for testing) using kwarg "namespace"
    """
    global catalog_registry

    namespace = kwargs.get("namespace", _default_namespace)
    if namespace not in catalog_registry:
        return False

    # Get the catalog actor for this namespace
    namespaced_catalogs = catalog_registry[namespace]

    try:
        # Try a simple operation on the actor - this will fail if the actor is dead
        ray.get(namespaced_catalogs.names.remote(), timeout=5)
        return True
    except Exception as e:
        # The actor is dead or inaccessible, so we're not properly initialized
        logger.info(f"Actor check for namespace '{namespace}' failed: {e}")
        catalog_registry.pop(namespace, None)  # Remove from registry
        return False


def init(
    catalogs: Union[Dict[str, Catalog], Catalog],
    default_catalog_name: Optional[str] = None,
    ray_init_args: Dict[str, Any] = None,
    *args,
    **kwargs,
) -> None:
    """
    Initialize DeltaCAT catalogs.

    :param catalogs: Either a single Catalog instance of a map of string to Catalog instance
    :param default_catalog_name: The Catalog to use by default. If only one Catalog is provided, it will
        be set as the default
    :param ray_init_args: kwargs to pass to ray initialization

    """

    force_reinitialize = kwargs.get("force_reinitialize", False)

    # get namespace from ray_init_args, then kwargs, then default
    if ray_init_args and "namespace" in ray_init_args:
        namespace = ray_init_args["namespace"]
    else:
        namespace = kwargs.get("namespace", _default_namespace)

    if is_initialized(**{"namespace": namespace}) and not force_reinitialize:
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

    global catalog_registry

    catalog_registry[namespace] = Catalogs.remote(
        catalogs=catalogs, default_catalog_name=default_catalog_name
    )


def get_catalog(name: Optional[str] = None, **kwargs) -> Catalog:
    """
    Get a catalog by name or the default catalog if no name is provided.

    Args:
        name: Name of catalog to retrieve (optional, uses default if not provided)

    Returns:
        The requested Catalog, or KeyError if it does not exist
    """
    namespace = kwargs.get("namespace", _default_namespace)

    global catalog_registry
    if namespace not in catalog_registry:
        raise KeyError(
            f"No catalogs available! Call "
            "`deltacat.init(catalogs={...})` to register one or more "
            "catalogs then retry."
        )

    catalog_actor = catalog_registry[namespace]

    if name is not None:
        catalog = ray.get(catalog_actor.get.remote(name))
    else:
        default = ray.get(catalog_actor.default.remote())
        if not default:
            available_catalogs = ray.get(catalog_actor.all.remote()).values()
            raise KeyError(
                f"Call to get_catalog without name failed because no default catalog is configured."
                f"Available catalogs are: {available_catalogs}"
            )
        catalog = default

    if not catalog:
        available_catalogs = ray.get(catalog_actor.all.remote()).values()
        raise KeyError(
            f"Catalog '{name}' not found in namespace '{namespace}'. "
            f"Available catalogs: {available_catalogs}."
        )
    return catalog


def put_catalog(
    name: str,
    *,
    impl: ModuleType = DeltacatCatalog,
    catalog: Catalog = None,
    ray_init_args: Dict[str, Any] = None,
    **kwargs,
) -> Catalog:
    """
    Add a named catalog to the global map of named catalogs. Initializes ray if not already initialized.

    You may explicitly provide an initialized Catalog instance, like from the Catalog constructor or
    from factory methods like Catalog.default or Catalog.iceberg

    Otherwise, this function initializes the catalog by using the catalog implementation provided by `impl`.

    Args:
        name: name of catalog
        catalog: catalog instance to use, if provided
        impl: catalog module to initialize. Only used if `catalog` param not provided
        ray_init_args: ray initialization args (used if ray must be initialized)
    """
    # Set namespace to default, or use value in kwargs or ray_init_args
    if kwargs.get("namespace") is not None:
        namespace = kwargs.get("namespace")
        # ensure ray init arg set with this namespace
        ray_init_args = ray_init_args if ray_init_args is not None else {}
        ray_init_args["namespace"] = kwargs.get("namespace")
    elif ray_init_args and "namespace" in ray_init_args:
        namespace = ray_init_args["namespace"]
    else:
        namespace = _default_namespace

    if catalog is None:
        catalog = Catalog(impl, **kwargs)
    elif catalog is not None and impl != DeltacatCatalog:
        raise ValueError(
            f"PutCatalog call provided both `impl` and `catalog` parameters. "
            f"You may only provide one of these parameters"
        )

    if is_initialized(**{"namespace": namespace}):
        try:
            get_catalog(name, namespace=namespace)
            raise ValueError(
                f"Catalog {name} already exists in namespace '{namespace}'."
            )
        except KeyError:
            # Catalog does not already exist. Add it
            global catalog_registry
            ray.get(catalog_registry[namespace].put.remote(name, catalog))
    else:
        init({name: catalog}, ray_init_args=ray_init_args, **{"namespace": namespace})
    return catalog
