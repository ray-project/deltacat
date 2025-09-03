# Allow self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
from types import ModuleType

from typing import Any, Dict, List, Optional, Union
from functools import partial
import ray

from deltacat import logs
from deltacat.catalog.main import impl as dcat
from deltacat.catalog.model.properties import CatalogProperties
from deltacat.constants import DEFAULT_CATALOG

all_catalogs: Optional[ray.actor.ActorHandle] = None

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class Catalog:
    def __init__(
        self,
        config: Optional[Union[CatalogProperties, Any]] = None,
        impl: ModuleType = dcat,
        *args,
        **kwargs,
    ):
        """
        Constructor for a Catalog.

        Invokes `impl.initialize(config, *args, **kwargs)` and stores its
        return value in the `inner` property. This captures all state required
        to deterministically reconstruct this Catalog instance on any node, and
        must be pickleable by Ray cloudpickle.
        """
        if not isinstance(self, Catalog):
            # self may contain the tuple returned from __reduce__ (ray pickle bug?)
            if callable(self[0]) and isinstance(self[1], tuple):
                logger.info(f"Invoking {self[0]} with positional args: {self[1]}")
                return self[0](*self[1])
            else:
                err_msg = f"Expected `self` to be {Catalog}, but found: {self}"
                raise RuntimeError(err_msg)

        self._config = config
        self._impl = impl
        self._inner = self._impl.initialize(config=config, *args, **kwargs)
        self._args = args
        self._kwargs = kwargs

    @property
    def config(self):
        return self._config

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
        return partial(self.__class__, **self._kwargs), (
            self._config,
            self._impl,
            *self._args,
        )

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
        default: Optional[str] = None,
    ):
        self._catalogs = {}
        self._default_catalog_name = None
        self._default_catalog = None
        self.update(catalogs, default)

    def all(self) -> Dict[str, Catalog]:
        return self._catalogs

    def update(
        self,
        catalogs: Union[Catalog, Dict[str, Catalog]],
        default: Optional[str] = None,
    ) -> None:
        if isinstance(catalogs, Catalog):
            catalogs = {DEFAULT_CATALOG: catalogs}
        elif not isinstance(catalogs, dict):
            raise ValueError(f"Expected Catalog or dict, but found: {catalogs}")
        self._catalogs.update(catalogs)
        if default:
            if default not in catalogs:
                raise ValueError(
                    f"Default catalog `{default}` not found in: {catalogs}"
                )
            self._default_catalog = self._catalogs[default]
            self._default_catalog_name = default
        elif len(catalogs) == 1:
            self._default_catalog = list(self._catalogs.values())[0]
        else:
            self._default_catalog = None

    def names(self) -> List[str]:
        return list(self._catalogs.keys())

    def put(self, name: str, catalog: Catalog, set_default: bool = False) -> None:
        self._catalogs[name] = catalog
        if set_default or len(self._catalogs) == 1:
            self._default_catalog = catalog

    def get(self, name) -> Optional[Catalog]:
        return self._catalogs.get(name)

    def pop(self, name) -> Optional[Catalog]:
        catalog = self._catalogs.pop(name, None)
        if catalog and self._default_catalog_name == name:
            if len(self._catalogs) == 1:
                self._default_catalog = list(self._catalogs.values())[0]
            else:
                self._default_catalog = None
        return catalog

    def clear(self) -> None:
        self._catalogs.clear()
        self._default_catalog = None

    def default(self) -> Optional[Catalog]:
        return self._default_catalog


def is_initialized(*args, **kwargs) -> bool:
    """
    Check if DeltaCAT is initialized.
    """
    global all_catalogs

    if not ray.is_initialized():
        # Any existing Catalogs actor reference must be stale - reset it
        all_catalogs = None
    return all_catalogs is not None


def raise_if_not_initialized(
    err_msg: str = "DeltaCAT is not initialized. Please call `deltacat.init()` and try again.",
) -> None:
    """
    Raises a RuntimeError with the given error message if DeltaCAT is not
    initialized.

    :param err_msg: Custom error message to raise if DeltaCAT is not
    initialized. If unspecified, the default error message is used.
    """
    if not is_initialized():
        raise RuntimeError(err_msg)


def init(
    catalogs: Union[Dict[str, Catalog], Catalog] = {},
    default: Optional[str] = None,
    ray_init_args: Dict[str, Any] = {},
    *,
    force=False,
) -> Optional[ray.runtime.BaseContext]:
    """
    Initialize DeltaCAT catalogs.

    :param catalogs: A single Catalog instance or a map of catalog names to
        Catalog instances.
    :param default: The name of the default Catalog. If only one Catalog is
        provided, it will always be the default.
    :param ray_init_args: Keyword arguments to pass to `ray.init()`.
    :param force: Whether to force DeltaCAT reinitialization. If True, reruns
        ray.init(**ray_init_args) and overwrites all previously registered
        catalogs.
    :returns: The Ray context object if Ray was initialized, otherwise None.
    """
    global all_catalogs

    if is_initialized() and not force:
        logger.warning("DeltaCAT already initialized.")
        return None

    # initialize ray (and ignore reinitialization errors)
    ray_init_args["ignore_reinit_error"] = True
    context = ray.init(**ray_init_args)

    # register custom serializer for catalogs since these may contain
    # unserializable objects like boto3 clients with SSLContext
    ray.util.register_serializer(
        Catalog, serializer=Catalog.__reduce__, deserializer=Catalog.__init__
    )
    # TODO(pdames): If no catalogs are provided then re-initialize DeltaCAT
    #  with all catalogs from the last session
    all_catalogs = Catalogs.remote(catalogs=catalogs, default=default)
    return context


def init_local(
    path: Optional[str] = None,
    ray_init_args: Dict[str, Any] = {},
    *,
    force=False,
) -> Optional[ray.runtime.BaseContext]:
    """
    Initialize DeltaCAT with a default local catalog.

    This is a convenience function that creates a default catalog for local usage.
    Equivalent to calling init(catalogs={"default": Catalog()}).

    :param path: Optional path for catalog root directory. If not provided, uses
        the default behavior of CatalogProperties (DELTACAT_ROOT env var or
        "./.deltacat/").
    :param ray_init_args: Keyword arguments to pass to `ray.init()`.
    :param force: Whether to force DeltaCAT reinitialization. If True, reruns
        ray.init(**ray_init_args) and overwrites all previously registered
        catalogs.
    :returns: The Ray context object if Ray was initialized, otherwise None.
    """
    from deltacat.catalog.model.properties import CatalogProperties

    config = CatalogProperties(root=path) if path is not None else None
    return init(
        catalogs={"default": Catalog(config=config)},
        default="default",
        ray_init_args=ray_init_args,
        force=force,
    )


def get_catalog(name: Optional[str] = None) -> Catalog:
    """
    Get a catalog by name, or the default catalog if no name is provided.

    Args:
        name: Name of catalog to retrieve (optional, uses default if not provided)

    Returns:
        The requested Catalog, or ValueError if it does not exist
    """
    global all_catalogs

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
    else:
        catalog = ray.get(all_catalogs.default.remote())
        if not catalog:
            available_catalogs = list(ray.get(all_catalogs.all.remote()).keys())
            raise ValueError(
                f"Call to get_catalog without name set failed because there "
                f"is no default Catalog set. Available catalogs: "
                f"{available_catalogs}."
            )
    return catalog


def clear_catalogs() -> None:
    """
    Clear all catalogs from the global map of named catalogs.
    """
    if all_catalogs:
        ray.get(all_catalogs.clear.remote())


def pop_catalog(name: str) -> Optional[Catalog]:
    """
    Remove a named catalog from the global map of named catalogs.

    Args:
        name: Name of the catalog to remove.

    Returns:
        The removed catalog, or None if not found.
    """
    global all_catalogs

    if not all_catalogs:
        return None
    catalog = ray.get(all_catalogs.pop.remote(name))
    return catalog


def put_catalog(
    name: str,
    catalog: Catalog = None,
    *,
    default: bool = False,
    ray_init_args: Dict[str, Any] = {},
    fail_if_exists: bool = False,
    **kwargs,
) -> Catalog:
    """
    Add a named catalog to the global map of named catalogs. Initializes
    DeltaCAT if not already initialized.

    Args:
        name: Name of the catalog.
        catalog: Catalog instance to use. If none is provided, then all
            additional keyword arguments will be forwarded to
            `CatalogProperties` for a default DeltaCAT native Catalog.
        default:  Make this the default catalog if multiple catalogs are
            available. If only one catalog is available, it will always be the
            default.
        ray_init_args: Ray initialization args (used only if ray is not already
            initialized).
        fail_if_exists: if True, raises an error if a catalog with the given
            name already exists. If False, inserts or replaces the given
            catalog name.
        kwargs: Additional keyword arguments to forward to `CatalogProperties`
            for a default DeltaCAT native Catalog.

    Returns:
        The catalog put in the named catalog map.
    """
    global all_catalogs

    if not catalog:
        catalog = Catalog(**kwargs)
    if name is None:
        raise ValueError("Catalog name cannot be None")

    # Initialize, if necessary
    if not is_initialized():
        # We are initializing a single catalog - make it the default
        if not default:
            logger.info(
                f"Calling put_catalog with set_as_default=False, "
                f"but still setting Catalog {catalog} as default since it is "
                f"the only catalog."
            )
        init({name: catalog}, ray_init_args=ray_init_args)
        return catalog

    # Fail if fail_if_exists and catalog already exists
    if fail_if_exists:
        try:
            get_catalog(name)
            # If we get here, catalog exists - raise error
            raise ValueError(
                f"Failed to put catalog {name} because it already exists and "
                f"fail_if_exists={fail_if_exists}"
            )
        except ValueError as e:
            if "not found" not in str(e):
                # Re-raise if it's not a "catalog not found" error
                raise
            # If catalog doesn't exist, continue normally
            pass

    # Add the catalog (which may overwrite existing if fail_if_exists=False)
    ray.get(all_catalogs.put.remote(name, catalog, default))
    return catalog
