# Allow self-referencing Type hints in Python 3.7.
from __future__ import annotations

import logging
from pathlib import Path
from types import ModuleType

from typing import Any, Dict, List, Optional, Union
from functools import partial
import ray
import yaml

from deltacat import logs
from deltacat.catalog.main import impl as dcat
from deltacat.catalog.model.properties import CatalogProperties
from deltacat.constants import DEFAULT_CATALOG
from deltacat import constants

all_catalogs: Optional[ray.actor.ActorHandle] = None

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


class Catalog:
    def __init__(
        self,
        config: Optional[Union[CatalogProperties, Any]] = None,
        impl: ModuleType = dcat,
        inner: Optional[CatalogProperties] = None,
        *args,
        **kwargs,
    ):
        """
        Constructor for a Catalog.

        If inner is not provided, invokes `impl.initialize(config, *args, **kwargs)` and
        stores its return value in the `inner` property. This captures all state requirem
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
        self._inner = inner or self._impl.initialize(config=config, *args, **kwargs)
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
        # non-native catalogs may fail to pickle, so exclude _inner
        # (e.g. Iceberg catalog w/ unserializable SSLContext from boto3 client)
        return partial(self.__class__, **self._kwargs), (
            self._config,
            self._impl,
            self._inner if isinstance(self._inner, CatalogProperties) else None,
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
        self.try_dump()

    def names(self) -> List[str]:
        return list(self._catalogs.keys())

    def put(self, name: str, catalog: Catalog, set_default: bool = False) -> None:
        self._catalogs[name] = catalog
        if set_default or len(self._catalogs) == 1:
            self._default_catalog = catalog
        self.try_dump()

    def get(self, name) -> Optional[Catalog]:
        return self._catalogs.get(name)

    def pop(self, name) -> Optional[Catalog]:
        catalog = self._catalogs.pop(name, None)
        if catalog and self._default_catalog_name == name:
            if len(self._catalogs) == 1:
                self._default_catalog = list(self._catalogs.values())[0]
            else:
                self._default_catalog = None
        self.try_dump()
        return catalog

    def clear(self) -> None:
        self._catalogs.clear()
        self._default_catalog = None
        self.try_dump()

    def dump(self, config_path: Optional[str] = None) -> None:
        dump_catalog_config(self._catalogs, config_path)

    def try_dump(self):
        try:
            self.dump()
        except Exception as e:
            logger.warning(f"Failed to save catalog config for this session: {e}")

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
    config_path: Optional[str] = None,
    restore_last_session: bool = False,
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
    :param config_path: Catalog config file path. Mutually exclusive with `catalogs`.
        If provided, catalogs will be loaded from this file. Any catalogs found at
        this path will take precedence over loading the prior catalogs config
        if `restore_last_session` is True.
    :param restore_last_session: Whether to restore the last session's catalogs.
        If True, catalogs will be loaded from the last session's catalog config file
        given by the DELTACAT_CONFIG_PATH environment variable
        (default: "~/.deltacat_config/config.yaml"). This argument is ignored if
        `config_path` is also provided and at least one catalog is found at the given
        path.
    :returns: The Ray context object if Ray was initialized, otherwise None.
    """
    global all_catalogs

    if is_initialized() and not force:
        logger.warning("DeltaCAT already initialized.")
        return None

    # If catalogs are provided and a config_path is also provided, raise ValueError
    if catalogs and config_path is not None:
        raise ValueError(
            "Cannot provide both `catalogs` and `config_path`. Please provide "
            "only one of these parameters."
        )
    # If no catalogs provided but config_path exists, load catalogs from config
    if not catalogs and config_path is not None:
        catalogs = load_catalog_config(config_path=config_path)

    # If no catalogs resolved and restore_last_session is True, load last session's catalogs
    if not catalogs and restore_last_session:
        logger.info(
            f"Loading last session's catalog config from: {constants.DELTACAT_CONFIG_PATH}"
        )
        catalogs = load_catalog_config(constants.DELTACAT_CONFIG_PATH)

    # initialize ray (and ignore reinitialization errors)
    ray_init_args["ignore_reinit_error"] = True
    context = ray.init(**ray_init_args)

    # register custom serializer for catalogs since these may contain
    # unserializable objects like boto3 clients with SSLContext
    ray.util.register_serializer(
        Catalog, serializer=Catalog.__reduce__, deserializer=Catalog.__init__
    )
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
    Equivalent to calling init(catalogs={DEFAULT_CATALOG: Catalog()}).

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
        catalogs={DEFAULT_CATALOG: Catalog(config=config)},
        default=DEFAULT_CATALOG,
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
                f"{available_catalogs}. Use `dc.init(default='my_catalog_name')` to "
                f"set the default catalog."
            )
    return catalog


def clear_catalogs() -> None:
    """
    Clear all catalogs from the global map of named catalogs.
    """
    global all_catalogs
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

    # Remove from in-memory actor
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
    DeltaCAT if not already initialized, and persists the catalog
    set to DELTACAT_CONFIG_PATH (merge if exists).

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
    # Initialize if necessary
    if not is_initialized():
        if not default:
            logger.info(
                f"Calling put_catalog with set_as_default=False, "
                f"but still setting Catalog {catalog} as default since it's "
                f"the only catalog."
            )
        init({name: catalog}, ray_init_args=ray_init_args)
    else:
        # Fail if requested
        if fail_if_exists:
            try:
                get_catalog(name)
                raise ValueError(
                    f"Failed to put catalog {name} because it already exists "
                    f"and fail_if_exists={fail_if_exists}"
                )
            except ValueError as e:
                if "not found" not in str(e):
                    raise
                # Doesn't exist → safe to add

        # Register in memory
        ray.get(all_catalogs.put.remote(name, catalog, default))

    return catalog


def save_catalogs(config_path: Optional[str] = None) -> None:
    """
    Save the current catalogs to the file at the given config_path. If config_path
    is not provided, uses the DELTACAT_CONFIG_PATH environment variable
    (default: "~/.deltacat_config/config.yaml").
    """
    if all_catalogs:
        ray.get(all_catalogs.dump.remote(config_path))


def load_catalog_config(config_path: str) -> Dict[str, Catalog]:
    """
    Load one or more catalog configs from a catalog config YAML file.

    Args:
        config_path: Path to the catalog config YAML file.

    Returns:
        Dict[str, CatalogProperties]: Mapping of Catalog name -> CatalogProperties.
    """
    config_path = Path(config_path).expanduser()
    if not config_path.exists():
        raise FileNotFoundError(
            f"Failed to restore catalog config. No file found at: {config_path}",
        )
    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f) or {}

    if not isinstance(config_data, dict):
        raise ValueError(
            f"Invalid catalog config at '{config_path}'. "
            f"Expected a YAML dictionary, but found: {type(config_data)}"
        )
    return {
        name: Catalog(CatalogProperties.from_serializable(props))
        for name, props in config_data.items()
    }


def dump_catalog_config(
    catalogs: Dict[str, Catalog],
    config_path: Optional[str] = None,
) -> None:
    """
    Write the given Catalog dictionary to a YAML file at the given config_path.
    If config_path is not provided, uses DELTACAT_CONFIG_PATH.
    """

    # Normalize inputs into { str: CatalogProperties }
    catalog_properties: Dict[str, CatalogProperties] = {}
    for name, obj in catalogs.items():
        if isinstance(obj, Catalog):
            if not isinstance(obj.inner, CatalogProperties):
                raise TypeError(
                    f"Expected CatalogProperties but found: {type(obj.inner)} "
                )
            catalog_properties[name] = obj.inner
        else:
            raise TypeError(f"Expected Catalog but found: {type(obj)}")

    # Serialize all values to primitive dictionaries.
    data = {name: props.to_serializable() for name, props in catalog_properties.items()}

    # Write out to YAML
    config_path = Path(config_path or constants.DELTACAT_CONFIG_PATH).expanduser()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w") as f:
        yaml.safe_dump(data, f, sort_keys=False)
