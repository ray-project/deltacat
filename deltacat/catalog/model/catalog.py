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
from deltacat.constants import DEFAULT_CATALOG, DELTACAT_CONFIG_PATH
from deltacat.utils.config_loader import load_catalog_configs_from_yaml

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
    config_path: Optional[str] = None,
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

    # If catalogs are provided and a config_path is also provided, raise ValueError
    if catalogs and config_path is not None:
        raise ValueError(
            "Cannot provide both `catalogs` and `config_path`. Please provide "
            "only one of these parameters."
        )
    # If no catalogs provided but a config_path exists, load configs from file
    if not catalogs and config_path is not None:
        catalogs = load_catalog_configs_from_yaml(config_path=config_path)

    # If neither catalogs nor config_path provided → try default location
    if not catalogs and config_path is None:
        cfg_path = Path(DELTACAT_CONFIG_PATH).expanduser()
        if cfg_path.exists():
            logger.info(f"Loading catalog configs from default path: {cfg_path}")
            catalogs = load_catalog_configs_from_yaml(str(cfg_path))
        else:
            logger.info(
                "No catalogs specified and no config file found at default path."
            )

    # If catalogs provided but no config_path exists, create a config file
    if catalogs and config_path is None:
        # Only dump if the catalogs are config-backed
        try:
            _dump_catalogs_to_yaml(catalogs)
        except TypeError:
            logger.debug(
                "Skipping dumping catalogs to YAML: non-CatalogProperties inner"
            )

        # Normalize everything to Dict[str, Catalog] ---
        if isinstance(catalogs, Catalog):
            # single Catalog object
            catalogs = {DEFAULT_CATALOG: catalogs}
        elif isinstance(catalogs, dict):
            normalized = {}
            for name, obj in catalogs.items():
                if isinstance(obj, Catalog):
                    normalized[name] = obj
                elif isinstance(obj, CatalogProperties):
                    # Wrap in a Catalog
                    normalized[name] = Catalog(config=obj)
                else:
                    raise TypeError(
                        f"Unsupported object type in catalogs dict: {type(obj)}"
                    )
            catalogs = normalized
        else:
            raise TypeError(
                f"Expected a Catalog or dict[str, Catalog|CatalogProperties], but got {type(catalogs)}"
            )

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

    # Remove from in-memory actor
    catalog = ray.get(all_catalogs.pop.remote(name))

    # --- Persist removal to disk ---
    try:
        cfg_path = Path(DELTACAT_CONFIG_PATH).expanduser()
        if cfg_path.exists() and cfg_path.stat().st_size > 0:  # empty file check
            try:
                # Load existing config
                loaded = load_catalog_configs_from_yaml(str(cfg_path))
                # Wrap into Catalog so _dump_catalogs_to_yaml works uniformly
                existing_catalogs = {
                    n: Catalog(config=props) for n, props in loaded.items()
                }
                if name in existing_catalogs:
                    del existing_catalogs[name]

                    # Now write full merged dictionary back to disk
                    _dump_catalogs_to_yaml(existing_catalogs, single_if_default=False)
            except Exception as e:
                logger.warning(f"Failed to update catalog config file after pop: {e}")
    except Exception as e:
        logger.warning(f"Failed to persist catalog removal {name} to config file: {e}")

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

    # --- Persist to disk (merge behavior) ---
    try:
        cfg_path = Path(DELTACAT_CONFIG_PATH).expanduser()
        existing_catalogs = {}

        if cfg_path.exists() and cfg_path.stat().st_size > 0:  # empty file check
            try:
                loaded = load_catalog_configs_from_yaml(str(cfg_path))
                # Wrap into Catalog so dump_catalogs_to_yaml can handle uniformly
                existing_catalogs = {
                    n: Catalog(config=props) for n, props in loaded.items()
                }
            except Exception as e:
                logger.warning(f"Failed to load existing catalog config file: {e}")

        # Merge / overwrite with the new catalog
        existing_catalogs[name] = catalog

        # Now write back full merged dictionary
        _dump_catalogs_to_yaml(existing_catalogs, single_if_default=False)

    except Exception as e:
        logger.warning(f"Failed to persist catalog {name} to config file: {e}")

    return catalog


def _dump_catalogs_to_yaml(
    catalogs: Union[
        Dict[str, Union["Catalog", CatalogProperties]],
        "Catalog",
        CatalogProperties,
    ],
    *,
    single_if_default: bool = True,
) -> None:
    """
    Write Catalog configs (Catalogs or CatalogProperties) to YAML.

    Supports:
        - Dict[str, Catalog]
        - Dict[str, CatalogProperties]
        - single Catalog
        - single CatalogProperties

    Always normalizes to {name: dict-of-primitive-keys}
    """

    # Normalize inputs into { str: CatalogProperties }
    if isinstance(catalogs, Catalog):
        catalogs = {"default": catalogs.inner}
    elif isinstance(catalogs, CatalogProperties):
        catalogs = {"default": catalogs}
    elif isinstance(catalogs, dict):
        normalized: Dict[str, CatalogProperties] = {}
        for name, obj in catalogs.items():
            if isinstance(obj, Catalog):
                if not isinstance(obj.inner, CatalogProperties):
                    raise TypeError(
                        f"Catalog.inner must be CatalogProperties for dumping, "
                        f"got {type(obj.inner)}"
                    )
                normalized[name] = obj.inner
            elif isinstance(obj, CatalogProperties):
                normalized[name] = obj
            else:
                raise TypeError(f"Unsupported catalog type: {type(obj)}")
        catalogs = normalized
    else:
        raise TypeError(f"Unsupported catalogs type: {type(catalogs)}")

    # Serialize all values to primitive dicts
    if single_if_default and set(catalogs.keys()) == {"default"}:
        data = CatalogProperties.ensure_serializable(catalogs["default"])
    else:
        data = {
            name: CatalogProperties.ensure_serializable(props)
            for name, props in catalogs.items()
        }

    # Write out to YAML
    config_path = Path(DELTACAT_CONFIG_PATH).expanduser()
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w") as f:
        yaml.safe_dump(data, f, sort_keys=False)
