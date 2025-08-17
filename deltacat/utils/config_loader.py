import yaml
from typing import Dict

from deltacat.catalog.model.properties import CatalogProperties


def load_catalog_configs_from_yaml(config_path: str) -> Dict[str, CatalogProperties]:
    """
    Load one or more catalog configs from a YAML file.

    The YAML can either be:
      1. A single unnamed config (a dict of properties) -> wrapped as {"default": CatalogProperties}
      2. A dictionary of named configs: name -> property-mapping

    Args:
        config_path: Path to the YAML config file.

    Returns:
        Dict[str, CatalogProperties]: Mapping of Catalog name -> CatalogProperties.
    """
    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)

    if not isinstance(config_data, dict):
        raise ValueError(
            f"Invalid YAML format in {config_path}. "
            f"Expected a dict, got {type(config_data)}"
        )

    # Case 1: single unnamed config
    # e.g. {"type": "iceberg", "uri": "...", "warehouse": "prod"}
    if all(
        isinstance(v, (str, int, float, bool, type(None))) or isinstance(v, list)
        for v in config_data.values()
    ):
        return {"default": CatalogProperties(**config_data)}

    # Case 2: top-level dict of name -> dict-of-properties
    catalogs: Dict[str, CatalogProperties] = {}
    for name, props in config_data.items():
        if not isinstance(props, dict):
            raise ValueError(
                f"Config for catalog '{name}' must be a mapping, got {type(props)}"
            )
        catalogs[name] = CatalogProperties(**props)

    return catalogs
