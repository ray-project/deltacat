"""
Common utilities for DeltaCAT compactor examples.

This module contains shared functionality used across bootstrap.py, explorer.py,
and compactor.py to reduce code duplication.
"""

from typing import Set, List, Optional, Tuple

import deltacat as dc
from deltacat import DeltaCatUrl
from deltacat.catalog import Catalog, put_catalog, get_table
from deltacat.catalog.model.properties import CatalogProperties
from deltacat.storage import metastore
from deltacat.storage.model.partition import PartitionLocator
from deltacat.storage.model.sort_key import SortKey
from deltacat.storage.model.types import SortOrder


def get_default_catalog_root() -> str:
    """Get the default catalog root directory."""
    return "/tmp/deltacat_test"


def initialize_catalog(
    catalog_root: Optional[str] = None, catalog_name: str = "default"
) -> CatalogProperties:
    """
    Initialize and register a DeltaCAT catalog.

    Args:
        catalog_root: Root directory for the catalog. If None, uses default.
        catalog_name: Name to register the catalog under.

    Returns:
        CatalogProperties instance for the initialized catalog.
    """
    if catalog_root is None:
        catalog_root = get_default_catalog_root()

    catalog = CatalogProperties(root=catalog_root)

    # Initialize catalog and register it
    catalog_obj = Catalog(config=catalog)
    put_catalog(catalog_name, catalog_obj)

    return catalog


def initialize_deltacat_url_catalog(
    catalog_root: Optional[str] = None, catalog_name: str = "compactor_test_catalog"
) -> DeltaCatUrl:
    """
    Initialize a DeltaCAT catalog using URL-based approach (used by explorer.py).

    Args:
        catalog_root: Root directory for the catalog. If None, uses default.
        catalog_name: Name for the catalog URL.

    Returns:
        DeltaCatUrl instance for the initialized catalog.
    """
    if catalog_root is None:
        catalog_root = get_default_catalog_root()

    dc.init()
    catalog_url = DeltaCatUrl(f"dc://{catalog_name}")
    dc.put(catalog_url, root=catalog_root)

    return catalog_url


def parse_primary_keys(primary_keys_str: str) -> Set[str]:
    """Parse comma-separated primary keys string into a set."""
    return set(key.strip() for key in primary_keys_str.split(",") if key.strip())


def parse_partition_values(partition_values_str: str) -> List[str]:
    """Parse comma-separated partition values string into a list."""
    if not partition_values_str.strip():
        return []
    return [value.strip() for value in partition_values_str.split(",") if value.strip()]


def parse_sort_keys(sort_keys_str: str) -> List[SortKey]:
    """Parse comma-separated sort keys string into a list of SortKey objects."""
    if not sort_keys_str or not sort_keys_str.strip():
        return []

    sort_keys = []
    for key in sort_keys_str.split(","):
        key = key.strip()
        if key:
            sort_keys.append(SortKey.of(key=key, sort_order=SortOrder.ASCENDING))
    return sort_keys


def create_partition_locator(
    namespace: str,
    table_name: str,
    table_version: str,
    partition_values: List[str],
) -> PartitionLocator:
    """
    Create a partition locator with the given parameters.
    Note: This creates a locator with partition_id=None, which may not work
    for all operations. Use get_actual_partition_locator() for operations
    that require the actual partition ID.
    """
    return PartitionLocator.of(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        partition_values=partition_values,
    )


def get_actual_partition_locator(
    namespace: str,
    table_name: str,
    table_version: str,
    partition_values: List[str],
    catalog: CatalogProperties,
    catalog_name: str = "default",
) -> PartitionLocator:
    """
    Get the actual partition locator by using metastore to find the partition.
    This matches the approach used in bootstrap.py and ensures compatibility.

    Args:
        namespace: Table namespace
        table_name: Table name
        table_version: Table version
        partition_values: Partition values (can be empty list)
        catalog: CatalogProperties instance
        catalog_name: Name of the registered catalog

    Returns:
        PartitionLocator with actual partition ID
    """
    try:
        # Initialize catalog like bootstrap.py does
        catalog_obj = Catalog(config=catalog)
        put_catalog(catalog_name, catalog_obj)

        # Get table definition first
        table_def = get_table(
            name=table_name, namespace=namespace, catalog=catalog_name
        )

        # Get the actual partition using the table's stream locator
        partition = metastore.get_partition(
            stream_locator=table_def.stream.locator,
            partition_values=partition_values if partition_values else None,
            catalog=catalog,
        )

        return partition.locator

    except Exception as e:
        print(f"⚠️  Failed to get actual partition locator: {e}")
        print(f"   Falling back to basic partition locator")
        return create_partition_locator(
            namespace, table_name, table_version, partition_values
        )


def format_partition_values_for_command(partition_values: Optional[List[str]]) -> str:
    """Format partition values for use in command line arguments."""
    if not partition_values:
        return ""
    return ",".join(str(v) for v in partition_values)


def get_max_stream_position_from_partition(
    namespace: str,
    table_name: str,
    table_version: str,
    partition_values: List[str],
    catalog: CatalogProperties,
    catalog_name: str = "default",
) -> int:
    """
    Get the maximum stream position from a partition by reading its deltas.

    Args:
        namespace: Table namespace
        table_name: Table name
        table_version: Table version
        partition_values: Partition values
        catalog: CatalogProperties instance
        catalog_name: Name of the registered catalog

    Returns:
        Maximum stream position found, or 1000 as fallback
    """
    try:
        # Get the actual partition locator
        partition_locator = get_actual_partition_locator(
            namespace,
            table_name,
            table_version,
            partition_values,
            catalog,
            catalog_name,
        )

        # Create a partition-like object for metastore API
        partition_like = type("obj", (object,), {"locator": partition_locator})()

        # Get deltas from the partition
        partition_deltas = metastore.list_partition_deltas(
            partition_like=partition_like,
            include_manifest=True,
            catalog=catalog,
        )

        delta_list = partition_deltas.all_items()
        if delta_list:
            return max(delta.stream_position for delta in delta_list)
        else:
            return 1000  # fallback

    except Exception as e:
        print(f"⚠️  Failed to get max stream position: {e}")
        return 1000  # fallback


def get_bootstrap_destination_info(
    source_namespace: str, source_table: str
) -> Tuple[str, str]:
    """
    Get the corresponding destination namespace and table name for bootstrap test tables.

    Args:
        source_namespace: Source namespace
        source_table: Source table name

    Returns:
        Tuple of (dest_namespace, dest_table_name)
    """
    if source_namespace == "compactor_test_source" and source_table == "events":
        return "compactor_test_dest", "events_compacted"
    else:
        # Generic fallback
        return source_namespace, f"{source_table}_compacted"


def print_section_header(title: str, char: str = "=", width: int = 80) -> None:
    """Print a formatted section header."""
    print(char * width)
    print(title)
    print(char * width)


def print_subsection_header(title: str, char: str = "-", width: int = 70) -> None:
    """Print a formatted subsection header."""
    print(char * width)
    print(title)
    print(char * width)
