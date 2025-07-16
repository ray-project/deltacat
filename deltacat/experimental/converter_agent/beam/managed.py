"""
DeltaCAT Job-based Managed I/O for Apache Beam

This module provides a job-based implementation of the DeltaCAT table monitor
that uses Ray jobs for better scalability and resource management instead of
threading.

Key Features:
- Uses DeltaCAT jobs for table monitoring
- Unique job IDs prevent duplicate monitoring jobs
- Supports both local and remote Ray clusters
- Backward compatible with existing managed.py interface
"""

import logging
from typing import Dict, Any

import apache_beam as beam
from pyiceberg.catalog import CatalogType

from deltacat.experimental.converter_agent.table_monitor import submit_table_monitor_job
from deltacat.compute.converter.constants import DEFAULT_CONVERTER_TASK_MAX_PARALLELISM
import deltacat.logs as logs

# Initialize DeltaCAT logger
logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

# Store original functions before monkey-patching
_original_write = beam.managed.Write


# Create a dictionary of Java catalog impl to CatalogType
JAVA_ICEBERG_CATALOG_IMPL_TO_TYPE = {
    "org.apache.iceberg.rest.restcatalog": CatalogType.REST,
    "org.apache.iceberg.hive.hivecatalog": CatalogType.HIVE,
    "org.apache.iceberg.aws.glue.gluecatalog": CatalogType.GLUE,
    "org.apache.iceberg.jdbc.jdbccatalog": CatalogType.SQL,
}


def _extract_catalog_config_from_beam(config: Dict[str, Any]) -> Dict[str, Any]:
    """Extract catalog configuration from Beam config."""
    catalog_properties = config.get("catalog_properties", {})

    # Extract catalog implementation class
    catalog_impl = catalog_properties.get("catalog-impl")

    # Extract catalog type
    catalog_type = catalog_properties.get("type")

    # Extract other relevant properties
    warehouse = catalog_properties.get("warehouse", "")
    uri = catalog_properties.get("uri", "")

    return {
        "catalog_impl": catalog_impl,
        "type": catalog_type,
        "warehouse": warehouse,
        "uri": uri,
        "catalog_properties": catalog_properties,
    }


def write(*args, **kwargs):
    """Wrapper over beam.managed.Write that automatically creates a DeltaCAT table monitor & converter job."""
    logger.debug(f"Starting DeltaCAT write operation")
    logger.debug(f"args: {args}")
    logger.debug(f"kwargs keys: {list(kwargs.keys()) if kwargs else 'None'}")

    # Extract and pop deltacat-specific config keys
    config = kwargs.get("config", {}).copy() if kwargs.get("config") else {}

    # Extract DeltaCAT converter properties from parent config or individual keys (for backward compatibility)
    deltacat_converter_properties = config.pop("deltacat_converter_properties", {})

    # Support both new nested structure and old flat structure for backward compatibility
    deltacat_converter_interval = deltacat_converter_properties.get(
        "deltacat_converter_interval", 3.0
    )

    merge_keys = deltacat_converter_properties.get("merge_keys")

    # Extract filesystem parameter (optional) - can be in converter properties or top-level config
    filesystem = deltacat_converter_properties.get("filesystem", None)

    # Extract cluster configuration file path (for remote jobs)
    cluster_cfg_file_path = deltacat_converter_properties.get(
        "cluster_cfg_file_path", None
    )

    # Extract max converter parallelism
    max_converter_parallelism = deltacat_converter_properties.get(
        "max_converter_parallelism",
        DEFAULT_CONVERTER_TASK_MAX_PARALLELISM,
    )

    # Extract ray inactivity timeout
    ray_inactivity_timeout = deltacat_converter_properties.get(
        "ray_inactivity_timeout", 10
    )

    # Extract table identifier and warehouse path
    table_identifier = config.get("table")
    if not table_identifier:
        raise ValueError("Table is required")

    if table_identifier and "." in table_identifier:
        namespace, table_name = table_identifier.split(".", 1)
    else:
        namespace = "default"
        table_name = table_identifier

    warehouse_path = config.get("catalog_properties", {}).get("warehouse", "")

    # Extract catalog configuration for monitoring
    beam_catalog_config = _extract_catalog_config_from_beam(config)

    # Derive CatalogType from "catalog_impl" or "type" property
    catalog_impl = beam_catalog_config.get("catalog_impl")
    if catalog_impl:
        catalog_type = JAVA_ICEBERG_CATALOG_IMPL_TO_TYPE.get(catalog_impl.lower())
        if not catalog_type:
            raise ValueError(f"Unsupported catalog implementation: {catalog_impl}")
    else:
        catalog_type_str = beam_catalog_config.get("type")
        if catalog_type_str:
            catalog_type = CatalogType(catalog_type_str.lower())
        else:
            raise ValueError(
                f"No catalog implementation or type found in config: {beam_catalog_config}"
            )

    # Update kwargs with the modified config
    if "config" in kwargs:
        kwargs["config"] = config

    logger.debug(f"Preparing to submit table monitor job...")
    logger.debug(f"table_name: {table_name}")
    logger.debug(f"deltacat_converter_interval: {deltacat_converter_interval}s")
    logger.debug(f"merge_keys: {merge_keys}")
    logger.debug(f"warehouse_path: {warehouse_path}")
    logger.debug(
        f"filesystem: {type(filesystem).__name__ if filesystem else 'None (auto-resolve)'}"
    )
    logger.debug(f"cluster_cfg_file_path: {cluster_cfg_file_path or 'None (local)'}")
    logger.debug(f"max_converter_parallelism: {max_converter_parallelism}")
    logger.debug(f"ray_inactivity_timeout: {ray_inactivity_timeout}s")
    logger.debug(
        f"using deltacat_converter_properties: {len(deltacat_converter_properties) > 0}"
    )
    logger.debug(f"catalog_type: {catalog_type}")

    # Submit monitoring job
    try:
        submit_table_monitor_job(
            warehouse_path=warehouse_path,
            catalog_type=catalog_type,
            catalog_uri=beam_catalog_config.get("uri"),
            namespace=namespace,
            table_name=table_name,
            merge_keys=merge_keys,
            monitor_interval=deltacat_converter_interval,
            filesystem=filesystem,
            cluster_cfg_file_path=cluster_cfg_file_path,
            max_converter_parallelism=max_converter_parallelism,
            ray_inactivity_timeout=ray_inactivity_timeout,
        )
    except Exception as e:
        # Don't fail the write operation, just log the error
        logger.error(f"Failed to submit table monitor job: {e}")
        logger.error(f"Exception traceback:", exc_info=True)
    logger.info(f"Delegating to beam.managed.Write")
    return _original_write(*args, **kwargs)
