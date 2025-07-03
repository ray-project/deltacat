#!/usr/bin/env python3
"""
DeltaCAT Table Monitor for Converter Agent

This script monitors Iceberg tables for changes and runs converter sessions
to resolve duplicates using positional deletes.
"""

import argparse
import logging
import time
from typing import List

import ray
import deltacat
import pyarrow.fs as pafs
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

from deltacat.compute.converter.converter_session import converter_session
from deltacat.compute.converter.model.converter_session_params import ConverterSessionParams
from deltacat.utils.filesystem import (
    resolve_path_and_filesystem, 
    FilesystemType,
)
import deltacat.logs as logs

# Initialize DeltaCAT logger
logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def monitor_table(
    catalog_impl: str,
    warehouse_path: str,
    catalog_uri: str,
    namespace: str,
    table_name: str,
    merge_keys: List[str],
    filesystem_type: FilesystemType = FilesystemType.LOCAL,
    monitor_interval: float = 5.0,
    max_converter_parallelism: int = 1,
    ray_inactivity_timeout: int = 10,
) -> None:
    """Monitor an Iceberg table for changes and run converter sessions when needed."""
    
    logger.info(f"Starting table monitor for {table_name}")
    logger.info(f"Monitor interval: {monitor_interval}s")
    logger.info(f"Merge keys: {merge_keys}")
    logger.info(f"Ray inactivity timeout: {ray_inactivity_timeout}s")
    
    # Create PyIceberg catalog
    catalog = load_catalog(
        "monitor_catalog", 
        type="rest" if "rest" in catalog_impl.lower() else "hive",
        warehouse=warehouse_path,
        uri=catalog_uri,
    )
    
    # Set up filesystem
    filesystem = FilesystemType.to_filesystem(filesystem_type)
    if filesystem_type == FilesystemType.UNKNOWN:
        normalized_warehouse_path, filesystem = resolve_path_and_filesystem(warehouse_path)
        warehouse_path = normalized_warehouse_path
    
    logger.info(f"Resolved filesystem: {type(filesystem).__name__}")
    logger.info(f"Normalized warehouse path: {warehouse_path}")
    
    # Parse table identifier
    if not namespace:
        namespace = "default"
    table_identifier = f"{namespace}.{table_name}"
    logger.info(f"  - Parsed table - namespace: '{namespace}', table: '{table_name}'")
    
    last_snapshot_id = None
    start_time = time.time()
    last_write_time = start_time  # Track last time we saw table activity
    
    while True:
        # Sleep before next iteration
        logger.debug(f"Sleeping for {monitor_interval}s before next check...")
        time.sleep(monitor_interval)

        logger.info(f"Checking table {table_identifier}")
        
        try:
            # Try to load the table
            try:
                tbl = catalog.load_table(table_identifier)
                current_snapshot_id = tbl.metadata.current_snapshot_id
                
                if last_snapshot_id != current_snapshot_id:
                    logger.info(f"New table version detected - snapshot ID: {current_snapshot_id}")
                    logger.info(f"Table has {len(tbl.metadata.snapshots)} snapshots")
                    logger.info(f"Table format version: {tbl.metadata.format_version}")
                    
                    # Update last activity time when we detect table changes
                    last_write_time = time.time()
                    
                    # Always run deduplication when there are snapshots (duplicates can exist within a single snapshot)
                    if tbl.metadata.snapshots and len(tbl.metadata.snapshots) >= 1:
                        logger.info(f"Table has data - triggering converter session to resolve any duplicates...")
                        logger.debug(f"Converter parameters:")
                        logger.debug(f"-catalog: {type(catalog).__name__}")
                        logger.debug(f"-table: {table_identifier}")
                        logger.debug(f"-warehouse: {warehouse_path}")
                        logger.debug(f"-merge_keys: {merge_keys}")
                        logger.debug(f"-filesystem: {type(filesystem).__name__}")
                        
                        # Run converter session
                        try:
                            converter_params = ConverterSessionParams.of({
                                "catalog": catalog,
                                "iceberg_namespace": namespace,
                                "iceberg_table_name": table_name,
                                "iceberg_warehouse_bucket_name": warehouse_path,
                                "merge_keys": merge_keys,
                                "enforce_primary_key_uniqueness": True,
                                "task_max_parallelism": max_converter_parallelism,
                                "filesystem": filesystem,
                                "location_provider_prefix_override": None,
                            })
                            
                            logger.info(f"Starting converter session...")
                            converter_session(params=converter_params)
                            logger.info(f"Converter session completed successfully")
                            
                        except Exception as e:
                            logger.error(f"Converter session failed: {e}")
                            logger.error(f"Exception traceback:", exc_info=True)
                    else:
                        logger.info(f"Table has no snapshots - no data to deduplicate")
                    
                    last_snapshot_id = current_snapshot_id
                else:
                    logger.debug(f"No table changes detected (snapshot ID: {current_snapshot_id})")
                    
            except NoSuchTableError:
                logger.info(f"Table {table_identifier} does not exist yet - waiting...")
                
        except Exception as e:
            logger.error(f"Error in table monitor: {e}")
            logger.error(f"Exception traceback:", exc_info=True)
            
        # Check for Ray inactivity timeout
        current_time = time.time()
        inactivity_duration = current_time - last_write_time
        
        if inactivity_duration >= ray_inactivity_timeout:
            logger.info(f"Ray inactivity timeout reached ({inactivity_duration:.1f}s >= {ray_inactivity_timeout}s)")
            logger.info(f"No table activity detected for {inactivity_duration:.1f} seconds, shutting down Ray...")
            
            try:
                if ray.is_initialized():
                    ray.shutdown()
                    logger.info("Ray cluster shut down successfully due to inactivity")
                else:
                    logger.info("Ray was not initialized, nothing to shut down")
            except Exception as e:
                logger.error(f"Error shutting down Ray: {e}")
            
            logger.info(f"Table monitor stopping due to inactivity timeout")
            break
    
    logger.info(f"Table monitor completed")


def run(
    catalog_impl: str,
    warehouse_path: str,
    catalog_uri: str,
    namespace: str,
    table_name: str,
    merge_keys: str,
    filesystem_type: str = "local",
    monitor_interval: float = 1.0,
    max_converter_parallelism: int = 1,
    ray_inactivity_timeout: int = 10,
) -> None:
    """Run table monitor with the given parameters."""
    
    # Parse merge keys
    merge_keys_list = [key.strip() for key in merge_keys.split(",") if key.strip()]
    
    # Run the monitor
    monitor_table(
        catalog_impl=catalog_impl,
        warehouse_path=warehouse_path,
        catalog_uri=catalog_uri,
        namespace=namespace,
        table_name=table_name,
        merge_keys=merge_keys_list,
        filesystem_type=filesystem_type,
        monitor_interval=monitor_interval,
        max_converter_parallelism=max_converter_parallelism,
        ray_inactivity_timeout=ray_inactivity_timeout,
    )


if __name__ == "__main__":
    """
    DeltaCAT Table Monitor - Monitor Iceberg tables and run converter sessions
    
    Example usage:
    $ python table_monitor.py \
    $   --catalog-impl 'org.apache.iceberg.rest.RESTCatalog' \
    $   --warehouse-path '/tmp/iceberg-warehouse' \
    $   --catalog-uri 'http://localhost:8181' \
    $   --namespace 'default' \
    $   --table-name 'demo_table' \
    $   --merge-keys 'id' \
    $   --monitor-interval 1.0 \
    $   --max-converter-parallelism 2 \
    $   --ray-inactivity-timeout 300.0
    """
    
    script_args = [
        (
            ["--catalog-impl"],
            {
                "help": "Catalog implementation class name",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--warehouse-path"],
            {
                "help": "Warehouse path",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--catalog-uri"],
            {
                "help": "Catalog URI",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--namespace"],
            {
                "help": "Table namespace",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--table-name"],
            {
                "help": "Table name to monitor",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--merge-keys"],
            {
                "help": "Comma-separated merge key column names",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--filesystem-type"],
            {
                "help": "Filesystem type",
                "type": str,
                "default": "local",
            },
        ),
        (
            ["--monitor-interval"],
            {
                "help": "Seconds between monitoring checks",
                "type": float,
                "default": 5.0,
            },
        ),
        (
            ["--max-converter-parallelism"],
            {
                "help": "Maximum number of concurrent converter tasks",
                "type": int,
                "default": 1,
            },
        ),

        (
            ["--ray-inactivity-timeout"],
            {
                "help": "Ray inactivity timeout in seconds (Ray will shutdown if no activity)",
                "type": int,
                "default": 300,
            },
        ),

    ]
    
    # Parse CLI input arguments
    parser = argparse.ArgumentParser(
        description="DeltaCAT Table Monitor - Monitor Iceberg tables and run converter sessions"
    )
    for args, kwargs in script_args:
        parser.add_argument(*args, **kwargs)
    args = parser.parse_args()
    print(f"[TABLE MONITOR] Command Line Arguments: {args}")
    
    # Initialize DeltaCAT
    deltacat.init()
    
    # Run the table monitor using the parsed arguments
    run(**vars(args)) 