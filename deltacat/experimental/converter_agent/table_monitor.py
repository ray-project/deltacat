"""
DeltaCAT Table Monitor Job. Automatically runs data converter sessions in response to table updates.
"""

import argparse
import hashlib
import json
import logging
import os
import time
from typing import List, Optional

import pyarrow.fs as pafs
import ray
import deltacat

from pyiceberg.catalog import load_catalog, CatalogType
from pyiceberg.exceptions import NoSuchTableError

from deltacat import job_client, local_job_client
from deltacat.constants import DEFAULT_NAMESPACE
from deltacat.compute.converter.converter_session import converter_session
from deltacat.compute.converter.model.converter_session_params import (
    ConverterSessionParams,
)
from deltacat.compute.jobs.client import DeltaCatJobClient
from deltacat.utils.filesystem import (
    resolve_path_and_filesystem,
    FilesystemType,
)
import deltacat.logs as logs

# Initialize DeltaCAT logger
logger = logs.configure_deltacat_logger(logging.getLogger(__name__))


def monitor_table(
    catalog_type: str,
    warehouse_path: str,
    catalog_uri: Optional[str],
    namespace: str,
    table_name: str,
    merge_keys: List[str],
    filesystem_type: FilesystemType = FilesystemType.LOCAL,
    monitor_interval: float = 5.0,
    max_converter_parallelism: int = 1,
    ray_inactivity_timeout: int = 10,
) -> None:
    """Monitor an Iceberg table for changes and run converter sessions when needed."""

    logger.info(
        f"Starting table monitor. Namespace: '{namespace}', Table: '{table_name}', "
        f"Warehouse: '{warehouse_path}', Catalog type: '{catalog_type}', "
        f"Catalog URI: '{catalog_uri or 'None'}', Merge keys: '{merge_keys}', "
        f"Filesystem type: '{filesystem_type}', Monitor interval: '{monitor_interval}s', "
        f"Max converter parallelism: '{max_converter_parallelism}', "
        f"Ray inactivity timeout: '{ray_inactivity_timeout}s'"
    )

    # Create PyIceberg catalog
    catalog = load_catalog(
        "monitor_catalog",
        type=catalog_type,
        warehouse=warehouse_path,
        uri=catalog_uri or None,
    )

    # Set up filesystem
    filesystem = FilesystemType.to_filesystem(filesystem_type)
    if filesystem_type == FilesystemType.UNKNOWN:
        normalized_warehouse_path, filesystem = resolve_path_and_filesystem(
            warehouse_path
        )
        warehouse_path = normalized_warehouse_path

    logger.info(f"Resolved filesystem: {type(filesystem).__name__}")
    logger.info(f"Normalized warehouse path: {warehouse_path}")

    # Parse table identifier
    if not namespace:
        namespace = DEFAULT_NAMESPACE
    table_identifier = f"{namespace}.{table_name}"
    logger.info(f"  - Parsed table - namespace: '{namespace}', table: '{table_name}'")

    last_snapshot_id = None
    start_time = time.time()
    last_write_time = start_time  # Track last time we saw table activity

    while True:
        # Sleep before starting the first iteration and all subsequent iterations
        logger.debug(f"Sleeping for {monitor_interval}s before next check...")
        time.sleep(monitor_interval)

        logger.info(f"Checking table {table_identifier} for updates...")

        # Try to load the table
        try:
            tbl = catalog.load_table(table_identifier)
            current_snapshot_id = tbl.metadata.current_snapshot_id
            if last_snapshot_id != current_snapshot_id:
                logger.info(
                    f"New table version detected - snapshot ID: {current_snapshot_id}"
                )
                logger.info(f"Table has {len(tbl.metadata.snapshots)} snapshots")
                logger.info(f"Table format version: {tbl.metadata.format_version}")

                # Update last activity time when we detect table changes
                last_write_time = time.time()

                # Always run deduplication when there are snapshots (duplicates can exist within a single snapshot)
                logger.info(
                    f"Table has data - triggering converter session to resolve any duplicates..."
                )

                # Run converter session
                try:
                    converter_params = ConverterSessionParams.of(
                        {
                            "catalog": catalog,
                            "iceberg_namespace": namespace,
                            "iceberg_table_name": table_name,
                            "iceberg_warehouse_bucket_name": warehouse_path,
                            "merge_keys": merge_keys,
                            "enforce_primary_key_uniqueness": True,
                            "task_max_parallelism": max_converter_parallelism,
                            "filesystem": filesystem,
                            "location_provider_prefix_override": None,
                        }
                    )

                    logger.debug(f"Converter Session Parameters: {converter_params}")

                    logger.info(f"Starting converter session...")
                    metadata, snapshot_id = converter_session(params=converter_params)
                    logger.info(f"Converter session completed successfully")
                    current_snapshot_id = snapshot_id
                    logger.info(
                        f"Current snapshot ID updated to: {current_snapshot_id}"
                    )
                except Exception as e:
                    logger.error(f"Converter session failed: {e}")
                    logger.error(f"Exception traceback:", exc_info=True)
                last_snapshot_id = current_snapshot_id
            else:
                logger.debug(
                    f"No table changes detected (snapshot ID: {current_snapshot_id})"
                )
        except NoSuchTableError:
            logger.info(f"Table {table_identifier} does not exist yet - waiting...")
        except Exception as e:
            logger.error(f"Error in table monitor: {e}")

        # Check for Ray inactivity timeout
        current_time = time.time()
        inactivity_duration = current_time - last_write_time

        if inactivity_duration >= ray_inactivity_timeout:
            logger.info(
                f"Ray inactivity timeout reached ({inactivity_duration:.1f}s >= {ray_inactivity_timeout}s)"
            )
            logger.info(
                f"No table activity detected for {inactivity_duration:.1f} seconds, shutting down Ray..."
            )

            try:
                if ray.is_initialized():
                    ray.shutdown()
                    logger.info("Ray shutdown successfully due to inactivity")
                else:
                    logger.info("Ray was not initialized, nothing to shut down")
            except Exception as e:
                logger.error(f"Error shutting down Ray: {e}")

            logger.info(f"Table monitor stopping due to inactivity timeout")
            break

    logger.info(f"Table monitor completed")


def _generate_job_name(warehouse_path: str, namespace: str, table_name: str) -> str:
    """
    Generate a unique job name based on warehouse path, namespace, and table name.

    Args:
        warehouse_path: Warehouse path
        namespace: Table namespace
        table_name: Table name

    Returns:
        Job name string.
    """
    # Create a sha1 digest of the warehouse path, namespace, and table name
    digest = hashlib.sha1(
        f"{warehouse_path}-{namespace}-{table_name}".encode()
    ).hexdigest()
    job_name = f"deltacat-monitor-{digest}"

    return job_name


def _cleanup_terminated_jobs_for_submission_id(
    client: DeltaCatJobClient, submission_id: str
) -> bool:
    """Clean up any terminated jobs with the given submission ID."""
    logger.debug(
        f"Searching for terminated jobs to cleanup with submission ID: {submission_id}"
    )
    try:
        all_jobs = client.list_jobs()
        logger.debug(f"All jobs: {all_jobs}")
        for job in all_jobs:
            if job.submission_id == submission_id and job.status.is_terminal():
                logger.info(
                    f"Cleaning up terminated job: {submission_id} (status: {job.status})"
                )
                client.delete_job(submission_id)
                return True
    except Exception as e:
        logger.warning(f"Cleanup failed for job '{submission_id}': {e}")
    return False


def submit_table_monitor_job(
    warehouse_path: str,
    catalog_type: CatalogType,
    catalog_uri: Optional[str],
    namespace: str,
    table_name: str,
    merge_keys: list,
    monitor_interval: float,
    max_converter_parallelism: int,
    filesystem: pafs.FileSystem = None,
    cluster_cfg_file_path: Optional[str] = None,
    ray_inactivity_timeout: int = 10,
) -> str:
    """
    Submit a table monitor job to Ray cluster.

    Args:
        warehouse_path: Warehouse path
        catalog_type: Catalog type
        catalog_uri: Catalog URI
        namespace: Table namespace
        table_name: Table name to monitor
        merge_keys: List of merge key column names
        monitor_interval: Seconds between monitoring checks
        max_converter_parallelism: Maximum number of concurrent converter tasks
        filesystem: PyArrow filesystem instance
        cluster_cfg_file_path: Path to cluster config file (None for local)
        ray_inactivity_timeout: Seconds to wait before shutting down Ray cluster
    Returns:
        Job ID of the submitted job
    """

    # Parse table identifier to extract namespace and table name
    if not namespace:
        namespace = DEFAULT_NAMESPACE

    # Generate unique job ID based on the warehouse and table path
    job_name = _generate_job_name(
        warehouse_path=warehouse_path, namespace=namespace, table_name=table_name
    )

    # Resolve the appropriate local or remote job client
    if cluster_cfg_file_path:
        # Submit to remote cluster
        logger.info(
            f"Preparing to submit job to remote cluster: {cluster_cfg_file_path}"
        )
        # Set the cluster name to the job ID to prevent starting multiple Ray clusters monitoring the same table.
        client = job_client(cluster_cfg_file_path, cluster_name_override=job_name)
    else:
        # Submit to local cluster using DeltaCAT local job client
        ray_init_args = {
            "local_mode": True,
            "resources": {"convert_task": max_converter_parallelism},
        }
        logger.info(
            f"Preparing to submit job locally with ray init args: {ray_init_args}"
        )
        client = local_job_client(ray_init_args=ray_init_args)

    # Add filesystem type - determine from filesystem instance
    filesystem_type = FilesystemType.from_filesystem(filesystem)

    # Build CLI arguments for table_monitor job
    table_monitor_script_dir = os.path.dirname(os.path.abspath(__file__))
    table_monitor_script_path = os.path.join(
        table_monitor_script_dir, "table_monitor.py"
    )

    logger.debug(f"Table monitor script path: {table_monitor_script_path}")
    logger.debug(
        f"Table monitor script exists: {os.path.exists(table_monitor_script_path)}"
    )

    cmd_args = [
        f"python {table_monitor_script_path}",
        f"--catalog-type '{catalog_type.value}'",
        f"--warehouse-path '{warehouse_path}'",
        f"--catalog-uri '{catalog_uri}'",
        f"--namespace '{namespace}'",
        f"--table-name '{table_name}'",
        f"--merge-keys '{json.dumps(merge_keys)}'",
        f"--monitor-interval {monitor_interval}",
        f"--max-converter-parallelism {max_converter_parallelism}",
        f"--ray-inactivity-timeout {ray_inactivity_timeout}",
        f"--filesystem-type '{filesystem_type}'",
    ]

    # Join all arguments
    entrypoint = " ".join(cmd_args)
    logger.debug(
        f"Submitting table monitor job '{job_name}' with entrypoint: {entrypoint}"
    )

    # Clean up any terminated jobs with the same submission ID to allow reuse
    _cleanup_terminated_jobs_for_submission_id(client, job_name)

    # Submit the job with the correct working directory
    # Working directory should be the converter_agent directory where table_monitor.py is located
    job_submission_id = client.submit_job(
        submission_id=job_name,
        entrypoint=entrypoint,
        runtime_env={"working_dir": table_monitor_script_dir},
    )

    logger.info(f"Table monitor job submitted successfully: {job_submission_id}")

    return job_submission_id


def run(
    catalog_type: str,
    warehouse_path: str,
    catalog_uri: Optional[str],
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
    merge_keys_list = json.loads(merge_keys)

    # Run the monitor
    monitor_table(
        catalog_type=catalog_type,
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
    $   --catalog-type 'rest' \
    $   --warehouse-path '/tmp/iceberg-warehouse' \
    $   --catalog-uri 'http://localhost:8181' \
    $   --namespace 'default' \
    $   --table-name 'demo_table' \
    $   --merge-keys '["id"]' \
    $   --monitor-interval 1.0 \
    $   --max-converter-parallelism 2 \
    $   --ray-inactivity-timeout 300.0
    """

    script_args = [
        (
            ["--catalog-type"],
            {
                "help": "Catalog type name (rest, hive, sql)",
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
