"""
DeltaCAT Table Monitor Job. Automatically runs data converter sessions in response to table updates.
"""
# Allow classes to use self-referencing Type hints in Python 3.7.
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import logging
import os
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

import pyarrow.fs as pafs
import ray
import deltacat

from pyiceberg.catalog import load_catalog, CatalogType
from pyiceberg.exceptions import NoSuchTableError

from deltacat import job_client, local_job_client
from deltacat.constants import DEFAULT_NAMESPACE, NANOS_PER_SEC
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


class CallbackStage(str, Enum):
    """
    Enumeration of callback stages in the table monitor lifecycle.
    """

    PRE = "pre"
    POST = "post"


class CallbackResult(dict):
    """
    Output result returned by table monitor callbacks.

    Common fields:
        status: Callback operation status (e.g., 'success', 'error', 'skipped')
        reason: Optional reason for errors or skipped callback operations
        properties: Optional dictionary for arbitrary user-defined values
        Additional callback-specific fields can be included as needed
    """

    @staticmethod
    def of(params: Dict[str, Any]) -> CallbackResult:
        """
        Create a CallbackResult from a dictionary.

        Args:
            params: Dictionary containing callback result data

        Returns:
            CallbackResult instance
        """
        return CallbackResult(params)

    @property
    def status(self) -> str:
        """Callback operation status (e.g., 'success', 'error', 'skipped')."""
        return self["status"]

    @property
    def reason(self) -> Optional[str]:
        """Reason for error or skipped callback operation (optional)."""
        return self.get("reason")

    @property
    def properties(self) -> Dict[str, Any]:
        """
        Dictionary for arbitrary user-defined properties.

        Returns empty dict if not set. Can be used to store additional
        callback-specific metadata or results.
        """
        if "properties" not in self:
            self["properties"] = {}
        return self["properties"]


class CallbackContext(dict):
    """
    Input context passed to table monitor callbacks.
    """

    @staticmethod
    def of(params: Dict[str, Any]) -> CallbackContext:
        """
        Create a CallbackContext from a dictionary.

        Args:
            params: Dictionary containing callback context parameters

        Returns:
            CallbackContext instance
        """
        return CallbackContext(params)

    @property
    def last_write_time(self) -> int:
        """Timestamp of last table write activity (epoch nanoseconds)."""
        return self["last_write_time"]

    @property
    def last_snapshot_id(self) -> Optional[str]:
        """Previous snapshot ID (may be None for first conversion)."""
        return self.get("last_snapshot_id")

    @property
    def snapshot_id(self) -> str:
        """Current snapshot ID."""
        return self["snapshot_id"]

    @property
    def catalog_uri(self) -> Optional[str]:
        """Catalog URI."""
        return self.get("catalog_uri")

    @property
    def namespace(self) -> str:
        """Table namespace."""
        return self["namespace"]

    @property
    def table_name(self) -> str:
        """Table name."""
        return self["table_name"]

    @property
    def merge_keys(self) -> List[str]:
        """List of merge key column names."""
        return self["merge_keys"]

    @property
    def max_converter_parallelism(self) -> int:
        """Maximum number of concurrent converter tasks."""
        return self["max_converter_parallelism"]

    @property
    def conversion_start_time(self) -> Optional[int]:
        """Timestamp when conversion started (epoch nanoseconds, post-conversion only)."""
        return self.get("conversion_start_time")

    @property
    def conversion_end_time(self) -> Optional[int]:
        """Timestamp when conversion ended (epoch nanoseconds, post-conversion only)."""
        return self.get("conversion_end_time")

    @property
    def stage(self) -> str:
        """Callback stage: 'pre' or 'post'."""
        return self["stage"]


# Type definitions for callbacks
CallbackType = Callable[[CallbackContext], Optional[CallbackResult]]
CallbackSpec = Union[CallbackType, str]  # Function or "module:function"


def _resolve_callback(callback_spec: Optional[CallbackSpec]) -> Optional[CallbackType]:
    """
    Resolve a callback specification to a callable function.

    Args:
        callback_spec: Either a callable function or a string in "module.path:function_name" format

    Returns:
        Resolved callable function or None

    Raises:
        ValueError: If string format is invalid
        TypeError: If callback_spec type is unsupported

    Examples:
        >>> _resolve_callback(my_function)  # Direct function
        <function my_function>

        >>> _resolve_callback("my_module.callbacks:post_conversion")  # String import
        <function post_conversion>
    """
    if callback_spec is None:
        return None

    if callable(callback_spec):
        return callback_spec

    if isinstance(callback_spec, str):
        # Parse "module.path:function_name" format
        if ":" not in callback_spec:
            raise ValueError(
                f"String callback must be in 'module:function' format (e.g., 'my_module:my_func'), "
                f"got: {callback_spec}"
            )

        module_path, func_name = callback_spec.rsplit(":", 1)
        try:
            module = importlib.import_module(module_path)
            callback_func = getattr(module, func_name)
            if not callable(callback_func):
                raise TypeError(
                    f"Resolved callback '{func_name}' from module '{module_path}' is not callable"
                )
            return callback_func
        except ImportError as e:
            raise ImportError(
                f"Failed to import module '{module_path}' for callback: {e}"
            )
        except AttributeError as e:
            raise AttributeError(
                f"Function '{func_name}' not found in module '{module_path}': {e}"
            )

    raise TypeError(
        f"Invalid callback type: {type(callback_spec).__name__}. "
        f"Expected callable or string in 'module:function' format."
    )


def _invoke_callback(
    callback: Optional[CallbackType], context: CallbackContext
) -> None:
    """
    Invoke a callback with the given context, handling errors gracefully.

    Args:
        callback: The callback function to invoke (or None)
        context: CallbackContext instance to pass to the callback
    """
    if callback is None:
        return

    try:
        stage_label = f"{context.stage}-conversion"
        logger.info(f"Invoking {stage_label} callback...")
        result = callback(context)
        if result is not None:
            logger.info(
                f"{stage_label.capitalize()} callback result: {json.dumps(result, default=str)}"
            )
    except Exception as e:
        stage_label = f"{context.stage}-conversion"
        logger.error(f"{stage_label.capitalize()} callback failed: {e}", exc_info=True)
        # Don't fail the conversion if callback fails


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
    pre_conversion_callback: Optional[CallbackSpec] = None,
    post_conversion_callback: Optional[CallbackSpec] = None,
    catalog: Optional[Any] = None,
) -> None:
    """
    Monitor an Iceberg table for changes and run converter sessions when needed.

    Args:
        catalog_type: Type of catalog (rest, hive, sql)
        warehouse_path: Path to the warehouse
        catalog_uri: URI of the catalog
        namespace: Table namespace
        table_name: Name of the table to monitor
        merge_keys: List of merge key column names
        filesystem_type: Type of filesystem to use
        monitor_interval: Seconds between monitoring checks
        max_converter_parallelism: Maximum number of concurrent converter tasks
        ray_inactivity_timeout: Seconds to wait before shutting down Ray cluster
        pre_conversion_callback: Optional callback invoked before converter session.
            Can be a callable or string in "module:function" format.
            Receives CallbackContext with all conversion parameters.
        post_conversion_callback: Optional callback invoked after converter session completes.
            Can be a callable or string in "module:function" format.
            Receives CallbackContext with all conversion parameters plus timing information.
            Return value is logged.
        catalog: Optional pre-initialized catalog instance. If provided, catalog_type,
            warehouse_path, and catalog_uri are ignored. Useful for tests and scenarios
            where catalog sharing is needed.
    """

    logger.info(
        f"Starting table monitor. Namespace: '{namespace}', Table: '{table_name}', "
        f"Warehouse: '{warehouse_path}', Catalog type: '{catalog_type}', "
        f"Catalog URI: '{catalog_uri or 'None'}', Merge keys: '{merge_keys}', "
        f"Filesystem type: '{filesystem_type}', Monitor interval: '{monitor_interval}s', "
        f"Max converter parallelism: '{max_converter_parallelism}', "
        f"Ray inactivity timeout: '{ray_inactivity_timeout}s'"
    )

    # Resolve callbacks
    pre_callback = _resolve_callback(pre_conversion_callback)
    post_callback = _resolve_callback(post_conversion_callback)

    if pre_callback:
        logger.info(f"Pre-conversion callback configured: {pre_callback.__name__}")
    if post_callback:
        logger.info(f"Post-conversion callback configured: {post_callback.__name__}")

    # Create or use provided PyIceberg catalog
    if catalog is None:
        logger.info(
            f"Loading new catalog: type={catalog_type}, warehouse={warehouse_path}, uri={catalog_uri}"
        )
        catalog = load_catalog(
            "monitor_catalog",
            type=catalog_type,
            warehouse=warehouse_path,
            uri=catalog_uri or None,
        )
    else:
        logger.info("Using provided catalog instance")

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
    start_time = time.time_ns()  # Use nanosecond precision
    last_write_time = start_time  # Track last time we saw table activity (nanoseconds)

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

                # Update last activity time when we detect table changes (nanoseconds)
                last_write_time = time.time_ns()

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

                    # Build callback context
                    callback_context = CallbackContext.of(
                        {
                            "last_write_time": last_write_time,
                            "last_snapshot_id": last_snapshot_id,
                            "snapshot_id": current_snapshot_id,
                            "catalog_uri": catalog_uri,
                            "namespace": namespace,
                            "table_name": table_name,
                            "merge_keys": merge_keys,
                            "max_converter_parallelism": max_converter_parallelism,
                            "stage": CallbackStage.PRE.value,
                        }
                    )

                    # Invoke pre-conversion callback
                    _invoke_callback(pre_callback, callback_context)

                    logger.info(f"Starting converter session...")
                    conversion_start_time = time.time_ns()  # Nanosecond precision
                    metadata, snapshot_id, metrics_data = converter_session(
                        params=converter_params
                    )
                    conversion_end_time = time.time_ns()  # Nanosecond precision

                    logger.info(f"Converter session completed successfully")
                    current_snapshot_id = snapshot_id
                    logger.info(
                        f"Current snapshot ID updated to: {current_snapshot_id}"
                    )

                    # Update callback context with conversion timing and final snapshot
                    callback_context["conversion_start_time"] = conversion_start_time
                    callback_context["conversion_end_time"] = conversion_end_time
                    callback_context["snapshot_id"] = snapshot_id
                    callback_context["stage"] = CallbackStage.POST.value

                    # Invoke post-conversion callback
                    _invoke_callback(post_callback, callback_context)

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
        current_time = time.time_ns()  # Nanoseconds
        inactivity_duration_ns = current_time - last_write_time
        inactivity_duration = (
            inactivity_duration_ns / NANOS_PER_SEC
        )  # Convert to seconds for comparison

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
    pre_conversion_callback: Optional[str] = None,
    post_conversion_callback: Optional[str] = None,
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
        pre_conversion_callback: Optional callback spec string in "module:function" format,
            invoked before converter session
        post_conversion_callback: Optional callback spec string in "module:function" format,
            invoked after converter session completes
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

    # Add optional callback arguments
    if pre_conversion_callback:
        cmd_args.append(f"--pre-conversion-callback '{pre_conversion_callback}'")
    if post_conversion_callback:
        cmd_args.append(f"--post-conversion-callback '{post_conversion_callback}'")

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
    pre_conversion_callback: Optional[str] = None,
    post_conversion_callback: Optional[str] = None,
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
        pre_conversion_callback=pre_conversion_callback,
        post_conversion_callback=post_conversion_callback,
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
        (
            ["--pre-conversion-callback"],
            {
                "help": "Optional callback invoked before converter session. Format: 'module.path:function_name'",
                "type": str,
                "default": None,
            },
        ),
        (
            ["--post-conversion-callback"],
            {
                "help": "Optional callback invoked after converter session completes. Format: 'module.path:function_name'",
                "type": str,
                "default": None,
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
