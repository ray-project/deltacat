import time
import threading
import traceback

import apache_beam as beam
import ray
import pyarrow.fs as pafs

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from deltacat.compute.converter.converter_session import converter_session
from deltacat.compute.converter.model.converter_session_params import ConverterSessionParams
from deltacat.utils.filesystem import resolve_path_and_filesystem


# Store original functions before monkey-patching
_original_write = beam.managed.Write
_original_read = beam.managed.Read

# Global dictionary to track monitoring threads
_monitoring_threads = {}

# Global Ray cluster management
_ray_cluster_initialized = False
_ray_last_activity_time = None
_ray_inactivity_timeout = 300  # Default 5 minutes
_ray_shutdown_timer = None


def _initialize_ray_if_needed() -> bool:
    """
    Initialize Ray cluster if not already initialized.
    
    Returns:
        bool: True if Ray is ready (either was already initialized or just initialized)
    """
    global _ray_cluster_initialized, _ray_last_activity_time
    
    if not _ray_cluster_initialized and not ray.is_initialized():
        try:
            ray.init(local_mode=True, ignore_reinit_error=True)
            _ray_cluster_initialized = True
            print(f"[DELTACAT DEBUG] Ray cluster initialized successfully")
        except Exception as e:
            print(f"[DELTACAT ERROR] Failed to initialize Ray cluster: {e}")
            return False
    elif ray.is_initialized():
        # Ray is already initialized but our flag wasn't set
        _ray_cluster_initialized = True
    
    # Update last activity time
    _ray_last_activity_time = time.time()
    return True


def _schedule_ray_shutdown():
    """
    Schedule Ray cluster shutdown after the configured timeout period.
    Cancels any existing shutdown timer.
    """
    global _ray_shutdown_timer
    
    # Cancel existing timer if any
    if _ray_shutdown_timer is not None:
        _ray_shutdown_timer.cancel()
    
    # Schedule new shutdown
    _ray_shutdown_timer = threading.Timer(_ray_inactivity_timeout, _shutdown_ray_cluster)
    _ray_shutdown_timer.daemon = True  # Don't prevent program exit
    _ray_shutdown_timer.start()
    
    print(f"[DELTACAT DEBUG] Scheduled Ray cluster shutdown in {_ray_inactivity_timeout} seconds")


def _shutdown_ray_cluster():
    """
    Shutdown the Ray cluster if it's been inactive for the timeout period.
    """
    global _ray_cluster_initialized, _ray_last_activity_time, _ray_shutdown_timer
    
    current_time = time.time()
    if _ray_last_activity_time and (current_time - _ray_last_activity_time) >= _ray_inactivity_timeout:
        try:
            if ray.is_initialized():
                print(f"[DELTACAT DEBUG] Shutting down Ray cluster after {_ray_inactivity_timeout}s of inactivity...")
                ray.shutdown()
                print(f"[DELTACAT DEBUG] Ray cluster shutdown completed")
            _ray_cluster_initialized = False
            _ray_last_activity_time = None
            _ray_shutdown_timer = None
        except Exception as e:
            print(f"[DELTACAT DEBUG] Ray shutdown error (can be ignored): {e}")
    else:
        # Activity detected, reschedule shutdown
        remaining_time = _ray_inactivity_timeout - (current_time - (_ray_last_activity_time or current_time))
        if remaining_time > 0:
            _ray_shutdown_timer = threading.Timer(remaining_time, _shutdown_ray_cluster)
            _ray_shutdown_timer.daemon = True
            _ray_shutdown_timer.start()


def _extract_catalog_config_from_beam(config):
    """Extract catalog configuration from Beam config."""
    catalog_properties = config.get("catalog_properties", {})
    
    # Extract catalog implementation class
    catalog_impl = catalog_properties.get("catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
    
    # Extract other relevant properties
    warehouse = catalog_properties.get("warehouse", "")
    uri = catalog_properties.get("uri", "")
    
    # Extract Ray cluster inactivity timeout (seconds) - support both old and new names
    ray_inactivity_timeout = config.get("ray_inactivity_timeout") or config.get("ray_cluster_shutdown_timeout", 300)
    
    return {
        "catalog_impl": catalog_impl,
        "warehouse": warehouse,
        "uri": uri,
        "ray_inactivity_timeout": ray_inactivity_timeout,
        "catalog_properties": catalog_properties
    }


def _raise_format_version_error(table_identifier: str) -> None:
    """
    Raise an informative error when encountering format version 1 tables.
    
    Args:
        table_identifier: Table identifier (e.g., "default.table_name")
        
    Raises:
        RuntimeError: With instructions to upgrade Iceberg installation
    """
    error_message = f"""
❌ TABLE FORMAT VERSION 1 DETECTED

Table '{table_identifier}' is using Iceberg format version 1, but DeltaCAT converter
requires format version 2 or higher for position delete support.

🔧 SOLUTION: Upgrade to Iceberg 1.4.0+ which defaults to format version 2

If using Docker, replace your current Iceberg REST catalog with:

    docker stop iceberg-rest-catalog
    docker rm iceberg-rest-catalog  
    docker run -d -p 8181:8181 --name iceberg-rest-catalog tabulario/iceberg-rest:1.6.0

📚 Why format version 2?
- Position deletes for efficient duplicate resolution
- Better performance and modern Iceberg features
- Industry standard since Iceberg 1.4.0

For more information, see: https://iceberg.apache.org/docs/latest/
    """.strip()
    
    raise RuntimeError(error_message)


def _run_converter_session_with_ray(
    catalog,
    table_identifier: str,
    warehouse_path: str,
    merge_keys: list,
    filesystem: pafs.FileSystem,
) -> bool:
    """
    Run converter session using managed Ray cluster.
    
    Args:
        catalog: Iceberg catalog instance
        table_identifier: Table identifier (e.g., "default.table_name")
        warehouse_path: Warehouse path
        merge_keys: List of merge key column names
        filesystem: PyArrow filesystem instance to use
        
    Returns:
        True if conversion was successful, False otherwise.
    """
    print(f"[DELTACAT DEBUG] Running converter session with managed Ray cluster...")
    
    # Initialize Ray cluster if needed
    if not _initialize_ray_if_needed():
        print(f"[DELTACAT ERROR] Failed to initialize Ray cluster")
        return False
    
    try:
        # Create converter session parameters using the provided filesystem
        converter_params = ConverterSessionParams.of({
            "catalog": catalog,
            "iceberg_table_name": table_identifier,
            "iceberg_warehouse_bucket_name": warehouse_path,
            "merge_keys": merge_keys,
            "enforce_primary_key_uniqueness": True,
            "task_max_parallelism": 1,  # Single task for local testing
            "filesystem": filesystem,
            "location_provider_prefix_override": None,
        })
        
        print(f"[DELTACAT DEBUG] Running converter session with parameters:")
        print(f"  - table: {table_identifier}")
        print(f"  - warehouse: {warehouse_path}")
        print(f"  - merge_keys: {merge_keys}")
        print(f"  - filesystem: {type(filesystem).__name__}")
        
        # Run the converter session
        result = converter_session(params=converter_params)
        
        print(f"[DELTACAT DEBUG] Converter session completed successfully")
        print(f"[DELTACAT DEBUG] Conversion result: {result}")
        
        # Schedule Ray cluster shutdown after successful conversion
        _schedule_ray_shutdown()
        
        return True
        
    except Exception as e:
        error_msg = str(e)
        print(f"[DELTACAT ERROR] Error during converter session: {error_msg}")
        
        # Handle specific errors 
        if "Cannot store delete manifests in a v1 table" in error_msg:
            print(f"[DELTACAT INFO] 🔍 Table format version 1 detected")
            # Raise informative error instead of attempting automatic upgrade
            _raise_format_version_error(table_identifier)
                
        elif "cannot schedule new futures after shutdown" in error_msg:
            print(f"[DELTACAT INFO] Ray cluster already shutdown, skipping duplicate conversion")
        else:
            print(f"[DELTACAT ERROR] Unexpected error during conversion:")
            traceback.print_exc()
        
        # Schedule Ray cluster shutdown after error (but keep it alive for retries)
        _schedule_ray_shutdown()
        
        return False


def _monitor_table_versions(
    beam_catalog_config: dict, table_name: str, interval: float, merge_keys: list, filesystem: pafs.FileSystem = None
):
    """Monitor table versions using PyIceberg catalog that matches the Beam catalog type."""
    
    print(
        f"[DELTACAT DEBUG] Starting table version monitor for {table_name} (interval: {interval}s, merge_keys: {merge_keys})"
    )

    # Set global Ray shutdown timeout from configuration
    global _ray_inactivity_timeout
    _ray_inactivity_timeout = beam_catalog_config.get("ray_inactivity_timeout", 300)
    print(f"[DELTACAT DEBUG] Ray cluster shutdown timeout: {_ray_inactivity_timeout}s")

    # Extract catalog configuration from Beam config
    beam_catalog_impl = beam_catalog_config["catalog_impl"]
    warehouse_path = beam_catalog_config["warehouse"]
    beam_uri = beam_catalog_config["uri"]
    
    print(f"[DELTACAT DEBUG] Beam catalog implementation: {beam_catalog_impl}")
    print(f"[DELTACAT DEBUG] Warehouse path: {warehouse_path}")
    print(f"[DELTACAT DEBUG] Beam URI: {beam_uri}")
    
    # If no filesystem provided, auto-resolve it from the warehouse path
    if filesystem is None:
        normalized_warehouse_path, filesystem = resolve_path_and_filesystem(warehouse_path)
        print(f"[DELTACAT DEBUG] Auto-resolved filesystem: {type(filesystem).__name__}")
        print(f"[DELTACAT DEBUG] Normalized warehouse path: {normalized_warehouse_path}")
        # Update warehouse_path to the normalized path
        warehouse_path = normalized_warehouse_path
    else:
        print(f"[DELTACAT DEBUG] Using provided filesystem: {type(filesystem).__name__}")
    
    # Mapping from Beam/Java catalog implementations to PyIceberg catalog types
    CATALOG_TYPE_MAPPING = {
        "org.apache.iceberg.hive.HiveCatalog": "hive", 
        "org.apache.iceberg.rest.RESTCatalog": "rest",
        "org.apache.iceberg.aws.glue.GlueCatalog": "glue",
        "org.apache.iceberg.jdbc.JdbcCatalog": "sql",
        # Unsupported catalogs in pyiceberg
        "org.apache.iceberg.hadoop.HadoopCatalog": None,
        "org.apache.iceberg.nessie.NessieCatalog": None,
    }
    
    # Determine PyIceberg catalog type
    pyiceberg_catalog_type = CATALOG_TYPE_MAPPING.get(beam_catalog_impl)
    
    if pyiceberg_catalog_type is None:
        supported_types = [k for k, v in CATALOG_TYPE_MAPPING.items() if v is not None]
        raise ValueError(
            f"Unsupported catalog type: {beam_catalog_impl}. "
            f"Supported types: {supported_types}"
        )
    
    print(f"[DELTACAT DEBUG] Mapping Beam catalog {beam_catalog_impl} to PyIceberg type: {pyiceberg_catalog_type}")
    
    # Create PyIceberg catalog with appropriate configuration
    catalog_config = {
        "type": pyiceberg_catalog_type,
        "warehouse": warehouse_path,
    }
    
    # Add catalog-specific configuration
    if pyiceberg_catalog_type == "hive":
        catalog_config["uri"] = beam_uri or "thrift://localhost:9083"  # Use Beam URI or default
    elif pyiceberg_catalog_type == "rest":
        catalog_config["uri"] = beam_uri or "http://localhost:8080"  # Use Beam URI or default
    elif pyiceberg_catalog_type == "glue":
        # AWS Glue catalog uses AWS credentials and region
        # Additional AWS-specific properties can be extracted from beam_catalog_config
        pass
    elif pyiceberg_catalog_type == "sql":
        catalog_config["uri"] = beam_uri or "jdbc:postgresql://localhost:5432/iceberg"  # Use Beam URI or default
    
    catalog = load_catalog("monitor_catalog", **catalog_config)
    
    # Parse table identifier to extract namespace and table name
    if "." in table_name:
        namespace, actual_table_name = table_name.split(".", 1)
    else:
        namespace = "default"
        actual_table_name = table_name
    
    print(f"[DELTACAT DEBUG] Parsed table - namespace: '{namespace}', table: '{actual_table_name}'")
    table_identifier = f"{namespace}.{actual_table_name}"
    
    last_snapshot_id = None

    while True:
        try:
            # Try to load the table directly from the catalog
            try:
                tbl = catalog.load_table(table_identifier)
                current_snapshot_id = tbl.metadata.current_snapshot_id
                
                if last_snapshot_id != current_snapshot_id:
                    print(f"[DELTACAT DEBUG] New table version detected - snapshot ID: {current_snapshot_id}")
                    print(f"[DELTACAT DEBUG] Merge keys configured: {merge_keys}")
                    print(f"[DELTACAT DEBUG] Table identifier: {table_identifier}")
                    
                    # Display table metadata information
                    print(f"[DELTACAT DEBUG] Schema ID: {tbl.metadata.current_schema_id}")
                    print(f"[DELTACAT DEBUG] Format version: {tbl.metadata.format_version}")
                    print(f"[DELTACAT DEBUG] Metadata location: {tbl.metadata.location}")
                    
                    # Display snapshot information if available  
                    if current_snapshot_id and tbl.metadata.snapshots:
                        current_snapshot = None
                        for snapshot in tbl.metadata.snapshots:
                            if snapshot.snapshot_id == current_snapshot_id:
                                current_snapshot = snapshot
                                break
                        
                        if current_snapshot:
                            print(f"[DELTACAT DEBUG] Snapshot timestamp: {current_snapshot.timestamp_ms}")
                            if hasattr(current_snapshot, 'operation'):
                                print(f"[DELTACAT DEBUG] Snapshot operation: {current_snapshot.operation}")
                            if current_snapshot.summary:
                                print(f"[DELTACAT DEBUG] Snapshot summary: {current_snapshot.summary}")
                    
                    # Check if table has potential duplicates (more than 1 snapshot indicates multiple writes)
                    if tbl.metadata.snapshots and len(tbl.metadata.snapshots) > 1:
                        print(f"[DELTACAT DEBUG] Triggering converter session to resolve duplicates...")
                        
                        # Run converter session with Ray using the resolved filesystem
                        conversion_success = _run_converter_session_with_ray(
                            catalog=catalog,
                            table_identifier=table_identifier,
                            warehouse_path=warehouse_path,
                            merge_keys=merge_keys,
                            filesystem=filesystem,
                        )
                        
                        if conversion_success:
                            print(f"[DELTACAT DEBUG] ✅ Converter session completed successfully for {table_identifier}")
                        else:
                            print(f"[DELTACAT ERROR] ❌ Converter session failed for {table_identifier}")
                    else:
                        print(f"[DELTACAT DEBUG] Single snapshot detected - no conversion needed")
                    
                    last_snapshot_id = current_snapshot_id
                    
            except NoSuchTableError:
                # For all supported catalog types, the table should exist in the external catalog
                print(f"[DELTACAT ERROR] Table {table_identifier} does not exist in {pyiceberg_catalog_type} catalog at: {warehouse_path}")
                    
            except Exception as e:
                print(f"[DELTACAT ERROR] Could not load table {table_identifier} from {pyiceberg_catalog_type} catalog at: {warehouse_path}: {e}")

            time.sleep(interval)

        except Exception as e:
            print(f"[DELTACAT DEBUG] Error in table monitor: {e}")
            time.sleep(interval)


def write(*args, **kwargs):
    """Wrapper function that automatically applies DeltaCatOptimizer to Write operations."""
    print(f"[DELTACAT DEBUG] Initializing DeltaCatOptimizer for WRITE operation")
    print(f"[DELTACAT DEBUG] WRITE operation called with:")
    print(f"  - args: {args}")
    print(f"  - kwargs keys: {list(kwargs.keys()) if kwargs else 'None'}")

    # Extract and pop deltacat-specific config keys
    config = kwargs.get("config", {}).copy() if kwargs.get("config") else {}
    
    # Extract DeltaCAT converter properties from parent config or individual keys (for backward compatibility)
    deltacat_converter_properties = config.pop("deltacat_converter_properties", {})
    
    # Support both new nested structure and old flat structure for backward compatibility
    deltacat_converter_interval = (
        deltacat_converter_properties.get("deltacat_converter_interval") or
        deltacat_converter_properties.get("deltacat_optimizer_interval") or
        config.pop("deltacat_converter_interval", None) or 
        config.pop("deltacat_optimizer_interval", 1.0)
    )
    
    merge_keys = (
        deltacat_converter_properties.get("merge_keys") or
        config.pop("merge_keys", ["id"])
    )
    
    ray_inactivity_timeout = (
        deltacat_converter_properties.get("ray_inactivity_timeout") or
        deltacat_converter_properties.get("ray_cluster_shutdown_timeout") or
        config.pop("ray_inactivity_timeout", None) or 
        config.pop("ray_cluster_shutdown_timeout", 300)
    )
    
    # Extract filesystem parameter (optional) - can be in converter properties or top-level config
    filesystem = (
        deltacat_converter_properties.get("filesystem") or
        config.pop("filesystem", None)
    )

    # Extract table name and warehouse path
    table_name = config.get("table", "unknown")
    warehouse_path = config.get("catalog_properties", {}).get("warehouse", "")

    print(f"  - table: {table_name}")
    print(f"  - deltacat_converter_interval: {deltacat_converter_interval}s")
    print(f"  - merge_keys: {merge_keys}")
    print(f"  - ray_inactivity_timeout: {ray_inactivity_timeout}s")
    print(f"  - warehouse_path: {warehouse_path}")
    print(f"  - filesystem: {type(filesystem).__name__ if filesystem else 'None (auto-resolve)'}")
    print(f"  - using deltacat_converter_properties: {len(deltacat_converter_properties) > 0}")

    # Extract catalog configuration for monitoring (including Ray timeout)
    beam_catalog_config = _extract_catalog_config_from_beam({
        **config,
        "ray_inactivity_timeout": ray_inactivity_timeout
    })
    print(f"  - catalog_impl: {beam_catalog_config['catalog_impl']}")

    # Update kwargs with the modified config
    if "config" in kwargs:
        kwargs["config"] = config

    # Start monitoring thread if not already running for this table
    monitor_key = f"{warehouse_path}_{table_name}"
    if (
        monitor_key not in _monitoring_threads
        or not _monitoring_threads[monitor_key].is_alive()
    ):
        monitor_thread = threading.Thread(
            target=_monitor_table_versions,
            args=(beam_catalog_config, table_name, deltacat_converter_interval, merge_keys, filesystem),
            daemon=True,
        )
        monitor_thread.start()
        _monitoring_threads[monitor_key] = monitor_thread
        print(f"[DELTACAT DEBUG] Started monitoring thread for {table_name}")
    else:
        print(f"[DELTACAT DEBUG] Monitoring thread already running for {table_name}")

    print(f"[DELTACAT DEBUG] Delegating to underlying WRITE transform")
    return _original_write(*args, **kwargs)


def read(*args, **kwargs):
    """Wrapper function that automatically applies DeltaCatOptimizer to Read operations."""
    print(f"[DELTACAT DEBUG] Initializing DeltaCatOptimizer for READ operation")
    print(f"[DELTACAT DEBUG] READ operation called with:")
    print(f"  - args: {args}")
    print(f"  - kwargs keys: {list(kwargs.keys()) if kwargs else 'None'}")

    # Extract table name from config if available
    if kwargs and "config" in kwargs and isinstance(kwargs["config"], dict):
        table_name = kwargs["config"].get("table", "unknown")
        print(f"  - table: {table_name}")

    print(f"[DELTACAT DEBUG] Delegating to underlying READ transform")
    return _original_read(*args, **kwargs)

