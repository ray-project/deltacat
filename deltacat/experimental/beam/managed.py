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


# Store original functions before monkey-patching
_original_write = beam.managed.Write
_original_read = beam.managed.Read

# Global dictionary to track monitoring threads
_monitoring_threads = {}


def _extract_catalog_config_from_beam(config):
    """Extract catalog configuration from Beam config."""
    catalog_properties = config.get("catalog_properties", {})
    
    # Extract catalog implementation class
    catalog_impl = catalog_properties.get("catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
    
    # Extract other relevant properties
    warehouse = catalog_properties.get("warehouse", "")
    uri = catalog_properties.get("uri", "")
    
    return {
        "catalog_impl": catalog_impl,
        "warehouse": warehouse,
        "uri": uri,
        "catalog_properties": catalog_properties
    }


def _upgrade_table_to_format_v2(catalog, table_identifier: str) -> bool:
    """
    Upgrade an Iceberg table from format version 1 to version 2.
    
    Args:
        catalog: PyIceberg catalog instance
        table_identifier: Table identifier (e.g., "default.table_name")
        
    Returns:
        bool: True if upgrade was successful, False otherwise
    """
    try:
        print(f"[DELTACAT INFO] 🔧 Attempting to upgrade table {table_identifier} to format version 2...")
        
        # Load the table
        tbl = catalog.load_table(table_identifier)
        current_version = tbl.metadata.format_version
        
        if current_version >= 2:
            print(f"[DELTACAT INFO] ✅ Table {table_identifier} is already format version {current_version}")
            return True
        
        print(f"[DELTACAT INFO] ⬆️  Upgrading table from format version {current_version} to version 2...")
        
        # Upgrade using transaction with proper commit
        with tbl.transaction() as tx:
            # Use PyIceberg's dedicated upgrade method
            tx.upgrade_table_version(format_version=2)
            # Also set merge-on-read properties for converter compatibility
            tx.set_properties(**{
                "write.format.default": "parquet",
                "write.delete.mode": "merge-on-read",
                "write.update.mode": "merge-on-read", 
                "write.merge.mode": "merge-on-read",
            })
            # Explicitly commit the transaction
            tx.commit_transaction()
        
        # Refresh and verify the upgrade
        tbl.refresh()
        new_version = tbl.metadata.format_version
        
        if new_version >= 2:
            print(f"[DELTACAT INFO] ✅ Successfully upgraded table {table_identifier} to format version {new_version}")
            print(f"[DELTACAT INFO] ✅ Position deletes are now supported for duplicate resolution")
            return True
        else:
            print(f"[DELTACAT ERROR] ❌ Table upgrade failed - still format version {new_version}")
            return False
            
    except Exception as e:
        print(f"[DELTACAT ERROR] ❌ Failed to upgrade table {table_identifier}: {str(e)}")
        return False


def _run_converter_session_with_ray(
    catalog,
    table_identifier: str,
    warehouse_path: str,
    merge_keys: list,
) -> bool:
    """
    Initialize Ray, run converter session, and shutdown Ray.
    
    Returns True if conversion was successful, False otherwise.
    """
    print(f"[DELTACAT DEBUG] Initializing Ray cluster for converter session...")
    
    try:
        # Initialize Ray the same way as setup_ray_cluster fixture
        ray.init(local_mode=True, ignore_reinit_error=True)
        print(f"[DELTACAT DEBUG] Ray cluster initialized successfully")
        
        # Determine appropriate filesystem based on warehouse path
        if warehouse_path.startswith("s3://") or warehouse_path.startswith("s3a://"):
            # For S3 paths, we'd need S3 filesystem configuration
            # For now, assume local filesystem for compatibility with REST catalog example
            filesystem = pafs.LocalFileSystem()
        else:
            # Local filesystem
            filesystem = pafs.LocalFileSystem()
        
        # Create converter session parameters following the test pattern
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
        
        return True
        
    except Exception as e:
        error_msg = str(e)
        print(f"[DELTACAT ERROR] Error during converter session: {error_msg}")
        
        # Handle specific errors with automatic remediation
        if "Cannot store delete manifests in a v1 table" in error_msg:
            print(f"[DELTACAT INFO] 🔍 Table format version 1 detected - attempting automatic upgrade...")
            
            # Attempt to upgrade the table automatically
            upgrade_success = _upgrade_table_to_format_v2(catalog, table_identifier)
            
            if upgrade_success:
                print(f"[DELTACAT INFO] 🔄 Retrying converter session with upgraded table...")
                try:
                    # Retry the converter session after successful upgrade
                    result = converter_session(params=converter_params)
                    print(f"[DELTACAT DEBUG] ✅ Converter session completed successfully after table upgrade")
                    print(f"[DELTACAT DEBUG] Conversion result: {result}")
                    return True
                except Exception as retry_error:
                    print(f"[DELTACAT ERROR] ❌ Converter session failed even after table upgrade: {str(retry_error)}")
                    return False
            else:
                print(f"[DELTACAT ERROR] ❌ Table upgrade failed - cannot resolve duplicates")
                print(f"[DELTACAT INFO] Manual intervention may be required")
                return False
                
        elif "cannot schedule new futures after shutdown" in error_msg:
            print(f"[DELTACAT INFO] Ray cluster already shutdown, skipping duplicate conversion")
        else:
            print(f"[DELTACAT ERROR] Unexpected error during conversion:")
            traceback.print_exc()
        
        return False
        
    finally:
        # Always try to shutdown Ray if it was initialized
        try:
            if ray.is_initialized():
                print(f"[DELTACAT DEBUG] Shutting down Ray cluster...")
                ray.shutdown()
                print(f"[DELTACAT DEBUG] Ray cluster shutdown completed")
        except Exception as shutdown_error:
            print(f"[DELTACAT DEBUG] Ray shutdown error (can be ignored): {shutdown_error}")


def _monitor_table_versions(
    beam_catalog_config: dict, table_name: str, interval: float, merge_keys: list
):
    """Monitor table versions using PyIceberg catalog that matches the Beam catalog type."""
    
    print(
        f"[DELTACAT DEBUG] Starting table version monitor for {table_name} (interval: {interval}s, merge_keys: {merge_keys})"
    )

    # Extract catalog configuration from Beam config
    beam_catalog_impl = beam_catalog_config["catalog_impl"]
    warehouse_path = beam_catalog_config["warehouse"]
    beam_uri = beam_catalog_config["uri"]
    
    print(f"[DELTACAT DEBUG] Beam catalog implementation: {beam_catalog_impl}")
    print(f"[DELTACAT DEBUG] Warehouse path: {warehouse_path}")
    print(f"[DELTACAT DEBUG] Beam URI: {beam_uri}")
    
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
                        
                        # Run converter session with Ray
                        conversion_success = _run_converter_session_with_ray(
                            catalog=catalog,
                            table_identifier=table_identifier,
                            warehouse_path=warehouse_path,
                            merge_keys=merge_keys,
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
    deltacat_optimizer_interval = config.pop("deltacat_optimizer_interval", 1.0)
    merge_keys = config.pop("merge_keys", ["id"])

    # Extract table name and warehouse path
    table_name = config.get("table", "unknown")
    warehouse_path = config.get("catalog_properties", {}).get("warehouse", "")

    print(f"  - table: {table_name}")
    print(f"  - deltacat_optimizer_interval: {deltacat_optimizer_interval}s")
    print(f"  - merge_keys: {merge_keys}")
    print(f"  - warehouse_path: {warehouse_path}")

    # Extract catalog configuration for monitoring
    beam_catalog_config = _extract_catalog_config_from_beam(config)
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
            args=(beam_catalog_config, table_name, deltacat_optimizer_interval, merge_keys),
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

