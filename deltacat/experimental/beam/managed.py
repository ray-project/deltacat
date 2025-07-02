import time
import threading
import posixpath

import apache_beam as beam

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from deltacat.utils.filesystem import resolve_path_and_filesystem, list_directory


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


def _monitor_table_versions(
    beam_catalog_config: dict, table_name: str, interval: float, merge_keys: list
):
    """Monitor table versions using PyIceberg catalog that matches the Beam catalog type."""
    import re
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
    from deltacat.utils.filesystem import resolve_path_and_filesystem, list_directory
    
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
        "org.apache.iceberg.hadoop.HadoopCatalog": "in-memory",
        "org.apache.iceberg.hive.HiveCatalog": "hive", 
        "org.apache.iceberg.rest.RESTCatalog": "rest",
        "org.apache.iceberg.aws.glue.GlueCatalog": "glue",
        "org.apache.iceberg.jdbc.JdbcCatalog": "sql",
        # Unsupported catalogs
        "org.apache.iceberg.nessie.NessieCatalog": None,  # Will raise error
    }
    
    # Determine PyIceberg catalog type
    pyiceberg_catalog_type = CATALOG_TYPE_MAPPING.get(beam_catalog_impl)
    
    if pyiceberg_catalog_type is None:
        raise ValueError(
            f"Unsupported catalog type: {beam_catalog_impl}. "
            f"Supported types: {list(CATALOG_TYPE_MAPPING.keys())}"
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
    
    # For in-memory catalogs, we need to create namespace and register table manually
    if pyiceberg_catalog_type == "in-memory":
        print(f"[DELTACAT DEBUG] Using in-memory catalog - will create namespace and register table manually")
        
        # Create namespace if it doesn't exist
        try:
            catalog.create_namespace(namespace)
            print(f"[DELTACAT DEBUG] Created namespace '{namespace}'")
        except NamespaceAlreadyExistsError:
            print(f"[DELTACAT DEBUG] Namespace '{namespace}' already exists")
    
    # For other catalog types, the namespace should already exist in the external catalog
    else:
        print(f"[DELTACAT DEBUG] Using {pyiceberg_catalog_type} catalog - assuming namespace exists")

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
                            
                    print(f"[DELTACAT DEBUG] TODO: Trigger converter job for table {table_name} with merge keys {merge_keys}")
                    
                    last_snapshot_id = current_snapshot_id
                    
            except NoSuchTableError:
                # Table not found in catalog
                if pyiceberg_catalog_type == "in-memory":
                    # For in-memory catalogs, try to register the table by finding metadata files
                    print(f"[DELTACAT DEBUG] Table not found in in-memory catalog - attempting to register from filesystem")
                    
                    try:
                        table_location = posixpath.join(warehouse_path, actual_table_name)
                        metadata_dir = posixpath.join(table_location, "metadata")
                        
                        # Use resolve_path_and_filesystem for proper filesystem handling
                        resolved_metadata_dir, filesystem = resolve_path_and_filesystem(metadata_dir)
                        print(f"[DELTACAT DEBUG] Using filesystem: {type(filesystem).__name__} for path: {resolved_metadata_dir}")
                        
                        try:
                            # Use list_directory to get all files in the metadata directory
                            # list_directory returns tuples of (file_path, file_size)
                            all_files_with_size = list_directory(metadata_dir, filesystem)
                            all_files = [file_path for file_path, file_size in all_files_with_size]
                            metadata_files = [
                                f for f in all_files 
                                if f.endswith('.metadata.json')
                            ]
                            
                            if metadata_files:
                                # Parse version numbers from both Iceberg spec naming schemes:
                                # 1. v<V>.metadata.json
                                # 2. <V>-<random-uuid>.metadata.json
                                def extract_version(filename):
                                    basename = posixpath.basename(filename)
                                    # Try v<V>.metadata.json pattern first
                                    match = re.match(r'v(\d+)\.metadata\.json$', basename)
                                    if match:
                                        return int(match.group(1))
                                    # Try <V>-<random-uuid>.metadata.json pattern
                                    match = re.match(r'(\d+)-[^.]+\.metadata\.json$', basename)
                                    if match:
                                        return int(match.group(1))
                                    return -1
                                
                                # Get the latest metadata file (highest version number)
                                latest_metadata = max(metadata_files, key=extract_version)
                                print(f"[DELTACAT DEBUG] Registering table with metadata: {posixpath.basename(latest_metadata)}")
                                
                                # Register the table with the latest metadata
                                catalog.register_table(table_identifier, latest_metadata)
                                print(f"[DELTACAT DEBUG] Successfully registered table {table_identifier}")
                            else:
                                print(f"[DELTACAT DEBUG] No metadata files found in {metadata_dir}")
                        
                        except Exception as list_error:
                            print(f"[DELTACAT DEBUG] Failed to list files in {metadata_dir}: {list_error}")
                        
                    except Exception as register_error:
                        print(f"[DELTACAT DEBUG] Failed to register table: {register_error}")
                        
                else:
                    # For other catalog types, the table should exist in the external catalog
                    raise ValueError(
                        f"Table {table_identifier} does not exist in {pyiceberg_catalog_type} 
                        catalog at: {warehouse_path}"
                    )
                    
            except Exception as e:
                print(f"[DELTACAT DEBUG] Could not load table {table_identifier}: {e}")

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
    # TODO: Change this from launching a separate thread to instead launching
    #       or connecting to an existing Ray Cluster (whose cluster name is
    #       based on the target table name).
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

