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

import os
import hashlib
from typing import Dict, Any, Optional

import apache_beam as beam
import pyarrow.fs as pafs

from deltacat import job_client, local_job_client
from deltacat.utils.filesystem import FilesystemType
from deltacat.compute.converter.constants import DEFAULT_CONVERTER_TASK_MAX_PARALLELISM


# Store original functions before monkey-patching
_original_write = beam.managed.Write


def _generate_job_name(warehouse_path: str, namespace: str, table_name: str) -> str:
    """
    Generate a unique job name based on warehouse path, namespace, and table name.
    
    Args:
        warehouse_path: Warehouse path
        namespace: Table namespace
        table_name: Table name
        
    Returns:
        Job name string
    """ 
    # Create a sha1 digest of the warehouse path, namespace, and table name
    digest = hashlib.sha1(f"{warehouse_path}-{namespace}-{table_name}".encode()).hexdigest()
    job_name = f"deltacat-monitor-{digest}"
    
    return job_name


def _extract_catalog_config_from_beam(config: Dict[str, Any]) -> Dict[str, Any]:
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


def _submit_table_monitor_job(
    beam_catalog_config: Dict[str, Any],
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
        beam_catalog_config: Beam catalog configuration
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
        namespace = "default"
    
    # Generate unique job ID based on the warehouse and table path
    job_name = _generate_job_name(
        warehouse_path=beam_catalog_config["warehouse"],
        namespace=namespace,
        table_name=table_name
    )
     
    # Resolve the appropriate local or remote job client
    if cluster_cfg_file_path:
        # Submit to remote cluster
        print(f"[MANAGED JOB] Preparing to submit job to remote cluster: {cluster_cfg_file_path}")
        # Set the cluster name to the job ID to prevent starting multiple Ray clusters monitoring the same table.
        client = job_client(cluster_cfg_file_path, cluster_name_override=job_name)
    else:
        # Submit to local cluster using DeltaCAT local job client
        ray_init_args = {"local_mode": True, "resources": {"convert_task": max_converter_parallelism}}
        print(f"[MANAGED JOB] Preparing to submit job to locally with ray init args: {ray_init_args}")
        client = local_job_client(ray_init_args=ray_init_args)

    # Check for any redundant jobs for the same table in a RUNNING or PENDING status
    # Note that this isn't a perfect solution since it is subject to race conditions,
    # but we have an additional safeguard for remote clusters by using the job ID as 
    # the cluster name to prevent starting multiple Ray clusters monitoring the same table.
    # Table monitoring and delete conversion jobs are also idempotent, so the worst case
    # of redundant local jobs is wasted compute resources.
    job_details_list = client.list_jobs()
    existing_job_details = [job_details for job_details in job_details_list if job_details.submission_id == job_name]
    live_job_ids = set([job_details.job_id for job_details in existing_job_details if job_details.status == "RUNNING" or job_details.status == "PENDING"])
    stopped_job_ids = set()
    # Stop any redundant jobs for the same table in a RUNNING or PENDING status
    if len(live_job_ids) > 1:
        for job_id in live_job_ids[1:]:
            print(f"[MANAGED JOB] Stopping redundant job ID: {job_id}")
            client.stop_job(job_id)
            stopped_job_ids.add(job_id)
    # Update the list of live job IDs
    live_job_ids -= stopped_job_ids
    if live_job_ids:
        if len(live_job_ids) > 1:
            raise ValueError(f"Expected to resolve 1 live job for table {table_name} but found: {live_job_ids}")
        return live_job_ids.pop()

    # Add filesystem type - determine from filesystem instance
    filesystem_type = FilesystemType.from_filesystem(filesystem)

    # Build CLI arguments for table_monitor job
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    table_monitor_script_path = os.path.join(parent_dir, "table_monitor.py")
    
    print(f"[MANAGED JOB] Path resolution:")
    print(f"  - Current dir: {current_dir}")
    print(f"  - Parent dir: {parent_dir}")
    print(f"  - Table monitor path: {table_monitor_script_path}")
    print(f"  - Table monitor exists: {os.path.exists(table_monitor_script_path)}")
    
    cmd_args = [
        f"python {table_monitor_script_path}",
        f"--catalog-impl '{beam_catalog_config['catalog_impl']}'",
        f"--warehouse-path '{beam_catalog_config['warehouse']}'",
        f"--catalog-uri '{beam_catalog_config['uri']}'",
        f"--namespace '{namespace}'",
        f"--table-name '{table_name}'",
        f"--merge-keys '{','.join(merge_keys)}'",
        f"--monitor-interval {monitor_interval}",
        f"--max-converter-parallelism {max_converter_parallelism}",
        f"--ray-inactivity-timeout {ray_inactivity_timeout}",
        f"--filesystem-type '{filesystem_type}'",
    ]
    
    # Join all arguments
    entrypoint = " ".join(cmd_args)
    
    print(f"[MANAGED JOB] Submitting table monitor job:")
    print(f"  - Job Name: {job_name}")
    print(f"  - Namespace: {namespace}")
    print(f"  - Table: {table_name}")
    print(f"  - Warehouse: {beam_catalog_config['warehouse']}")
    print(f"  - Merge keys: {merge_keys}")
    print(f"  - Monitor interval: {monitor_interval}s")
    print(f"  - Max converter parallelism: {max_converter_parallelism}")
    print(f"  - Ray inactivity timeout: {ray_inactivity_timeout}s")
    print(f"  - Filesystem type: {filesystem_type}")
    print(f"  - Entrypoint: {entrypoint}")
     
    # Submit the job with the correct working directory
    # Working directory should be the converter_agent directory where table_monitor.py is located
    job_submission_id = client.submit_job(
        submission_id=job_name,
        entrypoint=entrypoint,
        runtime_env={"working_dir": parent_dir},
    )
    
    print(f"[MANAGED JOB] Table monitor job submitted successfully: {job_submission_id}")
        
    return job_submission_id


def write(*args, **kwargs):
    """Wrapper that submits DeltaCAT table monitor jobs for beam.managed.Write operations."""
    print(f"[MANAGED JOB] Initializing DeltaCAT job-based monitor for WRITE operation")
    print(f"[MANAGED JOB] WRITE operation called with:")
    print(f"  - args: {args}")
    print(f"  - kwargs keys: {list(kwargs.keys()) if kwargs else 'None'}")

    # Extract and pop deltacat-specific config keys
    config = kwargs.get("config", {}).copy() if kwargs.get("config") else {}
    
    # Extract DeltaCAT converter properties from parent config or individual keys (for backward compatibility)
    deltacat_converter_properties = config.pop("deltacat_converter_properties", {})
    
    # Support both new nested structure and old flat structure for backward compatibility
    deltacat_converter_interval = deltacat_converter_properties.get("deltacat_converter_interval", 3.0)
    
    merge_keys = deltacat_converter_properties.get("merge_keys", ["id"])
    
    # Extract filesystem parameter (optional) - can be in converter properties or top-level config
    filesystem = deltacat_converter_properties.get("filesystem", None)
    
    # Extract cluster configuration file path (for remote jobs)
    cluster_cfg_file_path = deltacat_converter_properties.get("cluster_cfg_file_path", None)
    
    # Extract max converter parallelism
    max_converter_parallelism = deltacat_converter_properties.get(
        "max_converter_parallelism", 
        DEFAULT_CONVERTER_TASK_MAX_PARALLELISM,
    )
    
    # Extract ray inactivity timeout
    ray_inactivity_timeout = deltacat_converter_properties.get("ray_inactivity_timeout", 10)

    # Extract table identifier and warehouse path
    table_identifier = config.get("table", "unknown")
    if "." in table_identifier:
        namespace, table_name = table_identifier.split(".", 1)
    else:
        namespace = "default"
        table_name = table_identifier

    warehouse_path = config.get("catalog_properties", {}).get("warehouse", "")

    print(f"  - table: {table_name}")
    print(f"  - deltacat_converter_interval: {deltacat_converter_interval}s")
    print(f"  - merge_keys: {merge_keys}")
    print(f"  - warehouse_path: {warehouse_path}")
    print(f"  - filesystem: {type(filesystem).__name__ if filesystem else 'None (auto-resolve)'}")
    print(f"  - cluster_cfg_file_path: {cluster_cfg_file_path or 'None (local)'}")
    print(f"  - max_converter_parallelism: {max_converter_parallelism}")
    print(f"  - ray_inactivity_timeout: {ray_inactivity_timeout}s")
    print(f"  - using deltacat_converter_properties: {len(deltacat_converter_properties) > 0}")

    # Extract catalog configuration for monitoring
    beam_catalog_config = _extract_catalog_config_from_beam(config)
    print(f"  - catalog_impl: {beam_catalog_config['catalog_impl']}")

    # Update kwargs with the modified config
    if "config" in kwargs:
        kwargs["config"] = config

    # Submit monitoring job
    try:
        _submit_table_monitor_job(
            beam_catalog_config=beam_catalog_config,
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
        print(f"[MANAGED JOB] Failed to submit table monitor job: {e}")
    print(f"[MANAGED JOB] Delegating to beam.managed.Write")
    return _original_write(*args, **kwargs)
