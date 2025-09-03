import argparse
from typing import Optional

import deltacat
from deltacat.compute.compactor_v2.compaction_session import compact_partition
from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.storage import metastore
from deltacat.types.media import ContentType

# Import common utilities
from deltacat.examples.compactor.utils.common import (
    initialize_catalog,
    parse_primary_keys,
    parse_partition_values,
    parse_sort_keys,
    create_partition_locator,
    get_actual_partition_locator,
)


def print_package_version_info() -> None:
    """Print version information for debugging."""
    print(f"DeltaCAT version: {deltacat.__version__}")


def run(
    namespace: str,
    table_name: str,
    table_version: str,
    partition_values: str,
    dest_namespace: str,
    dest_table_name: str,
    dest_table_version: str,
    dest_partition_values: str,
    last_stream_position: int,
    primary_keys: str,
    catalog_root: Optional[str] = None,
    compactor_version: str = "V2",
    sort_keys: Optional[str] = None,
    hash_bucket_count: Optional[int] = None,
    records_per_file: int = 1000000,
    table_writer_compression: str = "lz4",
) -> None:
    """
    Run the compactor with the given parameters.

    Args:
        namespace: Source table namespace
        table_name: Source table name
        table_version: Source table version
        partition_values: Comma-separated partition values for source
        dest_namespace: Destination table namespace
        dest_table_name: Destination table name
        dest_table_version: Destination table version
        dest_partition_values: Comma-separated partition values for destination
        last_stream_position: Last stream position to compact
        primary_keys: Comma-separated primary keys
        catalog_root: Root path for catalog (defaults to temp directory)
        compactor_version: Compactor version to use (V1 or V2)
        sort_keys: Comma-separated sort keys (optional)
        hash_bucket_count: Number of hash buckets (required for V2)
        records_per_file: Records per compacted file
        table_writer_compression: Compression type for table writer
    """
    # Parse partition values
    partition_values_list = parse_partition_values(partition_values)
    dest_partition_values_list = parse_partition_values(dest_partition_values)

    # Initialize catalog
    catalog = initialize_catalog(catalog_root)

    # Get actual partition locators (with real partition IDs)
    source_partition_locator = get_actual_partition_locator(
        namespace, table_name, table_version, partition_values_list, catalog
    )

    # For destination, try actual first, fall back to basic if table doesn't exist yet
    try:
        dest_partition_locator = get_actual_partition_locator(
            dest_namespace,
            dest_table_name,
            dest_table_version,
            dest_partition_values_list,
            catalog,
        )
        print(f"✅ Using existing destination partition")
    except Exception:
        dest_partition_locator = create_partition_locator(
            dest_namespace,
            dest_table_name,
            dest_table_version,
            dest_partition_values_list,
        )
        print(f"✅ Creating new destination partition")

    # Parse primary keys and sort keys
    primary_keys_set = parse_primary_keys(primary_keys)
    sort_keys_list = parse_sort_keys(sort_keys) if sort_keys else None
    all_column_names = metastore.get_table_version_column_names(
        namespace,
        table_name,
        table_version,
        catalog=catalog,
    )
    # Create compaction parameters using the same approach as bootstrap.py
    params_dict = {
        "catalog": catalog,
        "compacted_file_content_type": ContentType.PARQUET,
        "deltacat_storage": metastore,
        "deltacat_storage_kwargs": {"catalog": catalog},
        "destination_partition_locator": dest_partition_locator,
        "last_stream_position_to_compact": last_stream_position,
        "list_deltas_kwargs": {
            "catalog": catalog,
            "equivalent_table_types": [],
        },
        "primary_keys": list(primary_keys_set),
        "all_column_names": all_column_names,
        "rebase_source_partition_locator": None,
        "rebase_source_partition_high_watermark": None,
        "records_per_compacted_file": records_per_file,
        "source_partition_locator": source_partition_locator,
        "table_writer_kwargs": {
            "compression": table_writer_compression,
            "version": "2.6",
            "use_dictionary": True,
        },
    }

    # Add sort keys if provided
    if sort_keys_list:
        params_dict["sort_keys"] = sort_keys_list

    # Add V2-specific parameters
    if compactor_version == "V2":
        if hash_bucket_count is None:
            raise ValueError("hash_bucket_count is required for V2 compactor")

        params_dict.update(
            {
                "hash_bucket_count": hash_bucket_count,
                "drop_duplicates": True,
                "dd_max_parallelism_ratio": 1.0,
            }
        )

    print(f"🚀 Starting {compactor_version} compaction...")
    print(f"   Source: {source_partition_locator}")
    print(f"   Destination: {dest_partition_locator}")
    print(f"   Primary Keys: {primary_keys_set}")
    print(
        f"   Sort Keys: {[sk.key for sk in sort_keys_list] if sort_keys_list else None}"
    )
    if compactor_version == "V2":
        print(f"   Hash Bucket Count: {hash_bucket_count}")

    # Run compaction
    compact_partition(CompactPartitionParams.of(params_dict))

    print(f"✅ Compaction completed successfully!")


if __name__ == "__main__":
    """
    DeltaCAT Compactor Example - Compact partitions using V1 or V2 compactor

    This script demonstrates how to compact partitions in DeltaCAT using either
    the V1 or V2 compactor. The compactor will read data from a source partition
    and write compacted data to a destination partition.

    Example 1: Basic V2 compaction (recommended):
    $ python compactor.py \
    $   --namespace 'test_namespace' \
    $   --table-name 'test_table' \
    $   --table-version '1' \
    $   --partition-values 'region=us-west-2' \
    $   --dest-namespace 'test_namespace' \
    $   --dest-table-name 'test_table_compacted' \
    $   --dest-table-version '1' \
    $   --dest-partition-values 'region=us-west-2' \
    $   --last-stream-position 5000 \
    $   --primary-keys 'user_id,event_id' \
    $   --sort-keys 'timestamp,event_type' \
    $   --compactor-version 'V2' \
    $   --hash-bucket-count 1 \
    $   --records-per-file 500000 \
    $   --table-writer-compression 'snappy'

    Example 2: V1 compaction (legacy):
    $ python compactor.py \
    $   --namespace 'events' \
    $   --table-name 'user_events' \
    $   --table-version '2' \
    $   --partition-values 'region=us-west-2' \
    $   --dest-namespace 'events' \
    $   --dest-table-name 'user_events_compacted' \
    $   --dest-table-version '1' \
    $   --dest-partition-values 'region=us-west-2' \
    $   --last-stream-position 5000 \
    $   --primary-keys 'user_id,event_id' \
    $   --sort-keys 'timestamp,event_type' \
    $   --compactor-version 'V1' \
    $   --records-per-file 500000 \
    $   --table-writer-compression 'snappy'

    Example 3: Submit this script as a local Ray job using a local job client:
    >>> from deltacat import local_job_client
    >>> client = local_job_client()
    >>> job_run_result = client.run_job(
    >>>     entrypoint="python compactor.py --namespace my_ns --table-name my_table ...",
    >>>     runtime_env={"working_dir": "./deltacat/examples/compactor/"},
    >>> )
    >>> print(f"Job ID {job_run_result.job_id} terminal state: {job_run_result.job_status}")
    >>> print(f"Job logs: {job_run_result.job_logs}")

    Example 4: Submit this script as a remote Ray job using a remote job client:
    >>> from deltacat import job_client
    >>> client = job_client("deltacat.yaml")  # or job_client() to use current directory
    >>> job_run_result = client.run_job(
    >>>     entrypoint="python compactor.py --namespace my_ns --table-name my_table ...",
    >>>     runtime_env={"working_dir": "./deltacat/examples/compactor/"},
    >>> )
    >>> print(f"Job completed with status: {job_run_result.job_status}")
    """
    script_args = [
        (
            ["--namespace"],
            {
                "help": "Source table namespace",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--table-name"],
            {
                "help": "Source table name",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--table-version"],
            {
                "help": "Source table version",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--partition-values"],
            {
                "help": "Comma-separated partition values for source (leave empty for no partition values)",
                "type": str,
                "default": "",
            },
        ),
        (
            ["--dest-namespace"],
            {
                "help": "Destination table namespace",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--dest-table-name"],
            {
                "help": "Destination table name",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--dest-table-version"],
            {
                "help": "Destination table version",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--dest-partition-values"],
            {
                "help": "Comma-separated partition values for destination (leave empty for no partition values)",
                "type": str,
                "default": "",
            },
        ),
        (
            ["--last-stream-position"],
            {
                "help": "Last stream position to compact",
                "type": int,
                "required": True,
            },
        ),
        (
            ["--primary-keys"],
            {
                "help": "Comma-separated primary keys",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--catalog-root"],
            {
                "help": "Root path for catalog (defaults to temp directory)",
                "type": str,
                "default": None,
            },
        ),
        (
            ["--compactor-version"],
            {
                "help": "Compactor version to use (V1 or V2)",
                "type": str,
                "choices": ["V1", "V2"],
                "default": "V2",
            },
        ),
        (
            ["--sort-keys"],
            {
                "help": "Comma-separated sort keys (optional)",
                "type": str,
                "default": None,
            },
        ),
        (
            ["--hash-bucket-count"],
            {
                "help": "Number of hash buckets (required for V2, ignored for V1)",
                "type": int,
                "default": None,
            },
        ),
        (
            ["--records-per-file"],
            {
                "help": "Records per compacted file",
                "type": int,
                "default": 1000000,
            },
        ),
        (
            ["--table-writer-compression"],
            {
                "help": "Compression type for table writer",
                "type": str,
                "choices": ["lz4", "snappy", "gzip", "brotli", "zstd"],
                "default": "lz4",
            },
        ),
    ]

    # Parse CLI input arguments
    parser = argparse.ArgumentParser(
        description="DeltaCAT Compactor Example - Compact partitions using V1 or V2 compactor"
    )
    for args, kwargs in script_args:
        parser.add_argument(*args, **kwargs)
    args = parser.parse_args()
    print(f"Command Line Arguments: {args}")

    # Initialize deltacat
    deltacat.init()

    # Run the compactor using the parsed arguments
    run(**vars(args))
