import argparse
import tempfile
from typing import Optional, Set, List

import ray
import deltacat
import pyarrow as pa
import pandas as pd
import numpy as np

from deltacat.compute.compactor.model.compact_partition_params import (
    CompactPartitionParams,
)
from deltacat.compute.compactor.compaction_session import (
    compact_partition_from_request as compact_partition_v1,
)
from deltacat.compute.compactor_v2.compaction_session import (
    compact_partition as compact_partition_v2,
)
from deltacat.storage import PartitionLocator, metastore
from deltacat.storage.model.sort_key import SortKey
from deltacat.catalog import CatalogProperties
from deltacat.types.media import ContentType


def print_package_version_info() -> None:
    print(f"DeltaCAT Version: {deltacat.__version__}")
    print(f"Ray Version: {ray.__version__}")
    print(f"NumPy Version: {np.__version__}")
    print(f"PyArrow Version: {pa.__version__}")
    print(f"Pandas Version: {pd.__version__}")


def parse_primary_keys(primary_keys_str: str) -> Set[str]:
    """Parse comma-separated primary keys string into a set."""
    if not primary_keys_str:
        return set()
    return {key.strip() for key in primary_keys_str.split(",")}


def parse_partition_values(partition_values_str: str) -> List[str]:
    """Parse comma-separated partition values string into a list."""
    if not partition_values_str:
        return []
    return [val.strip() for val in partition_values_str.split(",")]


def parse_sort_keys(sort_keys_str: str) -> List[SortKey]:
    """Parse comma-separated sort keys string into a list of SortKey objects."""
    if not sort_keys_str:
        return []
    sort_keys = []
    for key_str in sort_keys_str.split(","):
        key = key_str.strip()
        if key:
            sort_keys.append(SortKey.of(key=[key]))
    return sort_keys


def create_partition_locator(
    namespace: str,
    table_name: str,
    table_version: str,
    stream_id: str,
    partition_values: List[str],
) -> PartitionLocator:
    """Create a PartitionLocator from string components."""
    from deltacat.storage.model.types import StreamFormat

    return PartitionLocator.at(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_id=stream_id,
        stream_format=StreamFormat.DELTACAT,
        partition_values=partition_values if partition_values else None,
        partition_id=None,
    )


def run(
    namespace: str,
    table_name: str,
    table_version: str,
    stream_id: str,
    partition_values: str,
    dest_namespace: str,
    dest_table_name: str,
    dest_table_version: str,
    dest_stream_id: str,
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
    Run the compactor with the specified parameters.

    Args:
        namespace: Source table namespace
        table_name: Source table name
        table_version: Source table version
        stream_id: Source stream ID
        partition_values: Comma-separated partition values for source
        dest_namespace: Destination table namespace
        dest_table_name: Destination table name
        dest_table_version: Destination table version
        dest_stream_id: Destination stream ID
        dest_partition_values: Comma-separated partition values for destination
        last_stream_position: Last stream position to compact
        primary_keys: Comma-separated primary keys
        catalog_root: Root path for catalog (defaults to temp directory)
        compactor_version: Compactor version to use (V1 or V2)
        sort_keys: Comma-separated sort keys
        hash_bucket_count: Number of hash buckets (required for V2)
        records_per_file: Records per compacted file
        table_writer_compression: Compression type for table writer
    """
    # Print package version info
    print_package_version_info()

    # Create catalog
    if catalog_root is None:
        catalog_root = tempfile.mkdtemp()
        print(f"Using temporary catalog directory: {catalog_root}")

    catalog = CatalogProperties(root=catalog_root)

    # Parse input parameters
    partition_values_list = parse_partition_values(partition_values)
    dest_partition_values_list = parse_partition_values(dest_partition_values)
    primary_keys_set = parse_primary_keys(primary_keys)
    sort_keys_list = parse_sort_keys(sort_keys or "")

    # Create partition locators
    source_partition_locator = create_partition_locator(
        namespace, table_name, table_version, stream_id, partition_values_list
    )

    destination_partition_locator = create_partition_locator(
        dest_namespace,
        dest_table_name,
        dest_table_version,
        dest_stream_id,
        dest_partition_values_list,
    )

    # Set default hash bucket count for V2
    if compactor_version == "V2" and hash_bucket_count is None:
        hash_bucket_count = 2
        print(f"Using default hash_bucket_count={hash_bucket_count} for V2 compactor")

    # Create CompactPartitionParams
    params_dict = {
        "catalog": catalog,
        "source_partition_locator": source_partition_locator,
        "destination_partition_locator": destination_partition_locator,
        "last_stream_position_to_compact": last_stream_position,
        "primary_keys": primary_keys_set,
        "sort_keys": sort_keys_list,
        "records_per_compacted_file": records_per_file,
        "compacted_file_content_type": ContentType.PARQUET,
        "deltacat_storage": metastore,
        "deltacat_storage_kwargs": {"catalog": catalog},
        "list_deltas_kwargs": {"catalog": catalog, "equivalent_table_types": []},
        "table_writer_kwargs": {
            "compression": table_writer_compression,
            "version": "2.6",
            "use_dictionary": True,
        },
    }

    # Add V2-specific parameters
    if compactor_version == "V2":
        params_dict.update(
            {
                "hash_bucket_count": hash_bucket_count,
                "drop_duplicates": True,
                "dd_max_parallelism_ratio": 1.0,
            }
        )

    compact_partition_params = CompactPartitionParams.of(params_dict)

    print(f"Starting {compactor_version} compaction...")
    print(f"Source: {source_partition_locator}")
    print(f"Destination: {destination_partition_locator}")
    print(f"Primary keys: {primary_keys_set}")
    print(f"Sort keys: {[sk.key for sk in sort_keys_list]}")
    print(f"Last stream position: {last_stream_position}")
    print(f"Records per file: {records_per_file}")
    print(f"Compression: {table_writer_compression}")

    # Run compaction based on version
    if compactor_version == "V1":
        result = compact_partition_v1(compact_partition_params)
    elif compactor_version == "V2":
        result = compact_partition_v2(compact_partition_params)
    else:
        raise ValueError(
            f"Invalid compactor version: {compactor_version}. Use 'V1' or 'V2'"
        )

    print(f"Compaction completed successfully!")
    print(f"Round completion file: {result}")
    return result


if __name__ == "__main__":
    """
    Example 1: Run this script locally using Ray with V2 compactor:
    $ python compactor.py \
    $   --namespace 'my_namespace' \
    $   --table-name 'my_table' \
    $   --table-version '1' \
    $   --stream-id 'stream1' \
    $   --partition-values '' \
    $   --dest-namespace 'my_namespace' \
    $   --dest-table-name 'my_compacted_table' \
    $   --dest-table-version '1' \
    $   --dest-stream-id 'stream1' \
    $   --dest-partition-values '' \
    $   --last-stream-position 1000 \
    $   --primary-keys 'id,user_id' \
    $   --compactor-version 'V2' \
    $   --hash-bucket-count 4

    Example 2: Run with V1 compactor and sort keys:
    $ python compactor.py \
    $   --namespace 'events' \
    $   --table-name 'user_events' \
    $   --table-version '2' \
    $   --stream-id 'events_stream' \
    $   --partition-values 'region=us-west-2' \
    $   --dest-namespace 'events' \
    $   --dest-table-name 'user_events_compacted' \
    $   --dest-table-version '1' \
    $   --dest-stream-id 'compacted_stream' \
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
            ["--stream-id"],
            {
                "help": "Source stream ID",
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
            ["--dest-stream-id"],
            {
                "help": "Destination stream ID",
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
