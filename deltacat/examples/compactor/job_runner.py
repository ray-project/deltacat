import argparse
from typing import Optional


def run_compactor_local(
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
    Run the compactor locally using Ray.

    This function constructs the command line arguments and runs the compactor.py
    script directly in the current Python process.
    """
    # Build command arguments
    cmd_args = [
        f"--namespace '{namespace}'",
        f"--table-name '{table_name}'",
        f"--table-version '{table_version}'",
        f"--partition-values '{partition_values}'",
        f"--dest-namespace '{dest_namespace}'",
        f"--dest-table-name '{dest_table_name}'",
        f"--dest-table-version '{dest_table_version}'",
        f"--dest-partition-values '{dest_partition_values}'",
        f"--last-stream-position {last_stream_position}",
        f"--primary-keys '{primary_keys}'",
        f"--compactor-version '{compactor_version}'",
    ]

    # Add optional arguments
    if catalog_root:
        cmd_args.append(f"--catalog-root '{catalog_root}'")
    if sort_keys:
        cmd_args.append(f"--sort-keys '{sort_keys}'")
    if hash_bucket_count is not None:
        cmd_args.append(f"--hash-bucket-count {hash_bucket_count}")
    if records_per_file != 1000000:
        cmd_args.append(f"--records-per-file {records_per_file}")
    if table_writer_compression != "lz4":
        cmd_args.append(f"--table-writer-compression '{table_writer_compression}'")

    # Join all arguments
    cmd_str = " ".join(cmd_args)
    print(f"Running compactor with arguments: {cmd_str}")

    # Import and run the compactor directly
    from . import compactor

    # Parse arguments manually and call run function
    compactor.run(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        partition_values=partition_values,
        dest_namespace=dest_namespace,
        dest_table_name=dest_table_name,
        dest_table_version=dest_table_version,
        dest_partition_values=dest_partition_values,
        last_stream_position=last_stream_position,
        primary_keys=primary_keys,
        catalog_root=catalog_root,
        compactor_version=compactor_version,
        sort_keys=sort_keys,
        hash_bucket_count=hash_bucket_count,
        records_per_file=records_per_file,
        table_writer_compression=table_writer_compression,
    )


def run_compactor_local_job(
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
    Submit the compactor as a local Ray job using a local job client.

    This function creates a Ray job that runs the compactor.py script
    with the specified parameters.
    """
    from deltacat import local_job_client

    # Build command arguments
    cmd_args = [
        "python compactor.py",
        f"--namespace '{namespace}'",
        f"--table-name '{table_name}'",
        f"--table-version '{table_version}'",
        f"--partition-values '{partition_values}'",
        f"--dest-namespace '{dest_namespace}'",
        f"--dest-table-name '{dest_table_name}'",
        f"--dest-table-version '{dest_table_version}'",
        f"--dest-partition-values '{dest_partition_values}'",
        f"--last-stream-position {last_stream_position}",
        f"--primary-keys '{primary_keys}'",
        f"--compactor-version '{compactor_version}'",
    ]

    # Add optional arguments
    if catalog_root:
        cmd_args.append(f"--catalog-root '{catalog_root}'")
    if sort_keys:
        cmd_args.append(f"--sort-keys '{sort_keys}'")
    if hash_bucket_count is not None:
        cmd_args.append(f"--hash-bucket-count {hash_bucket_count}")
    if records_per_file != 1000000:
        cmd_args.append(f"--records-per-file {records_per_file}")
    if table_writer_compression != "lz4":
        cmd_args.append(f"--table-writer-compression '{table_writer_compression}'")

    # Join all arguments
    entrypoint = " ".join(cmd_args)
    print(f"Submitting local Ray job with entrypoint: {entrypoint}")

    # Submit the job
    client = local_job_client()
    job_run_result = client.run_job(
        entrypoint=entrypoint,
        runtime_env={"working_dir": "./deltacat/examples/compactor/"},
    )

    print(f"Job ID {job_run_result.job_id} terminal state: {job_run_result.job_status}")
    print(f"Job logs: {job_run_result.job_logs}")

    return job_run_result


def run_compactor_remote_job(
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
    Submit the compactor as a remote Ray job using a remote job client.

    This function creates a Ray job that runs the compactor.py script
    on a remote Ray cluster with the specified parameters.

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
    from deltacat import job_client

    # Build command arguments - same as local job
    cmd_args = [
        "python compactor.py",
        f"--namespace '{namespace}'",
        f"--table-name '{table_name}'",
        f"--table-version '{table_version}'",
        f"--partition-values '{partition_values}'",
        f"--dest-namespace '{dest_namespace}'",
        f"--dest-table-name '{dest_table_name}'",
        f"--dest-table-version '{dest_table_version}'",
        f"--dest-partition-values '{dest_partition_values}'",
        f"--last-stream-position {last_stream_position}",
        f"--primary-keys '{primary_keys}'",
        f"--compactor-version '{compactor_version}'",
    ]

    # Add optional arguments
    if catalog_root:
        cmd_args.append(f"--catalog-root '{catalog_root}'")
    if sort_keys:
        cmd_args.append(f"--sort-keys '{sort_keys}'")
    if hash_bucket_count is not None:
        cmd_args.append(f"--hash-bucket-count {hash_bucket_count}")
    if records_per_file != 1000000:
        cmd_args.append(f"--records-per-file {records_per_file}")
    if table_writer_compression != "lz4":
        cmd_args.append(f"--table-writer-compression '{table_writer_compression}'")

    # Join all arguments
    entrypoint = " ".join(cmd_args)
    print(f"Submitting remote Ray job with entrypoint: {entrypoint}")

    # Submit the job
    # TODO(pdames): Take cloud as an input parameter.
    client = job_client(
        "./aws/deltacat.yaml"
    )  # or job_client() to use current directory
    job_run_result = client.run_job(
        entrypoint=entrypoint,
        runtime_env={"working_dir": "./deltacat/examples/compactor/"},
    )

    print(f"Job completed with status: {job_run_result.job_status}")
    return job_run_result


if __name__ == "__main__":
    """
    DeltaCAT Job Runner Example - Run compactor jobs using different methods

    This script demonstrates three ways to run the DeltaCAT compactor:
    1. Locally in the current process
    2. As a local Ray job
    3. As a remote Ray job

    Example usage:
    $ python job_runner.py \
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
    $   --compactor-version 'V2' \
    $   --hash-bucket-count 1 \
    $   --job-type 'local'
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
        (
            ["--job-type"],
            {
                "help": "Type of job execution",
                "type": str,
                "choices": ["local", "local-job", "remote-job"],
                "default": "local",
            },
        ),
    ]

    # Parse CLI input arguments
    parser = argparse.ArgumentParser(
        description="DeltaCAT Job Runner Example - Run compactor jobs using different methods"
    )
    for args, kwargs in script_args:
        parser.add_argument(*args, **kwargs)
    args = parser.parse_args()
    print(f"Command Line Arguments: {args}")

    # Extract job type and remove it from args
    job_type = args.job_type
    delattr(args, "job_type")

    # Run the appropriate job type
    if job_type == "local":
        print("Running compactor locally...")
        run_compactor_local(**vars(args))
    elif job_type == "local-job":
        print("Submitting local Ray job...")
        run_compactor_local_job(**vars(args))
    elif job_type == "remote-job":
        print("Submitting remote Ray job...")
        run_compactor_remote_job(**vars(args))
    else:
        raise ValueError(f"Invalid job type: {job_type}")

    print("Job runner completed!")
