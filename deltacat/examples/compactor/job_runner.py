import argparse
import pathlib

from deltacat.compute import (
    job_client,
    JobStatus,
)


def run_async(
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
    compactor_version: str,
    jobs_to_submit: int,
    job_timeout: int,
    cloud: str,
    restart_ray: bool,
    hash_bucket_count: int = None,
    sort_keys: str = None,
    records_per_file: int = 1000000,
    table_writer_compression: str = "lz4",
    catalog_root: str = None,
):
    """
    Run multiple compactor jobs asynchronously on a Ray cluster.

    This function submits multiple compactor jobs to run in parallel, which is useful for:
    - Compacting multiple partitions of the same table
    - Running compaction jobs with different parameters
    - Load testing the compactor infrastructure
    """
    working_dir = pathlib.Path(__file__).parent
    cluster_cfg_file_path = working_dir.joinpath(cloud).joinpath("deltacat.yaml")
    job_number = 0
    client = job_client(cluster_cfg_file_path, restart_ray=restart_ray)
    job_ids = []

    while jobs_to_submit > 0:
        jobs_to_submit -= 1

        # Build the compactor command
        cmd_parts = [
            "python3 compactor.py",
            f"--namespace '{namespace}'",
            f"--table-name '{table_name}'",
            f"--table-version '{table_version}'",
            f"--stream-id '{stream_id}'",
            f"--partition-values '{partition_values}'",
            f"--dest-namespace '{dest_namespace}'",
            f"--dest-table-name '{dest_table_name}_{job_number}'",  # Unique dest per job
            f"--dest-table-version '{dest_table_version}'",
            f"--dest-stream-id '{dest_stream_id}'",
            f"--dest-partition-values '{dest_partition_values}'",
            f"--last-stream-position {last_stream_position}",
            f"--primary-keys '{primary_keys}'",
            f"--compactor-version '{compactor_version}'",
            f"--records-per-file {records_per_file}",
            f"--table-writer-compression '{table_writer_compression}'",
        ]

        # Add optional parameters
        if hash_bucket_count is not None:
            cmd_parts.append(f"--hash-bucket-count {hash_bucket_count}")
        if sort_keys:
            cmd_parts.append(f"--sort-keys '{sort_keys}'")
        if catalog_root:
            cmd_parts.append(f"--catalog-root '{catalog_root}'")

        entrypoint = " ".join(cmd_parts)

        job_id = client.submit_job(
            entrypoint=entrypoint,
            runtime_env={"working_dir": working_dir},
        )
        job_ids.append(job_id)
        print(f"Submitted job {job_number} with ID: {job_id}")
        job_number += 1

    print(f"Waiting for all {len(job_ids)} jobs to complete...")
    job_number = 0
    all_job_logs = ""

    for job_id in job_ids:
        job_status = client.await_job(job_id, timeout_seconds=job_timeout)
        if job_status != JobStatus.SUCCEEDED:
            print(f"Job `{job_id}` logs: ")
            print(client.get_job_logs(job_id))
            raise RuntimeError(f"Job `{job_id}` terminated with status: {job_status}")

        all_job_logs += f"\n=== Job #{job_number} (ID: {job_id}) logs ===\n"
        all_job_logs += client.get_job_logs(job_id)
        job_number += 1

    print("All jobs completed successfully!")
    print("=== Combined Job Logs ===")
    print(all_job_logs)


def run_sync(
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
    compactor_version: str,
    jobs_to_submit: int,
    job_timeout: int,
    cloud: str,
    restart_ray: bool,
    hash_bucket_count: int = None,
    sort_keys: str = None,
    records_per_file: int = 1000000,
    table_writer_compression: str = "lz4",
    catalog_root: str = None,
):
    """
    Run multiple compactor jobs synchronously on a Ray cluster.

    This function runs compactor jobs one after another, which is useful for:
    - Sequential compaction workflows
    - Debugging compaction issues
    - Resource-constrained environments
    """
    working_dir = pathlib.Path(__file__).parent
    cluster_cfg_file_path = working_dir.joinpath(cloud).joinpath("deltacat.yaml")
    client = job_client(cluster_cfg_file_path, restart_ray=restart_ray)
    job_number = 0

    while job_number < jobs_to_submit:
        # Build the compactor command
        cmd_parts = [
            "python3 compactor.py",
            f"--namespace '{namespace}'",
            f"--table-name '{table_name}'",
            f"--table-version '{table_version}'",
            f"--stream-id '{stream_id}'",
            f"--partition-values '{partition_values}'",
            f"--dest-namespace '{dest_namespace}'",
            f"--dest-table-name '{dest_table_name}_{job_number}'",  # Unique dest per job
            f"--dest-table-version '{dest_table_version}'",
            f"--dest-stream-id '{dest_stream_id}'",
            f"--dest-partition-values '{dest_partition_values}'",
            f"--last-stream-position {last_stream_position}",
            f"--primary-keys '{primary_keys}'",
            f"--compactor-version '{compactor_version}'",
            f"--records-per-file {records_per_file}",
            f"--table-writer-compression '{table_writer_compression}'",
        ]

        # Add optional parameters
        if hash_bucket_count is not None:
            cmd_parts.append(f"--hash-bucket-count {hash_bucket_count}")
        if sort_keys:
            cmd_parts.append(f"--sort-keys '{sort_keys}'")
        if catalog_root:
            cmd_parts.append(f"--catalog-root '{catalog_root}'")

        entrypoint = " ".join(cmd_parts)

        print(f"Running job {job_number}...")
        job_run_result = client.run_job(
            entrypoint=entrypoint,
            runtime_env={"working_dir": working_dir},
            timeout_seconds=job_timeout,
        )

        print(
            f"Job ID {job_run_result.job_id} terminal state: {job_run_result.job_status}"
        )
        print(f"Job ID {job_run_result.job_id} logs: ")
        print(job_run_result.job_logs)
        job_number += 1


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
    compactor_version: str,
    restart_ray: bool,
    jobs_to_submit: int,
    job_timeout: int,
    asynchronous: bool,
    cloud_provider: str,
    hash_bucket_count: int = None,
    sort_keys: str = None,
    records_per_file: int = 1000000,
    table_writer_compression: str = "lz4",
    catalog_root: str = None,
):
    """
    Run compactor jobs on a Ray cluster.

    Args:
        namespace: Source table namespace
        table_name: Source table name
        table_version: Source table version
        stream_id: Source stream ID
        partition_values: Partition values for source
        dest_namespace: Destination table namespace
        dest_table_name: Destination table name (will be suffixed with job number)
        dest_table_version: Destination table version
        dest_stream_id: Destination stream ID
        dest_partition_values: Partition values for destination
        last_stream_position: Last stream position to compact
        primary_keys: Primary keys for compaction
        compactor_version: Compactor version (V1 or V2)
        restart_ray: Whether to restart Ray cluster
        jobs_to_submit: Number of jobs to submit
        job_timeout: Job timeout in seconds
        asynchronous: Whether to run jobs asynchronously
        cloud_provider: Cloud provider (aws or gcp)
        hash_bucket_count: Number of hash buckets (for V2)
        sort_keys: Sort keys for compaction
        records_per_file: Records per compacted file
        table_writer_compression: Compression type
        catalog_root: Root path for catalog
    """
    run_func = run_async if asynchronous else run_sync
    run_func(
        namespace=namespace,
        table_name=table_name,
        table_version=table_version,
        stream_id=stream_id,
        partition_values=partition_values,
        dest_namespace=dest_namespace,
        dest_table_name=dest_table_name,
        dest_table_version=dest_table_version,
        dest_stream_id=dest_stream_id,
        dest_partition_values=dest_partition_values,
        last_stream_position=last_stream_position,
        primary_keys=primary_keys,
        compactor_version=compactor_version,
        jobs_to_submit=jobs_to_submit,
        job_timeout=job_timeout,
        cloud=cloud_provider,
        restart_ray=restart_ray,
        hash_bucket_count=hash_bucket_count,
        sort_keys=sort_keys,
        records_per_file=records_per_file,
        table_writer_compression=table_writer_compression,
        catalog_root=catalog_root,
    )


if __name__ == "__main__":
    """
    # Run this example through a command of the form:
    $ python ./deltacat/examples/compactor/job_runner.py \
    $ --namespace 'events' \
    $ --table-name 'user_events' \
    $ --table-version '1' \
    $ --stream-id 'events_stream' \
    $ --partition-values 'region=us-west-2' \
    $ --dest-namespace 'events' \
    $ --dest-table-name 'compacted_events' \
    $ --dest-table-version '1' \
    $ --dest-stream-id 'compacted_stream' \
    $ --dest-partition-values 'region=us-west-2' \
    $ --last-stream-position 10000 \
    $ --primary-keys 'user_id,event_id' \
    $ --compactor-version 'V2' \
    $ --hash-bucket-count 4 \
    $ --asynchronous \
    $ --jobs-to-submit 5 \
    $ --job-timeout 1800 \
    $ --cloud-provider aws
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
                "help": "Partition values for source",
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
                "help": "Destination table name (will be suffixed with job number)",
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
                "help": "Partition values for destination",
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
                "help": "Primary keys for compaction",
                "type": str,
                "required": True,
            },
        ),
        (
            ["--compactor-version"],
            {
                "help": "Compactor version (V1 or V2)",
                "type": str,
                "choices": ["V1", "V2"],
                "default": "V2",
            },
        ),
        (
            ["--restart-ray"],
            {
                "help": "Restart Ray on an existing cluster",
                "action": "store_true",
                "default": False,
            },
        ),
        (
            ["--asynchronous"],
            {
                "help": "Run jobs asynchronously",
                "action": "store_true",
                "default": False,
            },
        ),
        (
            ["--jobs-to-submit"],
            {
                "help": "Number of compactor jobs to submit for execution",
                "type": int,
                "default": 1,
            },
        ),
        (
            ["--job-timeout"],
            {
                "help": "Job timeout in seconds",
                "type": int,
                "default": 1800,
            },
        ),
        (
            ["--cloud-provider"],
            {
                "help": "Ray Cluster Cloud Provider ('aws' or 'gcp')",
                "type": str,
                "choices": ["aws", "gcp"],
                "default": "aws",
            },
        ),
        (
            ["--hash-bucket-count"],
            {
                "help": "Number of hash buckets (required for V2)",
                "type": int,
                "default": None,
            },
        ),
        (
            ["--sort-keys"],
            {
                "help": "Sort keys for compaction",
                "type": str,
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
            ["--catalog-root"],
            {
                "help": "Root path for catalog",
                "type": str,
                "default": None,
            },
        ),
    ]

    # Parse CLI input arguments
    parser = argparse.ArgumentParser(
        description="DeltaCAT Compactor Job Runner - Run multiple compactor jobs on Ray clusters"
    )
    for args, kwargs in script_args:
        parser.add_argument(*args, **kwargs)
    args = parser.parse_args()
    print(f"Command Line Arguments: {args}")

    # Run the job runner using the parsed arguments
    run(**vars(args))
