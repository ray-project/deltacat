import argparse
import pathlib

from deltacat.compute import (
    job_client,
    JobStatus,
)


def run_async(
    source: str,
    dest: str,
    jobs_to_submit: int,
    job_timeout: int,
    cloud: str,
    restart_ray: bool,
):
    # print package version info
    working_dir = pathlib.Path(__file__).parent
    cluster_cfg_file_path = working_dir.joinpath(cloud).joinpath("deltacat.yaml")
    job_number = 0
    client = job_client(cluster_cfg_file_path, restart_ray=restart_ray)
    job_ids = []
    while jobs_to_submit > 0:
        jobs_to_submit -= 1
        job_dest = dest + f".{job_number}"
        job_id = client.submit_job(
            # Entrypoint shell command to execute
            entrypoint=f"python3 indexer.py --source '{source}' --dest '{job_dest}'",
            # Path to the local directory that contains the indexer.py file
            # This entire directory will be zipped into a job package, so keep
            # it small.
            runtime_env={"working_dir": working_dir},
        )
        job_ids.append(job_id)
        job_number += 1

    print("Waiting for all jobs to complete...")
    job_number = 0
    all_job_logs = ""
    for job_id in job_ids:
        job_status = client.await_job(job_id, timeout_seconds=job_timeout)
        if job_status != JobStatus.SUCCEEDED:
            print(f"Job `{job_id}` logs: ")
            print(client.get_job_logs(job_id))
            raise RuntimeError(f"Job `{job_id}` terminated with status: {job_status}")
        all_job_logs += f"\nJob #{job_number} logs: \n"
        all_job_logs += client.get_job_logs(job_id)
        job_number += 1
    print("All jobs completed!")
    print("Job Logs: ")
    print(all_job_logs)


def run_sync(
    source: str,
    dest: str,
    jobs_to_submit: int,
    job_timeout: int,
    cloud: str,
    restart_ray: bool,
):
    working_dir = pathlib.Path(__file__).parent
    cluster_cfg_file_path = working_dir.joinpath(cloud).joinpath("deltacat.yaml")
    client = job_client(cluster_cfg_file_path, restart_ray=restart_ray)
    job_number = 0
    while job_number < jobs_to_submit:
        job_dest = dest + f".{job_number}"
        job_run_result = client.run_job(
            # Entrypoint shell command to execute
            entrypoint=f"python3 indexer.py --source '{source}' --dest '{job_dest}'",
            # Path to the local directory that contains the indexer.py file
            # This entire directory will be zipped into a job package, so keep
            # it small.
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
    source: str,
    dest: str,
    restart_ray: bool,
    jobs_to_submit: int,
    job_timeout: int,
    asynchronous: bool,
    cloud_provider: str,
):
    run_func = run_async if asynchronous else run_sync
    run_func(
        source=source,
        dest=dest,
        jobs_to_submit=jobs_to_submit,
        job_timeout=job_timeout,
        cloud=cloud_provider,
        restart_ray=restart_ray,
    )


if __name__ == "__main__":
    """
    This example shows how to submit jobs to a remote Ray cluster that indexes source files into arbitrary destinations with
    optional file format conversion using DeltaCAT URLs. It provides the option to run multiple sequential or concurrent jobs
    for benchmarking.

    # For example, the following command launches a remote Ray Cluster on AWS, downloads an external OpenAlex dataset text file,
    # converts it to Parquet, and writes it back to AWS S3. It submits 100 jobs in parallel, each with a timeout of 90 seconds:
    $ python ./deltacat/examples/job_runner.py -- \
    $ --source text+s3://openalex-mag-format/data_dump_v1/2022-07-08/nlp/PaperAbstractsInvertedIndex.txt_part31 \
    $ --dest parquet+s3://deltacat-example-output/openalex/PaperAbstractsInvertedIndex.part31.parquet \
    $ --asynchronous \
    $ --jobs-to-submit 100 \
    $ --job-timeout 90 \
    $ --cloud-provider aws
    """
    script_args = [
        (
            [
                "--source",
            ],
            {
                "help": "Source DeltaCAT URL to index.",
                "type": str,
                "default": "text+s3://openalex-mag-format/data_dump_v1/2022-07-08/nlp/PaperAbstractsInvertedIndex.txt_part31",
            },
        ),
        (
            [
                "--dest",
            ],
            {
                "help": "Destination DeltaCAT URL to store the indexed file.",
                "type": str,
                "default": "parquet+s3://deltacat-example-output/openalex/PaperAbstractsInvertedIndex.part31.parquet",
            },
        ),
        (
            [
                "--restart-ray",
            ],
            {
                "help": "Restart Ray on an existing cluster.",
                "action": "store_true",
                "default": False,
            },
        ),
        (
            [
                "--asynchronous",
            ],
            {
                "help": "Run jobs asynchronously.",
                "action": "store_true",
                "default": False,
            },
        ),
        (
            [
                "--jobs-to-submit",
            ],
            {
                "help": "Number of indexer jobs to submit for execution.",
                "type": int,
                "default": 1,
            },
        ),
        (
            [
                "--job-timeout",
            ],
            {
                "help": "Job timeout in seconds.",
                "type": int,
                "default": 300,
            },
        ),
        (
            [
                "--cloud-provider",
            ],
            {
                "help": "Ray Cluster Cloud Provider ('aws' or 'gcp')",
                "type": str,
                "default": "aws",
            },
        ),
    ]

    # parse CLI input arguments
    parser = argparse.ArgumentParser()
    for args, kwargs in script_args:
        parser.add_argument(*args, **kwargs)
    args = parser.parse_args()
    print(f"Command Line Arguments: {args}")

    # run the example using os.environ as kwargs
    run(**vars(args))
