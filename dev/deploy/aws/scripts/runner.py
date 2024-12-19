"""
Helper for testing DeltaCAT scripts in the cloud.

If you update this script, please also update the usage instructions in README-development.md as required!
"""
import os
import argparse
import subprocess
import boto3
import json

from botocore.exceptions import ClientError
from ray.scripts import scripts


def aws_ec2(args, stage, dc_url):
    cluster_launcher_cfg_file = args.cluster_launcher_config
    run_script_path = os.path.relpath(args.script_path)
    script_args = f"--STAGE '{stage}'"

    cli_args = args.script_args or "{}"
    for key, val in json.loads(cli_args).items():
        script_args += f" {key} '{val}'"

    # directly run the underlying function for `ray submit`
    scripts.submit(
        [
            "--start",
            "--disable-usage-stats",
            cluster_launcher_cfg_file,
            run_script_path,
            f"{script_args}",
        ],
        standalone_mode=True,
    )


def aws_glue(args, stage, dc_url):
    # upload the script to run to S3
    bucket_name = f"deltacat-glue-scripts-{stage}"
    print(f"Ensuring Glue Job Script Bucket exists: {bucket_name}")
    create_bucket_kwargs = {"Bucket": bucket_name}
    if region and str.lower(region) != "us-east-1":
        create_bucket_kwargs["CreateBucketConfiguration":{"LocationConstraint": region}]
    s3_client.create_bucket(**create_bucket_kwargs)
    print(f"Using Glue Job Script Bucket: {bucket_name}")

    # create or update the glue job
    glue_job_name = args.glue_job_name
    if not glue_job_name:
        glue_job_name = f"deltacat-runner-{stage}"
    run_script_path = os.path.relpath(args.script_path)
    s3_glue_script_file_path = f"s3://{bucket_name}/{run_script_path}"
    print(f"Uploading script '{run_script_path}' to '{s3_glue_script_file_path}'")
    s3_client.upload_file(
        run_script_path,
        bucket_name,
        run_script_path,
    )
    print(f"Uploaded Glue Job Script to: {s3_glue_script_file_path}")
    glue_max_workers = args.glue_max_workers
    glue_iam_role = args.glue_iam_role
    glue_job_config = {
        "Role": glue_iam_role,
        "GlueVersion": "4.0",
        "ExecutionProperty": {"MaxConcurrentRuns": 2},
        "WorkerType": "Z.2X",
        "NumberOfWorkers": glue_max_workers,
        "Command": {
            "Name": "glueray",
            "Runtime": "Ray2.4",
            "PythonVersion": "3.9",
            "ScriptLocation": s3_glue_script_file_path,
        },
    }
    glue_client = boto3.client(
        "glue",
        region_name=region,
    )
    # create or update a glue job
    try:
        response = glue_client.get_job(JobName=glue_job_name)
        print(f"Found Existing Glue Job `{glue_job_name}`: {response}")
        if (
            job_cmd_name := response.get("Job")
            and response["Job"].get("Command")
            and response["Job"]["Command"].get("Name")
        ):
            if job_cmd_name != "glueray":
                raise RuntimeError(
                    f"Expected Job Type 'glueray' but Found '{job_cmd_name}'"
                )
            print(f"Found Expected Job Type: {job_cmd_name}")
        print(f"Updating Existing Glue Job `{glue_job_name}`")
        print(f"Updated Glue Job Config: `{glue_job_config}`")
        if not glue_iam_role:
            glue_job_config["Role"] = response["Job"]["Role"]
        glue_client.update_job(
            JobName=glue_job_name,
            JobUpdate=glue_job_config,
        )
    except ClientError as e:
        if (
            e.response.get("Error")
            and e.response["Error"].get("Code") == "EntityNotFoundException"
        ):
            if not glue_iam_role:
                err_msg = "No Glue Job IAM Role found! Please rerun this script with the --glue-iam-role param set."
                raise ValueError(err_msg)
            print(f"Glue Job `{glue_job_name}` Doesn't Exist. Creating it.")
            glue_client.create_job(
                Name=glue_job_name,
                **glue_job_config,
            )
        else:
            raise RuntimeError(f"Unexpected Error While Getting/Updating Glue Job: {e}")
    print(f"New Glue Job Config: {glue_job_config}")

    print(f"Getting Last Job Run Arguments for {glue_job_name}")
    response = glue_client.get_job_runs(
        JobName=glue_job_name,
    )
    job_runs = response.get("JobRuns")
    last_job_run_args = (
        job_runs[0]["Arguments"] if job_runs and "Arguments" in job_runs[0] else {}
    )
    print(f"Last Job Run Args for {glue_job_name}: {last_job_run_args}\n\n")

    cli_args = args.script_args
    last_job_run_pip = last_job_run_args.get("--pip-install")
    glue_start_job_run_args = {
        "--pip-install": last_job_run_pip,
        "--STAGE": stage,
        # set deltacat logging environment variables to be picked up by Glue
        "--DELTACAT_APP_LOG_LEVEL": "DEBUG",
        "--DELTACAT_SYS_LOG_LEVEL": "DEBUG",
        "--DELTACAT_APP_LOG_DIR": "/tmp/ray/session_latest/logs/",
        "--DELTACAT_SYS_LOG_DIR": "/tmp/ray/session_latest/logs/",
        "--DELTACAT_APP_INFO_LOG_BASE_FILE_NAME": "worker-dc.app.info.out",
        "--DELTACAT_SYS_INFO_LOG_BASE_FILE_NAME": "worker-dc.sys.info.out",
        "--DELTACAT_APP_DEBUG_LOG_BASE_FILE_NAME": "worker-dc.app.debug.out",
        "--DELTACAT_SYS_DEBUG_LOG_BASE_FILE_NAME": "worker-dc.sys.debug.out",
        # Glue joins all logs to a single stream, so use a single handler to avoid duplicates
        "--DELTACAT_LOGGER_USE_SINGLE_HANDLER": "True",
    }

    new_package = None
    if dc_url:
        new_package = f"deltacat @ {dc_url}"
    elif dc_version:
        new_package = f"deltacat=={dc_version}"
    elif not last_job_run_pip:
        # install the latest version of deltacat
        new_package = "deltacat"
    if new_package:
        glue_start_job_run_args["--pip-install"] = new_package

    if cli_args:
        extra_args_dict = json.loads(cli_args)
        glue_start_job_run_args.update(extra_args_dict)

    # start a glue job run
    print(
        f"Starting Job Run for {glue_job_name} with Args: {glue_start_job_run_args}\n\n"
    )
    response = glue_client.start_job_run(
        JobName=glue_job_name,
        Arguments=glue_start_job_run_args,
    )
    print(f"Start {glue_job_name} response: {response}\n\n")

    # print command to tail logs
    print(
        "Tail real-time logs and worker standard out for your job run by running: "
        f"aws --region {region} logs tail /aws-glue/ray/jobs/ray-worker-out-logs --follow"
    )
    print(
        "Tail real-time Ray driver standard out from your job run by running: "
        f"aws --region {region} logs tail /aws-glue/ray/jobs/script-log --follow"
    )


# command line parsers
root_parser = argparse.ArgumentParser()
root_subparser = root_parser.add_subparsers(
    title="Cloud Provider",
    description="The cloud provider that will run your script.",
    help="Run your script on Amazon Web Services.",
    required=True,
)
aws_parser = root_subparser.add_parser("aws")
aws_subparser = aws_parser.add_subparsers(
    title="AWS Service",
    description="The AWS service that will run your script.",
    help="Run your script on Amazon EC2 or AWS Glue.",
    required=True,
)
ec2_parser = aws_subparser.add_parser("ec2")
ec2_parser.set_defaults(func=aws_ec2)
glue_parser = aws_subparser.add_parser("glue")
glue_parser.set_defaults(func=aws_glue)
aws_subparser_leaf_nodes = all_subparser_leaf_nodes = [ec2_parser, glue_parser]

# command line arguments common to all parsers
common_args_list = [
    (
        ["script_path"],
        {
            "help": "Path to the script to run (e.g. 'deltacat/examples/hello_world.py').",
            "type": str,
        },
    ),
    (
        [
            "-a",
            "--script-args",
        ],
        {
            "help": "JSON dictionary of arguments to set when running the script.",
            "type": str,
        },
    ),
]
# mutually exclusive command line arguments common to all parsers
common_args_mutex_list = [
    (
        [
            "-d",
            "--deploy-local-deltacat",
        ],
        {
            "help": "Builds and deploys your local workspace code for DeltaCAT (reusable for 7 days).",
            "action": "store_true",
        },
    ),
    (
        [
            "-v",
            "--deltacat-version",
        ],
        {
            "help": "DeltaCAT version to install from PyPi.",
            "type": str,
        },
    ),
    (
        [
            "-u",
            "--deltacat-url",
        ],
        {
            "help": "Installs DeltaCAT from the given URL.",
            "type": str,
        },
    ),
]
# command line arguments common to all aws parsers
aws_common_args_list = [
    (
        [
            "-r",
            "--region",
        ],
        {
            "help": "AWS region to setup (default: us-east-1).",
            "default": "us-east-1",
            "type": str,
        },
    ),
]

# AWS EC2 positional command line arguments. For consistency with
# `ray submit`, cluster launcher config file should come before script.
ec2_parser.add_argument(
    "cluster_launcher_config",
    help="Ray cluster launcher config file.",
)

# Add all common args directly to subparser leaf nodes so that help only prints
# relevant args for fully-qualified commands containing all required subparsers
# (e.g. even though `script_name` is common to all commands, don't add it to
# the root parser since then argparse will tell users that they must specify
# `script_name` when they only run `runner.py` or `runner.py -h` even though
# this still won't work; instead, only tell users they must specify
# `script_name` once they run an otherwise fully qualified command like
# `runner.py aws ec2`... this also gets rid of some other weird behavior
# like argparse requiring root parser flags to be specified directly after
# `runner.py` rather than allowing them to be added at the end of the cmd).
for parser in all_subparser_leaf_nodes:
    # add common args directly to all subparsers
    for args, kwargs in common_args_list:
        parser.add_argument(*args, **kwargs)
    # add common mutually exclusive group args directly to all subparsers
    args_mutex = parser.add_mutually_exclusive_group()
    for args, kwargs in common_args_mutex_list:
        args_mutex.add_argument(*args, **kwargs)
for aws_parser in aws_subparser_leaf_nodes:
    # add common aws args directly to all aws subparsers
    for args, kwargs in aws_common_args_list:
        aws_parser.add_argument(*args, **kwargs)

# AWS Glue command line arguments
glue_parser.add_argument(
    "-i",
    "--glue-iam-role",
    help="Name of IAM role onboarded for use with Glue.",
    type=str,
)
glue_parser.add_argument(
    "-w",
    "--glue-max-workers",
    help="Maximum number of glue workers (default: 25).",
    default=25,
    type=int,
)
glue_parser.add_argument(
    "-n",
    "--glue-job-name",
    help="Name of the Glue Job to create/update (default: deltacat-runner-$USER).",
    type=str,
)

# path constants
ROOT_DIR = os.path.dirname(__file__)

if __name__ == "__main__":
    args = root_parser.parse_args()
    print(args)

    this_script_dir = os.path.realpath(ROOT_DIR)
    print(f"Script Parent Directory: {this_script_dir}")

    # get any custom version or uri of deltacat to install
    dc_version = args.deltacat_version
    dc_url = args.deltacat_url

    # run a build & deploy if needed
    deploy_dc = args.deploy_local_deltacat
    if deploy_dc:
        # delegate build and deploy to S3 to shell script
        cmd = [f"{this_script_dir}/s3-build-and-deploy.sh"]
        print(f"Running Command: {cmd}")
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            # redirect stderr to stdout
            stderr=subprocess.STDOUT,
            # set working directory to deltacat root workspace directory
            cwd=f"{this_script_dir}/../../../..",
        )
        while True:
            line = proc.stdout.readline().decode("utf-8")
            print(f"{line}")
            if not line and proc.poll() is not None:
                break
            line = line.strip()
            if line.find("DELTACAT_PRESIGNED_URL=") != -1:
                dc_url = line.split("=", 1)[1]
                print(f"Found New DeltaCAT Presigned URL: {dc_url}")
        returncode = proc.poll()
        if returncode != 0:
            raise RuntimeError(f"Command {cmd} Failed. Exit Code: {returncode}")

    region = args.region
    s3_client = boto3.client("s3", region_name=region)

    # set the stage to the current user retrieved from system environment vars
    stage = os.environ.get("DELTACAT_STAGE") or os.environ["USER"]
    print(f"Developer Stage: {stage}")

    # call into the registered default handler hook for this sub-parser (e.g. ec2, glue, etc.)
    args.func(args, stage, dc_url)
