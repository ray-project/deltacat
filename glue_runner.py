"""
Setup script to configure your AWS account to run any other example script using
AWS Glue on Ray.

Usage Prerequisites (DO THIS FIRST):
    1. Install and configure the latest version of the AWS CLI:
        * https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html#getting-started-install-instructions
    2. Create an AWS Glue IAM Role that can create and run jobs:
        * https://docs.aws.amazon.com/glue/latest/dg/getting-started-access.html
    3. Install and configure boto3:
        * https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
        * https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html

Quickstart Guide:
    1. Print Usage Instructions and Exit:
        $ python glue_runner.py -h
    2. Create a new Glue job and run your first script in us-east-1 (the default region):
        $ python glue_runner.py deltacat/examples/hello_world.py \
          --glue-iam-role "AWSGlueServiceRole" \
    3. Run an example in us-east-1 using the last job config and DeltaCAT deploy:
        $ python glue_runner.py deltacat/examples/hello_world.py
    4. Run an example in us-east-1 using your local workspace copy of DeltaCAT:
        $ python glue_runner.py deltacat/examples/hello_world.py --deploy-local-deltacat
       Note that the deployed package is referenced by an S3 URL that expires in 7 days.
       After 7 days, you must deploy a new DeltaCAT package to avoid receiving a 403 error!
    5. Create a new job and run an example in us-west-2:
        $ python glue_runner.py deltacat/examples/hello_world.py \
          --region us-west-2
          --glue-iam-role "AWSGlueServiceRole" \
    6. Pass arguments into an example script as environment variables:
        $ python glue_runner.py deltacat/examples/logging.py\
          --example-script-args '{"--var1":"Try that", "--var2":"DeltaCAT"}' \

During execution this script will:
    1. Create a S3 bucket at `s3://deltacat-packages-$USER` if it doesn't already exist.
    2. [Optional] Build and deploy your local DeltaCAT workspace to s3://deltacat-packages-$USER/.
    3. Create a S3 bucket at `s3://deltacat-glue-scripts-$USER` if it doesn't already exist.
    4. Upload deltacat/examples/$EXAMPLE_SCRIPT_NAME to s3://deltacat-glue-scripts-$USER.
    5. Create or update the Glue Job "deltacat-example-runner-$USER" to run this example.
    6. Run the "deltacat-example-runner-$USER" Glue Job with either the newly built DeltaCAT wheel or the last used wheel.

Where every instance of $USER will be replaced by the value of `os.environ["USER"]`.

"""
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "script_path",
    help="Path to the script to run (e.g. 'deltacat/examples/hello_world.py').",
    type=str,
)
parser.add_argument(
    "-a",
    "--example-script-args",
    help="JSON dictionary of arguments to set when running the example script.",
    type=str,
)
parser.add_argument(
    "-r",
    "--region",
    help="AWS region to setup (default: us-east-1).",
    default="us-east-1",
    type=str,
)
parser.add_argument(
    "-i",
    "--glue-iam-role",
    help="Name of IAM role onboarded for use with Glue.",
    type=str,
)
parser.add_argument(
    "-w",
    "--glue-max-workers",
    help="Maximum number of glue workers (default: 25).",
    default=25,
    type=int,
)
parser.add_argument(
    "-n",
    "--glue-job-name",
    help="Name of the Glue Job to create/update (default: deltacat-example-runner-$USER).",
    type=str,
)
args_mutex = parser.add_mutually_exclusive_group()
args_mutex.add_argument(
    "-d",
    "--deploy-local-deltacat",
    help="Builds and deploys your local workspace code for DeltaCAT "
    "(reusable for 7 days).",
    action="store_true",
)
args_mutex.add_argument(
    "-v",
    "--deltacat-version",
    help="DeltaCAT version to install from PyPi.",
    type=str,
)
args_mutex.add_argument(
    "-u",
    "--deltacat-url",
    help="Installs DeltaCAT from the given URL (via pip).",
    type=str,
)

ROOT_DIR = os.path.dirname(__file__)


if __name__ == "__main__":
    import subprocess
    import boto3
    import json
    from botocore.exceptions import ClientError

    this_script_dir = os.path.realpath(ROOT_DIR)
    print(f"Script Parent Directory: {this_script_dir}")

    # set the stage to the current user retrieved from system environment vars
    stage = os.environ["USER"]
    print(f"Developer Stage: {stage}")
    args = parser.parse_args()

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
            # set working directory to this script's directory
            cwd=f"{this_script_dir}",
        )
        while True:
            line = proc.stdout.readline().decode("utf-8")
            print(f"{line}")
            if not line and proc.poll() is not None:
                break
            line = line.strip()
            if line.find("DELTACAT_PRESIGNED_URL=") != -1:
                dc_url = line.split("=", 1)[1]
        returncode = proc.poll()
        if returncode != 0:
            raise RuntimeError(f"Command {cmd} Failed. Exit Code: {returncode}")

    s3_client = boto3.client("s3")

    # upload the example runner script to S3
    bucket_name = f"deltacat-glue-scripts-{stage}"
    print(f"Ensuring Glue Job Script Bucket exists: {bucket_name}")
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Using Glue Job Script Bucket: {bucket_name}")

    # create or update the glue job
    glue_job_name = args.glue_job_name
    if not glue_job_name:
        glue_job_name = f"deltacat-example-runner-{stage}"
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
        "ExecutionProperty": {"MaxConcurrentRuns": 10},
        "WorkerType": "Z.2X",
        "NumberOfWorkers": glue_max_workers,
        "Command": {
            "Name": "glueray",
            "Runtime": "Ray2.4",
            "PythonVersion": "3.9",
            "ScriptLocation": s3_glue_script_file_path,
        },
    }
    region = args.region
    glue_client = boto3.client(
        "glue",
        region,
    )
    try:
        response = glue_client.get_job(JobName=glue_job_name)
        print(f"Found Existing Glue Job `{glue_job_name}`: {response}")
        print(f"Updating Existing Glue Job `{glue_job_name}`")
        if not glue_iam_role:
            glue_job_config["Role"] = response["Job"]["Role"]
        glue_client.update_job(
            JobName=glue_job_name,
            JobUpdate=glue_job_config,
        )
    except ClientError as e:
        print(f"Glue Job `{glue_job_name}` doesn't exist. Creating it.")
        # create or update a glue job
        glue_client.create_job(
            Name=glue_job_name,
            **glue_job_config,
        )
    print(f"New Glue Job Config: {glue_job_config}")

    print(f"Getting Last Job Run Arguments for {glue_job_name}")
    response = glue_client.get_job_runs(
        JobName=glue_job_name,
    )
    job_runs = response["JobRuns"]
    last_job_run_args = (
        job_runs[0]["Arguments"] if job_runs and "Arguments" in job_runs[0] else {}
    )
    print(f"Last Job Run Args for {glue_job_name}: {last_job_run_args}\n\n")

    cli_args = args.example_script_args
    last_job_run_packages = last_job_run_args.get("--PACKAGE")
    last_job_run_pip = last_job_run_args.get("--pip-install")
    glue_start_job_run_args = {
        "--PACKAGE": last_job_run_packages,
        "--pip-install": last_job_run_pip,
        "--STAGE": stage,
        # set deltacat logging environment variables to be picked up by Glue
        "--DELTACAT_APP_LOG_LEVEL": "DEBUG",
        "--DELTACAT_SYS_LOG_LEVEL": "DEBUG",
        "--DELTACAT_APP_LOG_DIR": "/home/glue-ray-engine/var/log/",
        "--DELTACAT_SYS_LOG_DIR": "/home/glue-ray-engine/var/log/",
        "--DELTACAT_APP_INFO_LOG_BASE_FILE_NAME": "customer_script.log.dc.app.info",
        "--DELTACAT_SYS_INFO_LOG_BASE_FILE_NAME": "customer_script.log.dc.sys.info",
        "--DELTACAT_APP_DEBUG_LOG_BASE_FILE_NAME": "customer_script.log.dc.app.debug",
        "--DELTACAT_SYS_DEBUG_LOG_BASE_FILE_NAME": "customer_script.log.dc.sys.debug",
    }

    new_package = None
    if dc_url:
        new_package = f"deltacat @ {dc_url}"
    elif dc_version:
        new_package = f"deltacat=={dc_version}"
    elif not (last_job_run_packages and last_job_run_pip):
        # install the latest version of deltacat
        new_package = "deltacat"
    if new_package:
        glue_start_job_run_args["--pip-install"] = new_package
        glue_start_job_run_args["--PACKAGE"] = new_package

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
        "Tail real-time Ray driver logs for your job run by running: "
        "aws logs tail /aws-glue/ray/jobs/script-log --follow"
    )
    print(
        "Tail real-time Ray remote worker logs for your job run by running: "
        "aws logs tail /aws-glue/ray/jobs/ray-worker-out-logs --follow"
    )
