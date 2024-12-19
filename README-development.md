# DeltaCAT Developer Quickstart Guide

## Local Development
### Verifying Code Changes
Before publishing a pull request, ensure that the following validations pass:
#### Unit Tests
```shell
make test
```
#### Integration Tests
```shell
make test-integration
```
#### Code-Style Checks
```shell
make lint
```


### Makefile Targets
We use `make` to automate common development tasks. If you feel that any recurring development routine is
missing from the current set of Makefile targets, please propose a new one!

#### venv
```shell
make venv
```
Creates a virtual environment in the `venv` directory with all required development dependencies installed. You usually
don't need to run this command directly, since it will be invoked automatically by any other target that needs it.

#### clean-venv
```shell
make clean-venv
```
Removes all artifacts created by the `venv` Makefile target.

#### build
```shell
make build
```
Builds a redistributable wheel to the `build` directory. Stores intermediate artifacts in the `dist` directory.

#### clean-build
```shell
make clean-build
```
Removes all non-virtual-environment artifacts created by the `build` Makefile target.

#### rebuild
```shell
make rebuild
```
Runs `clean-build` followed by `build`.

#### clean
```shell
make clean
```
Removes all artifacts created by the `build` and `venv` Makefile targets.

#### deploy-s3
```shell
make deploy-s3
```
Builds and uploads a wheel to S3. See [Build and Deploy an S3 Wheel](#build-and-deploy-an-s3-wheel).

#### install
```shell
make install
```
Installs all developer requirements from `dev-requirements.txt` in your virtual environment.

#### lint
```shell
make lint
```
Runs the linter to ensure that code in your local workspace conforms to code-style guidelines.

#### test
```shell
make test
```
Runs all unit tests.

#### test-integration
```shell
make test-integration
```
Runs all integration tests.

#### test-integration-rebuild
```shell
make test-integration-rebuild
```
Rebuild the integration test environment.


## Cloud Integration Testing
### AWS
You can deploy and test your local DeltaCAT changes on any AWS environment that can run Ray applications (e.g. EC2, Glue
for Ray, EKS, etc.).


#### AWS Glue for Ray
> [!CAUTION]
> Iceberg script execution on Glue for Ray is currently broken. DeltaCAT and PyIceberg v0.5+ depend on a version of
> pydantic that is incompatible with ray v2.4 used by Glue. See
> [Ray Issue #37372](https://github.com/ray-project/ray/issues/37372) for additional details.

Use the Glue Runner at `dev/deploy/aws/scripts/runner.py aws glue` to configure your AWS account to run any Python script
using AWS Glue for Ray. The Glue Runner can also be used to build and upload changes in your workspace to an S3 wheel
used during execution instead of the default PyPi DeltaCAT wheel.

##### Usage Prerequisites
1. Install and configure the latest version of the AWS CLI:
   * https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html#getting-started-install-instructions
2. Create an AWS Glue IAM Role that can create and run jobs:
   * https://docs.aws.amazon.com/glue/latest/dg/getting-started-access.html
3. Install and configure boto3:
   * https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
   * https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html

##### Quickstart Guide
1. Print Usage Instructions and Exit
```shell
python runner.py aws glue -h
```
2. Create a new Glue job and run your first script in us-east-1 (the default region)
```shell
python runner.py aws glue deltacat/examples/hello_world.py --glue-iam-role "AWSGlueServiceRole"
```
3. Run an example in us-east-1 using the last job config and DeltaCAT deploy
```shell
python runner.py aws glue deltacat/examples/hello_world.py
```
4. Run an example in us-east-1 using your local workspace copy of DeltaCAT
```shell
python runner.py aws glue deltacat/examples/hello_world.py --deploy-local-deltacat
```
> [!NOTE]
> The deployed package is referenced by an S3 URL that expires in 7 days.
> After 7 days, you must deploy a new DeltaCAT package to avoid receiving a 403 error!
5. Create a new job and run an example in us-west-2:
```shell
python runner.py aws glue deltacat/examples/hello_world.py \
--region us-west-2
--glue-iam-role "AWSGlueServiceRole" \
```
6. Pass arguments into an example script as environment variables:
```shell
python runner.py aws glue deltacat/examples/basic_logging.py \
--script-args '{"--var1":"Try that", "--var2":"DeltaCAT"}' \
```

##### What Does it Do?
1. Creates an S3 bucket at `s3://deltacat-packages-{stage}` if it doesn't already exist.
2. **[Optional]** Builds a wheel containing your local workspace changes and uploads it to
`s3://deltacat-packages-{stage}/` if the `--deploy-local-deltacat` flag is set.
> [!IMPORTANT]
> {stage} is replaced with `os.environ["USER"]` unless you set the `$DELTACAT_STAGE` environment variable.
3. Creates an S3 bucket at `s3://deltacat-glue-scripts-{stage}` if it doesn't already exist.
4. Uploads the script to run to `s3://deltacat-glue-scripts-$USER`.
5. Creates or updates the Glue Job `deltacat-runner-{stage}` to run this example.
6. Run the `deltacat-runner-{stage}` Glue Job with either the newly built DeltaCAT wheel or the last used wheel.


#### Other Environments: Install Wheel from a Signed S3 URL
If you'd like to run integration tests in any other custom environment, you can run a single command to package your
local changes in a wheel, upload it to S3, then install it on your Ray cluster from a signed S3 URL.

###### Default S3 Bucket
Simply run `make deploy-s3` to upload your local workspace to a wheel at
`s3://deltacat-packages-{stage}/deltacat-{version}-{timestamp}-{python}-{abi}-{platform}.whl`.

If the deploy succeeds, you should see some text printed telling you how to install this wheel from a signed S3 URL:
```
to install run:
pip install deltacat @ `s3://deltacat-packages-{stage}/deltacat-{version}-{timestamp}-{python}-{abi}-{platform}.whl`
```
The variables in the above S3 URL will be replaced as follows:

> **stage**: The runtime value of the `$DELTACAT_STAGE` environment variable if defined or the `$USER` environment
> variable if not.

> **version**: The current DeltaCAT distribution version. See https://peps.python.org/pep-0491/.

> **timestamp**: Second-precision epoch timestamp build tag. See https://peps.python.org/pep-0491/.

> **python**: Language implementation and version tag (e.g. ‘py27’, ‘py2’, ‘py3’). See https://peps.python.org/pep-0491/.

> **abi**: ABI tag (e.g. ‘cp33m’, ‘abi3’, ‘none’). See https://peps.python.org/pep-0491/.

> **platform**: Platform tag (e.g. ‘linux_x86_64’, ‘any’). See https://peps.python.org/pep-0491/.

###### Custom S3 Bucket
Use the `$DELTACAT_STAGE` environment variable to change the S3 bucket that your workspace wheel is uploaded to:
```shell
export DELTACAT_STAGE=dev
make deploy-s3
```
This uploads a wheel to
`s3://deltacat-packages-dev/deltacat-{version}-{timestamp}-{python}-{abi}-{platform}.whl`.


## Coding Quirks
### Cloudpickle
Some DeltaCAT compute functions interact with Cloudpickle differently than the typical Ray application. This allows
us to improve compute stability and efficiency at the cost of managing our own distributed object garbage collection
instead of relying on Ray's automatic distributed object reference counting and garbage collection. For example, see
the comment at `deltacat/compute/compactor/utils/primary_key_index.py` for an explanation of our custom
`cloudpickle.dumps` usage.
