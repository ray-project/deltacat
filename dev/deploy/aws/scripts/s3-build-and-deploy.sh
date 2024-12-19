#!/bin/bash

# text output formatting vars
YAY=$(tput setaf 47) # for good events
INFO=$(tput setaf 39) # for important events
CLI=$(tput setaf 214) # for code and commands
BARF=$(tput setaf 1) # for bad events
PLAIN=$(tput sgr0) # for everything else

function info()
{
    echo "${INFO}$1${PLAIN}"
}

function cli()
{
    echo "${CLI}$1${PLAIN}"
}

function yay()
{
    echo "${YAY}$1${PLAIN}"
}

function barf()
{
    echo "${BARF}$1${PLAIN}"
}

function cleanup()
{
  if [ -n "${UNIQUE_WHEEL+x}" ]; then
    rm -f "$UNIQUE_WHEEL"
  fi
}

# fail fast on relevant errors (unbound variables are permitted), for more information see:
# https://buildkite.com/docs/pipelines/writing-build-scripts#configuring-bash
set -eo pipefail

# ensure that we always cleanup if the script is interrupted
trap cleanup EXIT SIGINT SIGTERM

if [[ -z "${DELTACAT_STAGE}" ]]; then
  S3_PACKAGE_BUCKET_NAME="deltacat-packages-$USER"
else
  S3_PACKAGE_BUCKET_NAME="deltacat-packages-$DELTACAT_STAGE"
fi
S3_PACKAGE_BUCKET_URL="s3://$S3_PACKAGE_BUCKET_NAME"
CURRENT_UNIX_TIME=$(date +"%s")

info "user=$USER"
info "current unix time=$CURRENT_UNIX_TIME"
info "working directory=$(pwd)"
info "creating s3 bucket @ $S3_PACKAGE_BUCKET_NAME"
aws s3api create-bucket --bucket "$S3_PACKAGE_BUCKET_NAME"
# cleanup old deltacat build artifacts and build a new deltacat wheel
info "rebuilding deltacat wheels"
make rebuild
# copy the new deltacat wheel to a unique wheel name w/ timestamp build tag
pushd dist
for file in deltacat-*.whl
do
    if [ -z ${UNIQUE_WHEEL+x} ]; then
      UNIQUE_WHEEL=${file//-py3-none-any/-$CURRENT_UNIX_TIME-py3-none-any}
      info "copying $file to $UNIQUE_WHEEL"
      cp "$file" "$UNIQUE_WHEEL"
    else
      barf "ERROR: multiple deltacat wheels found: ${ls dist/deltacat-*.whl}"
      exit 1
    fi
done
# upload the new deltacat wheel to s3 and create a presigned URL for it
info "uploading $UNIQUE_WHEEL to $S3_PACKAGE_BUCKET_URL"
aws s3 cp "$UNIQUE_WHEEL" "$S3_PACKAGE_BUCKET_URL"
DELTACAT_PRESIGNED_URL=$(aws s3 presign "$S3_PACKAGE_BUCKET_URL/$UNIQUE_WHEEL" --expires-in 604800)
yay "=== SUCCESS ==="
echo "DELTACAT_PRESIGNED_URL=$DELTACAT_PRESIGNED_URL"
info "to install run:"
cli "pip install deltacat @ $DELTACAT_PRESIGNED_URL"
# cleanup workspace changes and temporary artifacts
cleanup
popd # dist
