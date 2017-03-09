#! /usr/bin/env bash

usage() {
  echo "usage: gsutil_upload.sh s3://bucket/boto.config gs://gsbucket/path/ true|false"
  echo
  echo "The contents of INPUT1_STAGING_DIR (${INPUT1_STAGING_DIR}) will be uploaded to the GS path."
  exit 3
}

if ([ -z "$1" ] || [ -z "$2" ]); then
  echo "ERROR: missing arguments"
  usage
fi

if [ -z "${INPUT1_STAGING_DIR}" ]; then
  echo "ERROR: INPUT1_STAGING_DIR must be specified"
  usage
fi

declare -a on_exit_items

function on_exit() {
  for i in "${on_exit_items[@]}"; do
    eval ${i}
  done
}

function add_on_exit() {
  local n=${#on_exit_items[*]}
  on_exit_items[$n]="$*"
  if [[ ${n} -eq 0 ]]; then
    trap on_exit EXIT
  fi
}

set -xe

WORKING_DIR=$(mktemp -d)
cd ${WORKING_DIR}
add_on_exit rm -rf ${WORKING_DIR}

# This is the name of the tarball
TARBALL="gsutil.tar.gz"

# This is the download location for the latest gsutil release
GSUTIL_URL="https://storage.googleapis.com/pub/${TARBALL}"

# This is the configuration file for gsutil
export BOTO_CONFIG="boto.gsutil"

# This is the source of the boto config file
BOTO_CONFIG_SOURCE="$1"

# This is the destination Google Storage location
OUTPUT_GOOGLE_STORAGE="$2"

# This is the flag to determine whether to use recursive option or not.
RECURSIVE="${3:-false}"

# Download and extract the tarball.
# We use --no-check-certificate because Google are naughty with their certificates.
wget --no-verbose --no-check-certificate ${GSUTIL_URL}
tar -xzf ${TARBALL}

# Download the boto configuration
aws s3 cp ${BOTO_CONFIG_SOURCE} ${BOTO_CONFIG}

NUM_INPUT_FILES=$(ls ${INPUT1_STAGING_DIR}/* | wc -l | awk '{print $1}')

if [ "${NUM_INPUT_FILES}" -eq "0" ]; then
  echo "ERROR: no input files provided - not uploading"
  exit 3
fi

if [[ ${RECURSIVE} == "true" ]]; then
    # To perform a parallel (multi-threaded/multi-processing) copy use -m option.
    # To copy an entire directory tree use the -r option.
    ./gsutil/gsutil -m cp -r ${INPUT1_STAGING_DIR}/* ${OUTPUT_GOOGLE_STORAGE}
else
    ./gsutil/gsutil cp ${INPUT1_STAGING_DIR}/* ${OUTPUT_GOOGLE_STORAGE}
fi
