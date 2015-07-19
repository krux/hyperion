#! /usr/bin/env bash

usage() {
  echo "usage: gsutil_download.sh s3://bucket/boto.config gs://gsbucket/path/"
  echo
  echo "The contents of the GS path will be downloaded to OUTPUT1_STAGING_DIR (${OUTPUT1_STAGING_DIR})"
  exit 3
}

if ([ -z "$1" ] || [ -z "$2" ]); then
  echo "ERROR: missing arguments"
  usage
fi

if [ -z "${OUTPUT1_STAGING_DIR}" ]; then
  echo "ERROR: OUTPUT1_STAGING_DIR must be specified"
  usage
fi

set -xe

# This is the name of the tarball
TARBALL="gsutil.tar.gz"

# This is the download location for the latest gsutil release
GSUTIL_URL="https://storage.googleapis.com/pub/${TARBALL}"

# This is the configuration file for gsutil
export BOTO_CONFIG="boto.gsutil"

# This is the source of the boto config file
BOTO_CONFIG_SOURCE="$1"

# This is the destination Google Storage location
INPUT_GOOGLE_STORAGE="$2"

# Download and extract the tarball.
# We use --no-check-certificate because Google are naughty with their certificates.
wget --no-verbose --no-check-certificate ${GSUTIL_URL}
tar -xzf ${TARBALL}

# Download the boto configuration
aws s3 cp ${BOTO_CONFIG_SOURCE} ${BOTO_CONFIG}

./gsutil/gsutil cp ${INPUT_GOOGLE_STORAGE}/* ${OUTPUT1_STAGING_DIR}/

