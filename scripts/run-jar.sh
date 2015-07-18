#! /usr/bin/env bash

REMOTE_JAR="${1?$0 JAR_LOCATION}"
LOCAL_JAR=$(basename ${REMOTE_JAR})

set -x

# Remove the jar filename from the arguments
shift 1

# Download the jar if it doesn't exist locally
if [ ! -f ${LOCAL_JAR} ]; then
  echo "Downloading ${REMOTE_JAR} to ${LOCAL_JAR}"
  aws s3 cp ${REMOTE_JAR} ${LOCAL_JAR}
fi

# Ensure the local jar actually exists
[ -f ${LOCAL_JAR} ] || exit 3

# Run the jar itself.
echo "Running jar ${LOCAL_JAR} $@"
exec java -cp ${LOCAL_JAR} $@
