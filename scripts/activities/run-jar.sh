#! /usr/bin/env bash

REMOTE_JAR="${1?$0 JAR_LOCATION}"
LOCAL_JAR=$(basename ${REMOTE_JAR})

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

# Remove the jar filename from the arguments
shift 1

# Download the jar
echo "Downloading ${REMOTE_JAR} to ${LOCAL_JAR}"
aws s3 cp ${REMOTE_JAR} ${LOCAL_JAR}

# Ensure the local jar actually exists
[ -f ${LOCAL_JAR} ] || exit 3

# Run the jar itself.
echo "Running jar ${LOCAL_JAR} $@"
echo java -cp ${LOCAL_JAR} $@ | exec sh
