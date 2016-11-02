#! /usr/bin/env bash

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

function download() {
  echo "Downloading $1 to $2"
  aws s3 cp $1 $2
  if [[ ! -f $2 ]]; then
    echo "Failed to download $2"
    exit 3
  fi
}

set -xe

# Set up a working directory for the activity
WORKING_DIR=$(mktemp -d)
cd ${WORKING_DIR}
add_on_exit rm -rf ${WORKING_DIR}

# Parse arguments
CLASSPATH=""

while [[ $# > 0 ]]; do
  case $1 in
    --env)
      shift
      REMOTE_ENV_SH="$1"
      LOCAL_ENV_SH=$(basename ${REMOTE_ENV_SH})
      shift

      download ${REMOTE_ENV_SH} ${LOCAL_ENV_SH}

      # Now source the environment shell script to pull in the variables it sets
      source ${LOCAL_ENV_SH}
      ;;

    --cp)
      REMOTE_CP_JAR="$1"
      LOCAL_CP_JAR=$(basename ${REMOTE_CP_JAR})
      shift

      # Download the remote classpath jar
      download ${REMOTE_CP_JAR} ${LOCAL_CP_JAR}

      # Add it to the CLASSPATH variable
      CLASSPATH="${CLASSPATH}:${LOCAL_CP_JAR}"
      ;;

    --jar)
      # Just skip this, for compatibility
      shift
      ;;

    *)
      REMOTE_JAR="$1"
      LOCAL_JAR=$(basename ${REMOTE_JAR})
      shift

      download ${REMOTE_JAR} ${LOCAL_JAR}
      break
      ;;
  esac
done

# Run the jar itself.
echo "Running jar ${LOCAL_JAR} $@"
java -cp ${LOCAL_JAR}${CLASSPATH} "$@"
