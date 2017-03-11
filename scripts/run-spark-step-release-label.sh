#!/usr/bin/env bash

# Run a spark job on a EMR yarn cluster with release label > 4.x.x.

# Die if anything happens
set -ex

# Usage
if [[ "$#" -lt 2 ]]; then
  echo "Usage: $0 [<spark options>] <jar-s3-uri> <class> [<args>]" >&2
  exit 1
fi

# Parse the spark options as any option until the first non-option (the jar)
SPARK_OPTIONS=""

while [[ $# > 0 ]]; do
  key=$1

  case ${key} in
    --*)
      shift
      SPARK_OPTIONS="${SPARK_OPTIONS} ${key} $1"
      shift
      ;;

    *)
      break
      ;;
  esac
done

EMR_HOME="/mnt"
ENV_FILE="${EMR_HOME}/hyperion_env.sh"

[ -f ${ENV_FILE} ] && source ${ENV_FILE}

HYPERION_HOME="${EMR_HOME}/hyperion"

mkdir -p ${HYPERION_HOME}

REMOTE_JAR_LOCATION=$1; shift
JOB_CLASS=$1; shift

LOCAL_JAR_DIR="$(mktemp -p $HYPERION_HOME -d -t jars_XXXXXX)"
JAR_NAME="${REMOTE_JAR_LOCATION##*/}"
LOCAL_JAR="${LOCAL_JAR_DIR}/${JAR_NAME}"

# Download JAR file from S3 to local
hadoop fs -get ${REMOTE_JAR_LOCATION} ${LOCAL_JAR}

exec spark-submit ${SPARK_OPTIONS} --class ${JOB_CLASS} ${LOCAL_JAR} $@
