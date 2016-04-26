#! /usr/bin/env bash

set -e

if [[ "$#" -ne 1 ]]; then
  echo "Usage: $0 <env-file-s3-path>"
  exit 1
fi

S3_ENV_FILE=$1; shift
ENV_FILE="/home/hadoop/hyperion_env.sh"

rm -f ${ENV_FILE}

echo "Downloading env file from ${S3_ENV_FILE}"
if [ -x "$(command -v aws)" ];
then
    aws s3 cp ${S3_ENV_FILE} ${ENV_FILE}
else
    hadoop fs -get ${S3_ENV_FILE} ${ENV_FILE}
fi

echo "Done!"
