#! /usr/bin/env bash

usage() {
  echo "usage: email-file.sh FILENAME FROM SUBJECT BODY TO CC?"
  exit 3
}

FILENAME="$1"
FROM="$2"
SUBJECT="$3"
BODY="$4"
TO="$5"
CC="$6"

if [ -z "$FILENAME" ]; then
  echo "ERROR: FILENAME not specified"
  usage
fi

if [ -z "$FROM" ]; then
  echo "ERROR: FROM not specified"
  usage
fi

if [ -z "$SUBJECT" ]; then
  echo "ERROR: SUBJECT not specified"
  usage
fi

if [ -z "$BODY" ]; then
  echo "ERROR: BODY not specified"
  usage
fi

if [ -z "$TO" ]; then
  echo "ERROR: TO not specified"
  usage
fi

if [ -n "$CC" ]; then
  CC="-c $CC"
fi

if [ -z "${INPUT1_STAGING_DIR}" ]; then
  echo "ERROR: INPUT1_STAGING_DIR must be set"
  usage
fi

set -xe

BASENAME=$(basename ${FILENAME} .gz)
ATTACHMENTS=""
n=0

for dir in ${INPUT1_STAGING_DIR} ${INPUT2_STAGING_DIR} ${INPUT3_STAGING_DIR} ${INPUT4_STAGING_DIR} ${INPUT5_STAGING_DIR} ${INPUT6_STAGING_DIR} ${INPUT7_STAGING_DIR} ${INPUT8_STAGING_DIR} ${INPUT9_STAGING_DIR} ${INPUT10_STAGING_DIR}; do
  # Decompress the files if required
  find ${dir} -name *.gz | xargs gunzip

  # Figure out what this file should be called
  THISFILE="${BASENAME}"
  if [ ${n} -ne 0 ]; then
    THISFILE="${BASENAME%.*}(${n}).${BASENAME##*.}"
  fi

  # Merge the files
  cat ${dir}/* > ${THISFILE}

  # Check whether the output should be compressed
  if [[ ${FILENAME} == *.gz ]]; then
    gzip ${THISFILE}
    ATTACHMENTS="${ATTACHMENTS} -a ${THISFILE}.gz"
  else
    ATTACHMENTS="${ATTACHMENTS} -a ${THISFILE}"
  fi

  n=$((n+1))
done

sudo yum install -y mailx

echo $BODY | mail -v ${ATTACHMENTS} -r $FROM -s "$SUBJECT" $CC $TO
