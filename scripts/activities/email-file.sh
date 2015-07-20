#! /usr/bin/env bash

USAGE="usage: email-file.sh FILENAME FROM SUBJECT BODY TO CC?"
INSTALL_MAILER=${INSTALL_MAILER:-sudo yum install -y mailx}
MAILER="${MAILER:-mail}"
FILENAME="${1?$USAGE}"
FROM="${2?$USAGE}"
SUBJECT="${3?$USAGE}"
BODY="${4?$USAGE}"
TO="${5?$USAGE}"
CC="$6"

: ${INPUT1_STAGING_DIR?$USAGE}

if [ -n "${CC}" ]; then
  CC="-c ${CC}"
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

BASENAME=$(basename ${FILENAME} .gz)
ATTACHMENTS=""
n=0
CURRENT_DIR=$(pwd)

for dir in ${INPUT1_STAGING_DIR} ${INPUT2_STAGING_DIR} ${INPUT3_STAGING_DIR} ${INPUT4_STAGING_DIR} ${INPUT5_STAGING_DIR} ${INPUT6_STAGING_DIR} ${INPUT7_STAGING_DIR} ${INPUT8_STAGING_DIR} ${INPUT9_STAGING_DIR} ${INPUT10_STAGING_DIR}; do
  # Create a temporary working dir for this file
  if [ -z "${WORKING_DIR}" ]; then
    WORKING_DIR=$(mktemp)
    add_on_exit rm -rf ${WORKING_DIR}
  fi
  cd ${WORKING_DIR}

  # Decompress the files if required
  find ${dir} -name \*.gz | xargs gunzip

  # Figure out what this file should be called
  THISFILE="${BASENAME}"
  if [ ${n} -ne 0 ]; then
    THISFILE="${BASENAME%.*}(${n}).${BASENAME##*.}"
  fi

  # Merge the files
  cat ${dir}/* > "${THISFILE}"

  # Check whether the output should be compressed
  if [[ ${FILENAME} == *.gz ]]; then
    gzip -f ${THISFILE}
    ATTACHMENTS="${ATTACHMENTS} -a ${THISFILE}.gz"
  else
    ATTACHMENTS="${ATTACHMENTS} -a ${THISFILE}"
  fi

  n=$((n+1))

  cd ${CURRENT_DIR}
done

${INSTALL_MAILER}

echo ${BODY} | ${MAILER} -v ${ATTACHMENTS} -r ${FROM} -s "$SUBJECT" ${CC} ${TO}
