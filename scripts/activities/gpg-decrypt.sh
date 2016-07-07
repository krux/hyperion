#! /usr/bin/env bash

usage() {
  NAME=$(basename $0)
  cat <<EOF
Usage: ${NAME} KEY
Decrypts each file in INPUT1_STAGING_DIR (${INPUT1_STAGING_DIR})
into OUTPUT1_STAGING_DIR (${OUTPUT1_STAGING_DIR})
using the private key given in the file KEY while removing any ".gpg" extension
from each filename.
EOF
  exit 3
}

if [ $# -ne 1 ]; then
  echo "ERROR: wrong number of arguments"
  usage
fi

if [ -z "${INPUT1_STAGING_DIR}" ]; then
  echo "ERROR: INPUT1_STAGING_DIR must be specified"
  usage
fi

if [ -z "${OUTPUT1_STAGING_DIR}" ]; then
  echo "ERROR: OUTPUT1_STAGING_DIR must be specified"
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

# get a file, whether local or on S3
# $1 the path to the unencrypted file
# $2 the working directory
function get_file() {
  LOCAL="$2"/"$(basename "$1")"
  case "$1" in
    s3:*)
      aws s3 cp "$1" "${LOCAL}"
      ;;
    *)
      cp "$1" "${LOCAL}"
      ;;
  esac
  echo "${LOCAL}"
}

set -xe
IFS=$'\n\t'

# create a temporary working directory
if [ -x /usr/local/bin/gmktemp ]; then
  # for testing on MacOS
  WORKING_DIR="$(gmktemp -d)"
else
  WORKING_DIR="$(mktemp -d)"
fi
add_on_exit rm -rf \"${WORKING_DIR}\"

# the file(s) to decrypt
FILES=($(find "${INPUT1_STAGING_DIR}" -type f))
if [ "${#FILES[@]}" -eq 0 ]; then
  echo "ERROR: INPUT1_STAGING_DIR must contain at least one file to decrypt"
  exit 3
fi

# the private encryption key file
KEY=$(get_file "$1" "${WORKING_DIR}")
if [ ! -s "${KEY}" ]; then
  echo "ERROR: cannot find public key file ${KEY}"
  exit 3
fi

# decrypt each file in INPUT1_STAGING_DIR
for FILE in "${FILES[@]}" ; do
  DECRYPTED="${OUTPUT1_STAGING_DIR}"/"$(basename "${FILE}" .gpg)"
  gpg --batch --yes --decrypt --passphrase-fd 0 --output "${DECRYPTED}" "${FILE}" < "${KEY}"
done
