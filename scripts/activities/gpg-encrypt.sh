#! /usr/bin/env bash

usage() {
  NAME=$(basename $0)
  cat <<EOF
Usage: ${NAME} [--mark-successful-jobs] KEY
Encrypts each file in INPUT1_STAGING_DIR (${INPUT1_STAGING_DIR})
into OUTPUT1_STAGING_DIR (${OUTPUT1_STAGING_DIR})
using the public key given in the file KEY while appending ".gpg" to each
filename.
    --mark-successful-jobs  creates a _SUCCESS file in OUTPUT1_STAGING_DIR
                            if the encryption is successful
Example: ${NAME} --mark-successful-jobs s3://path/to/public/key
EOF
  exit 3
}

# process options
MARK_SUCCESSFUL_JOBS=0
while [ $# -gt 0 ]; do
  case "$1" in
    --mark-successful-jobs)
      MARK_SUCCESSFUL_JOBS=1
      shift
      ;;
    -*)
      echo "ERROR: unrecognized option: $1"
      usage
      ;;
    *)
      break
      ;;
  esac
done

if [ $# -lt 1 ]; then
  echo "ERROR: too few arguments"
  usage
fi

if [ $# -gt 1 ]; then
  echo "ERROR: too many arguments"
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

# the file(s) to encrypt
FILES=($(find "${INPUT1_STAGING_DIR}" -name ".*" -prune -o -type f -not -empty -not -name "_*" -print))
if [ "${#FILES[@]}" -eq 0 ]; then
  echo "ERROR: INPUT1_STAGING_DIR must contain at least one file to encrypt"
  exit 3
fi

# the public encryption key file
KEY=$(get_file "$1" "${WORKING_DIR}")
if [ ! -s "${KEY}" ]; then
  echo "ERROR: cannot find public key file ${KEY}"
  exit 3
fi

# import the key and remember the name
RECIPIENT="$(gpg --batch --yes --import "${KEY}" 2>&1 | fgrep 'gpg: key' | cut -d \" -f 2)"

# cleanup by deleting the key on exit
add_on_exit gpg --batch --yes --delete-key \"${RECIPIENT}\"

# encrypt each file in INPUT1_STAGING_DIR
for FILE in "${FILES[@]}" ; do
  ENCRYPTED="${OUTPUT1_STAGING_DIR}"/"$(basename "${FILE}")".gpg
  gpg --batch --yes --always-trust --encrypt --recipient "${RECIPIENT}" --output "${ENCRYPTED}" "${FILE}"
done

# mark success
if [ ${MARK_SUCCESSFUL_JOBS} -eq 1 ]; then
  touch "${OUTPUT1_STAGING_DIR}"/_SUCCESS
fi
