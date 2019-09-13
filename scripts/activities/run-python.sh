#! /usr/bin/env bash

# Usage: $0 s3://something.{py,gz} (-r requirements.txt)? (-m module)? (-i index-url)? (--extra-index-url url)? script.py? -- args

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

PY_REMOTE=${1?Python URL required}; shift
PY_LOCAL="$(basename ${PY_REMOTE})"
PY_EXT="${PY_LOCAL##*.}"
REQUIREMENTS="requirements.txt"
WORKING_DIR=$(mktemp -d)
cd ${WORKING_DIR}
add_on_exit rm -rf ${WORKING_DIR}

while [[ $# > 0 ]]; do
  key=$1; shift

  case ${key} in
    -r)
      REQUIREMENTS=$1; shift
      ;;

    -m)
      PY_MODULE="-m $1"; shift
      ;;

    -i)
      INDEX_URL="-i $1"; shift
      ;;

    --extra-index-url)
      EXTRA_INDEX_URLS="${EXTRA_INDEX_URLS} --extra-index-url $1"; shift
      ;;

    --)
      break
      ;;

    *)
      PY_SCRIPT=${key}
      ;;
  esac
done

# Copy the script or archive locally
echo "Downloading ${PY_REMOTE} to ${PY_LOCAL}"
aws s3 cp ${PY_REMOTE} ${PY_LOCAL}
[ -f ${PY_LOCAL} ] || exit 3

# If we downloaded an archive, unzip it and setup a virtual environment
if [ "${PY_EXT}" == "tgz" ]; then
  echo "Uncompressing ${PY_LOCAL}"
  tar zxvf ${PY_LOCAL}
  PY_LOCAL="${PY_LOCAL%.*}"
  PY_EXT="${PY_LOCAL##*.}"
else
  if [ "${PY_EXT}" == "gz" ]; then
    echo "Uncompressing ${PY_LOCAL}"
    gunzip -f ${PY_LOCAL}
    PY_LOCAL="${PY_LOCAL%.*}"
    PY_EXT="${PY_LOCAL##*.}"
  fi

  if [ "${PY_EXT}" == "tar" ]; then
    echo "Expanding ${PY_LOCAL}"
    tar xvf ${PY_LOCAL}
    PY_LOCAL="${PY_LOCAL%.*}"
    PY_EXT="${PY_LOCAL##*.}"
  fi
fi

# If a script hasn't been specified
if [ -z "${PY_SCRIPT}" ]; then
  if [ "${PY_EXT}" == "py" ]; then
    # And the extension is .py then the script is the local file
    PY_SCRIPT="${PY_LOCAL}"
  else
    if [ $(find . -name \*.py | wc -l) -eq 1 ]; then
      # If there is only one .py file, then execute that
      PY_SCRIPT=$(find . -name \*.py)
    else
      # Otherwise, we don't know what to do
      echo "ERROR: Script name required"
      exit 3
    fi
  fi
fi

# If a requirements file exists, then setup a virtual environment
# and install the requirements into that
if [ -f ${REQUIREMENTS} ]; then
  echo "Setting up virtual environment"
  sudo easy_install virtualenv

  virtualenv -v .ENV

  source .ENV/bin/activate

  echo "Installing requirements"
  pip install -v ${INDEX_URL} ${EXTRA_INDEX_URLS} -r ${REQUIREMENTS}
fi

echo "Executing script ${PY_MODULE} ${PY_SCRIPT} $@"

python ${PY_MODULE} ${PY_SCRIPT} $@

