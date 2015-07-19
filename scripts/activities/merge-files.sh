#! /usr/bin/env bash

USAGE="usage: merge-files.sh OUTPUT_FILENAME HEADER?"
OUTPUT_FILENAME=${1?$USAGE}
: ${INPUT1_STAGING_DIR?$USAGE} ${OUTPUT1_STAGING_DIR?$USAGE}

set -xe

BASENAME=$(basename ${OUTPUT_FILENAME} .gz)
MERGED_FILE="${OUTPUT1_STAGING_DIR}/${BASENAME}"

# Add the header
if [ -n "$2" ]; then
  echo "$2" > ${MERGED_FILE}
fi

# Append all of the input directories
for dir in ${INPUT1_STAGING_DIR} ${INPUT2_STAGING_DIR} ${INPUT3_STAGING_DIR} ${INPUT4_STAGING_DIR} ${INPUT5_STAGING_DIR} ${INPUT6_STAGING_DIR} ${INPUT7_STAGING_DIR} ${INPUT8_STAGING_DIR} ${INPUT9_STAGING_DIR} ${INPUT10_STAGING_DIR}; do
  # Decompress files if they are compressed (only GZIP supported at the moment)
  find ${dir} -name *.gz | xargs gunzip

  # Cat all of the files together
  cat ${dir}/* >> ${MERGED_FILE}
done

# Compress the output file if required
if [[ ${OUTPUT_FILENAME} == *.gz ]]; then
  gzip ${MERGED_FILE}
  MERGED_FILE="${MERGED_FILE}.gz"
fi

# Make copies in the other output staging directories
for dir in ${OUTPUT2_STAGING_DIR} ${OUTPUT3_STAGING_DIR} ${OUTPUT4_STAGING_DIR} ${OUTPUT5_STAGING_DIR} ${OUTPUT6_STAGING_DIR} ${OUTPUT7_STAGING_DIR} ${OUTPUT8_STAGING_DIR} ${OUTPUT9_STAGING_DIR} ${OUTPUT10_STAGING_DIR}; do
  # XXX TODO - could we just ln the files?
  cp ${MERGED_FILE} ${dir}/
done
