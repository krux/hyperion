#!/usr/bin/env bash

UNCOMPRESSED_FILENAME="report.tsv"
COMPRESSED_FILENAME="${UNCOMPRESSED_FILENAME}.gz"
ARGS="FROM SUBJECT BODY TO CC"
export INSTALL_MAILER="echo 'installing mailx'"
export MAILER="echo mail"

function cleanUp() {
  rm -rf inputs output report*.tsv*
}

function setUp() {
  cleanUp
  mkdir -p inputs/{1,2,3,4,5} output/
  echo "1,2" | tee inputs/{1,2,3,4}/part-0000{1,2,3} > /dev/null
  gzip inputs/{1,2}/part-*
  echo "========================================================================================================================="
  echo "Test Case $1"
  echo "========================================================================================================================="
}

function fail() {
  echo "FAIL: $1"
  exit 3
}

rm -rf test-logs/
mkdir -p test-logs/

setUp "1. No arguments"
./email-file.sh > test-logs/1.out 2> test-logs/1.err
[ $? -ne 1 ] && fail
echo "SUCCESS"

setUp "2. No input directory"
./email-file.sh ${ARGS} > test-logs/2.out 2> test-logs/2.err
[ $? -ne 1 ] && fail
echo "SUCCESS"

setUp "3. No input files"
INPUT1_STAGING_DIR=inputs/5 ./email-file.sh ${COMPRESSED_FILENAME} ${ARGS} > test-logs/3.out 2> test-logs/3.err
[ $? -ne 1 ] && fail
echo "SUCCESS"

setUp "4. One compressed input, output compressed"
INPUT1_STAGING_DIR=inputs/1 ./email-file.sh ${COMPRESSED_FILENAME} ${ARGS} > test-logs/4.out 2> test-logs/4.err
[ $? -gt 0 ] && fail
[ ! -f report.tsv.gz ] && fail
[ "$(head -1 test-logs/4.out)" != "'installing mailx'" ] && fail
[ "$(tail -1 test-logs/4.out)" != "mail -v -a 'report.tsv.gz' -r FROM -s SUBJECT -c CC TO" ] && fail
[ $(gzcat report.tsv.gz | wc -l) -ne 3 ] && fail
echo "SUCCESS"

setUp "5. One uncompressed input, output compressed"
INPUT1_STAGING_DIR=inputs/3 ./email-file.sh ${COMPRESSED_FILENAME} ${ARGS} > test-logs/5.out 2> test-logs/5.err
[ $? -gt 0 ] && fail
[ ! -f report.tsv.gz ] && fail
[ "$(head -1 test-logs/5.out)" != "'installing mailx'" ] && fail
[ "$(tail -1 test-logs/5.out)" != "mail -v -a 'report.tsv.gz' -r FROM -s SUBJECT -c CC TO" ] && fail
[ $(gzcat report.tsv.gz | wc -l) -ne 3 ] && fail
echo "SUCCESS"

setUp "6. Two compressed inputs, output compressed"
INPUT1_STAGING_DIR=inputs/1 INPUT2_STAGING_DIR=inputs/2 ./email-file.sh ${COMPRESSED_FILENAME} ${ARGS} > test-logs/6.out 2> test-logs/6.err
[ $? -gt 0 ] && fail
[ ! -f report.tsv.gz ] && fail
[ ! -f report\(1\).tsv.gz ] && fail
[ "$(head -1 test-logs/6.out)" != "'installing mailx'" ] && fail
[ "$(tail -1 test-logs/6.out)" != "mail -v -a 'report.tsv.gz' -a 'report(1).tsv.gz' -r FROM -s SUBJECT -c CC TO" ] && fail
[ $(gzcat report.tsv.gz | wc -l) -ne 3 ] && fail
[ $(gzcat report\(1\).tsv.gz | wc -l) -ne 3 ] && fail
echo "SUCCESS"

setUp "7. Two uncompressed inputs, output compressed"
INPUT1_STAGING_DIR=inputs/3 INPUT2_STAGING_DIR=inputs/4 ./email-file.sh ${COMPRESSED_FILENAME} ${ARGS} > test-logs/7.out 2> test-logs/7.err
[ $? -gt 0 ] && fail
[ ! -f report.tsv.gz ] && fail
[ ! -f report\(1\).tsv.gz ] && fail
[ "$(head -1 test-logs/7.out)" != "'installing mailx'" ] && fail
[ "$(tail -1 test-logs/7.out)" != "mail -v -a 'report.tsv.gz' -a 'report(1).tsv.gz' -r FROM -s SUBJECT -c CC TO" ] && fail
[ $(gzcat report.tsv.gz | wc -l) -ne 3 ] && fail
[ $(gzcat report\(1\).tsv.gz | wc -l) -ne 3 ] && fail
echo "SUCCESS"

setUp "8. Two mixed compression inputs, output compressed"
INPUT1_STAGING_DIR=inputs/2 INPUT2_STAGING_DIR=inputs/3 ./email-file.sh ${COMPRESSED_FILENAME} ${ARGS} > test-logs/8.out 2> test-logs/8.err
[ $? -gt 0 ] && fail
[ ! -f report.tsv.gz ] && fail
[ ! -f report\(1\).tsv.gz ] && fail
[ "$(head -1 test-logs/8.out)" != "'installing mailx'" ] && fail
[ "$(tail -1 test-logs/8.out)" != "mail -v -a 'report.tsv.gz' -a 'report(1).tsv.gz' -r FROM -s SUBJECT -c CC TO" ] && fail
[ $(gzcat report.tsv.gz | wc -l) -ne 3 ] && fail
[ $(gzcat report\(1\).tsv.gz | wc -l) -ne 3 ] && fail
echo "SUCCESS"

setUp "9. One compressed input, output uncompressed"
INPUT1_STAGING_DIR=inputs/1 ./email-file.sh ${UNCOMPRESSED_FILENAME} ${ARGS} > test-logs/9.out 2> test-logs/9.err
[ $? -gt 0 ] && fail
[ ! -f report.tsv ] && fail
[ "$(head -1 test-logs/9.out)" != "'installing mailx'" ] && fail
[ "$(tail -1 test-logs/9.out)" != "mail -v -a 'report.tsv' -r FROM -s SUBJECT -c CC TO" ] && fail
[ $(cat report.tsv | wc -l) -ne 3 ] && fail
echo "SUCCESS"

setUp "10. One uncompressed input, output uncompressed"
INPUT1_STAGING_DIR=inputs/3 ./email-file.sh ${UNCOMPRESSED_FILENAME} ${ARGS} > test-logs/10.out 2> test-logs/10.err
[ $? -gt 0 ] && fail
[ ! -f report.tsv ] && fail
[ "$(head -1 test-logs/10.out)" != "'installing mailx'" ] && fail
[ "$(tail -1 test-logs/10.out)" != "mail -v -a 'report.tsv' -r FROM -s SUBJECT -c CC TO" ] && fail
[ $(cat report.tsv | wc -l) -ne 3 ] && fail
echo "SUCCESS"

setUp "11. Two compressed inputs, output uncompressed"
INPUT1_STAGING_DIR=inputs/1 INPUT2_STAGING_DIR=inputs/2 ./email-file.sh ${UNCOMPRESSED_FILENAME} ${ARGS} > test-logs/11.out 2> test-logs/11.err
[ $? -gt 0 ] && fail
[ ! -f report.tsv ] && fail
[ ! -f report\(1\).tsv ] && fail
[ "$(head -1 test-logs/11.out)" != "'installing mailx'" ] && fail
[ "$(tail -1 test-logs/11.out)" != "mail -v -a 'report.tsv' -a 'report(1).tsv' -r FROM -s SUBJECT -c CC TO" ] && fail
[ $(cat report.tsv | wc -l) -ne 3 ] && fail
[ $(cat report\(1\).tsv | wc -l) -ne 3 ] && fail
echo "SUCCESS"

setUp "12. Two uncompressed inputs, output uncompressed"
INPUT1_STAGING_DIR=inputs/3 INPUT2_STAGING_DIR=inputs/4 ./email-file.sh ${UNCOMPRESSED_FILENAME} ${ARGS} > test-logs/12.out 2> test-logs/12.err
[ $? -gt 0 ] && fail
[ ! -f report.tsv ] && fail
[ ! -f report\(1\).tsv ] && fail
[ "$(head -1 test-logs/12.out)" != "'installing mailx'" ] && fail
[ "$(tail -1 test-logs/12.out)" != "mail -v -a 'report.tsv' -a 'report(1).tsv' -r FROM -s SUBJECT -c CC TO" ] && fail
[ $(cat report.tsv | wc -l) -ne 3 ] && fail
[ $(cat report\(1\).tsv | wc -l) -ne 3 ] && fail
echo "SUCCESS"

setUp "13. Two mixed compression inputs, output uncompressed"
INPUT1_STAGING_DIR=inputs/2 INPUT2_STAGING_DIR=inputs/3 ./email-file.sh ${UNCOMPRESSED_FILENAME} ${ARGS} > test-logs/13.out 2> test-logs/13.err
[ $? -gt 0 ] && fail
[ ! -f report.tsv ] && fail
[ ! -f report\(1\).tsv ] && fail
[ "$(head -1 test-logs/13.out)" != "'installing mailx'" ] && fail
[ "$(tail -1 test-logs/13.out)" != "mail -v -a 'report.tsv' -a 'report(1).tsv' -r FROM -s SUBJECT -c CC TO" ] && fail
[ $(cat report.tsv | wc -l) -ne 3 ] && fail
[ $(cat report\(1\).tsv | wc -l) -ne 3 ] && fail
echo "SUCCESS"

cleanUp
