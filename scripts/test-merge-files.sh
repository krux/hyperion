#!/usr/bin/env bash

function cleanUp() {
  rm -rf inputs outputs
}

function setUp() {
  cleanUp
  mkdir -p inputs/{1,2,3,4,5} outputs/{1,2}
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

rm -rf test-logs
mkdir -p test-logs

setUp "1. No arguments"
./merge-files.sh > test-logs/1.out 2> test-logs/1.err
[ $? -ne 1 ] && fail
echo "SUCCESS"

setUp "2. No output directory"
INPUT1_STAGING_DIR=inputs/5 ./merge-files.sh foo.tsv > test-logs/2.out 2> test-logs/2.err
[ $? -ne 1 ] && fail
echo "SUCCESS"

setUp "3. No input files"
INPUT1_STAGING_DIR=inputs/5 OUTPUT1_STAGING_DIR=outputs/1 ./merge-files.sh foo.tsv > test-logs/3.out 2> test-logs/3.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv ] && fail
[ $(cat outputs/1/foo.tsv | wc -l) -ne 0 ] && fail
echo "SUCCESS"

setUp "4. Single compressed input, single compressed output with header"
INPUT1_STAGING_DIR=inputs/1 OUTPUT1_STAGING_DIR=outputs/1 ./merge-files.sh foo.tsv.gz column1,column2 > test-logs/4.out 2> test-logs/4.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv.gz ] && fail
[ $(gzcat outputs/1/foo.tsv.gz | head | grep column1,column2 | wc -l) -eq 0 ] && fail
echo "SUCCESS"

setUp "5. Single compressed input, single uncompressed output without header"
INPUT1_STAGING_DIR=inputs/1 OUTPUT1_STAGING_DIR=outputs/1 ./merge-files.sh foo.tsv > test-logs/5.out 2> test-logs/5.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv ] && fail
echo "SUCCESS"

setUp "6. Single compressed input, single compressed output with header"
INPUT1_STAGING_DIR=inputs/1 OUTPUT1_STAGING_DIR=outputs/1 ./merge-files.sh foo.tsv.gz > test-logs/6.out 2> test-logs/6.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv.gz ] && fail
echo "SUCCESS"

setUp "7. Single uncompressed input, multiple uncompressed outputs with header"
INPUT1_STAGING_DIR=inputs/3 OUTPUT1_STAGING_DIR=outputs/1 OUTPUT2_STAGING_DIR=outputs/2 ./merge-files.sh foo.tsv column1,column2 > test-logs/7.out 2> test-logs/7.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv ] && fail
[ ! -f outputs/2/foo.tsv ] && fail
[ $(cat outputs/1/foo.tsv | head | grep column1,column2 | wc -l) -eq 0 ] && fail
[ $(cat outputs/2/foo.tsv | head | grep column1,column2 | wc -l) -eq 0 ] && fail
echo "SUCCESS"

setUp "8. Single uncompressed input, multiple compressed outputs with header"
INPUT1_STAGING_DIR=inputs/3 OUTPUT1_STAGING_DIR=outputs/1 OUTPUT2_STAGING_DIR=outputs/2 ./merge-files.sh foo.tsv.gz column1,column2 > test-logs/8.out 2> test-logs/8.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv.gz ] && fail
[ ! -f outputs/2/foo.tsv.gz ] && fail
[ $(gzcat outputs/1/foo.tsv.gz | head | grep column1,column2 | wc -l) -eq 0 ] && fail
[ $(gzcat outputs/2/foo.tsv.gz | head | grep column1,column2 | wc -l) -eq 0 ] && fail
echo "SUCCESS"

setUp "9. Single uncompressed input, multiple uncompressed outputs without header"
INPUT1_STAGING_DIR=inputs/3 OUTPUT1_STAGING_DIR=outputs/1 OUTPUT2_STAGING_DIR=outputs/2 ./merge-files.sh foo.tsv > test-logs/9.out 2> test-logs/9.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv ] && fail
[ ! -f outputs/2/foo.tsv ] && fail
echo "SUCCESS"

setUp "10. Multiple compressed inputs, single compressed output without header"
INPUT1_STAGING_DIR=inputs/1 INPUT2_STAGING_DIR=inputs/2 OUTPUT1_STAGING_DIR=outputs/1 ./merge-files.sh foo.tsv.gz > test-logs/10.out 2> test-logs/10.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv.gz ] && fail
echo "SUCCESS"

setUp "11. Multiple mixed inputs, multiple compressed outputs with header"
INPUT1_STAGING_DIR=inputs/1 INPUT2_STAGING_DIR=inputs/3 OUTPUT1_STAGING_DIR=outputs/1 OUTPUT2_STAGING_DIR=outputs/2 ./merge-files.sh foo.tsv.gz column1,column2 > test-logs/11.out 2> test-logs/11.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv.gz ] && fail
[ ! -f outputs/2/foo.tsv.gz ] && fail
[ $(gzcat outputs/1/foo.tsv.gz | head | grep column1,column2 | wc -l) -eq 0 ] && fail
[ $(gzcat outputs/2/foo.tsv.gz | head | grep column1,column2 | wc -l) -eq 0 ] && fail
echo "SUCCESS"

setUp "12. Multiple uncompressed inputs, single compressed output with header"
INPUT1_STAGING_DIR=inputs/3 INPUT2_STAGING_DIR=inputs/4 OUTPUT1_STAGING_DIR=outputs/1 ./merge-files.sh foo.tsv.gz column1,column2 > test-logs/12.out 2> test-logs/12.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv.gz ] && fail
[ $(gzcat outputs/1/foo.tsv.gz | head | grep column1,column2 | wc -l) -eq 0 ] && fail
echo "SUCCESS"

setUp "13. Multiple compressed inputs, multiple uncompressed outputs without header"
INPUT1_STAGING_DIR=inputs/1 INPUT2_STAGING_DIR=inputs/2 OUTPUT1_STAGING_DIR=outputs/1 OUTPUT2_STAGING_DIR=outputs/2 ./merge-files.sh foo.tsv > test-logs/13.out 2> test-logs/13.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv ] && fail
[ ! -f outputs/2/foo.tsv ] && fail
echo "SUCCESS"

setUp "14. Multiple mixed inputs, single uncompressed output with header"
INPUT1_STAGING_DIR=inputs/1 INPUT2_STAGING_DIR=inputs/3 OUTPUT1_STAGING_DIR=outputs/1 ./merge-files.sh foo.tsv > test-logs/14.out 2> test-logs/14.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv ] && fail
echo "SUCCESS"

setUp "15. Multiple uncompressed inputs, single uncompressed output with header"
INPUT1_STAGING_DIR=inputs/3 INPUT2_STAGING_DIR=inputs/4 OUTPUT1_STAGING_DIR=outputs/1 OUTPUT2_STAGING_DIR=outputs/2 ./merge-files.sh foo.tsv column1,column2 > test-logs/15.out 2> test-logs/15.err
[ $? -gt 1 ] && fail
[ ! -f outputs/1/foo.tsv ] && fail
[ ! -f outputs/2/foo.tsv ] && fail
[ $(cat outputs/1/foo.tsv | head | grep column1,column2 | wc -l) -eq 0 ] && fail
[ $(cat outputs/2/foo.tsv | head | grep column1,column2 | wc -l) -eq 0 ] && fail
echo "SUCCESS"

cleanUp
