#!/bin/sh

DIR=`dirname $0`
#echo dir=$DIR

for LOG_FILE in `find . -name "*.log*"` ; do
    CAT=cat
    if [ "${FILE##*.}" == "gz" ] ; then
      CAT=zcat
    fi
    FAILED_TESTS=`$CAT $LOG_FILE | perl -ne '/Test .*\(.*\.(.*)\) (failed|skipped)\./ && print "$1\n";' | sort -u`

    if [ -z "$FAILED_TESTS" ]
    then
      echo "Processing log file: $LOG_FILE. No failed tests."
    else
      echo "Processing log file: $LOG_FILE. Failed tests: $FAILED_TESTS"
    fi

    for TEST in $FAILED_TESTS ; do
      echo "Processing $TEST from $LOG_FILE "
      TESTFILE=$(echo ${TEST}.log | tr / _)
      TESTFILE=${TESTFILE//[^a-zA-Z0-9-_.]/}
      $CAT $LOG_FILE | python $DIR/greplog.py "\b$TEST\b" | perl -npe "s/$TEST-Node/Node/g" > $TESTFILE
    done

    rm $LOG_FILE
done