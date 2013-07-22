#!/bin/sh

DIR=`dirname $0`
echo dir=$DIR

for LOG_FILE in `find . -name "*.log"` ; do
    FAILED_TESTS=`cat $LOG_FILE | egrep "Test .* failed\." | sed -e 's/.*(//' -e 's/).*//' | awk -F. ' { print $NF } ' | sort | uniq`

    if [ -z $FAILED_TESTS ]
    then
      echo "Processing log file: $LOG_FILE. No failed tests."
    else
      echo "Processing log file: $LOG_FILE. Failed tests:  $FAILED_TESTS "
    fi

    for TEST in $FAILED_TESTS ; do
      echo "Processing $TEST from $LOG_FILE "
      echo "" > $TEST.log
      SHORTNAME=`perl -e '$t = $ARGV[0]; chomp $t; $t =~ s/[a-z0-9]//g; print $t;' $TEST`
      cat $LOG_FILE | $DIR/greplog.py "\b$TEST\b" | perl -npe "s/(?![a-zA-Z.])$TEST(?![a-zA-Z.])/$SHORTNAME/g" > $TEST.log
    done
done

#delete the large log files
find . -name "infinispan-*.log" --delete