#!/bin/sh

DIR=`dirname $0`
#echo dir=$DIR

for LOG_FILE in `find . -name "*.log*"` ; do
    $DIR/failedTestLogs.sh $LOG_FILE

    rm $LOG_FILE
done
