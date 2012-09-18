#!/bin/bash


########################
# Script that allows the run of the xsite test suite or of an xsite individual test.
########################


if [ "x$1" = "x" ] ; then
   for testfile in `grep -lr 'groups[ ]*=[ ]*"xsite"' .`
   do
     filename=$(basename "$testfile")
     filename="${filename%.*}"
     mvn test -Ptest-xsite -Dinfinispan.test.parallel.threads=1 -Dtest=$filename | grep 'Test '
   done
else
     mvn test -Ptest-xsite -Dinfinispan.test.parallel.threads=1 -Dtest=$1
fi


