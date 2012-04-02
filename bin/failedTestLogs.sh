#!/bin/sh

DIR=`dirname $0`
echo dir=$DIR
if [ -n $1 ] ; then FAILED_TESTS=$1 ; fi

FILE=infinispan.log
ZFILE=infinispan.log.gz
CAT=cat
if [ \( ! -f $FILE \) -o \( $FILE -ot $ZFILE \) ] ; then FILE=$ZFILE ; CAT=zcat ; fi

if [ ! -f $FILE ] ; then
  echo "Could not open infinispan.log or infinispan.log.gz"
fi

echo $CAT $FILE
export CAT FILE

if [ -z $FAILED_TESTS ] ; then
  FAILED_TESTS=`$CAT $FILE | egrep "(Test failed)|(skipped\$)" | perl -ne '/.*\(.*\.(.*)\)$/; print "$1\n";' | sort | uniq`
fi

for TEST in $FAILED_TESTS ; do
  SHORTNAME=`perl -e '$t = $ARGV[0]; chomp $t; $t =~ s/[a-z0-9]//g; print $t;' $TEST`
  LOWSHORTNAME=`perl -e '$t = $ARGV[0]; chomp $t; $t =~ s/[a-z0-9]//g; print lc $t;' $TEST`
  echo "$TEST > $LOWSHORTNAME.log"
  $CAT $FILE | $DIR/greplog.py "\b$TEST\b" | perl -npe "s/(?![a-zA-Z.])$TEST(?![a-zA-Z.])/$SHORTNAME/g" > $LOWSHORTNAME.log
done
