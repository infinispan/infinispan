#!/bin/bash

DIRNAME=`dirname $0`

# Setup ISPN_HOME
if [ "x$ISPN_HOME" = "x" ]; then
    # get the full path (without any relative bits)
    ISPN_HOME=`cd $DIRNAME/..; pwd`
fi
export ISPN_HOME

if [ -e ${ISPN_HOME}/modules/core/lib ]
then
   for JAR in ${ISPN_HOME}/modules/core/lib/*
   do
      CLASSPATH=$CLASSPATH:$JAR
   done
fi
CLASSPATH=${ISPN_HOME}/modules/core/infinispan-core.jar:$CLASSPATH
java -classpath $CLASSPATH org.infinispan.config.parsing.ConfigFilesConvertor ${*}
