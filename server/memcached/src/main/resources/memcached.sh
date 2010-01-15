#!/bin/bash

DIRNAME=`dirname $0`

# Setup ISPN_HOME
if [ "x$ISPN_HOME" = "x" ]; then
    # get the full path (without any relative bits)
    ISPN_HOME=`cd $DIRNAME/..; pwd`
fi
export ISPN_HOME

CP=${CP}:${ISPN_HOME}/infinispan-core.jar

if [ -e ${ISPN_HOME}/lib ]
then
   for JAR in ${ISPN_HOME}/lib/*
   do
      CP=$CP:$JAR
   done
fi

CP=${CP}:${ISPN_HOME}/modules/memcached/infinispan-server-memcached.jar

if [ -e ${ISPN_HOME}/modules/memcached/lib ]
then
   for JAR in ${ISPN_HOME}/modules/memcached/lib/*
   do
      CP=$CP:$JAR
   done
fi

JVM_PARAMS="${JVM_PARAMS} -Dlog4j.configuration=file:${ISPN_HOME}/etc/log4j.xml"

# Sample JPDA settings for remote socket debuging
#JVM_PARAMS="$JVM_PARAMS -Xrunjdwp:transport=dt_socket,address=8686,server=y,suspend=n"

java -cp $CP ${JVM_PARAMS} org.infinispan.server.memcached.Main ${*}