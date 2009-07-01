#!/bin/bash

DIRNAME=`dirname $0`

# Setup ISPN_HOME
if [ "x$ISPN_HOME" = "x" ]; then
    # get the full path (without any relative bits)
    ISPN_HOME=`cd $DIRNAME/..; pwd`
fi
export ISPN_HOME

CP=${CP}:${ISPN_HOME}/etc

for i in ${ISPN_HOME}/modules/core/*.jar ; do
   CP=${i}:${CP}
done

for i in ${ISPN_HOME}/modules/core/lib/*.jar ; do
   CP=${i}:${CP}
done

for i in ${ISPN_HOME}/modules/gui-demo/*.jar ; do
   CP=${i}:${CP}
done

for i in ${ISPN_HOME}/modules/gui-demo/lib/*.jar ; do
   CP=${i}:${CP}
done

JVM_PARAMS="${JVM_PARAMS} -Dbind.address=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=file:${ISPN_HOME}/etc/log4j.xml"

java -cp ${CP} ${JVM_PARAMS} org.infinispan.demo.InfinispanDemo &
