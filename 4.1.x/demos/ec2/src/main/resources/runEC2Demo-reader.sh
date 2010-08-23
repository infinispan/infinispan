#!/bin/bash

DIRNAME=`dirname $0`

# Setup ISPN_HOME
if [ "x$ISPN_HOME" = "x" ]; then
    # get the full path (without any relative bits)
    ISPN_HOME=`cd $DIRNAME/..; pwd`
fi
export ISPN_HOME

CP=${CP}:${ISPN_HOME}/etc:${ISPN_HOME}/etc/config-samples/ec2-demo

for i in `find ${ISPN_HOME}/modules -name "*.jar"` ; do
  CP=${CP}:${i}
done

for i in `find ${ISPN_HOME}/lib -name "*.jar"` ; do
  CP=${CP}:${i}
done

CP=${ISPN_HOME}/infinispan-core.jar:$CP

JVM_PARAMS="-Xmx512m ${JVM_PARAMS} -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=file:${ISPN_HOME}/etc/log4j.xml"
JVM_PARAMS="${JVM_PARAMS} -DCFGPath=${ISPN_HOME}/etc/config-samples/ec2-demo/"

DEMO_ARGS=" -c ${ISPN_HOME}/etc/config-samples/ec2-demo/infinispan-ec2-config.xml"
DEMO_ARGS="${DEMO_ARGS} -r "
DEMO_ARGS="${DEMO_ARGS} -i ${ISPN_HOME}/etc/Amazon-TestData/influenza.dat"

java -cp ${CP} ${JVM_PARAMS} org.infinispan.ec2demo.InfinispanFluDemo ${DEMO_ARGS}
