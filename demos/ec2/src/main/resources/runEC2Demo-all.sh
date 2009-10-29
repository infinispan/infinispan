#!/bin/bash

DIRNAME=`dirname $0`

# Setup ISPN_HOME
if [ "x$ISPN_HOME" = "x" ]; then
    # get the full path (without any relative bits)
    ISPN_HOME=`cd $DIRNAME/..; pwd`
fi
export ISPN_HOME

CP=${CP}:${ISPN_HOME}/etc

for i in `find ${ISPN_HOME}/modules -name "*.jar"` ; do
  CP=${CP}:${i}
done

JVM_PARAMS="${JVM_PARAMS} -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=file:${ISPN_HOME}/etc/log4j.xml"
JVM_PARAMS="${JVM_PARAMS} -DEC2Demo-jgroups-config=${ISPN_HOME}/etc/jgroups-s3_ping-aws.xml"

# Sample JPDA settings for remote socket debuging
#JVM_PARAMS="$JVM_PARAMS -Xrunjdwp:transport=dt_socket,address=8686,server=y,suspend=n"

#Load protein file => -p   e.g. -p /opt/influenza-data-files/influenza_aa.dat
#Load nucleotide file => -n  e.g. -n /opt/influenza-data-files/influenza_na.dat
#Load Influenze virus file => -i    e.g. -i /opt/influenza-data-files/influenza.dat

gunzip ${ISPN_HOME}/etc/Amazon-TestData/*.gz > /dev/null

DEMO_ARGS="-p ${ISPN_HOME}/etc/Amazon-TestData/influenza_aa.dat"
DEMO_ARGS="${DEMO_ARGS} -n ${ISPN_HOME}/etc/Amazon-TestData/influenza_na.dat"
DEMO_ARGS="${DEMO_ARGS} -i ${ISPN_HOME}/etc/Amazon-TestData/influenza.dat"
java -cp ${CP} ${JVM_PARAMS} org.infinispan.ec2demo.InfinispanFluDemo ${DEMO_ARGS}
