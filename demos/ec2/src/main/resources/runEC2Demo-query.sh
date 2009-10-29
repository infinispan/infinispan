#!/bin/bash

DIRNAME=`dirname $0`

# Setup ISPN_HOME
if [ "x$ISPN_HOME" = "x" ]; then
    # get the full path (without any relative bits)
    ISPN_HOME=`cd $DIRNAME/..; pwd`
fi
export ISPN_HOME

CP=${CP}:${ISPN_HOME}/etc

#add the modules/ec2 dir
for i in ${ISPN_HOME}/modules/ec2/*.jar ; do
   CP=${i}:${CP}
done

#add the modules/ec2/libs
for i in ${ISPN_HOME}/modules/ec2/lib/*.jar ; do
   CP=${i}:${CP}
done

for i in ${ISPN_HOME}/modules/core/*.jar ; do
   CP=${i}:${CP}
done

for i in ${ISPN_HOME}/modules/core/lib/*.jar ; do
   CP=${i}:${CP}
done

JVM_PARAMS="${JVM_PARAMS} -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=file:${ISPN_HOME}/etc/log4j.xml"
JVM_PARAMS="${JVM_PARAMS} -DEC2Demo-jgroups-config=${ISPN_HOME}/etc/jgroups-s3_ping-aws.xml"

# Sample JPDA settings for remote socket debuging
#JVM_PARAMS="$JVM_PARAMS -Xrunjdwp:transport=dt_socket,address=8686,server=y,suspend=n"

#Load protein file => -p   e.g. -p /opt/influenza-data-files/influenza_aa.dat
#Load nucleotide file => -n  e.g. -n /opt/influenza-data-files/influenza_na.dat
#Load Influenze virus file => -i    e.g. -i /opt/influenza-data-files/influenza.dat
DEMO_ARGS="-q "
java -cp ${CP} ${JVM_PARAMS} org.infinispan.ec2demo.InfinispanFluDemo ${DEMO_ARGS} 
