#!/bin/bash

add_to_classpath()
{
  DIR=${1}
  for i in ${DIR}/*.jar ; do
    CP=${CP}:${i}
  done
}

DIRNAME=`dirname $0`

# Detect Cygwin
# Cygwin fix courtesy of Supin Ko
cygwin=false
case "`uname`" in
CYGWIN*) cygwin=true;;
esac

# Setup ISPN_HOME
if [ "x$ISPN_HOME" = "x" ]; then
    # get the full path (without any relative bits)
    ISPN_HOME=`cd $DIRNAME/..; pwd`
fi
export ISPN_HOME

CP=${CP}:${ISPN_HOME}/etc

add_to_classpath ${ISPN_HOME}
add_to_classpath ${ISPN_HOME}/lib
add_to_classpath ${ISPN_HOME}/modules/gui
add_to_classpath ${ISPN_HOME}/modules/gui/lib

if $cygwin; then
   # Turn paths into Windows style for Cygwin
   CP=`cygpath -wp ${CP}`
   LOG4J_CONFIG=`cygpath -w ${ISPN_HOME}/etc/log4j.xml`
else
   LOG4J_CONFIG=${ISPN_HOME}/etc/log4j.xml
fi

JVM_PARAMS="${JVM_PARAMS} -Dbind.address=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=file:${LOG4J_CONFIG}"

# Sample JPDA settings for remote socket debuging
JVM_PARAMS="$JVM_PARAMS -Xrunjdwp:transport=dt_socket,address=8686,server=y,suspend=n"

java -cp ${CP} ${JVM_PARAMS} org.infinispan.demo.InfinispanDemo &
