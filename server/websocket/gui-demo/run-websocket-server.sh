#!/bin/bash

echo "Downloading dependencies..."
mvn -f ../pom.xml dependency:copy-dependencies > /dev/null

for i in ../target/dependency/*.jar ; do
   CP=${i}:${CP}
done

JVM_PARAMS="${JVM_PARAMS} -Dorg.infinispan.ws.cache.config-file=${ISPN_HOME}/etc/config-samples/gui-demo-cache-config.xml -Dbind.address=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=file:${ISPN_HOME}/etc/log4j.xml"

# Sample JPDA settings for remote socket debuging
#JVM_PARAMS="$JVM_PARAMS -Xrunjdwp:transport=dt_socket,address=8686,server=y,suspend=y"

CP=../target/infinispan-server-websocket.jar:${CP}

COMMAND=" -cp ${CP} ${JVM_PARAMS} org.infinispan.websocket.WebSocketServer"
# echo ${COMMAND}

echo "Starting Infinispan Websocket Server..."
java ${COMMAND}
