#!/bin/bash

## TODO: A Windows .cmd version of this script!

CP=./etc

for i in modules/core/*.jar ; do
   CP=${i}:${CP}
done

for i in modules/core/lib/*.jar ; do
   CP=${i}:${CP}
done

for i in modules/gui-demo/*.jar ; do
   CP=${i}:${CP}
done

for i in modules/gui-demo/lib/*.jar ; do
   CP=${i}:${CP}
done

JVM_PARAMS="-Dbind.address=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=./etc/log4j.xml"

java -cp ${CP} ${JVM_PARAMS} org.infinispan.demo.InfinispanDemo &

