#!/bin/bash

curdir=`pwd`
echo $curdir | grep "/bin$"


if [ "$?" -ne "0" ] ; then
  basedir="./"
else
  basedir="../"
fi

CP=${basedir}etc

for i in ${basedir}modules/core/*.jar ; do
   CP=${i}:${CP}
done

for i in ${basedir}modules/core/lib/*.jar ; do
   CP=${i}:${CP}
done

for i in ${basedir}modules/gui-demo/*.jar ; do
   CP=${i}:${CP}
done

for i in ${basedir}modules/gui-demo/lib/*.jar ; do
   CP=${i}:${CP}
done

JVM_PARAMS="-Dbind.address=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=${basedir}etc/log4j.xml"

java -cp ${CP} ${JVM_PARAMS} org.infinispan.demo.InfinispanDemo &

