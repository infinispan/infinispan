#!/bin/bash

########################
# Helper script to start a profile test on a remote node
########################

JVM_OPTS="$JVM_OPTS -Xms512M -Xmx512M -Dbind.address=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dprotocol.stack=udp"

## Set up a classpath.
if [ -z $ISPN_HOME ] ; then
   dn=`dirname $0`
   ISPN_HOME="$dn/.."
fi

cd $ISPN_HOME

CP="target/classes:target/test-classes:core/target/classes:core/target/test-classes:core/src/test/resources"
rm -rf .tmp_profile_script
mkdir .tmp_profile_script

if ! [ -d target/distribution ] ; then
   mvn clean install -Dmaven.test.skip.exec=true -Pdistribution
fi

unzip -q target/distribution/*-bin.zip -d .tmp_profile_script
for i in `find .tmp_profile_script/*/modules/core/lib -name "*.jar"` ; do
  CP=$CP:$i
done

java ${JVM_OPTS} -cp $CP ${*}

