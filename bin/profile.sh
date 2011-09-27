#!/bin/bash

########################
# Helper script to start a profile test on a remote node
########################

JVM_OPTS="$JVM_OPTS -Xms512M -Xmx512M -Djgroups.bind_addr=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dprotocol.stack=udp"

PROFILED=false
if [ $1 = "-p" ] ; then
  PROFILED=true
  SESSION_ID=$2
  shift
  shift
fi

## Set up a classpath.
if [ -z $ISPN_HOME ] ; then
   dn=`dirname $0`
   ISPN_HOME="$dn/.."
fi

cd $ISPN_HOME

CP="target/classes:target/test-classes:core/target/classes:core/target/test-classes:core/src/test/resources"

if [ "x$SKIP_MAKE" = "x" ] ; then
  rm -rf .tmp_profile_script
  mkdir .tmp_profile_script

  if ! [ -d target/distribution ] ; then
     mvn clean install -Dmaven.test.skip.exec=true -Pdistribution
  fi

  unzip -q target/distribution/*-bin.zip -d .tmp_profile_script
fi

for i in `find .tmp_profile_script/*/modules/core/lib -name "*.jar"` ; do
  CP=$CP:$i
done

if [ $PROFILED = "true" ] ; then
  JVM_OPTS="$JVM_OPTS 
-agentlib:jprofilerti=offline,id=${SESSION_ID},config=/opt/jprofiler_cfg/config.xml  -Xbootclasspath/a:/opt/jprofiler/bin/agent.jar"
fi

export LD_LIBRARY_PATH=/opt/jprofiler/bin/linux-x86:$LD_LIBRARY_PATH

java ${JVM_OPTS} -cp $CP ${*}

