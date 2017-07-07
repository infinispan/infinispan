#!/bin/sh

function getfiles {
  FILES=$(ls $1*.jar)
  echo ${FILES} | sed -e "s| |:|g"
}

AGENT_BOND_JAR=$(getfiles $JBOSS_HOME/modules/system/layers/base/io/fabric8/agent-bond/main/)
JAVA_OPTS="$JAVA_OPTS -javaagent:$AGENT_BOND_JAR=$1"
