#!/bin/sh

AGENT_BOND_JAR=$(ls $JBOSS_HOME/modules/system/layers/base/io/fabric8/agent-bond/main/*.jar | sed -e "s| |:|g")
JAVA_OPTS="$JAVA_OPTS -javaagent:$AGENT_BOND_JAR=$1"
