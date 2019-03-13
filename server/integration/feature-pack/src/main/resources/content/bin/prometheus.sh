#!/bin/sh

if [ -z "$JMX_CONF" ]
then
   if [ -z "$1" ]
   then
       echo "Neither JMX_CONF nor input parameters are defined. Exiting"
       exit 1
   else
       JMX_CONF=$1
   fi
fi

PROMETHEUS_JAR=$(ls $JBOSS_HOME/modules/system/add-ons/@infinispan.module.slot.prefix@/io/prometheus/jmx/@infinispan.module.slot@/*.jar | sed -e "s| |:|g")
JBOSS_LOG_MANAGER_LIB=$(ls $JBOSS_HOME/modules/system/layers/base/org/jboss/logmanager/main/*.jar | sed -e "s| |:|g")
WILDFLY_COMMON_LIB=$(ls $JBOSS_HOME/modules/system/layers/base/org/wildfly/common/main/*.jar | sed -e "s| |:|g")
LOGGING="-Dsun.util.logging.disableCallerCheck=true -Djava.util.logging.manager=org.jboss.logmanager.LogManager -Djboss.modules.system.pkgs=org.jboss.byteman,org.jboss.logmanager -Xbootclasspath/a:$JBOSS_LOG_MANAGER_LIB:$WILDFLY_COMMON_LIB"
export JAVA_OPTS="$JAVA_OPTS $LOGGING -javaagent:$PROMETHEUS_JAR=$JMX_CONF"
