#!/bin/sh

LOADER_CLASS=org.infinispan.server.loader.Loader
MAIN_CLASS=org.infinispan.server.security.UserTool
ARGUMENTS=
PROCESS_NAME=${infinispan.brand.short-name}-user-tool

PROGNAME=`basename "$0"`
DIRNAME=`dirname "$0"`

. "$DIRNAME/common.sh"

eval \"$JAVA\" $JAVA_OPTS \
   -Dvisualvm.display.name=$PROCESS_NAME \
   -Djava.util.logging.manager=org.jboss.logmanager.LogManager \
   -Dinfinispan.server.home.path=\""$ISPN_HOME"\" \
   -classpath \""$CLASSPATH"\" "$LOADER_CLASS" "$MAIN_CLASS" "$ARGUMENTS"
