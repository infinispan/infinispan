#!/bin/sh

MAIN_CLASS=org.jgroups.tests.Probe
ARGUMENTS=
PROCESS_NAME=${infinispan.brand.short-name}-probe

PROGNAME=$(basename "$0")
DIRNAME=$(dirname "$0")

. "$DIRNAME/common.sh"

eval "$JAVA" $JAVA_OPTS \
  -Dvisualvm.display.name="$PROCESS_NAME" \
  -Djava.util.logging.manager=org.infinispan.server.loader.LogManager \
  -Dinfinispan.server.home.path=\""$ISPN_HOME"\" \
  -classpath "$ISPN_HOME"/boot/*:"$ISPN_HOME"/lib/*:"$ISPN_ROOT_DIR"/lib/* \
  "$MAIN_CLASS" "$ARGUMENTS"
