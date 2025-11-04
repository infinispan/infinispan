#!/bin/sh
ARGUMENTS=
PROCESS_NAME=${infinispan.brand.short-name}-cli


PROGNAME=$(basename "$0")
DIRNAME=$(dirname "$0")

. "$DIRNAME/common.sh"

# Execute the JVM
   eval "$JAVA" $JAVA_OPTS \
      -Dvisualvm.display.name="$PROCESS_NAME" \
      -Dinfinispan.server.home.path=\""$ISPN_HOME"\" \
      -jar "$ISPN_HOME"/lib/infinispan-cli-client-*.jar \
      "$ARGUMENTS"
