#!/bin/sh

MAIN_CLASS=org.jgroups.stack.GossipRouter
ARGUMENTS=
PROCESS_NAME=${infinispan.brand.short-name}-gossip-router

PROGNAME=$(basename "$0")
DIRNAME=$(dirname "$0")

. "$DIRNAME/common.sh"

while true; do
   # Execute the JVM in the background
   eval \"$JAVA\" $JAVA_OPTS \
      -Dvisualvm.display.name="$PROCESS_NAME" \
      -Djava.util.logging.manager=org.infinispan.server.loader.LogManager \
      -Dlog4j.configurationFile="$DIRNAME/gossiprouter.log4j2.xml" \
      -Dinfinispan.server.home.path=\""$ISPN_HOME"\" \
      -classpath "$ISPN_HOME"/boot/*:"$ISPN_HOME"/lib/*:"$ISPN_ROOT_DIR"/lib/* \
      "$MAIN_CLASS" "$ARGUMENTS" "&"
   GR_PID=$!
   # Trap common signals and relay them to the server process
   trap "kill -HUP  $GR_PID" HUP
   trap "kill -TERM $GR_PID" INT
   trap "kill -QUIT $GR_PID" QUIT
   trap "kill -PIPE $GR_PID" PIPE
   trap "kill -TERM $GR_PID" TERM
   # Wait until the background process exits
   WAIT_STATUS=128
   while [ "$WAIT_STATUS" -ge 128 ]; do
      wait $GR_PID 2>/dev/null
      WAIT_STATUS=$?
      if [ "$WAIT_STATUS" -gt 128 ]; then
         SIGNAL=$((WAIT_STATUS - 128))
         SIGNAL_NAME=$(kill -l $SIGNAL)
         echo "*** Gossip Router process ($GR_PID) received $SIGNAL_NAME signal ***" >&2
      fi
   done
   if [ "$WAIT_STATUS" -lt 127 ]; then
      ISPN_STATUS=$WAIT_STATUS
   else
      ISPN_STATUS=0
   fi
   if [ "$ISPN_STATUS" -ne 10 ]; then
         # Wait for a complete shutdown
         wait $GR_PID 2>/dev/null
   fi
   if [ "$ISPN_STATUS" -eq 10 ]; then
      echo "Restarting Gossip Router..."
   else
      exit $ISPN_STATUS
   fi
done
