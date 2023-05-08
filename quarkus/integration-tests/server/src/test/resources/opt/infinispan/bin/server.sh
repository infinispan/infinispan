#!/bin/sh

PROGNAME=$(basename "$0")
DIRNAME=$(dirname "$0")

while true; do
   # Execute the process in the background in order for signals to be correctly handled
  "$DIRNAME"/server-runner $* &

   ISPN_PID=$!
   # Trap common signals and relay them to the server process
   trap "kill -HUP  $ISPN_PID" HUP
   trap "kill -TERM $ISPN_PID" INT
   trap "kill -QUIT $ISPN_PID" QUIT
   trap "kill -PIPE $ISPN_PID" PIPE
   trap "kill -TERM $ISPN_PID" TERM
   if [ "x$ISPN_PIDFILE" != "x" ]; then
      echo $ISPN_PID > "$ISPN_PIDFILE"
   fi
   # Wait until the background process exits
   WAIT_STATUS=128
   while [ "$WAIT_STATUS" -ge 128 ]; do
      wait $ISPN_PID 2>/dev/null
      WAIT_STATUS=$?
      if [ "$WAIT_STATUS" -gt 128 ]; then
         SIGNAL=$(expr $WAIT_STATUS - 128)
         SIGNAL_NAME=$(kill -l "$SIGNAL")
         echo "*** Server process ($ISPN_PID) received $SIGNAL_NAME signal ***" >&2
      fi
   done
   if [ "$WAIT_STATUS" -lt 127 ]; then
      ISPN_STATUS=$WAIT_STATUS
   else
      ISPN_STATUS=0
   fi
   if [ "$ISPN_STATUS" -ne 10 ]; then
         # Wait for a complete shudown
         wait $ISPN_PID 2>/dev/null
   fi
   if [ "x$ISPN_PIDFILE" != "x" ]; then
         grep "$ISPN_PID" "$ISPN_PIDFILE" && rm "$ISPN_PIDFILE"
   fi
   if [ "$ISPN_STATUS" -eq 10 ]; then
      echo "Restarting server..."
   else
      exit $ISPN_STATUS
   fi
done
