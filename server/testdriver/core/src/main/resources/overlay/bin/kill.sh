#!/usr/bin/env bash
PID=`pgrep "server.sh"`
kill -SIGKILL $PID
echo "Killed server with PID=$PID"
