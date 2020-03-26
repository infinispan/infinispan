#!/usr/bin/env bash
PID=`pgrep "server.sh"`
kill -SIGTERM $PID
echo "TERM sent to server with PID=$PID"
