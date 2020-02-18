#!/usr/bin/env bash
PID=`jps -lm | grep "org.infinispan.server.loader.Loader org.infinispan.server.Bootstrap" | cut -f1 -d' '`
kill -SIGSTOP $PID
echo "Paused server with PID=$PID"
