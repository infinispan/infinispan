#!/usr/bin/env bash
PID=`pgrep -f "org.infinispan.server.loader.Loader org.infinispan.server.Bootstrap"`
kill -SIGSTOP $PID
echo "Paused server with PID=$PID"
