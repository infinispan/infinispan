#!/usr/bin/env bash
PID=`pgrep -f "org.infinispan.server.loader.Loader org.infinispan.server.Bootstrap"`
kill -SIGCONT $PID
echo "Resumed server with PID=$PID"
