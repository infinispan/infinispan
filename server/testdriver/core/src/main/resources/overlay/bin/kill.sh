#!/usr/bin/env bash
PID=`pgrep -f "org.infinispan.server.loader.Loader org.infinispan.server.Bootstrap"`
kill -SIGKILL $PID
echo "Killed server with PID=$PID"
