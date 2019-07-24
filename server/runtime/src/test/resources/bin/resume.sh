#!/usr/bin/env bash
jps -lm | grep "org.infinispan.server.loader.Loader org.infinispan.server.Bootstrap" | cut -f1 -d' ' | xargs kill -SIGCONT
