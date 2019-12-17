#!/usr/bin/env bash
jps -lm | grep "org.infinispan.server.loader.InfinispanServerLoader org.infinispan.server.Bootstrap" | cut -f1 -d' ' | xargs kill -SIGSTOP
