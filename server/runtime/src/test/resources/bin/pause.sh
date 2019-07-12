#!/usr/bin/env bash
jps -l|grep infinispan-server-runtime|cut -f1 -d' '|xargs kill -SIGSTOP
