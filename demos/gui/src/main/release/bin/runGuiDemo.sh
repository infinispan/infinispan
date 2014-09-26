#!/bin/bash

source "`dirname "$0"`/functions.sh"

add_classpath ${ISPN_HOME}/*.jar
add_classpath ${ISPN_HOME}/demos/gui/etc
add_classpath ${ISPN_HOME}/demos/gui/*.jar
add_classpath ${ISPN_HOME}/demos/gui/etc/runtime-classpath.txt

add_jvm_args $JVM_PARAMS
add_jvm_args '-Djgroups.bind_addr=127.0.0.1'
add_jvm_args '-Djava.net.preferIPv4Stack=true'

# RHQ monitoring options
add_jvm_args '-Dcom.sun.management.jmxremote.ssl=false'
add_jvm_args '-Dcom.sun.management.jmxremote.authenticate=false'
add_jvm_args -Dcom.sun.management.jmxremote.port=$(find_tcp_port)

# Workaround for JDK6 NPE: http://bugs.sun.com/view_bug.do?bug_id=6427854
add_jvm_args '-Dsun.nio.ch.bugLevel=""'

# Sample JPDA settings for remote socket debugging
#add_jvm_args "-Xrunjdwp:transport=dt_socket,address=8686,server=y,suspend=n"

add_program_args $@

start org.infinispan.demo.InfinispanDemo &
