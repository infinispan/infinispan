#!/bin/bash

source "`dirname "$0"`/functions.sh"

add_classpath ${ISPN_HOME}/modules/cli-client/*.jar
add_classpath ${ISPN_HOME}/modules/cli-client/runtime-classpath.txt

add_jvm_args $JVM_PARAMS

# Sample JPDA settings for remote socket debugging
#add_jvm_args "-Xrunjdwp:transport=dt_socket,address=8686,server=y,suspend=n"

add_program_args "$@"

start org.infinispan.cli.Main

