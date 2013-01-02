#!/bin/bash

source "`dirname "$0"`/functions.sh"

add_classpath ${ISPN_HOME}/*.jar
add_classpath ${ISPN_HOME}/runtime-classpath.txt

add_program_args "$@"

start org.infinispan.config.parsing.ConfigFilesConvertor
