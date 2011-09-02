#!/bin/bash

source "`dirname "$0"`/functions.sh"

add_classpath "${ISPN_HOME}"/*.jar
add_classpath "${ISPN_HOME}/etc"
add_classpath "${ISPN_HOME}/etc/config-samples/ec2-demo"
add_classpath "${ISPN_HOME}/lib"
add_classpath "${ISPN_HOME}/modules"

add_jvm_args '-Xmx512m'
add_jvm_args $JVM_PARAMS
add_jvm_args '-Djava.net.preferIPv4Stack=true'
add_jvm_args "-DCFGPath=${ISPN_HOME}/etc/config-samples/ec2-demo/"

# RHQ monitoring options
#add_jvm_args '-Dcom.sun.management.jmxremote.ssl=false'
#add_jvm_args '-Dcom.sun.management.jmxremote.authenticate=false'
#add_jvm_args '-Dcom.sun.management.jmxremote.port=6996'

# Sample JPDA settings for remote socket debugging
#add_jvm_args "-Xrunjdwp:transport=dt_socket,address=8686,server=y,suspend=n"

add_program_args -c "${ISPN_HOME}/etc/config-samples/ec2-demo/infinispan-ec2-config.xml"
add_program_args -q

start org.infinispan.ec2demo.InfinispanFluDemo

