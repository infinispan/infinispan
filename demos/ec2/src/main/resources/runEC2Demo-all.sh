#!/bin/bash

source "`dirname "$0"`/functions.sh"

add_classpath "${ISPN_HOME}"/*.jar
add_classpath "${ISPN_HOME}/etc"
add_classpath "${ISPN_HOME}/etc/config-sameples/ec2-demo"
add_classpath "${ISPN_HOME}/lib"
add_classpath "${ISPN_HOME}/modules/ec2"

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

#Load protein file => -p   e.g. -p /opt/influenza-data-files/influenza_aa.dat
#Load nucleotide file => -n  e.g. -n /opt/influenza-data-files/influenza_na.dat
#Load Influenze virus file => -i    e.g. -i /opt/influenza-data-files/influenza.dat

gunzip "${ISPN_HOME}"/etc/Amazon-TestData/*.gz > /dev/null

add_program_args -c "${ISPN_HOME}/etc/config-samples/ec2-demo/infinispan-ec2-config.xml"
add_program_args -p "${ISPN_HOME}/etc/Amazon-TestData/influenza_aa.dat"
add_program_args -n "${ISPN_HOME}/etc/Amazon-TestData/influenza_na.dat"
add_program_args -i "${ISPN_HOME}/etc/Amazon-TestData/influenza.dat"

start org.infinispan.ec2demo.InfinispanFluDemo

