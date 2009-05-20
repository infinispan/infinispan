#!/bin/bash
if [ -z $1 ]
then
   echo Usage:
   echo      $0 [source_file] [destination_file] [source_file_type]
   echo        supported source types are: "JBossCache3x"
   exit 1;
fi
if [ -e ../modules/core/lib ]
then
   for JAR in ../modules/core/lib/*
   do
      CLASSPATH=$CLASSPATH:$JAR
   done
fi
CLASSPATH=../modules/core/infinispan-core.jar$CLASSPATH
echo classpath is $CLASSPATH
java -classpath $CLASSPATH -Dsource=$1 -Ddestination=$2 -Dtype=$3 org.infinispan.config.parsing.ConfigFilesConvertor