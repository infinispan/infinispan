#!/bin/bash
if [ -e ../modules/core/lib ]
then
   for JAR in ../modules/core/lib/*
   do
      CLASSPATH=$CLASSPATH:$JAR
   done
fi
CLASSPATH=../modules/core/infinispan-core.jar$CLASSPATH
java -classpath $CLASSPATH org.infinispan.config.parsing.ConfigFilesConvertor ${*}