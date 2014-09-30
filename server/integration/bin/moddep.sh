#!/bin/sh
DIRNAME=`dirname "$0"`
BASEDIR=$DIRNAME/..
MODULES=`find $BASEDIR/build/target/infinispan-server-* -name 'module.xml'`
$DIRNAME/moduleTools.py $@ $BASEDIR/build/target/generated-configs/standalone/configuration/clustered.xml $MODULES

# moddep.sh | tr . / | sed 's/$/\/main\/\*\*/' > trim-modules.txt
