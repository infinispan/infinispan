#!/bin/sh
DIRNAME=`dirname "$0"`
BASEDIR=$DIRNAME/..
MODULES=`find $BASEDIR/server/target -name 'module.xml'`
$DIRNAME/moduleTools.py $@ $BASEDIR/server/target/generated-configs/standalone/configuration/standalone-ha.xml $MODULES

# moddep.sh | tr . / | sed 's/$/\/main\/\*\*/' > trim-modules.txt
