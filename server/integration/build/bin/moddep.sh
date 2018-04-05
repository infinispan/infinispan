#!/bin/sh
DIRNAME=`dirname "$0"`
BASEDIR=$DIRNAME/..
MODULES=`find $BASEDIR/target/infinispan-server-* -name 'module.xml'`
$DIRNAME/moduleTools.py $@ $BASEDIR/target/infinispan-server-*/domain/configuration/domain.xml $MODULES

# moddep.sh | tr . / | sed 's/$/\/main\/\*\*/' > trim-modules.txt
