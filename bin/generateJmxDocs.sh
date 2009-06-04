#!/bin/bash


if [[ $0 =~ bin/.*.sh ]] ; then
   DIR_PREFIX=".."
else
   DIR_PREFIX="."
fi

cd ${DIR_PREFIX}
mvn install -Pjmxdoc -Dmaven.test.skip.exec=true

