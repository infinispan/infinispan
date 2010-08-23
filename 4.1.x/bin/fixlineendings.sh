#!/bin/bash


if [[ $0 =~ bin/.*.sh ]] ; then
   DIR_PREFIX="."
else
   DIR_PREFIX=".."
fi

find ${DIR_PREFIX}/ -name \*.java -or -name \*.xml -or -name \*.txt -or -name \*.xsd -or -name \*.sh -or -name \*.py | xargs svn ps --force svn:eol-style LF
find ${DIR_PREFIX}/ -name \*.java -or -name \*.xml -or -name \*.txt -or -name \*.xsd -or -name \*.sh -or -name \*.py | xargs svn ps --force svn:keywords 'Id Revision'
find ${DIR_PREFIX}/ -name \*.sh -or -name \*.py | xargs svn ps svn:executable on
