#!/bin/bash
find src -name \*.java -or -name \*.xml -or -name \*.txt -or -name \*.xsd -or -name \*.sh | xargs svn ps --force svn:eol-style LF
find src -name \*.java -or -name \*.xml -or -name \*.txt -or -name \*.xsd -or -name \*.sh | xargs svn ps --force svn:keywords 'Id Revision'
find src -name \*.sh | xargs svn ps svn:executable on
svn ps --force svn:eol-style LF *.txt *.sh
svn ps --force svn:keywords 'Id Revision' *.txt *.sh
svn ps svn:executable on *.sh
