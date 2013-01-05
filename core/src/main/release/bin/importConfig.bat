@echo off

setlocal enabledelayedexpansion
set ISPN_HOME=%~dp0
set ISPN_HOME="%ISPN_HOME%.."

set /p CP=<%ISPN_HOME%\runtime-classpath.txt
set CP=%CP::=;%
set CP=%CP:$ISPN_HOME=!ISPN_HOME!%
set CP=%ISPN_HOME%\infinispan-core.jar;%CP%
rem echo libs: %CP%

java -classpath "%CP%" org.infinispan.config.parsing.ConfigFilesConvertor %1 %2 %3 %4 %5 %6
