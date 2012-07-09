@echo off

setlocal enabledelayedexpansion
set ISPN_HOME=%~dp0
set ISPN_HOME="%ISPN_HOME%.."

set /p CP=<%ISPN_HOME%\modules\cli-client\runtime-classpath.txt
set CP=%CP::=;%
set CP=%CP:$ISPN_HOME=!ISPN_HOME!%
set CP=%ISPN_HOME%\modules\cli-client\infinispan-cli-client.jar;%CP%
rem echo libs: %CP%

java -classpath "%CP%" org.infinispan.cli.Main %*
