@echo off

setlocal enabledelayedexpansion
set ISPN_HOME=%~dp0
set ISPN_HOME="%ISPN_HOME%.."

set CP=%ISPN_HOME%\modules\cli\infinispan-cli.jar;%CP%
rem echo libs: %CP%

java -classpath "%CP%" org.infinispan.cli.Main %*
