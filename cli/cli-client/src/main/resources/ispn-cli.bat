@echo off

setlocal enabledelayedexpansion

set LIB=
for %%f in (..\modules\cli-client\lib\*.jar) do set LIB=!LIB!;%%f
rem echo libs: %LIB%

set CP=%LIB%
set CP=..\modules\cli-client\infinispan-cli-client.jar;%CP%

java -classpath "%CP%" org.infinispan.cli.Main %*

:fileEnd