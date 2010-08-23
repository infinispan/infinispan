@echo off

setlocal enabledelayedexpansion

set LIB=
for %%f in (..\lib\*.jar) do set LIB=!LIB!;%%f
rem echo libs: %LIB%

set CP=%LIB%;..\infinispan-core.jar;%CP%

java -classpath "%CP%" org.infinispan.config.parsing.ConfigFilesConvertor %1 %2 %3 %4 %5 %6
:fileEnd