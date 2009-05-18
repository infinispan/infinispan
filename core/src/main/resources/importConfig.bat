@echo off

if "%1a" == "a" goto noParams
if "%2a" == "a" goto noParams
if "%3a" == "a" goto noParams

setlocal enabledelayedexpansion

set LIB=
for %%f in (..\modules\core\lib\*.jar) do set LIB=!LIB!;%%f
rem echo libs: %LIB%

set CP=%LIB%;..\modules\core\infinispan-core.jar;%CP%
rem echo cp  is %CP%

java -classpath "%CP%" -Dsource=%1 -Ddestination=%2 -Dtype=%3 org.infinispan.config.parsing.ConfigFilesConvertor

goto fileEnd

:noParams
echo usage: "%0 <file_to_transform> <destination_file> <source_file_type>"
echo        supported source types are: "JBossCache3x"

:fileEnd