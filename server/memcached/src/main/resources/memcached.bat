@echo off

setlocal enabledelayedexpansion

set LIB=
for %%f in (..\lib\*.jar) do set LIB=!LIB!;%%f
for %%f in (..\modules\memcached\lib\*.jar) do set LIB=!LIB!;%%f
rem echo libs: %LIB%

set CP=%LIB%;..\infinispan-core.jar;..\modules\memcached\infinispan-server-memcached.jar;%CP%

java -classpath "%CP%" org.infinispan.server.memcached.Main %1 %2 %3 %4 %5 %6 %7
:fileEnd