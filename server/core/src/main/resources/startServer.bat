@echo off

setlocal enabledelayedexpansion

set LIB=
for %%f in (..\lib\*.jar) do set LIB=!LIB!;%%f
for %%f in (..\modules\memcached\lib\*.jar) do set LIB=!LIB!;%%f
for %%f in (..\modules\hotrod\lib\*.jar) do set LIB=!LIB!;%%f
for %%f in (..\modules\websocket\lib\*.jar) do set LIB=!LIB!;%%f
rem echo libs: %LIB%

set CP=%LIB%;..\infinispan-core.jar;..\modules\core\infinispan-server-memcached.jar;%CP%
set CP=..\modules\memcached\infinispan-server-memcached.jar;%CP%
set CP=..\modules\hotrod\infinispan-server-hotrod.jar;%CP%
set CP=..\modules\websocket\infinispan-server-websocket.jar;%CP%

:test
set /a "TESTPORT=%RANDOM%+2000"
netstat -an | findstr ":%TESTPORT% "
if %ERRORLEVEL%==0 goto test

java -classpath "%CP%" -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=%TESTPORT% -Djgroups.bind_addr=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=..\etc\log4j.xml org.infinispan.server.core.Main %*

:fileEnd