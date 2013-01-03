@echo off

setlocal enabledelayedexpansion
set ISPN_HOME=%~dp0
set ISPN_HOME="%ISPN_HOME%.."

set /p CP1=<%ISPN_HOME%\modules\memcached\runtime-classpath.txt
set /p CP2=<%ISPN_HOME%\modules\hotrod\runtime-classpath.txt
set /p CP3=<%ISPN_HOME%\modules\websocket\runtime-classpath.txt
set /p CP4=<%ISPN_HOME%\modules\cli-server\runtime-classpath.txt
set CP=%CP1%;%CP2%;%CP3%;%CP4%
set CP=%CP::=;%
set CP=%ISPN_HOME%\etc;%CP%
set CP=%CP:$ISPN_HOME=!ISPN_HOME!%
set CP=%ISPN_HOME%\modules\memcached\infinispan-server-memcached.jar;%CP%
set CP=%ISPN_HOME%\modules\hotrod\infinispan-server-hotrod.jar;%CP%
set CP=%ISPN_HOME%\modules\websocket\infinispan-server-websocket.jar;%CP%
set CP=%ISPN_HOME%\modules\cli-server\infinispan-cli-server.jar;%CP%
rem echo libs: %CP%

:test
set /a "TESTPORT=%RANDOM%+2000"
netstat -an | findstr ":%TESTPORT% "
if %ERRORLEVEL%==0 goto test

java -classpath "%CP%" -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=%TESTPORT% -Djgroups.bind_addr=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=%ISPN_HOME%\etc\log4j.xml -Dsun.nio.ch.bugLevel="" org.infinispan.server.core.Main %*
