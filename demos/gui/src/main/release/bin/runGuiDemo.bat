@echo off

setlocal enabledelayedexpansion
set ISPN_HOME=%~dp0
set ISPN_HOME="%ISPN_HOME%.."
set DEMO_HOME=%ISPN_HOME%\demos\gui

set CP=
for %%i in (%ISPN_HOME%\infinispan-embedded-*.jar) do call :append_to_cp %%i

set /p RUNTIME_CP=<%DEMO_HOME%\etc\runtime-classpath.txt
set CP=%RUNTIME_CP::=;%;%CP%

set CP=%DEMO_HOME%\etc;%CP%
set CP=%CP:$ISPN_HOME=!ISPN_HOME!%
set CP=%DEMO_HOME%\infinispan-gui-demo.jar;%CP%
rem echo libs: %CP%

:test
set /a "TESTPORT=%RANDOM%+2000"
netstat -an | findstr ":%TESTPORT% "
if %ERRORLEVEL%==0 goto test

java -cp "%CP%" -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=%TESTPORT% -Djgroups.bind_addr=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=%ISPN_HOME%\configs\log4j\log4j.xml -Dsun.nio.ch.bugLevel="" org.infinispan.demo.InfinispanDemo

goto :eof

:append_to_cp
set CP=%CP%;%1
goto :eof
