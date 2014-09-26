@echo off

setlocal enabledelayedexpansion
set ISPN_HOME=%~dp0
set ISPN_HOME="%ISPN_HOME%.."

for %%i in (%ISPN_HOME%\*.jar) do (
   set CP=%CP%;%%i
)
set /p CP=<%ISPN_HOME%\demos\lucene-directory-demo\etc\runtime-classpath.txt
set CP=%CP::=;%
set CP=%ISPN_HOME%\etc;%CP%
set CP=%CP:$ISPN_HOME=!ISPN_HOME!%
set CP=%ISPN_HOME%\demos\lucene-directory-demo\infinispan-lucene-demo.jar;%CP%
rem echo libs: %CP%

:test
set /a "TESTPORT=%RANDOM%+2000"
netstat -an | findstr ":%TESTPORT% "
if %ERRORLEVEL%==0 goto test

java -cp "%CP%" -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=%TESTPORT% -Djgroups.bind_addr=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=%ISPN_HOME%\configs\log4j\log4j.xml -Dsun.nio.ch.bugLevel="" org.infinispan.lucenedemo.DemoDriver
