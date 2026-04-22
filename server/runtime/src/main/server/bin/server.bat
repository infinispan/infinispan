@echo off
setlocal
set ARGUMENTS=
set PROCESS_NAME=infinispan-server

set "DIRNAME=%~dp0%"

if exist "%DIRNAME%server-conf.bat" call "%DIRNAME%server-conf.bat"

call "%DIRNAME%common.bat" %*

for %%J in ("%ISPN_HOME%\lib\infinispan-server-runtime-*.jar") do (
"%JAVA%" %JAVA_OPTS% ^
   -Dvisualvm.display.name=%PROCESS_NAME% ^
   "-Dinfinispan.server.home.path=%ISPN_HOME%" ^
   -jar %%J %ARGUMENTS%
)
if "%HELP%" == "true" (
   echo Script options:
   echo   --debug ^<port^>                Activate debug mode with an optional argument to override the default port ^(8787^).
   echo   --debug-client                Activate debug mode in client mode ^(defaults to server mode^).
   echo   --debug-suspend               Activate debug mode and suspend the JVM.
   echo   --jmx ^<port^>                  Activate JMX remoting with an optional argument to override the default port ^(9999^).
)
endlocal
