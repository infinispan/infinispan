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
endlocal
