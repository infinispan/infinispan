@echo off

setlocal
set ARGUMENTS=
set PROCESS_NAME=infinispan-cli

set "DIRNAME=%~dp0%"

if exist "%DIRNAME%cli-conf.bat" call "%DIRNAME%cli-conf.bat"

call "%DIRNAME%common.bat" %*

for %%J in ("%ISPN_HOME%\lib\infinispan-cli-client-*.jar") do (
"%JAVA%" %JAVA_OPTS% ^
   -Dvisualvm.display.name=%PROCESS_NAME% ^
   "-Dinfinispan.server.home.path=%ISPN_HOME%" ^
   -jar %%J %ARGUMENTS%
)
endlocal
