@echo off

set ARGUMENTS=
set PROCESS_NAME=infinispan-server

set "DIRNAME=%~dp0%"

call "%DIRNAME%common.bat" %*

FOR %%J IN ("%ISPN_HOME%\lib\infinispan-server-runtime-*.jar") DO (
"%JAVA%" %JAVA_OPTS% ^
   -Dvisualvm.display.name=%PROCESS_NAME% ^
   "-Dinfinispan.server.home.path=%ISPN_HOME%" ^
   -jar %%J %ARGUMENTS%
)
