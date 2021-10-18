@echo off

set LOADER_CLASS=org.infinispan.server.loader.Loader
set MAIN_CLASS=org.infinispan.server.Bootstrap
set ARGUMENTS=
set PROCESS_NAME=${infinispan.brand.short-name}-server

set "DIRNAME=%~dp0%"

setlocal EnableDelayedExpansion
call "!DIRNAME!common.bat" %*
setlocal DisableDelayedExpansion

"%JAVA%" %JAVA_OPTS% ^
   -Dvisualvm.display.name=%PROCESS_NAME% ^
   "-Dinfinispan.server.home.path=%ISPN_HOME%" ^
   -classpath "%CLASSPATH%" %LOADER_CLASS% %MAIN_CLASS% %ARGUMENTS%
