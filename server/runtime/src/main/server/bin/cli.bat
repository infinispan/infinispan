@echo off

set LOADER_CLASS=org.infinispan.server.loader.Loader
set MAIN_CLASS=org.infinispan.cli.CLI
set ARGUMENTS=

set "DIRNAME=%~dp0%"

setlocal EnableDelayedExpansion
call "!DIRNAME!common.bat" %*
setlocal DisableDelayedExpansion

"%JAVA%" %JAVA_OPTS% ^
   -Djava.util.logging.manager=org.jboss.logmanager.LogManager ^
   "-Dinfinispan.server.home.path=%ISPN_HOME%" ^
   -classpath %CLASSPATH% %LOADER_CLASS% %MAIN_CLASS% %ARGUMENTS%
