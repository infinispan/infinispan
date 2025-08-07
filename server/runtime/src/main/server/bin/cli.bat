@echo off

set LOADER_CLASS=org.infinispan.server.loader.Loader
set MAIN_CLASS=org.infinispan.cli.commands.CLI
set ARGUMENTS=
set PROCESS_NAME=${infinispan.brand.short-name}-cli

set "DIRNAME=%~dp0%"

call "%DIRNAME%common.bat" %*

"%JAVA%" %JAVA_OPTS% ^
   -Dvisualvm.display.name=%PROCESS_NAME% ^
   -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager ^
   -Dlog4j.configurationFile="%DIRNAME%\cli.log4j2.xml" ^
   "-Dinfinispan.server.home.path=%ISPN_HOME%" ^
   -classpath "%CLASSPATH%" %LOADER_CLASS% %MAIN_CLASS% %ARGUMENTS%
