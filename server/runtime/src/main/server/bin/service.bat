@echo off

REM -------------------------------------------------------------------------
REM  ISPN Service Script for Windows
REM    It has to reside in %SERVER_HOME%\bin
REM    It is expecting that prunsrv.exe reside in one of:
REM      %SERVER_HOME%\bin
REM      %SERVER_HOME%\..\..\jbcs-jsvc-*\sbin
REM     example command for installation:
REM     service.bat install /user admin /pass admin 
REM      /controller localhost:11222 /shutdown-file-name shutdown-command.txt 
REM      /node-name doc /name ISPNService /desc "ISPN service" /loglevel DEBUG
REM
REM  v2 2021-07-08 Generalised the script for upstream ISPN
REM  v1	2021-06-11 initial edit
REM
REM Author: Lei Yu
REM ========================================================
setlocal EnableExtensions EnableDelayedExpansion

set DEBUG=0
if "%DEBUG%" == "1" (
	echo "Debug info enabled"
	echo on
)

set "DIRNAME=%~dp0%"
if "%DEBUG%" == "1" (
	echo DIRNAME "%DIRNAME%x"
)

set TEMP_SERVER_HOME=%DIRNAME%..\

pushd %TEMP_SERVER_HOME%
set "RESOLVED_SERVER_HOME=%CD%"
popd

rem echo %RESOLVED_SERVER_HOME%

set DIRNAME=

if "x%SERVER_HOME%" == "x" (
  set "SERVER_HOME=%RESOLVED_SERVER_HOME%"
)
rem echo %SERVER_HOME%

if "%DEBUG%" == "1" (
	echo SERVER_HOME="%SERVER_HOME%"
)

rem echo "server home" %SERVER_HOME%


set PRUNSRV=
rem Attempt to find prunsrv.exe under the same root directory that contains SERVER_HOME.
rem Note that only the last match will be used. This *typically* means the most recent version will be matched.
rem Also note that only jbcs-jsvc-* will be used as a pattern, so jbcs-jsvc-1.0 and jbcs-jsvc-1.1 will match.
for /d %%a in ( "%SERVER_HOME%\..\jbcs-jsvc-*" ) do (
  if "%DEBUG%" == "1" (
    echo FOUND "%%~fa"
  )
  
  if exist "%%~fa\sbin\prunsrv.exe" (
    set "PRUNSRV=%%~fa\sbin\prunsrv.exe"
  )
)

rem prunsrv.exe was not found above, so try to find it under SERVER_HOME\bin.
if "%PRUNSRV%" == "" (
  if exist "%SERVER_HOME%\bin\prunsrv.exe" (
    set PRUNSRV="%SERVER_HOME%\bin\prunsrv.exe"
  ) else (
    echo Please install native utilities into expected location %SERVER_HOME%\..\jbcs-jsvc-1.1
    goto cmdEnd
  )
)

if "%DEBUG%" == "1" (
	echo PRUNSRV %PRUNSRV%
)

echo(

rem defaults
set SHORTNAME=ISPN
set DISPLAYNAME=ISPN
rem NO quotes around the description here !
set DESCRIPTION=Windows service for ISPN server
set CONTROLLER=localhost:11222
set LOGLEVEL=INFO
set LOGPATH=
set ISPNUSER=
set ISPNPASS=
set SERVICE_USER=
set SERVICE_PASS=
set STARTUP_MODE=manual
set ISDEBUG=
set CONFIG=
set SHUTDOWN-FILE-NAME=shutdown-command.txt


set COMMAND=%1
shift
if /I "%COMMAND%" == "install"   goto cmdInstall
if /I "%COMMAND%" == "uninstall" goto cmdUninstall
if /I "%COMMAND%" == "start"     goto cmdStart
if /I "%COMMAND%" == "stop"      goto cmdStop
if /I "%COMMAND%" == "restart"   goto cmdRestart

echo ERROR: invalid command

:cmdUsage
echo ISPN Server Service Script for Windows
echo Usage:
echo(
echo   service install ^<options^>  , where the options are:
echo(
echo     /startup                  : Set the service to auto start
echo                                 Not specifying sets the service to manual
echo(
echo     /user ^<username^>     : ISPN username to use for the shutdown command.
echo(
echo     /pass ^<password^>     : Password for /user
echo(
echo     /controller ^<host:port^>   : The host:port of the cli interface.
echo                                 default: localhost:11222
echo(
echo     /shutdown-file-name ^<filename^>   : The file contains the shutdown command, it is 
echo                                        expected to be in the %SERVER_HOME%\bin directory.
echo                                        The content can be, for example
echo                                        shutdown server server1
echo(
echo     /node-name ^<node-name^>      : The name for ISPN server
echo
echo(
echo Options to use when multiple services or different accounts are needed:
echo(
echo     /name ^<servicename^>       : The name of the service
echo(
echo                                 default: %SHORTNAME%
echo     /desc ^<description^>       : The description of the service, use double
echo                                 quotes to allow spaces.
echo                                 Maximum 1024 characters.
echo                                 default: %DESCRIPTION%
echo(
echo     /serviceuser ^<username^>   : Specifies the name of the account under which
echo                                 the service should run.
echo                                 Use an account name in the form of
echo                                 DomainName\UserName
echo                                 default: not used, the service runs as
echo                                 Local System Account.
echo
echo     /servicepass ^<password^>   : password for /serviceuser
echo(
echo Advanced options:
echo(
echo     /loglevel ^<level^>         : The log level for the service:  Error, Info,
echo                                 Warn or Debug ^(Case insensitive^)
echo                                 default: %LOGLEVEL%
echo     /logpath ^<path^>           : Path of the log
echo                                 
echo(
echo     /debug                    : run the service install in debug mode
echo(
echo Other commands:
echo(
echo   service uninstall [/name ^<servicename^>]
echo   service start [/name ^<servicename^>]
echo   service stop [/name ^<servicename^>]
echo   service restart [/name ^<servicename^>]
echo(
echo     /name  ^<servicename^>      : Name of the service: should not contain spaces
echo                                 default: %SHORTNAME%
echo(
goto endBatch

:cmdInstall

:LoopArgs
if "%~1" == "" goto doInstall

if /I "%~1"== "/debug" (
  set ISDEBUG=true
  shift
  goto LoopArgs
)
if /I "%~1"== "/startup" (
  set STARTUP_MODE=auto
  shift
  goto LoopArgs
)

if /I "%~1"== "/config" (
  set CONFIG=
  if not "%~2"=="" (
    set T=%~2
    if not "!T:~0,1!"=="/" (
      set CONFIG=%~2
    )
  )
  if "!CONFIG!" == "" (
    echo ERROR: You need to specify a server-config name
    goto endBatch
  )
  shift
  shift
  goto LoopArgs
)

if /I "%~1"== "/controller" (
  set CONTROLLER=
  if not "%~2"=="" (
    set T=%~2
    if not "!T:~0,1!"=="/" (
      set CONTROLLER=%~2
    )
  )
  if "!CONTROLLER!" == "" (
    echo ERROR: The ISPN server URL should be specified in the format host:port, example:  127.0.0.1:11222
    goto endBatch
  )
  shift
  shift
  goto LoopArgs
)

if /I "%~1"== "/node-name" (
  set NODE-NAME=
  if not "%~2"=="" (
    set T=%~2
    if not "!T:~0,1!"=="/" (
      set NODE-NAME=%~2
    )
  )
  if "!NODE-NAME!" == "" (
    echo ERROR: The ISPN server server name must be specified 
    goto endBatch
  )
  shift
  shift
  goto LoopArgs
)


if /I "%~1"== "/shutdown-file-name" (
   set SHUTDOWN-FILE-NAME=
   if not "%~2"=="" (
    set T=%~2
    if not "!T:~0,1!"=="/" (
      set SHUTDOWN-FILE-NAME=%~2
    )
  )

  if "!SHUTDOWN-FILE-NAME!" == "" (
    echo ERROR: The shutdown command file name must be specified, the file can contain the command for shutting down ISPN server, e.g shutdown server ^<server-name^>
    goto endBatch
  )

  shift
  shift
  goto LoopArgs
)

if /I "%~1"== "/name" (
  set SHORTNAME=
  if not "%~2"=="" (
    set T=%~2
    if not "!T:~0,1!"=="/" (
      set SHORTNAME=%~2
      set DISPLAYNAME=%~2
    )
  )
  if "!SHORTNAME!" == "" (
    echo ERROR: You need to specify a service name
    goto endBatch
  )
  shift
  shift
  goto LoopArgs
)

if /I "%~1"== "/desc" (
  set DESCRIPTION=
  if not "%~2"=="" (
    set T=%~2
    if not "!T:~0,1!"=="/" (
      set DESCRIPTION=%~2
    )
  )
  if "!DESCRIPTION!" == "" (
    echo ERROR: You need to specify a description, maximum of 1024 characters
    goto endBatch
  )
  shift
  shift
  goto LoopArgs
)

if /I "%~1"== "/user" (
  set ISPNUSER=
  if not "%~2"=="" (
    set T=%~2
    if not "!T:~0,1!"=="/" (
      set ISPNUSER=%~2
    )
  )
  if "!ISPNUSER!" == "" (
    echo ERROR: You need to specify a username
    goto endBatch
  )
  shift
  shift
  goto LoopArgs
)

if /I "%~1"== "/pass" (
  set ISPNPASS=
  if not "%~2"=="" (
    set T=%~2
    if not "!T:~0,1!"=="/" (
      set ISPNPASS=%~2
    )
  )
  if "!ISPNPASS!" == "" (
    echo ERROR: You need to specify a password for /pass
    goto endBatch
  )
  shift
  shift
  goto LoopArgs
)

if /I "%~1"== "/serviceuser" (
  set SERVICE_USER=
  if not "%~2"=="" (
    set T=%~2
    if not "!T:~0,1!"=="/" (
      set SERVICE_USER=%~2
    )
  )
  if "!SERVICE_USER!" == "" (
    echo ERROR: You need to specify a username in the format DOMAIN\USER, or .\USER for the local domain for the Windows service
    goto endBatch
  )
  shift
  shift
  goto LoopArgs
)
if /I "%~1"== "/servicepass" (
  set SERVICE_PASS=
  if not "%~2"=="" (
    set T=%~2
    if not "!T:~0,1!"=="/" (
      set SERVICE_PASS=%~2
    )
  )
  if "!SERVICE_PASS!" == "" (
    echo ERROR: You need to specify a password for /servicepass
    goto endBatch
  )
  shift
  shift
  goto LoopArgs
)

if /I "%~1"== "/loglevel" (
  if /I not "%~2"=="Error" if /I not "%~2"=="Info" if /I not "%~2"=="Warn" if /I not "%~2"=="Debug" (
    echo ERROR: /loglevel must be set to Error, Info, Warn or Debug ^(Case insensitive^)
    goto endBatch
  )
  set LOGLEVEL=%~2
  shift
  shift
  goto LoopArgs
)
if /I "%~1"== "/logpath" (
  set LOGPATH=
  if not "%~2"=="" (
    set T=%~2
    if not "!T:~0,1!"=="/" (
      set LOGPATH=%~2
	)
  )
  if "!LOGPATH!" == "" (
    echo ERROR: You need to specify a path for the service log
    goto endBatch
  )
  shift
  shift
  goto LoopArgs
)

echo ERROR: Unrecognised option: %1
echo(
goto cmdUsage

:doInstall

if not "%ISPNUSER%" == "" (
  if "%ISPNPASS%" == "" (
    echo When specifying a user, you need to specify the password
    goto endBatch
  )
)

set RUNAS=
if not "%SERVICE_USER%" == "" (
  if "%SERVICE_PASS%" == "" (
    echo When specifying a user, you need to specify the password
    goto endBatch
  )
  set RUNAS=--ServiceUser="%SERVICE_USER%" --ServicePassword="%SERVICE_PASS%"
)

set ISPN_URL="http://"%ISPNUSER%":"%ISPNPASS%"@"%CONTROLLER%

if "%CONFIG%"=="" set CONFIG=infinispan.xml

set FINAL-SHUTDOWN-FILE-NAME=%SERVER_HOME%\bin\%SHUTDOWN-FILE-NAME%

if "%STDOUT%"=="" set STDOUT=auto
if "%STDERR%"=="" set STDERR=auto

if "%START_PATH%"=="" set START_PATH="%SERVER_HOME%\bin"
if "%STOP_PATH%"=="" set STOP_PATH="%SERVER_HOME%\bin"

if "%STOP_SCRIPT%"=="" set STOP_SCRIPT=cli.bat -c %ISPN_URL% -f %FINAL-SHUTDOWN-FILE-NAME%

if "%START_SCRIPT%"=="" set START_SCRIPT=server.bat
 rem  set STARTPARAM="/c#set#NOPAUSE=Y#&&#!START_SCRIPT!#-Dinfinispan.server.home.path=!BASE!#--server-config=!CONFIG!"
  set STARTPARAM="/c#set#NOPAUSE=Y#&&#!START_SCRIPT!#-n %NODE-NAME% #-c !CONFIG!"
  set STOPPARAM="/c !STOP_SCRIPT! "


if "%LOGPATH%"=="" set LOGPATH="%SERVER_HOME%\server\log"



if /I "%ISDEBUG%" == "true" (
  echo SERVER_HOME="%SERVER_HOME%"
  echo RUNAS=%RUNAS%
  echo SHORTNAME="%SHORTNAME%"
  echo DESCRIPTION="%DESCRIPTION%"
  echo STARTPARAM=%STARTPARAM%
  echo STOPPARAM=%STOPPARAM%
  echo LOGLEVEL=%LOGLEVEL%
  echo LOGPATH=%LOGPATH%
  echo CREDENTIALS=%CREDENTIALS%
  echo BASE="%BASE%"
  echo CONFIG="%CONFIG%"
  echo START_SCRIPT=%START_SCRIPT%
  echo START_PATH=%START_PATH%
  echo STOP_SCRIPT=%STOP_SCRIPT%
  echo STOP_PATH=%STOP_PATH%
  echo STDOUT="%STDOUT%"
  echo STDERR="%STDERR%"
)
if /I "%ISDEBUG%" == "true" (
  @echo on
)

@rem quotes around the "%DESCRIPTION%" but nowhere else
echo %PRUNSRV% install %SHORTNAME% %RUNAS% --DisplayName=%DISPLAYNAME% --Description="%DESCRIPTION%" --LogLevel=%LOGLEVEL% --LogPath=%LOGPATH% --LogPrefix=service --StdOutput=%STDOUT% --StdError=%STDERR% --StartMode=exe --Startup=%STARTUP_MODE% --StartImage=cmd.exe --StartPath=%START_PATH% ++StartParams=%STARTPARAM% --StopMode=exe --StopImage=cmd.exe --StopPath=%STOP_PATH%  ++StopParams=%STOPPARAM%

%PRUNSRV% install %SHORTNAME% %RUNAS% --DisplayName=%DISPLAYNAME% --Description="%DESCRIPTION%" --LogLevel=%LOGLEVEL% --LogPath=%LOGPATH% --LogPrefix=service --StdOutput=%STDOUT% --StdError=%STDERR% --StartMode=exe --Startup=%STARTUP_MODE% --StartImage=cmd.exe --StartPath=%START_PATH% ++StartParams=%STARTPARAM% --StopMode=exe --StopImage=cmd.exe --StopPath=%STOP_PATH%  ++StopParams=%STOPPARAM%


@if /I "%ISDEBUG%" == "true" (
  @echo off
)

if errorlevel 8 (
  echo ERROR: The service %SHORTNAME% already exists
  goto endBatch
)
if errorlevel 0 (
  echo Service %SHORTNAME% installed
  goto endBatch
)
goto cmdEnd


REM the other commands take a /name parameter - if there is no ^<servicename^> passed as second parameter,
REM we silently ignore this and use the default SHORTNAME

:cmdUninstall
if /I "%~1"=="/name" (
  if not "%~2"=="" (
    set SHORTNAME="%~2"
  )
)
%PRUNSRV% stop %SHORTNAME%
if errorlevel 0 (
  %PRUNSRV% delete %SHORTNAME%
  if errorlevel 0 (
    echo Service %SHORTNAME% uninstalled
  )
) else (
  echo Unable to stop the service %SHORTNAME%
)
goto cmdEnd

:cmdStart
if /I "%~1"=="/name" (
  if not "%~2"=="" (
    set SHORTNAME="%~2"
  )
)
%PRUNSRV% start %SHORTNAME%
echo Service %SHORTNAME% starting...
goto cmdEnd

:cmdStop
if /I "%~1"=="/name" (
  if not "%~2"=="" (
    set SHORTNAME="%~2"
  )
)
%PRUNSRV% stop %SHORTNAME%
echo Service %SHORTNAME% stopping...
goto cmdEnd

:cmdRestart
if /I "%~1"=="/name" (
  if not "%~2"=="" (
    set SHORTNAME="%~2"
  )
)
%PRUNSRV% stop %SHORTNAME%
echo Service %SHORTNAME% stopping...
if "%errorlevel%" == "0" (
  %PRUNSRV% start %SHORTNAME%
  echo Service %SHORTNAME% starting...
) else (
  echo Unable to stop the service %SHORTNAME%
)
goto cmdEnd


:cmdEnd
REM if there is a need to add other error messages, make sure to list higher numbers first !
if errorlevel 2 (
  echo ERROR: Failed to load service %SHORTNAME% configuration
  goto endBatch
)
if errorlevel 0 (
  goto endBatch
)
echo "Unforseen error=%errorlevel%"

rem nothing below, exit
:endBatch
