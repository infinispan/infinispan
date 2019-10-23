@echo off

pushd "%DIRNAME%.."
set "RESOLVED_ISPN_HOME=%CD%"
popd

if "x%ISPN_HOME%" == "x" (
   set "ISPN_HOME=%RESOLVED_ISPN_HOME%"
)

pushd "%ISPN_HOME%"
set "SANITIZED_ISPN_HOME=%CD%"
popd

if /i "%RESOLVED_ISPN_HOME%" NEQ "%SANITIZED_ISPN_HOME%" (
   echo.
   echo   WARNING: The ISPN_HOME ^("%SANITIZED_ISPN_HOME%"^) that this script uses points to a different installation than the one that this script resides in ^("%RESOLVED_ISPN_HOME%"^). Unpredictable results may occur.
   echo.
   echo       ISPN_HOME: "%ISPN_HOME%"
   echo.
)

if "x%JAVA_HOME%" == "x" (
   set JAVA=java
) else (
   set "JAVA=%JAVA_HOME%\bin\java"
)

set "JAVA_OPTS="

set "CLASSPATH="

for %%j in (%ISPN_HOME%\boot\*.jar) do (
   set "CLASSPATH=%CLASSPATH%;%%~j"
)

set DEBUG_MODE=false
set DEBUG_PORT=8787
set "JAVA_OPTS_EXTRA="
:ARGS_LOOP_START
if "%~1" == "" (
   goto ARGS_LOOP_END
) else if "%~1" == "--debug" (
   set DEBUG_MODE=true
   if "%~2"=="" goto ARGS_LOOP_END
   set "var="&for /f "delims=0123456789" %%i in ("%~2") do set var=%%i
   if defined var (
      rem Use the default port
   ) else (
      set DEBUG_PORT=%2
      shift
   )
) else if "%~1:~0,2" == "-D" (
   set "JAVA_OPTS_EXTRA=%JAVA_OPTS_EXTRA% %1"
) else (
   set "ARGUMENTS=%ARGUMENTS% %1"
)
shift
goto ARGS_LOOP_START

:ARGS_LOOP_END

rem Set debug settings if not already set
if "%DEBUG_MODE%" == "true" (
   echo "%JAVA_OPTS%" | findstr /I "\-agentlib:jdwp" > nul
  if errorlevel == 1 (
     set "JAVA_OPTS=%JAVA_OPTS% -agentlib:jdwp=transport=dt_socket,address=%DEBUG_PORT%,server=y,suspend=n"
  ) else (
     echo Debug already enabled in JAVA_OPTS, ignoring --debug argument
  )
)
