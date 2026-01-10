@echo off

pushd "%DIRNAME%.."
set "RESOLVED_ISPN_HOME=%CD%"
popd

if "%ISPN_HOME%" == "" (
   set "ISPN_HOME=%RESOLVED_ISPN_HOME%"
)

pushd "%ISPN_HOME%"
set "SANITIZED_ISPN_HOME=%CD%"
popd

if /i "%RESOLVED_ISPN_HOME%" neq "%SANITIZED_ISPN_HOME%" (
   echo.
   echo   WARNING: The ISPN_HOME ^("%SANITIZED_ISPN_HOME%"^) that this script uses points to a different installation than the one that this script resides in ^("%RESOLVED_ISPN_HOME%"^). Unpredictable results may occur.
   echo.
   echo       ISPN_HOME: "%ISPN_HOME%"
   echo.
)

if "%ISPN_ROOT_DIR%" == "" (
   set "ISPN_ROOT_DIR=%ISPN_HOME%\server"
)
if "%ISPN_LOG_DIR%" == "" (
   set "ISPN_LOG_DIR=%ISPN_ROOT_DIR%\log"
)
if "%ISPN_CONFIG_DIR%" == "" (
   set "ISPN_CONFIG_DIR=%ISPN_ROOT_DIR%\conf"
)
if "%ISPN_TMP_DIR%" == "" (
   set "ISPN_TMP_DIR=%ISPN_ROOT_DIR%\tmp"
)

if "%JAVA_HOME%" == "" (
   set JAVA=java
) else (
   set "JAVA=%JAVA_HOME%\bin\java"
)

for /F "tokens=3" %%V in ('java -version 2^>^&1 ^| findstr /i "version"') do (
   set "VERSION=%%V"
)
set "VERSION=%VERSION:"=,%"
for /F "delims=., tokens=1" %%a in ("%VERSION%") do (
   set "JAVA_VERSION=%%a"
)

if %JAVA_VERSION% geq 24 (
    set "JAVA_OPTS=--enable-native-access=ALL-UNNAMED %JAVA_OPTS%"
)
if %JAVA_VERSION% geq 25 (
   set "JAVA_OPTS=-XX:+UseCompactObjectHeaders %JAVA_OPTS%"
)

set DEBUG_MODE=false
set DEBUG_PORT=8787
set "JAVA_OPTS_EXTRA="
:ARGS_LOOP_START
set "ARG=%~1"
if "%ARG%" == "" (
   goto ARGS_LOOP_END
) else if "%ARG%" == "--debug" (
   set DEBUG_MODE=true
   if "%~2"=="" goto ARGS_LOOP_END
   set "var="&for /f "delims=0123456789" %%i in ("%~2") do set var=%%i
   if defined var (
      rem Use the default port
   ) else (
      set DEBUG_PORT=%2
      shift
   )
) else if "%ARG:~0,2%"=="-D" (
   set "JAVA_OPTS_EXTRA=%JAVA_OPTS_EXTRA% %ARG%=%2"
   shift
) else (
   set "ARGUMENTS=%ARGUMENTS% %ARG%"
)
shift
goto ARGS_LOOP_START

:ARGS_LOOP_END
if exist "%ISPN_LOG_DIR%" (
   mkdir %ISPN_LOG_DIR%
)
set "PREPEND_JAVA_OPTS="
set "JAVA_OPTS=--add-exports java.naming/com.sun.jndi.ldap=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED %JAVA_OPTS% %JAVA_OPTS_EXTRA%"
rem Set debug settings if not already set
if "%DEBUG_MODE%" == "true" (
   echo "%JAVA_OPTS%" | findstr /I "\-agentlib:jdwp" > nul
  if errorlevel == 1 (
     set "PREPEND_JAVA_OPTS=JAVA_OPTS% -agentlib:jdwp=transport=dt_socket,address=%DEBUG_PORT%,server=y,suspend=n"
  ) else (
     echo Debug already enabled in JAVA_OPTS, ignoring --debug argument
  )
)
if "%GC_LOG%" == "true" (
   set "PREPEND_JAVA_OPTS=%PREPEND_JAVA_OPTS% -Xlog:gc*:file=%ISPN_LOG_DIR%\gc.log:time,uptimemillis:filecount=5,filesize=3M"
)
if "%HEAP_DUMP%" == "true" (
   set "PREPEND_JAVA_OPTS=%PREPEND_JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%ISPN_LOG_DIR% -XX:ErrorFile=%ISPN_LOG_DIR%\hs_err_pid_%%p.log -XX:ReplayDataFile=%ISPN_LOG_DIR%\replay_pid%%p.log"
)

set "JAVA_OPTS=%PREPEND_JAVA_OPTS% %JAVA_OPTS%"
