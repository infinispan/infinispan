@echo off
setlocal enabledelayedexpansion

:: Get current script directory
set "DIRNAME=%~dp0"
set "ISPN_HOME=%~dp0.."

:: 1. Handle PID detection using PowerShell
if "%~1"=="" (
    :: Find PIDs for processes with "server.sh" or "server.bat" in the command line
    set "COUNT=0"
    for /f "tokens=*" %%A in ('powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*server.*' } | Select-Object -ExpandProperty ProcessId"') do (
        set "SERVER_PID=%%A"
        set /a COUNT+=1
    )

    if !COUNT! GTR 1 (
        echo Multiple processes detected. Specify a PID as the first argument.
        exit /b 1
    )
    if "!SERVER_PID!"=="" (
        echo No running server detected.
        exit /b 1
    )
) else (
    set "SERVER_PID=%~1"
    shift
)

:: 2. Create Temporary Directory
set "TMPDIR=%TEMP%\infinispan-server.%RANDOM%"
mkdir "%TMPDIR%"
mkdir "%TMPDIR%\%SERVER_PID%"

:: 3. Gather System Info (PowerShell Replacements)
powershell -NoProfile -Command "Get-ComputerInfo" > "%TMPDIR%\os-release"
ipconfig /all > "%TMPDIR%\ip-address"
route print > "%TMPDIR%\ip-route"
powershell -NoProfile -Command "Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, MaxClockSpeed" > "%TMPDIR%\cpuinfo"
powershell -NoProfile -Command "Get-CimInstance Win32_OperatingSystem | Select-Object TotalVisibleMemorySize, FreePhysicalMemory" > "%TMPDIR%\meminfo"
ver > "%TMPDIR%\uname"
powershell -NoProfile -Command "Get-CimInstance Win32_LogicalDisk | Select-Object DeviceID, Size, FreeSpace" > "%TMPDIR%\df"
netstat -an > "%TMPDIR%\ss-all"
tasklist /v > "%TMPDIR%\lsof"

:: 4. Setup ISPN_ROOT
if "%~1"=="" (
    set "ISPN_ROOT=%ISPN_HOME%\server"
) else (
    set "ISPN_ROOT=%~1"
    shift
)

:: 5. Java Thread Dump
if defined JAVA_HOME (
    "%JAVA_HOME%\bin\jstack" %SERVER_PID% > "%TMPDIR%\%SERVER_PID%\thread-dump"
)

:: 6. Locate Server Root from command line (PowerShell replacement for sed/wmic)
for /f "usebackq tokens=*" %%B in (`powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter 'ProcessId=%SERVER_PID%' | Select-Object -ExpandProperty CommandLine"`) do set "FULL_CMD=%%B"
:: Set default
set "SERVER_PID_ISPN_ROOT=%ISPN_ROOT%"

:: 7. Packaging (Using native Windows tar)
set "ARCHIVE_NAME=%TMPDIR%.tar.gz"
tar -czf "%ARCHIVE_NAME%" -C "%TMPDIR%" .

:: Clean up
rd /s /q "%TMPDIR%"

echo %ARCHIVE_NAME%
endlocal
