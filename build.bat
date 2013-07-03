@ECHO OFF

SETLOCAL ENABLEEXTENSIONS

IF DEFINED M2_HOME GOTO :CHECK_MVN

ECHO The M2_HOME environment has not been defined
GOTO :EOF

:CHECK_MVN

IF EXIST %M2_HOME%\bin\mvn.bat GOTO :RUN_MVN

ECHO Cannot find the mvn.bat executable in %M2_HOME%\bin
GOTO :EOF

:RUN_MVN
SET GOAL=%1
IF "%GOAL%"=="" SET GOAL=install

echo Running %M2_HOME%\bin\mvn.bat -s maven-settings.xml %GOAL% %2 %3 %4 %5 %6 %7 %8 %9

%M2_HOME%\bin\mvn.bat -s maven-settings.xml %GOAL% %2 %3 %4 %5 %6 %7 %8 %9

