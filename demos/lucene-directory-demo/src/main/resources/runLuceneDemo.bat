@echo off

:noenvreset
set my_classpath=
for /f "tokens=* delims=" %%f in ('dir /s /b /a-d "..\lib\*.jar"') do (
		call set my_classpath=%%my_classpath%%;%%~f)
	)
set my_classpath=%my_classpath:~1%

set my_classpath=%my_classpath%;..\infinispan-core.jar

for /f "tokens=* delims=" %%f in ('dir /s /b /a-d "..\modules\demos\lucene-directory-demo\*.jar"') do (
                call set my_classpath=%%my_classpath%%;%%~f)
        )
set my_classpath=%my_classpath:~1%

for /f "tokens=* delims=" %%f in ('dir /s /b /a-d "..\modules\demos\lucene-directory-demo\lib\*.jar"') do (
                call set my_classpath=%%my_classpath%%;%%~f)
        )
set my_classpath=%my_classpath:~1%

:test
set /a "TESTPORT=%RANDOM%+2000"
netstat -an | findstr ":%TESTPORT% "
if %ERRORLEVEL%==0 goto test

java -cp "%my_classpath%" -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=%TESTPORT% -Djgroups.bind_addr=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=..\etc\log4j.xml -Dsun.nio.ch.bugLevel="" org.infinispan.lucenedemo.DemoDriver
