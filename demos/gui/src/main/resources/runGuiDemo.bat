@echo off

:noenvreset
set my_classpath=
for /f "tokens=* delims=" %%f in ('dir /s /b /a-d "..\lib\*.jar"') do (
		call set my_classpath=%%my_classpath%%;%%~f)
	)
set my_classpath=%my_classpath:~1%

set my_classpath=%my_classpath%;..\infinispan-core.jar

for /f "tokens=* delims=" %%f in ('dir /s /b /a-d "..\modules\gui\*.jar"') do (
                call set my_classpath=%%my_classpath%%;%%~f)
        )
set my_classpath=%my_classpath:~1%

for /f "tokens=* delims=" %%f in ('dir /s /b /a-d "..\modules\gui\lib\*.jar"') do (
                call set my_classpath=%%my_classpath%%;%%~f)
        )
set my_classpath=%my_classpath:~1%

java -cp "%my_classpath%" -Dbind.address=127.0.0.1 -Djava.net.preferIPv4Stack=true -Dlog4j.configuration=..\etc\log4j.xml org.infinispan.demo.InfinispanDemo


