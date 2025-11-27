REM CLI bootstrap Script Configuration
REM
REM Specify the location of the Java home directory.  If set then $JAVA will
REM be defined to $JAVA_HOME\bin\java, else $JAVA will be "java".
REM
REM set JAVA_HOME="c:\path\to\jdk"

REM
REM Specify the exact Java VM executable to use.
REM
REM set JAVA=""

REM Uncomment the following line to prevent manipulation of JVM options
REM by shell scripts.
REM
REM PRESERVE_JAVA_OPTS=true

REM
REM Specify options to pass to the Java VM.
REM
if "%JAVA_OPTS%" == "" (
   set "JAVA_OPTS=-Djava.awt.headless=true -Djava.net.preferIPv4Stack=true -XX:+ExitOnOutOfMemoryError -XX:MetaspaceSize=64M -Xms32m -Xmx128m %CLI_JAVA_OPTIONS%"
) else (
   echo "JAVA_OPTS already set in environment; overriding default settings with values: %JAVA_OPTS%"
)

REM Sample JPDA settings for remote socket debugging
REM set "JAVA_OPTS=%JAVA_OPTS% -agentlib:jdwp=transport=dt_socket,address=8787,server=y,suspend=n"

REM Sample JPDA settings for shared memory debugging
REM set "JAVA_OPTS=%JAVA_OPTS% -agentlib:jdwp=transport=dt_shmem,server=y,suspend=n,address=infinispan"

REM enable garbage collection logging if not set in environment differently
if "%GC_LOG%" == "" (
   set "GC_LOG=true"
) else (
   echo "GC_LOG set in environment to %GC_LOG%"
)

REM enable heap dump on OnOutOfMemoryError
if "%HEAP_DUMP%" == "" (
   set "HEAP_DUMP=true"
) else (
   echo "HEAP_DUMP set in environment to %HEAP_DUMP%"
)
