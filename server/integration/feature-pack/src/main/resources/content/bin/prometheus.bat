set PROMETHEUS_JAR=
for /r %JBOSS_HOME%\modules\system\add-ons\@infinispan.module.slot.prefix@\io\prometheus\jmx\@infinispan.module.slot@ %%X in (*.jar) do (
  set PROMETHEUS_JAR=%%X
)

set JBOSS_LOG_MANAGER_LIB=
for /r %JBOSS_HOME%\modules\system\layers\base\org\jboss\logmanager\main %%X in (*.jar) do (
  set JBOSS_LOG_MANAGER_LIB=%%X
)

set WILDFLY_COMMON_LIB=
for /r %JBOSS_HOME%\modules\system\layers\base\org\wildfly\common\main %%X in (*.jar) do (
  set WILDFLY_COMMON_LIB=%%X
)


set JAVA_OPTS=%JAVA_OPTS% -Djboss.modules.system.pkgs=org.jboss.byteman,org.jboss.logmanager -Djava.util.logging.manager=org.jboss.logmanager.LogManager -Xbootclasspath/p:%JBOSS_LOG_MANAGER_LIB% -Xbootclasspath/p:%WILDFLY_COMMON_LIB% -javaagent:%PROMETHEUS_JAR%=%1
