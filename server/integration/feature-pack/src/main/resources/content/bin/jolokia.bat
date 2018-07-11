set AGENT_BOND_JAR=
for /r %JBOSS_HOME%\modules\system\add-ons\@infinispan.module.slot.prefix@\io\fabric8\agent-bond\@infinispan.module.slot@ %%X in (*.jar) do (
  set AGENT_BOND_JAR=%%X
)
set JAVA_OPTS=%JAVA_OPTS% -javaagent:%AGENT_BOND_JAR%=%1