set AGENT_BOND_JAR=
for /r %JBOSS_HOME%\modules\system\layers\base\io\fabric8\agent-bond\main %%X in (*.jar) do (
  set AGENT_BOND_JAR=%%X
)
set JAVA_OPTS=%JAVA_OPTS% -javaagent:%AGENT_BOND_JAR%=%1