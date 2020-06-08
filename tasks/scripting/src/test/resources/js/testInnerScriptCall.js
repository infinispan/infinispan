//mode=local,language=javascript,parameters=[a]
var TaskContext = Java.type("org.infinispan.tasks.TaskContext")

cache.put("processValue", "script1");

scriptingManager.runScript("/js/testExecWithoutProp.js");
scriptingManager.runScript("/js/test.js", new TaskContext().addParameter("a", a));

cache.get("processValue");
