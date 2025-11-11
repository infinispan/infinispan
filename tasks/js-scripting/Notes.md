
- ScriptingManagerImpl is a bit of a leaky abstractions as ScriptingTaskEngine uses the Impl:
private final ScriptingManagerImpl scriptingManager, also in ScriptRunner
Override? modify? keep compatibility?
- the ScriptCache is, ideally, handled automatically by QuickJs4J:
https://github.com/roastedroot/quickjs4j/blob/main/core/src/main/java/io/roastedroot/quickjs4j/core/ScriptCache.java
what to do?
- those things:
var Function = Java.type("java.util.function.Function")
var Collectors = Java.type("java.util.stream.Collectors")
var Arrays = Java.type("org.infinispan.scripting.utils.JSArrays")
are not going to work

initial plan:
- use Builtins to craft the "default Java API" for JS
- pass the "more dynamic" objects through arguments
