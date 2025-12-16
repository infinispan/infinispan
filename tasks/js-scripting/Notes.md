
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
- pass the user arguments through arguments
- expose context through builtins

- re-enable commit hook at the end

user_input and system_input -> passing positional arguments to the function is not viable as they are stored in a map
better to assume a flat object
system-input -> is mostly unused now

- JS engine is now stateless instead of stateful
- entrySet is passed as a materialized array of values, do we need it lazy?
- re-evaluate system-input -> is it covered by the ScriptingAPI already?
- the script API is now strict and well-defined, most things are happening in js and is not possible to invoke Java from JS
