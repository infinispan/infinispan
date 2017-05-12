// mode=distributed,language=javascript
var Function = Java.type("java.util.function.Function")
var Serializable = Java.type("java.io.Serializable")
var Collectors = Java.type("java.util.stream.Collectors")
var Arrays = Java.type("org.infinispan.scripting.utils.JSArrays")
var CacheCollectors = Java.type("org.infinispan.stream.CacheCollectors");
var SerializableFunction = Java.extend(Function, Serializable);
var SerializableSupplier = Java.extend(Java.type("org.infinispan.util.function.SerializableSupplier"))

var e = new SerializableFunction( {
   apply: function(object) {
      return object.getValue().toLowerCase().split(/[\W]+/)
   }
})

var f = new SerializableFunction({
   apply: function(f) {
      return Arrays.stream(f)
   }
})

var s = new SerializableSupplier({
   get: function() {
      return Collectors.groupingBy(Function.identity(), Collectors.counting())
   }
})

    cache.entrySet().stream().map(e)
       .flatMap(f)
       .collect(CacheCollectors.serializableCollector(s));
