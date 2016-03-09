// mode=local,language=javascript
var Function = Java.type("java.util.function.Function")
var Serializable = Java.type("java.io.Serializable")
var Collectors = Java.type("java.util.stream.Collectors")
var Arrays = Java.type("org.infinispan.scripting.utils.JSArrays")
var CacheCollectors = Java.type("org.infinispan.stream.CacheCollectors");
var HashMap = Java.type("java.util.HashMap");
var SerializableFunction = Java.extend(Function, Serializable);
var SerializableSupplier = Java.extend(Java.type("org.infinispan.stream.SerializableSupplier"))
var SerializableTriConsumer = Java.extend(Java.type("org.infinispan.util.TriConsumer"))

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

var c = new SerializableFunction({
   apply: function(f) {
      return f.getCache().entrySet().stream().map( e).flatMap(f).collect(CacheCollectors.serializableCollector(s))
   }
})

var map = new HashMap();

var triConsumer = new SerializableTriConsumer({
   accept: function(a, i, t) {
      if (t != null) {
          print(t);
       } else {
          list.putAll(i);
       }
   }
})

cacheManager.executor().submitConsumer(c, triConsumer).get();
map