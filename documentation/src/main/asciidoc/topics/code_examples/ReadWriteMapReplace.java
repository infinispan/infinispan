ReadWriteMap<String, String> readWriteMap = ...

String oldValue = "old-value";
CompletableFuture<Boolean> replaceFuture = readWriteMap.eval("key1", "value1", (v, view) -> {
   return view.find().map(prev -> {
      if (prev.equals(oldValue)) {
         rw.set(v);
         return true; // previous value present and equals to the expected one
      }
      return false; // previous value associated with key does not match
   }).orElse(false); // no value associated with this key
});
replaceFuture.thenAccept(replaced -> System.out.printf("Value was replaced? %s%n", replaced));
