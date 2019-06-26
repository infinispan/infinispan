import org.infinispan.commons.api.functional.EntryView.*;
import org.infinispan.commons.api.functional.FunctionalMap.*;

ReadWriteMap<String, String> readWriteMap = ...

CompletableFuture<Optional<String>> readWriteFuture = readWriteMap.eval("key1", "value1",
   (v, view) -> {
      Optional<V> prev = rw.find();
      view.set(v);
      return prev;
   });
readWriteFuture.thenAccept(System.out::println);
