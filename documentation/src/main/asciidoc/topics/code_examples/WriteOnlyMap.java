import org.infinispan.functional.EntryView.*;
import org.infinispan.functional.FunctionalMap.*;

WriteOnlyMap<String, String> writeOnlyMap = ...
ReadOnlyMap<String, String> readOnlyMap = ...

CompletableFuture<Void> writeFuture = writeOnlyMap.eval("key1", "value1",
   (v, view) -> view.set(v));
CompletableFuture<String> readFuture = writeFuture.thenCompose(r ->
   readOnlyMap.eval("key1", ReadEntryView::get));
readFuture.thenAccept(System.out::println);
