import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.FunctionalMap.ReadOnlyMap;

ReadOnlyMap<String, String> readOnlyMap = ...
CompletableFuture<Optional<String>> readFuture = readOnlyMap.eval("key1", ReadEntryView::find);
readFuture.thenAccept(System.out::println);
