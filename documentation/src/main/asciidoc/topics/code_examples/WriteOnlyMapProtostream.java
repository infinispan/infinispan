import org.infinispan.functional.EntryView.*;
import org.infinispan.functional.FunctionalMap.*;
import org.infinispan.protostream.annotations.Proto;

WriteOnlyMap<String, String> writeOnlyMap = ...

Consumer<WriteEntryView<String>> function = new SetStringConstant<>();
CompletableFuture<Void> writeFuture = writeOnlyMap.eval("key1", function);

@Proto
class SetStringConstant implements Consumer<WriteEntryView<String>> {
   @Override
   public void accept(WriteEntryView<String> view) {
      view.set("value1");
   }
}
