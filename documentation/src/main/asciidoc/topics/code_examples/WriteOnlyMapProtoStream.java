import org.infinispan.functional.EntryView.*;
import org.infinispan.functional.FunctionalMap.*;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

WriteOnlyMap<String, String> writeOnlyMap = ...

// Force a function to be Serializable
Consumer<WriteEntryView<String>> function = new SetStringConstant<>("value1");
CompletableFuture<Void> writeFuture = writeOnlyMap.eval("key1", function);

class SetStringConstant implements Consumer<WriteEntryView<String>> {

   @ProtoField(1)
   final String target;

   @ProtoFactory
   public SetStringConstant(String target) {
      this.target = target;
   }

   @Override
   public void accept(WriteEntryView<String> view) {
      view.set(target);
   }
}
