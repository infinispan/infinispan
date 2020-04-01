import org.infinispan.functional.EntryView.*;
import org.infinispan.functional.FunctionalMap.*;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeFunctionWith;

WriteOnlyMap<String, String> writeOnlyMap = ...

// Force a function to be Serializable
Consumer<WriteEntryView<String>> function = new SetStringConstant<>();
CompletableFuture<Void> writeFuture = writeOnlyMap.eval("key1", function);

@SerializeFunctionWith(value = SetStringConstant.Externalizer0.class)
class SetStringConstant implements Consumer<WriteEntryView<String>> {
   @Override
   public void accept(WriteEntryView<String> view) {
      view.set("value1");
   }

   public static final class Externalizer0 implements Externalizer<Object> {
      public void writeObject(ObjectOutput oo, Object o) {
         // No-op
      }
      public Object readObject(ObjectInput input) {
         return new SetStringConstant<>();
      }
   }
}
