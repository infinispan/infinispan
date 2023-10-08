package org.infinispan.counter.impl.function;

import java.util.function.Function;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * It removes the {@link CounterValue} from the cache.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_FUNCTION_REMOVE)
public class RemoveFunction<K extends CounterKey> implements
      Function<EntryView.ReadWriteEntryView<K, CounterValue>, Void> {

   private static final RemoveFunction INSTANCE = new RemoveFunction();

   private RemoveFunction() {
   }

   @ProtoFactory
   public static <K extends CounterKey> RemoveFunction<K> getInstance() {
      //noinspection unchecked
      return INSTANCE;
   }

   @Override
   public String toString() {
      return "RemoveFunction{}";
   }

   @Override
   public Void apply(EntryView.ReadWriteEntryView<K, CounterValue> entry) {
      if (entry.find().isPresent()) {
         entry.remove();
      }
      return null;
   }
}
