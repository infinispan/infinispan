package org.infinispan.counter.impl.function;

import java.util.function.Function;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Read function that returns the current counter's delta.
 * <p>
 * Singleton class. Use {@link ReadFunction#getInstance()} to retrieve it.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_FUNCTION_READ)
public class ReadFunction<K> implements Function<EntryView.ReadEntryView<K, CounterValue>, Long> {

   private static final ReadFunction INSTANCE = new ReadFunction();

   private ReadFunction() {
   }

   @ProtoFactory
   public static <K> ReadFunction<K> getInstance() {
      //noinspection unchecked
      return INSTANCE;
   }

   @Override
   public String toString() {
      return "ReadFunction{}";
   }

   @Override
   public Long apply(EntryView.ReadEntryView<K, CounterValue> view) {
      return view.find().map(CounterValue::getValue).orElse(null);
   }
}
