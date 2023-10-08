package org.infinispan.counter.impl.function;

import static org.infinispan.counter.impl.entries.CounterValue.newCounterValue;

import java.util.Optional;
import java.util.function.Function;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.impl.CounterConfigurationMetaParam;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Function that initializes the {@link CounterValue} and {@link CounterConfigurationMetaParam} if they don't exists.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COUNTER_FUNCTION_INITIALIZE_COUNTER)
public class InitializeCounterFunction<K extends CounterKey> implements
      Function<EntryView.ReadWriteEntryView<K, CounterValue>, CounterValue> {

   @ProtoField(1)
   final CounterConfiguration counterConfiguration;

   @ProtoFactory
   public InitializeCounterFunction(CounterConfiguration counterConfiguration) {
      this.counterConfiguration = counterConfiguration;
   }

   @Override
   public CounterValue apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView) {
      Optional<CounterValue> currentValue = entryView.find();
      if (currentValue.isPresent()) {
         return currentValue.get();
      }
      CounterValue newValue = newCounterValue(counterConfiguration);
      entryView.set(newValue, new CounterConfigurationMetaParam(counterConfiguration));
      return newValue;
   }

   @Override
   public String toString() {
      return "InitializeCounterFunction{" +
            "counterConfiguration=" + counterConfiguration +
            '}';
   }
}
