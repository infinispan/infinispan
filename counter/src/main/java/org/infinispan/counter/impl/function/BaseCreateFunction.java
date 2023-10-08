package org.infinispan.counter.impl.function;

import java.util.function.Function;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.functional.impl.CounterConfigurationMetaParam;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * A base function to update a counter, even if it doesn't exist.
 * <p>
 * If the counter doesn't exist, it is created based on the configuration.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
abstract class BaseCreateFunction<K extends CounterKey, R> implements
      Function<EntryView.ReadWriteEntryView<K, CounterValue>, R> {

   @ProtoField(1)
   final CounterConfiguration configuration;

   BaseCreateFunction(CounterConfiguration configuration) {
      this.configuration = configuration;
   }

   @Override
   public final R apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView) {
      return entryView.find().isPresent() ?
             checkMetadataAndApply(entryView) :
             createAndApply(entryView);
   }

   private R checkMetadataAndApply(EntryView.ReadWriteEntryView<K, CounterValue> entryView) {
      CounterConfigurationMetaParam metadata = entryView.findMetaParam(CounterConfigurationMetaParam.class)
            .orElseGet(this::createMetadata);
      return apply(entryView, entryView.get(), metadata);
   }

   private R createAndApply(EntryView.ReadWriteEntryView<K, CounterValue> entryView) {
      CounterValue value = CounterValue.newCounterValue(configuration);
      return apply(entryView, value, createMetadata());
   }

   abstract R apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, CounterValue currentValue,
         CounterConfigurationMetaParam metadata);

   private CounterConfigurationMetaParam createMetadata() {
      return new CounterConfigurationMetaParam(configuration);
   }
}
