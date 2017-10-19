package org.infinispan.counter.impl.function;

import java.util.function.Function;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.metadata.ConfigurationMetadata;
import org.infinispan.functional.EntryView;

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
      ConfigurationMetadata metadata = entryView.findMetaParam(ConfigurationMetadata.class)
            .orElseGet(this::createMetadata);
      return apply(entryView, entryView.get(), metadata);
   }

   private R createAndApply(EntryView.ReadWriteEntryView<K, CounterValue> entryView) {
      CounterValue value = CounterValue.newCounterValue(configuration);
      return apply(entryView, value, createMetadata());
   }

   abstract R apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, CounterValue currentValue,
         ConfigurationMetadata metadata);

   private ConfigurationMetadata createMetadata() {
      return new ConfigurationMetadata(configuration);
   }
}
