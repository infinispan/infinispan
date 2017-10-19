package org.infinispan.counter.impl.function;

import java.util.Optional;
import java.util.function.Function;

import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.metadata.ConfigurationMetadata;
import org.infinispan.counter.logging.Log;
import org.infinispan.functional.EntryView;

/**
 * A base function to update an existing counter.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
abstract class BaseFunction<K extends CounterKey, R> implements
      Function<EntryView.ReadWriteEntryView<K, CounterValue>, R> {

   @Override
   public final R apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView) {
      Optional<CounterValue> value = entryView.find();
      if (!value.isPresent()) {
         return null;
      }
      Optional<ConfigurationMetadata> metadata = entryView.findMetaParam(ConfigurationMetadata.class);
      if (!metadata.isPresent()) {
         throw getLog().metadataIsMissing(entryView.key().getCounterName());
      }
      return apply(entryView, metadata.get());
   }

   abstract R apply(EntryView.ReadWriteEntryView<K, CounterValue> entryView, ConfigurationMetadata metadata);

   protected abstract Log getLog();
}
