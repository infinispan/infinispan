package org.infinispan.counter.impl.listener;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;
import org.infinispan.counter.impl.externalizers.ExternalizerIds;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.util.ByteString;

/**
 * A {@link org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter} to produce events for a
 * specific counter.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class CounterFilterAndConvert<T extends CounterKey> extends
      AbstractCacheEventFilterConverter<T, CounterValue, CounterValue> {

   public static final AdvancedExternalizer<CounterFilterAndConvert> EXTERNALIZER = new Externalizer();
   private final ByteString counterName;

   public CounterFilterAndConvert(ByteString counterName) {
      this.counterName = Objects.requireNonNull(counterName);
   }

   @Override
   public CounterValue filterAndConvert(T key, CounterValue oldValue, Metadata oldMetadata, CounterValue newValue,
         Metadata newMetadata, EventType eventType) {
      if (this.counterName.equals(key.getCounterName()) && newValue != null && eventType.isModified()) {
         return newValue;
      }
      return null;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      CounterFilterAndConvert<?> that = (CounterFilterAndConvert<?>) o;

      return counterName.equals(that.counterName);
   }

   @Override
   public int hashCode() {
      return counterName.hashCode();
   }

   @Override
   public String toString() {
      return "CounterFilterAndConvert{" +
            "counterName=" + counterName +
            '}';
   }

   private static class Externalizer implements AdvancedExternalizer<CounterFilterAndConvert> {

      @Override
      public Set<Class<? extends CounterFilterAndConvert>> getTypeClasses() {
         return Collections.singleton(CounterFilterAndConvert.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CONVERTER_AND_FILTER;
      }

      @Override
      public void writeObject(ObjectOutput output, CounterFilterAndConvert object) throws IOException {
         ByteString.writeObject(output, object.counterName);
      }

      @Override
      public CounterFilterAndConvert readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CounterFilterAndConvert(ByteString.readObject(input));
      }
   }
}
