package org.infinispan.notifications.cachelistener.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.metadata.Metadata;

/**
 * {@link CacheEventFilterConverter} that uses an underlying {@link KeyValueFilterConverter} to do the conversion and
 * filtering. The new value and metadata are used as arguments to the underlying filter converter as it doesn't take
 * both new and old.
 * @author wburns
 * @since 9.4
 */
public class KeyValueFilterConverterAsCacheEventFilterConverter<K, V, C> implements CacheEventFilterConverter<K, V, C> {
   private final KeyValueFilterConverter<K, V, C> keyValueFilterConverter;

   public KeyValueFilterConverterAsCacheEventFilterConverter(KeyValueFilterConverter<K, V, C> keyValueFilterConverter) {
      this.keyValueFilterConverter = keyValueFilterConverter;
   }

   @Override
   public C filterAndConvert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return keyValueFilterConverter.convert(key, newValue, newMetadata);
   }

   @Override
   public C convert(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return keyValueFilterConverter.convert(key, newValue, newMetadata);
   }

   @Override
   public boolean accept(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      return keyValueFilterConverter.accept(key, newValue, newMetadata);
   }

   @Inject
   protected void injectDependencies(ComponentRegistry cr) {
      cr.wireDependencies(keyValueFilterConverter);
   }

   public static class Externalizer implements AdvancedExternalizer<KeyValueFilterConverterAsCacheEventFilterConverter> {
      @Override
      public void writeObject(ObjectOutput output, KeyValueFilterConverterAsCacheEventFilterConverter object) throws IOException {
         output.writeObject(object.keyValueFilterConverter);
      }

      @Override
      public KeyValueFilterConverterAsCacheEventFilterConverter readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new KeyValueFilterConverterAsCacheEventFilterConverter((KeyValueFilterConverter)input.readObject());
      }

      @Override
      public Set<Class<? extends KeyValueFilterConverterAsCacheEventFilterConverter>> getTypeClasses() {
         return Collections.singleton(KeyValueFilterConverterAsCacheEventFilterConverter.class);
      }

      @Override
      public Integer getId() {
         return Ids.KEY_VALUE_FILTER_CONVERTER_AS_CACHE_EVENT_FILTER_CONVERTER;
      }
   }
}
