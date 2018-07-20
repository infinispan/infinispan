package org.infinispan.cache.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.InjectiveFunction;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;

/**
 * {@link java.util.function.Function} that uses an encoder to converter entries from the configured storage format to
 * the requested format.
 */
public class EncoderEntryMapper<K, V, T extends Map.Entry<K, V>> implements InjectiveFunction<T, T> {
   @Inject
   private transient InternalEntryFactory entryFactory;

   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;

   public static <K, V> EncoderEntryMapper<K, V, Map.Entry<K, V>> newEntryMapper(DataConversion keyDataConversion,
                                                                                 DataConversion valueDataConversion, InternalEntryFactory entryFactory) {
      EncoderEntryMapper<K, V, Map.Entry<K, V>> mapper = new EncoderEntryMapper<>(keyDataConversion, valueDataConversion);
      mapper.entryFactory = entryFactory;
      return mapper;
   }

   public static <K, V> EncoderEntryMapper<K, V, CacheEntry<K, V>> newCacheEntryMapper(
         DataConversion keyDataConversion, DataConversion valueDataConversion, InternalEntryFactory entryFactory) {
      EncoderEntryMapper<K, V, CacheEntry<K, V>> mapper = new EncoderEntryMapper<>(keyDataConversion, valueDataConversion);
      mapper.entryFactory = entryFactory;
      return mapper;
   }

   private EncoderEntryMapper(DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @Inject
   public void injectDependencies(ComponentRegistry registry) {
      registry.wireDependencies(keyDataConversion);
      registry.wireDependencies(valueDataConversion);
   }

   @Override
   public T apply(T e) {
      K key = e.getKey();
      V value = e.getValue();
      Object newKey = keyDataConversion.fromStorage(key);
      Object newValue = valueDataConversion.fromStorage(value);
      if (key != newKey || value != newValue) {
         if (e instanceof CacheEntry) {
            CacheEntry<K, V> ce = (CacheEntry<K, V>) e;
            return (T) entryFactory.create(newKey, newValue, ce.getMetadata().version(), ce.getCreated(),
                  ce.getLifespan(), ce.getLastUsed(), ce.getMaxIdle());
         } else {
            return (T) entryFactory.create(newKey, newValue, (Metadata) null);
         }
      }
      return e;
   }

   public static class Externalizer implements AdvancedExternalizer<EncoderEntryMapper> {

      @Override
      public Set<Class<? extends EncoderEntryMapper>> getTypeClasses() {
         return Collections.singleton(EncoderEntryMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.ENCODER_ENTRY_MAPPER;
      }

      @Override
      public void writeObject(UserObjectOutput output, EncoderEntryMapper object) throws IOException {
         DataConversion.writeTo(output, object.keyDataConversion);
         DataConversion.writeTo(output, object.valueDataConversion);
      }

      @Override
      @SuppressWarnings("unchecked")
      public EncoderEntryMapper readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new EncoderEntryMapper(DataConversion.readFrom(input), DataConversion.readFrom(input));
      }
   }
}
