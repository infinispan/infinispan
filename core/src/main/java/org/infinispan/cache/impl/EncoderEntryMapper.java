package org.infinispan.cache.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.function.RemovableFunction;

/**
 * {@link java.util.function.Function} that uses an encoder to converter entries from the configured storage format to
 * the requested format.
 */
public class EncoderEntryMapper<K, V> implements RemovableFunction<CacheEntry<K, V>, CacheEntry<K, V>> {
   private transient InternalEntryFactory entryFactory;

   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;

   public EncoderEntryMapper(DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @Inject
   public void injectDependencies(ComponentRegistry registry, InternalEntryFactory factory) {
      this.entryFactory = factory;
      registry.wireDependencies(keyDataConversion);
      registry.wireDependencies(valueDataConversion);
   }

   private Object decode(Object o, Encoder encoder) {
      if (o == null) return null;
      return encoder.fromStorage(o);
   }

   @Override
   @SuppressWarnings("unchecked")
   public CacheEntry<K, V> apply(CacheEntry<K, V> e) {
      boolean keyFilterable = keyDataConversion.isStorageFormatFilterable();
      boolean valueFilterable = valueDataConversion.isStorageFormatFilterable();
      K key = e.getKey();
      Object unwrapped = keyDataConversion.getWrapper().unwrap(key);
      Object newKey = keyFilterable ? unwrapped : decode(unwrapped, keyDataConversion.getEncoder());
      V value = e.getValue();

      Object unwrappedValue = valueDataConversion.getWrapper().unwrap(value);
      Object newValue = valueFilterable ? unwrappedValue : decode(unwrappedValue, valueDataConversion.getEncoder());
      if (key != newKey || value != newValue) {
         return (CacheEntry<K, V>) entryFactory.create(newKey, newValue, e.getMetadata().version(), e.getCreated(),
               e.getLifespan(), e.getLastUsed(), e.getMaxIdle());
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
      public void writeObject(ObjectOutput output, EncoderEntryMapper object) throws IOException {
         output.writeObject(object.keyDataConversion);
         output.writeObject(object.valueDataConversion);
      }

      @Override
      @SuppressWarnings("unchecked")
      public EncoderEntryMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new EncoderEntryMapper((DataConversion) input.readObject(), (DataConversion) input.readObject());
      }
   }
}
