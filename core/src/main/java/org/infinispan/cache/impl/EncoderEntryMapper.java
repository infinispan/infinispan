package org.infinispan.cache.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.util.function.RemovableFunction;

/**
 * {@link java.util.function.Function} that uses an encoder to converter entries from the configured storage format to
 * the requested format.
 */
public class EncoderEntryMapper<K, V> implements RemovableFunction<CacheEntry<K, V>, CacheEntry<K, V>> {
   private transient InternalEntryFactory entryFactory;

   private final Class<? extends Encoder> keyEncoderClass;
   private final Class<? extends Encoder> valueEncoderClass;
   private final Class<? extends Wrapper> keyWrapperClass;
   private final Class<? extends Wrapper> valueWrapperClass;
   private transient Encoder keyEncoder;
   private transient Encoder valueEncoder;
   private transient Wrapper keyWrapper;
   private transient Wrapper valueWrapper;

   public EncoderEntryMapper(Class<? extends Encoder> keyEncoderClass,
                             Class<? extends Encoder> valueEncoderClass,
                             Class<? extends Wrapper> keyWrapperClass,
                             Class<? extends Wrapper> valueWrapperClass) {
      this.keyEncoderClass = keyEncoderClass;
      this.valueEncoderClass = valueEncoderClass;
      this.keyWrapperClass = keyWrapperClass;
      this.valueWrapperClass = valueWrapperClass;
   }

   @Inject
   public void injectDependencies(EncoderRegistry encoderRegistry, InternalEntryFactory factory) {
      this.entryFactory = factory;
      this.keyEncoder = encoderRegistry.getEncoder(keyEncoderClass);
      this.valueEncoder = encoderRegistry.getEncoder(valueEncoderClass);
      this.keyWrapper = encoderRegistry.getWrapper(keyWrapperClass);
      this.valueWrapper = encoderRegistry.getWrapper(valueWrapperClass);
   }

   private Object decode(Object o, Encoder encoder) {
      if (o == null) return null;
      return encoder.fromStorage(o);
   }

   @Override
   @SuppressWarnings("unchecked")
   public CacheEntry<K, V> apply(CacheEntry<K, V> e) {
      boolean keyFilterable = keyEncoder.isStorageFormatFilterable();
      boolean valueFilterable = valueEncoder.isStorageFormatFilterable();
      K key = e.getKey();
      Object unwrapped = keyWrapper.unwrap(key);
      Object newKey = keyFilterable ? unwrapped : decode(unwrapped, keyEncoder);
      V value = e.getValue();

      Object unwrappedValue = valueWrapper.unwrap(value);
      Object newValue = valueFilterable ? unwrappedValue : decode(unwrappedValue, valueEncoder);
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
         output.writeObject(object.keyEncoderClass);
         output.writeObject(object.valueEncoderClass);
         output.writeObject(object.keyWrapperClass);
         output.writeObject(object.valueWrapperClass);
      }

      @Override
      @SuppressWarnings("unchecked")
      public EncoderEntryMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new EncoderEntryMapper((Class<? extends Encoder>) input.readObject(),
               (Class<? extends Encoder>) input.readObject(), (Class<? extends Wrapper>) input.readObject(),
               (Class<? extends Wrapper>) input.readObject());
      }
   }
}
