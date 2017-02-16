package org.infinispan.compat;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.function.RemovableFunction;

/**
 * Implementation of {@link java.util.function.Function} that uses a converter utilizing the unbox methods.
 * @author wburns
 * @since 9.0
 */
public class ConverterEntryMapper<K, V> implements RemovableFunction<CacheEntry<K, V>, CacheEntry<K, V>> {
   private transient InternalEntryFactory entryFactory;
   private transient TypeConverter converter;

   @Inject
   public void injectFactory(InternalEntryFactory factory, TypeConverter converter) {
      this.entryFactory = factory;
      this.converter = converter;
   }

   @Override
   public CacheEntry<K, V> apply(CacheEntry<K, V> e) {
      K key = e.getKey();
      Object newKey = converter.unboxKey(key);
      V value = e.getValue();
      Object newValue = converter.unboxValue(value);
      if (key != newKey || value != newValue) {
         return (CacheEntry<K, V>) entryFactory.create(newKey, newValue, e.getMetadata().version(), e.getCreated(),
               e.getLifespan(), e.getLastUsed(), e.getMaxIdle());
      }
      return e;
   }

   public static class Externalizer implements AdvancedExternalizer<ConverterEntryMapper> {

      @Override
      public Set<Class<? extends ConverterEntryMapper>> getTypeClasses() {
         return Collections.singleton(ConverterEntryMapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.CONVERTER_ENTRY_MAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, ConverterEntryMapper object) throws IOException {

      }

      @Override
      public ConverterEntryMapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ConverterEntryMapper();
      }
   }
}
