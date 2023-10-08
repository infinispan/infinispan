package org.infinispan.cache.impl;

import java.util.Map;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * {@link java.util.function.Function} that uses an encoder to converter entries from the configured storage format to
 * the requested format.
 */
@ProtoTypeId(ProtoStreamTypeIds.ENCODER_ENTRY_MAPPER)
@Scope(Scopes.NAMED_CACHE)
public class EncoderEntryMapper<K, V, T extends Map.Entry<K, V>> implements EncodingFunction<T>, InjectableComponent {
   @Inject
   transient InternalEntryFactory entryFactory;

   @ProtoField(1)
   final DataConversion keyDataConversion;

   @ProtoField(2)
   final DataConversion valueDataConversion;

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

   @ProtoFactory
   EncoderEntryMapper(DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @Inject
   public void injectDependencies(ComponentRegistry registry) {
      registry.wireDependencies(keyDataConversion);
      registry.wireDependencies(valueDataConversion);
   }

   @Override
   public void inject(ComponentRegistry registry) {
      injectDependencies(registry);
      entryFactory = registry.getInternalEntryFactory().running();
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
}
