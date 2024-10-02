package org.infinispan.client.hotrod.impl;

import java.util.function.Function;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.cache.CacheEntryImpl;
import org.infinispan.client.hotrod.impl.cache.CacheEntryMetadataImpl;

@SuppressWarnings({"unchecked", "rawtypes"})
final class CacheEntryConversion {

   private CacheEntryConversion() { }

   private static final Function READ_VALUE = entry -> entry != null
         ? ((MetadataValue<Object>) entry).getValue()
         : null;

   static <V> Function<MetadataValue<V>, V> extractValue() {
      return (Function<MetadataValue<V>, V>) READ_VALUE;
   }

   static <K, V> Function<MetadataValue<V>, CacheEntry<K, V>> createCacheEntry(K key) {
      return metadata -> metadata == null
            ? null
            : new CacheEntryImpl<>(key, metadata.getValue(), new CacheEntryMetadataImpl<>(metadata));
   }
}
