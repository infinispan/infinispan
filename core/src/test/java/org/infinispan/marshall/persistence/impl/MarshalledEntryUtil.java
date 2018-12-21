package org.infinispan.marshall.persistence.impl;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;

public class MarshalledEntryUtil {

   public static <K,V> MarshallableEntry<K,V> create(K key, V value, Cache cache) {
      return create(key, value, null, cache);
   }

   public static <K,V> MarshallableEntry<K,V> create(K key, V value, Metadata metadata, Cache cache) {
      return create(key, value, metadata, -1, -1, cache);
   }

   public static <K,V> MarshallableEntry<K,V> create(K key, V value, Metadata metadata, long created, long lastUsed, Cache cache) {
      MarshallableEntryFactory entryFactory = cache.getAdvancedCache().getComponentRegistry().getComponent(MarshallableEntryFactory.class);
      return entryFactory.create(key, value, metadata, created, lastUsed);
   }

   public static <K,V> MarshallableEntry<K,V> create(K key, Marshaller m) {
      return create(key, null, m);
   }

   public static <K,V> MarshallableEntry<K,V> create(K key, V value, Marshaller m) {
      return create(key, value, null, -1, -1, m);
   }

   public static <K,V> MarshallableEntry<K,V> create(K key, V value, Metadata metadata, long created, long lastUsed, Marshaller m) {
      return new MarshallableEntryImpl<>(key, value, metadata, created, lastUsed, m);
   }

   public static <K, V> MarshallableEntry<K, V> create(InternalCacheEntry<K, V> ice, Marshaller m) {
      return new MarshallableEntryImpl<>(ice.getKey(), ice.getValue(), ice.getMetadata(), ice.getCreated(), ice.getLastUsed(), m);
   }
}
