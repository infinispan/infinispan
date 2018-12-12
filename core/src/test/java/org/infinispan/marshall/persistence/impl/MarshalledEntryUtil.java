package org.infinispan.marshall.persistence.impl;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.MarshallableEntryFactory;

public class MarshalledEntryUtil {

   public static <K,V> MarshallableEntry<K,V> create(K key, V value, Cache cache) {
      return create(key, value, null, cache);
   }

   public static <K,V> MarshallableEntry<K,V> create(K key, V value, InternalMetadata im, Cache cache) {
      MarshallableEntryFactory entryFactory = cache.getAdvancedCache().getComponentRegistry().getComponent(MarshallableEntryFactory.class);
      return entryFactory.create(key, value, im);
   }

   public static <K,V> MarshallableEntry<K,V> create(K key, Marshaller m) {
      return create(key, (V) null, m);
   }

   public static <K,V> MarshallableEntry<K,V> create(K key, V value, Marshaller m) {
      return new MarshalledEntryImpl<>(key, value, null, m);
   }

   public static <K, V> MarshallableEntry<K, V> create(InternalCacheEntry<K, V> ice, Marshaller m) {
      return new MarshalledEntryImpl<>(ice.getKey(), ice.getValue(), PersistenceUtil.internalMetadata(ice), m);
   }
}
