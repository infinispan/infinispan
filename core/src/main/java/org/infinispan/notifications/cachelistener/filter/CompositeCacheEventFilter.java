package org.infinispan.notifications.cachelistener.filter;

import org.infinispan.metadata.Metadata;

/**
 * Allows AND-composing several cache event filters.
 *
 * @author wburns
 * @since 7.0
 */
public class CompositeCacheEventFilter<K, V> implements CacheEventFilter<K, V> {
   private final CacheEventFilter<? super K, ? super V>[] filters;

   public CompositeCacheEventFilter(CacheEventFilter<? super K, ? super V>... filters) {
      this.filters = filters;
   }

   @Override
   public boolean accept(K key, V oldValue, Metadata oldMetadata, V newValue, Metadata newMetadata, EventType eventType) {
      for (CacheEventFilter<? super K, ? super V> k : filters)
         if (!k.accept(key, oldValue, oldMetadata, newValue, newMetadata, eventType)) return false;
      return true;
   }
}
