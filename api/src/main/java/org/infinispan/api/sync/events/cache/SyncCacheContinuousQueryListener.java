package org.infinispan.api.sync.events.cache;

import org.infinispan.api.common.events.cache.CacheEntryEvent;

/**
 * @since 14.0
 **/
public interface SyncCacheContinuousQueryListener<K, V> extends SyncCacheEntryListener<K, V> {
   default void onJoin(CacheEntryEvent<K, V> event) {
   }

   default void onLeave(CacheEntryEvent<K, V> event) {
   }

   default void onUpdate(CacheEntryEvent<K, V> event) {
   }
}
