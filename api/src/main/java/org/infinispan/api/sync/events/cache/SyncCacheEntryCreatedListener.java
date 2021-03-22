package org.infinispan.api.sync.events.cache;

import org.infinispan.api.common.events.cache.CacheEntryEvent;

/**
 * @since 14.0
 **/
public interface SyncCacheEntryCreatedListener<K, V> extends SyncCacheEntryListener<K, V> {
   void onCreate(CacheEntryEvent<K, V> event);
}
