package org.infinispan.api.sync.events.cache;

import org.infinispan.api.common.events.cache.CacheEntryEvent;

/**
 * @since 14.0
 **/
public interface SyncCacheEntryUpdatedListener<K, V> extends SyncCacheEntryListener<K, V> {
   void onUpdate(CacheEntryEvent<K, V> event);
}
