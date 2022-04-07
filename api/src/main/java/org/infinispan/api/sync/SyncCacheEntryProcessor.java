package org.infinispan.api.sync;

import org.infinispan.api.common.MutableCacheEntry;
import org.infinispan.api.common.process.CacheEntryProcessorContext;

/**
 * @since 14.0
 **/
@FunctionalInterface
public interface SyncCacheEntryProcessor<K, V, T> {
   T process(MutableCacheEntry<K, V> entry, CacheEntryProcessorContext context);
}
