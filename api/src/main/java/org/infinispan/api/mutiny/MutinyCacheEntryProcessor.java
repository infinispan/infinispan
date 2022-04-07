package org.infinispan.api.mutiny;

import org.infinispan.api.common.MutableCacheEntry;
import org.infinispan.api.common.process.CacheEntryProcessorContext;
import org.infinispan.api.common.process.CacheEntryProcessorResult;

import io.smallrye.mutiny.Multi;

/**
 * @since 14.0
 **/
@FunctionalInterface
public interface MutinyCacheEntryProcessor<K, V, T> {
   Multi<CacheEntryProcessorResult<K, T>> process(Multi<MutableCacheEntry<K,V>> entries, CacheEntryProcessorContext context);
}
