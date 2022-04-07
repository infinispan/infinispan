package org.infinispan.api.async;

import java.util.concurrent.Flow;

import org.infinispan.api.common.MutableCacheEntry;
import org.infinispan.api.common.process.CacheEntryProcessorContext;
import org.infinispan.api.common.process.CacheEntryProcessorResult;

/**
 * @since 14.0
 **/
@FunctionalInterface
public interface AsyncCacheEntryProcessor<K, V, T> {
   Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Flow.Publisher<MutableCacheEntry<K, V>> entries, CacheEntryProcessorContext context);
}
