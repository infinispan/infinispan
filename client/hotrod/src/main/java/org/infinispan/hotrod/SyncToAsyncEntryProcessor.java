package org.infinispan.hotrod;

import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncCacheEntryProcessor;
import org.infinispan.api.common.MutableCacheEntry;
import org.infinispan.api.common.process.CacheEntryProcessorContext;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.sync.SyncCacheEntryProcessor;
import org.reactivestreams.FlowAdapters;

import io.reactivex.rxjava3.core.Flowable;

public class SyncToAsyncEntryProcessor<K, V, T> implements AsyncCacheEntryProcessor<K, V, T> {
   private final SyncCacheEntryProcessor<K, V, T> syncCacheEntryProcessor;

   public SyncToAsyncEntryProcessor(SyncCacheEntryProcessor<K, V, T> syncCacheEntryProcessor) {
      this.syncCacheEntryProcessor = syncCacheEntryProcessor;
   }

   @Override
   public Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Flow.Publisher<MutableCacheEntry<K, V>> entries, CacheEntryProcessorContext context) {
      Flowable<CacheEntryProcessorResult<K, T>> flowable = Flowable.fromPublisher(FlowAdapters.toPublisher(entries))
            .map(e -> {
               try {
                  return CacheEntryProcessorResult.onResult(e.key(), syncCacheEntryProcessor.process(e, context));
               } catch (Throwable t) {
                  return CacheEntryProcessorResult.onError(e.key(), t);
               }
            });
      return FlowAdapters.toFlowPublisher(flowable);
   }
}
