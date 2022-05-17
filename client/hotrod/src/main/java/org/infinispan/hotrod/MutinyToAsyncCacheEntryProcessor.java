package org.infinispan.hotrod;

import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncCacheEntryProcessor;
import org.infinispan.api.common.MutableCacheEntry;
import org.infinispan.api.common.process.CacheEntryProcessorContext;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.mutiny.MutinyCacheEntryProcessor;
import org.reactivestreams.FlowAdapters;

import io.smallrye.mutiny.Multi;

public class MutinyToAsyncCacheEntryProcessor<K, V, T> implements AsyncCacheEntryProcessor<K, V, T> {
   private final MutinyCacheEntryProcessor<K, V, T> mutinyCacheEntryProcessor;

   public MutinyToAsyncCacheEntryProcessor(MutinyCacheEntryProcessor<K, V, T> mutinyCacheEntryProcessor) {
      this.mutinyCacheEntryProcessor = mutinyCacheEntryProcessor;
   }

   @Override
   public Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Flow.Publisher<MutableCacheEntry<K, V>> entries, CacheEntryProcessorContext context) {
      return FlowAdapters.toFlowPublisher(mutinyCacheEntryProcessor.process(Multi.createFrom().publisher(FlowAdapters.toPublisher(entries)), context)
            .convert().toPublisher());
   }
}
