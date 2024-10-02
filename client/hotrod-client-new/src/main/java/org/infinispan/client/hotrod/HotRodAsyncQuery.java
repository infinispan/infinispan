package org.infinispan.client.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.Experimental;
import org.infinispan.api.async.AsyncCacheEntryProcessor;
import org.infinispan.api.async.AsyncQuery;
import org.infinispan.api.async.AsyncQueryResult;
import org.infinispan.api.common.events.cache.CacheContinuousQueryEvent;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;

@Experimental
final class HotRodAsyncQuery<K, V, R> implements AsyncQuery<K, V, R> {


   @Override
   public AsyncQuery<K, V, R> param(String name, Object value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public AsyncQuery<K, V, R> skip(long skip) {
      throw new UnsupportedOperationException();
   }

   @Override
   public AsyncQuery<K, V, R> limit(int limit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<AsyncQueryResult<R>> find() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Flow.Publisher<CacheContinuousQueryEvent<K, R>> findContinuously(String query) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Long> execute() {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(AsyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(CacheProcessor processor, CacheProcessorOptions options) {
      throw new UnsupportedOperationException();
   }
}
