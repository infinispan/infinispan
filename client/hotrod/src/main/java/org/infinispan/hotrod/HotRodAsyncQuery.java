package org.infinispan.hotrod;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncCacheEntryProcessor;
import org.infinispan.api.async.AsyncQuery;
import org.infinispan.api.async.AsyncQueryResult;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.events.cache.CacheContinuousQueryEvent;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.hotrod.impl.cache.RemoteQuery;

/**
 * @since 14.0
 **/
public class HotRodAsyncQuery<K, V, R> implements AsyncQuery<K, V, R> {
   private final RemoteQuery query;

   HotRodAsyncQuery(String query, CacheOptions options) {
      this.query = new RemoteQuery(query, options);
   }

   @Override
   public AsyncQuery<K, V, R> param(String name, Object value) {
      query.param(name, value);
      return this;
   }

   @Override
   public AsyncQuery<K, V, R> skip(long skip) {
      query.skip(skip);
      return this;
   }

   @Override
   public AsyncQuery<K, V, R> limit(int limit) {
      query.limit(limit);
      return this;
   }

   @Override
   public CompletionStage<AsyncQueryResult<R>> find() {
      return null;
   }

   @Override
   public Flow.Publisher<CacheContinuousQueryEvent<K, R>> findContinuously(String query) {
      return null;
   }

   @Override
   public CompletionStage<Long> execute() {
      return null;
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(AsyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      return null;
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(CacheProcessor processor, CacheProcessorOptions options) {
      return null;
   }
}
