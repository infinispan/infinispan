package org.infinispan.embedded;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import org.infinispan.api.async.AsyncCacheEntryProcessor;
import org.infinispan.api.async.AsyncQuery;
import org.infinispan.api.async.AsyncQueryResult;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.events.cache.CacheContinuousQueryEvent;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.commons.api.query.Query;

/**
 * @since 15.0
 */
public class EmbeddedAsyncQuery<K, V, R> implements AsyncQuery<K, V, R> {
   private final Query<R> query;

   EmbeddedAsyncQuery(Query<R> query, CacheOptions options) {
      this.query = query;
      options.timeout().ifPresent(d -> query.timeout(d.toNanos(), TimeUnit.NANOSECONDS));
   }

   @Override
   public AsyncQuery<K, V, R> param(String name, Object value) {
      query.setParameter(name, value);
      return this;
   }

   @Override
   public AsyncQuery<K, V, R> skip(long skip) {
      query.startOffset(skip);
      return this;
   }

   @Override
   public AsyncQuery<K, V, R> limit(int limit) {
      query.maxResults(limit);
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
