package org.infinispan.embedded;

import java.util.concurrent.TimeUnit;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.events.cache.CacheContinuousQueryEvent;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.mutiny.MutinyCacheEntryProcessor;
import org.infinispan.api.mutiny.MutinyQuery;
import org.infinispan.api.mutiny.MutinyQueryResult;
import org.infinispan.commons.api.query.Query;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @param <K>
 * @param <V>
 * @param <R>
 * @since 15.0
 */
public class EmbeddedMutinyQuery<K, V, R> implements MutinyQuery<K, V, R> {
   private final Query<R> query;

   EmbeddedMutinyQuery(Query<R> query, CacheOptions options) {
      this.query = query;
      options.timeout().ifPresent(d -> query.timeout(d.toNanos(), TimeUnit.NANOSECONDS));
   }

   @Override
   public MutinyQuery<K, V, R> param(String name, Object value) {
      query.setParameter(name, value);
      return this;
   }

   @Override
   public MutinyQuery<K, V, R> skip(long skip) {
      query.startOffset(skip);
      return this;
   }

   @Override
   public MutinyQuery<K, V, R> limit(int limit) {
      query.maxResults(limit);
      return this;
   }

   @Override
   public Uni<MutinyQueryResult<R>> find() {
      return null;
   }

   @Override
   public <R1> Multi<CacheContinuousQueryEvent<K, R1>> findContinuously() {
      return null;
   }

   @Override
   public Uni<Long> execute() {
      return null;
   }

   @Override
   public <T> Multi<CacheEntryProcessorResult<K, T>> process(MutinyCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      return null;
   }

   @Override
   public <T> Multi<CacheEntryProcessorResult<K, T>> process(CacheProcessor processor, CacheProcessorOptions options) {
      return null;
   }
}
