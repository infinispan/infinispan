package org.infinispan.hotrod;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.events.cache.CacheContinuousQueryEvent;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.mutiny.MutinyCacheEntryProcessor;
import org.infinispan.api.mutiny.MutinyQuery;
import org.infinispan.api.mutiny.MutinyQueryResult;
import org.infinispan.hotrod.impl.cache.RemoteQuery;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyQuery<K, V, R> implements MutinyQuery<K, V, R> {

   private final RemoteQuery query;

   HotRodMutinyQuery(String query, CacheOptions options) {
      this.query = new RemoteQuery(query, options);
   }

   @Override
   public MutinyQuery<K, V, R> param(String name, Object value) {
      query.param(name, value);
      return this;
   }

   @Override
   public MutinyQuery<K, V, R> skip(long skip) {
      query.skip(skip);
      return this;
   }

   @Override
   public MutinyQuery<K, V, R> limit(int limit) {
      query.limit(limit);
      return this;
   }

   @Override
   public Uni<MutinyQueryResult<R>> find() {
      throw new UnsupportedOperationException();
   }

   @Override
   public <R1> Multi<CacheContinuousQueryEvent<K, R1>> findContinuously() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Long> execute() {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Multi<CacheEntryProcessorResult<K, T>> process(MutinyCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Multi<CacheEntryProcessorResult<K, T>> process(CacheProcessor processor, CacheProcessorOptions options) {
      throw new UnsupportedOperationException();
   }
}
