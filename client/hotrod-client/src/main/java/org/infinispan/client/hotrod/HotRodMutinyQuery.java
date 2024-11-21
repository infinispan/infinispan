package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.common.events.cache.CacheContinuousQueryEvent;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.mutiny.MutinyCacheEntryProcessor;
import org.infinispan.api.mutiny.MutinyQuery;
import org.infinispan.api.mutiny.MutinyQueryResult;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

@Experimental
final class HotRodMutinyQuery<K, V, R> implements MutinyQuery<K, V, R> {
   @Override
   public MutinyQuery<K, V, R> param(String name, Object value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public MutinyQuery<K, V, R> skip(long skip) {
      throw new UnsupportedOperationException();
   }

   @Override
   public MutinyQuery<K, V, R> limit(int limit) {
      throw new UnsupportedOperationException();
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
