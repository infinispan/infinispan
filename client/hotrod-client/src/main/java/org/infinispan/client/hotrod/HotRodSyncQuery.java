package org.infinispan.client.hotrod;

import java.util.Map;

import org.infinispan.api.Experimental;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.sync.SyncCacheEntryProcessor;
import org.infinispan.api.sync.SyncQuery;
import org.infinispan.api.sync.SyncQueryResult;
import org.infinispan.api.sync.events.cache.SyncCacheContinuousQueryListener;

@Experimental
final class HotRodSyncQuery<K, V, R> implements SyncQuery<K, V, R> {

   HotRodSyncQuery() { }

   @Override
   public SyncQuery<K, V, R> param(String name, Object value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public SyncQuery<K, V, R> skip(long skip) {
      throw new UnsupportedOperationException();
   }

   @Override
   public SyncQuery<K, V, R> limit(int limit) {
      throw new UnsupportedOperationException();
   }

   @Override
   public SyncQueryResult<R> find() {
      throw new UnsupportedOperationException();
   }

   @Override
   public <R1> AutoCloseable findContinuously(SyncCacheContinuousQueryListener<K, V> listener) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int execute() {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Map<K, T> process(SyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Map<K, T> process(CacheProcessor processor, CacheProcessorOptions options) {
      throw new UnsupportedOperationException();
   }
}
