package org.infinispan.hotrod;

import java.util.Map;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.sync.SyncCacheEntryProcessor;
import org.infinispan.api.sync.SyncQuery;
import org.infinispan.api.sync.SyncQueryResult;
import org.infinispan.api.sync.events.cache.SyncCacheContinuousQueryListener;
import org.infinispan.hotrod.impl.cache.RemoteQuery;

/**
 * @since 14.0
 **/
public class HotRodSyncQuery<K, V, R> implements SyncQuery<K, V, R> {
   private final RemoteQuery query;

   HotRodSyncQuery(String query, CacheOptions options) {
      this.query = new RemoteQuery(query, options);
   }

   @Override
   public SyncQuery<K, V, R> param(String name, Object value) {
      query.param(name, value);
      return this;
   }

   @Override
   public SyncQuery<K, V, R> skip(long skip) {
      query.skip(skip);
      return this;
   }

   @Override
   public SyncQuery<K, V, R> limit(int limit) {
      query.limit(limit);
      return this;
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
