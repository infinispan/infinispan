package org.infinispan.embedded;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.sync.SyncCacheEntryProcessor;
import org.infinispan.api.sync.SyncQuery;
import org.infinispan.api.sync.SyncQueryResult;
import org.infinispan.api.sync.events.cache.SyncCacheContinuousQueryListener;
import org.infinispan.commons.api.query.Query;

/**
 * @since 15.0
 */
public class EmbeddedSyncQuery<K, V, R> implements SyncQuery<K, V, R> {
   private final Query<R> query;

   EmbeddedSyncQuery(Query<R> query, CacheOptions options) {
      this.query = query;
      options.timeout().ifPresent(d -> query.timeout(d.toNanos(), TimeUnit.NANOSECONDS));
   }

   @Override
   public SyncQuery<K, V, R> param(String name, Object value) {
      query.setParameter(name, value);
      return this;
   }

   @Override
   public SyncQuery<K, V, R> skip(long skip) {
      query.startOffset(skip);
      return this;
   }

   @Override
   public SyncQuery<K, V, R> limit(int limit) {
      query.maxResults(limit);
      return this;
   }

   @Override
   public SyncQueryResult<R> find() {
      return new EmbeddedSyncQueryResult<>(query.execute());
   }

   @Override
   public <R1> AutoCloseable findContinuously(SyncCacheContinuousQueryListener<K, V> listener) {
      return null;
   }

   @Override
   public int execute() {
      return query.executeStatement();
   }

   @Override
   public <T> Map<K, T> process(SyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      return null;
   }

   @Override
   public <T> Map<K, T> process(CacheProcessor processor, CacheProcessorOptions options) {
      return null;
   }
}
