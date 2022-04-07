package org.infinispan.api.sync;

import java.util.Map;

import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.sync.events.cache.SyncCacheContinuousQueryListener;

/**
 * Parameterized Query builder
 *
 * @param <K>
 * @param <V>
 * @param <R> the result type for the query
 */
public interface SyncQuery<K, V, R> {
   /**
    * Sets the named parameter to the specified value
    *
    * @param name
    * @param value
    * @return
    */
   SyncQuery<K, V, R> param(String name, Object value);

   /**
    * Skips the first specified number of results
    *
    * @param skip
    * @return
    */
   SyncQuery<K, V, R> skip(long skip);

   /**
    * Limits the number of results
    *
    * @param limit
    * @return
    */
   SyncQuery<K, V, R> limit(int limit);

   /**
    * Executes the query
    */
   SyncQueryResult<R> find();

   /**
    * Continuously listen on query
    *
    * @param listener
    * @param <R>
    * @return A {@link AutoCloseable} that allows to remove the listener via {@link AutoCloseable#close()}.
    */
   <R> AutoCloseable findContinuously(SyncCacheContinuousQueryListener<K, V> listener);

   /**
    * Executes the manipulation statement (UPDATE, REMOVE)
    *
    * @return the number of entries that were processed
    */
   int execute();

   /**
    * Processes entries using an {@link SyncCacheEntryProcessor}. If the cache is embedded, the consumer will be executed
    * locally on the owner of the entry. If the cache is remote, entries will be retrieved, manipulated locally and put
    * back. The query <b>MUST NOT</b> use projections.
    *
    * @param processor the entry processor
    */
   default <T> Map<K, T> process(SyncCacheEntryProcessor<K, V, T> processor) {
      return process(processor, CacheProcessorOptions.DEFAULT);
   }

   /**
    * Processes entries using a {@link SyncCacheEntryProcessor}. If the cache is embedded, the consumer will be executed
    * locally on the owner of the entry. If the cache is remote, entries will be retrieved, manipulated locally and put
    * back. The query <b>MUST NOT</b> use projections.
    *
    * @param <T>
    * @param processor the entry processor
    * @param options
    */
   <T> Map<K, T> process(SyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options);

   /**
    * Processes entries matched by the query using a named {@link CacheProcessor}. The query <b>MUST NOT</b> use projections.
    * If the cache processor returns a non-null value for an entry, it will be returned as an entry of a {@link Map}.
    *
    * @param processor the entry processor
    * @return
    */
   default <T> Map<K, T> process(CacheProcessor processor) {
      return process(processor, CacheProcessorOptions.DEFAULT);
   }

   /**
    * Processes entries matched by the query using a named {@link CacheProcessor}. The query <b>MUST NOT</b> use projections.
    * If the cache processor returns a non-null value for an entry, it will be returned as an entry of a {@link Map}.
    *
    * @param <T>
    * @param processor the named entry processor
    * @param options
    * @return
    */
   <T> Map<K, T> process(CacheProcessor processor, CacheProcessorOptions options);
}
