package org.infinispan.api.mutiny;

import org.infinispan.api.common.events.cache.CacheContinuousQueryEvent;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * Parameterized Query builder
 *
 * @param <K>
 * @param <V>
 * @param <R> the result type for the query
 */
public interface MutinyQuery<K, V, R> {
   /**
    * Sets the named parameter to the specified value
    *
    * @param name
    * @param value
    * @return
    */
   MutinyQuery<K, V, R> param(String name, Object value);

   /**
    * Skips the first specified number of results
    *
    * @param skip
    * @return
    */
   MutinyQuery<K, V, R> skip(long skip);

   /**
    * Limits the number of results
    *
    * @param limit
    * @return
    */
   MutinyQuery<K, V, R> limit(int limit);

   /**
    * Executes the query
    */
   Uni<MutinyQueryResult<R>> find();

   /**
    * Executes the query and returns a {@link Multi} with the results
    *
    * @return a {@link Multi} which produces {@link CacheContinuousQueryEvent} items.
    */
   <R> Multi<CacheContinuousQueryEvent<K, R>> findContinuously();

   /**
    * Executes the manipulation statement (UPDATE, REMOVE)
    *
    * @return the number of entries that were processed
    */
   Uni<Long> execute();

   /**
    * Processes entries matched by the query using a {@link MutinyCacheEntryProcessor}. The query <b>MUST NOT</b>
    * use projections. If the cache is remote, entries will be retrieved, manipulated locally and put back. The query
    * <b>MUST NOT</b> use projections.
    *
    * @param processor the entry consumer task
    */
   default <T> Multi<CacheEntryProcessorResult<K, T>> process(MutinyCacheEntryProcessor<K, V, T> processor) {
      return process(processor, CacheProcessorOptions.DEFAULT);
   }

   /**
    * Processes entries matched by the query using a {@link MutinyCacheEntryProcessor}. The query <b>MUST NOT</b>
    * use projections. If the cache is remote, entries will be retrieved, manipulated locally and put back. The query
    * <b>MUST NOT</b> use projections.
    *
    * @param processor the entry consumer task
    */
   <T> Multi<CacheEntryProcessorResult<K, T>> process(MutinyCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options);


   /**
    * Processes entries matched by the query using a named {@link CacheProcessor}. The query <b>MUST NOT</b> use
    * projections. If the cache processor returns a non-null value for an entry, it will be returned through the
    * publisher.
    *
    * @param <T>
    * @param processor the entry processor
    * @return
    */
   default <T> Multi<CacheEntryProcessorResult<K, T>> process(CacheProcessor processor) {
      return process(processor, CacheProcessorOptions.DEFAULT);
   }

   /**
    * Processes entries matched by the query using a named {@link CacheProcessor}. The query <b>MUST NOT</b> use
    * projections. If the cache processor returns a non-null value for an entry, it will be returned through the
    * publisher.
    *
    * @param <T>
    * @param processor the named entry processor
    * @param options
    * @return
    */
   <T> Multi<CacheEntryProcessorResult<K, T>> process(CacheProcessor processor, CacheProcessorOptions options);
}
