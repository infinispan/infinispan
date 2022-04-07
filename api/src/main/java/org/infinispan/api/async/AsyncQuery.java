package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.common.events.cache.CacheContinuousQueryEvent;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.common.process.CacheProcessorOptions;

/**
 * Parameterized Query builder
 *
 * @param <K>
 * @param <V>
 * @param <R> the result type for the query
 * @since 14.0
 */
public interface AsyncQuery<K, V, R> {
   /**
    * Sets the named parameter to the specified value
    *
    * @param name
    * @param value
    * @return
    */
   AsyncQuery<K, V, R> param(String name, Object value);

   /**
    * Skips the first specified number of results
    *
    * @param skip
    * @return
    */
   AsyncQuery<K, V, R> skip(long skip);

   /**
    * Limits the number of results
    *
    * @param limit
    * @return
    */
   AsyncQuery<K, V, R> limit(int limit);

   /**
    * Executes the query
    */
   CompletionStage<AsyncQueryResult<R>> find();

   /**
    * Executes the query and returns a {@link java.util.concurrent.Flow.Publisher} with the results
    *
    * @param query query String
    * @return a {@link Flow.Publisher} which produces {@link CacheContinuousQueryEvent} items.
    */
   Flow.Publisher<CacheContinuousQueryEvent<K, R>> findContinuously(String query);

   /**
    * Executes the manipulation statement (UPDATE, REMOVE)
    *
    * @return the number of entries that were processed
    */
   CompletionStage<Long> execute();

   /**
    * @param <T>
    * @param processor the entry processor task
    * @return
    */
   default <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(AsyncCacheEntryProcessor<K, V, T> processor) {
      return process(processor, CacheProcessorOptions.DEFAULT);
   }

   /**
    * @param <T>
    * @param processor the entry processor task
    * @param options
    * @return
    */
   <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(AsyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options);

   /**
    * Processes entries matched by the query using a named {@link CacheProcessor}. The query <b>MUST NOT</b> use
    * projections. If the cache processor returns a non-null value for an entry, it will be returned through the
    * publisher.
    *
    * @param <T>
    * @param processor the entry processor
    * @return
    */
   default <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(CacheProcessor processor) {
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
   <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(CacheProcessor processor, CacheProcessorOptions options);
}
