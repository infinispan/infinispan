package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

import org.infinispan.api.common.CacheEntry;

/**
 * Parameterized Query builder
 *
 * @param <R> the result type for the query
 * @since 14.0
 */
public interface AsyncQuery<R> {
   /**
    * Sets the named parameter to the specified value
    *
    * @param name
    * @param value
    * @return
    */
   AsyncQuery param(String name, Object value);

   /**
    * Skips the first specified number of results
    *
    * @param skip
    * @return
    */
   AsyncQuery skip(long skip);

   /**
    * Limits the number of results
    *
    * @param limit
    * @return
    */
   AsyncQuery limit(int limit);

   /**
    * Executes the query
    */
   Flow.Publisher<R> find();

   /**
    * Removes all entries which match the query.
    *
    * @return
    */
   CompletionStage<Void> remove();

   /**
    * Updates entries using a {@link Consumer}. If the cache is embedded, the consumer will be executed locally on the
    * owner of the entry. If the cache is remote, entries will be retrieved, manipulated locally and put back.
    */
   <K, V> CompletionStage<Void> update(Consumer<CacheEntry<K, V>> entryConsumer);

   /**
    * Updates entries using a named task. The task must be an EntryConsumerTask.
    */
   CompletionStage<Void> update(String taskName, Object... args);
}
