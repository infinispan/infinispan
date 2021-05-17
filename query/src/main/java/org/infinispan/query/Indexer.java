package org.infinispan.query;

import java.util.concurrent.CompletionStage;

/**
 * Interacts directly with cache indexes.
 *
 * @since 11.0
 */
public interface Indexer {

   /**
    * Deletes all indexes for the cache and rebuilds them.
    * The indexing operation can take a long time to complete, depending on the size of the cache.
    * You should not query caches until the indexing operation is complete because it affects query performance and results.
    */
   CompletionStage<Void> run();

   /**
    * same as {@link #run()} but will only re-index data from the local member.
    */
   CompletionStage<Void> runLocal();


   /**
    * Re-indexes values associated with the provided keys only.
    */
   CompletionStage<Void> run(Object... keys);

   /**
    * Removes all indexes from the cache.
    */
   CompletionStage<Void> remove();

   /**
    * Removes all entities of a particular class from the index of the cache.
    */
   CompletionStage<Void> remove(Class<?>... entities);

   /**
    * @return true if the indexer process was started on this node and has not finished yet.
    */
   boolean isRunning();
}
