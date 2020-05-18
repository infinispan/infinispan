package org.infinispan.query;

import java.util.concurrent.CompletionStage;

/**
 * The indexer is used to interact directly with a cache's index.
 *
 * @since 11.0
 */
public interface Indexer {

   /**
    * Deletes all the indexes from a cache and rebuilds them from the existing data.
    * This operation can take a long time depending on the cache size, and searches
    * during this process will be affected.
    */
   CompletionStage<Void> run();

   /**
    * Re-indexes only the values associated with the provided keys.
    */
   CompletionStage<Void> run(Object... keys);

   /**
    * Remove all the indexes from the cache.
    */
   CompletionStage<Void> remove();

   /**
    * Remove all entities of particular class from the index of a cache.
    */
   CompletionStage<Void> remove(Class<?>... entities);

   /**
    * @return true if the indexer process was started on this node and hasn't finished yet.
    */
   boolean isRunning();
}
