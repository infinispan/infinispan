package org.infinispan.query;

import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.query.impl.ComponentRegistryUtils;

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
    * Returns whether a mass indexer operation involving this node is in progress.
    *
    * <p>
    * This is {@code true} from the moment an operation started on this node acquires its lock until it finishes, and
    * while this node's index is being rebuilt or removed, whichever node started the operation. It is
    * {@code false} otherwise.
    * </p>
    *
    * @return {@code true} if a mass indexer operation started on this node, or a rebuild of this node's index, has
    * not finished yet; {@code false} otherwise.
    */
   boolean isRunning();

   static <K, V> Indexer of(Cache<K, V> cache) {
      return ComponentRegistryUtils.getIndexer(cache.getAdvancedCache());
   }
}
