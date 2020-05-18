package org.infinispan.query;

import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;

/**
 * Component to rebuild the indexes from the existing data.
 * This process starts by removing all existing indexes, and then a distributed
 * task is executed to rebuild the indexes. This task can take a long time to run,
 * depending on data size, used stores, indexing complexity.
 * <p>
 * While reindexing is being performed queries should not be executed as they
 * will very likely miss many or all results.
 *
 * @author Sanne Grinovero &lt   ;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 * @deprecated Since 11.0, replaced by {@link Indexer}, obtained from {@link Search#getIndexer(Cache)}
 */
@Deprecated
public interface MassIndexer {

   /**
    * @deprecated Since 11.0, use {@link Indexer#run()} from {@link Search#getIndexer(Cache)} and wait for completion.
    */
   @Deprecated
   void start();

   /**
    * Deletes all the indexes and skip the reindexing.
    * @deprecated Since 11.0, use {@link Indexer#remove()} from {@link Search#getIndexer(Cache)}.
    */
   @Deprecated
   CompletableFuture<Void> purge();

   /**
    * @return {@link CompletableFuture}
    * @deprecated Since 11.0, use {@link Indexer#run()} from {@link Search#getIndexer(Cache)}.
    */
   @Deprecated
   CompletableFuture<Void> startAsync();

   /**
    * @deprecated Since 11.0, use {@link Indexer#run(Object...)} from {@link Search#getIndexer(Cache)}.
    */
   @Deprecated
   CompletableFuture<Void> reindex(Object... keys);

   /**
    * @return true if the MassIndexer process was started on this node and hasn't finished yet.
    * @deprecated Since 11.0, use {@link Indexer#isRunning()} from {@link Search#getIndexer(Cache)}.
    */
   @Deprecated
   boolean isRunning();

}
