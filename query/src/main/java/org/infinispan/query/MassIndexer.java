package org.infinispan.query;

import java.util.concurrent.CompletableFuture;

/**
 * Component to rebuild the indexes from the existing data.
 * This process starts by removing all existing indexes, and then a distributed
 * task is executed to rebuild the indexes. This task can take a long time to run,
 * depending on data size, used stores, indexing complexity.
 * <p>
 * While reindexing is being performed queries should not be executed as they
 * will very likely miss many or all results.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 */
public interface MassIndexer {

   void start();

   /**
    * Deletes all the indexes and skip the reindexing.
    */
   CompletableFuture<Void> purge();

   /**
    * @return {@link CompletableFuture}
    */
   CompletableFuture<Void> startAsync();

   CompletableFuture<Void> reindex(Object... keys);

   /**
    * @return true if the MassIndexer process was started on this node and hasn't finished yet.
    */
   boolean isRunning();

}
