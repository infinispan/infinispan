package org.infinispan.query;

import java.util.concurrent.CompletableFuture;

import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;

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
@MBean(objectName = "MassIndexer",
      description = "Component that rebuilds the index from the cached data")
public interface MassIndexer {

   @ManagedOperation(description = "Starts rebuilding the index", displayName = "Rebuild index")
   void start();

   /**
    * @return {@link CompletableFuture}
    */
   CompletableFuture<Void> startAsync();

   CompletableFuture<Void> reindex(Object... keys);

}
