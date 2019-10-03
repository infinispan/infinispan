package org.infinispan.client.rest;

public enum RestQueryMode {
   /**
    * Query is sent to all nodes, and results are combined before returning to the caller. This allows each node to have
    * its own index, and the query will return the cluster wide results.
    */
   BROADCAST,

   /**
    * Query is executed locally in the caller. The whole index must be available in order to return full results,
    * otherwise only results available at the caller's local index are returned.
    */
   FETCH
}
