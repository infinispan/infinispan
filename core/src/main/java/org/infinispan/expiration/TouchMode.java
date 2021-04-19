package org.infinispan.expiration;

/**
 * Control how the timestamp of read keys are updated on all the key owners in a cluster.
 *
 * @since 13.0
 * @author Dan Berindei
 */
public enum TouchMode {
   /**
    * Delay read operations until the other owners confirm updating the timestamp of the entry
    */
   SYNC,
   /**
    * Send touch commands to other owners, but do not wait for their confirmation.
    *
    * This allows read operations to return the value of a key even if another node has started expiring it.
    * When that happens, the read won't extend the lifespan of the key.
    */
   ASYNC
}
