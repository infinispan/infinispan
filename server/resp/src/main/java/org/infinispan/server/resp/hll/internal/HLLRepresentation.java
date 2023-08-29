package org.infinispan.server.resp.hll.internal;

/**
 * The base class for the HyperLogLog representations.
 *
 * @since 15.0
 */
public interface HLLRepresentation {

   /**
    * Add the given {@param data} to the representation set.
    *
    * @param data: The data to include.
    * @return true if the data was added, and false otherwise.
    */

   boolean set(byte[] data);

   /**
    * Estimates the cardinality of the set.
    *
    * @return An estimation of the real cardinality.
    */
   long cardinality();
}
