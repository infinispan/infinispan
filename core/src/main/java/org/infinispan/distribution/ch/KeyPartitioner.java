package org.infinispan.distribution.ch;

import org.infinispan.configuration.cache.HashConfiguration;

/**
 * Map keys to segments.
 *
 * @author Dan Berindei
 * @since 8.2
 */
public interface KeyPartitioner {
   /**
    * Initialization.
    *
    * <p>The partitioner can also use injection to access other cache-level or global components.
    * This method will be called before any other injection methods.</p>
    *
    * <p>Does not need to be thread-safe (Infinispan safely publishes the instance after initialization).</p>
    * @param configuration
    */
   default void init(HashConfiguration configuration) {
      // Do nothing
   }

   /**
    * Obtains the segment for a key.
    *
    * Must be thread-safe.
    */
   int getSegment(Object key);
}
