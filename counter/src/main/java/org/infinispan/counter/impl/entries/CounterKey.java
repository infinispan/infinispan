package org.infinispan.counter.impl.entries;

import org.infinispan.util.ByteString;

/**
 * Interface that represents the key stored in the cache.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface CounterKey {

   /**
    * @return The counter name.
    */
   ByteString getCounterName();

}
