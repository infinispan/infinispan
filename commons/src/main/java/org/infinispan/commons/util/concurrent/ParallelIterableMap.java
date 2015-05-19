package org.infinispan.commons.util.concurrent;

import java.util.function.BiConsumer;

/**
 * Map implementing this interface provide a mechanism for parallel key/value iteration.
 *
 *
 * @author Vladimir Blagojevic
 * @since 7.0
 */
public interface ParallelIterableMap<K,V>{

   /**
    * Performs the given action for each (key, value) but traversing entries in parallel.
    *
    * @param parallelismThreshold the (estimated) number of elements
    * needed for this operation to be executed in parallel
    * @param action the action
    * @since 7.0
    */
   public void forEach(long parallelismThreshold, BiConsumer<? super K,? super V> action) throws InterruptedException;
}