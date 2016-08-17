package org.infinispan.distexec;

import java.util.Set;
import java.util.concurrent.Callable;

import org.infinispan.Cache;

/**
 * A task that returns a result and may throw an exception capable of being executed in another JVM.
 * <p>
 *
 *
 * @see Callable
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 *
 * @since 5.0
 *
 */
public interface DistributedCallable<K, V, T> extends Callable<T> {

   /**
    * Invoked by execution environment after DistributedCallable has been migrated for execution to
    * a specific Infinispan node.
    *
    * @param cache
    *           cache whose keys are used as input data for this DistributedCallable task
    * @param inputKeys
    *           keys used as input for this DistributedCallable task
    */
   void setEnvironment(Cache<K, V> cache, Set<K> inputKeys);

}
