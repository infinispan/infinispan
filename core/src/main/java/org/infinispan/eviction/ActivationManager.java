package org.infinispan.eviction;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Controls activation of cache entries that have been passivated.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public interface ActivationManager {

   /**
    * Remove key and associated value from cache store
    * and update the activation counter.
    *
    * @param key Key to remove
    */
   void activate(Object key);

   /**
    * Get number of activations executed.
    *
    * @return A long representing the number of activations
    */
   long getActivationCount();

}
