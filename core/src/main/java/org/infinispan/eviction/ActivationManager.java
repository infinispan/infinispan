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
    * Remove key and associated value from cache store and update the activation counter.
    *
    * @param key      Key to remove
    * @param newEntry {@code true} if the entry does not exists in-memory
    */
   void onUpdate(Object key, boolean newEntry);

   /**
    * Remove key and associated value from cache store and update the activation counter.
    * <p/>
    * The key is also removed from the shared configured stores.
    *
    * @param key      Key to activate
    * @param newEntry {@code true} if the entry does not exists in-memory
    */
   void onRemove(Object key, boolean newEntry);

   /**
    * Get number of activations executed.
    *
    * @return A long representing the number of activations
    */
   long getActivationCount();

}
