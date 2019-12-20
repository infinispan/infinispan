package org.infinispan.eviction.impl;

import java.util.concurrent.CompletionStage;

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
    * Activates an entry, effectively removing it from the underlying persistence store. Note that the removal may
    * be done asynchronously and when the returned Stage is complete the removal is also completed.
    * @param key key to activate
    * @param segment segment the key maps to
    * @return stage that when complete the entry has been activated
    */
   CompletionStage<Void> activateAsync(Object key, int segment);

   long getPendingActivationCount();

   /**
    * Get number of activations executed.
    *
    * @return A long representing the number of activations
    */
   long getActivationCount();

}
