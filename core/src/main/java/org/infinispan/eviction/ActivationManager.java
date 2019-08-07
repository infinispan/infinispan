package org.infinispan.eviction;

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
    * This method should no longer be used - please use {@link #activateAsync(Object, int)} instead.
    * @deprecated since 10.0
    */
   @Deprecated
   void onUpdate(Object key, boolean newEntry);

   /**
    * This method should no longer be used - please use {@link #activateAsync(Object, int)} instead.
    * @deprecated since 10.0
    */
   @Deprecated
   void onRemove(Object key, boolean newEntry);

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
