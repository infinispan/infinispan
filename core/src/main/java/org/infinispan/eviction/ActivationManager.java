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
    * Almost the same as {@link #onUpdateAsync(Object, boolean)} except that it is performed
    * synchronously on the same thread that invoked it. This method will eventually be removed when
    * the data container can handle asynchronous passivation/activation.
    * @deprecated since 10.0 - please use {@link #onUpdateAsync(Object, boolean)} instead.
    */
   @Deprecated
   void onUpdate(Object key, boolean newEntry);

   /**
    * Almost the same as {@link #onRemoveAsync(Object, boolean)} except that it is performed
    * synchronously on the same thread that invoked it. This method will eventually be removed when
    * the data container can handle asynchronous passivation/activation.
    * @deprecated since 10.0 - please use {@link #onRemoveAsync(Object, boolean)} instead.
    */
   @Deprecated
   void onRemove(Object key, boolean newEntry);

   /**
    * Remove key and associated value from cache store and update the activation counter.
    *
    * @param key      Key to remove
    * @param newEntry {@code true} if the entry does not exists in-memory
    * @return stage then when complete has updated appropriate stores
    * @deprecated
    */
   @Deprecated
   CompletionStage<Void> onUpdateAsync(Object key, boolean newEntry);

   /**
    * Remove key and associated value from cache store and update the activation counter.
    * <p/>
    * The key is also removed from the shared configured stores.
    *
    * @param key      Key to activate
    * @param newEntry {@code true} if the entry does not exists in-memory
    * @return stage then when complete has updated appropriate stores
    * @deprecated
    */
   @Deprecated
   CompletionStage<Void> onRemoveAsync(Object key, boolean newEntry);

   CompletionStage<Void> activateAsync(Object key, int segment);

   long getPendingActivationCount();

   /**
    * Get number of activations executed.
    *
    * @return A long representing the number of activations
    */
   long getActivationCount();

}
