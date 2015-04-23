package org.infinispan.eviction;

import java.util.Map;

import net.jcip.annotations.ThreadSafe;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Central component that deals with eviction of cache entries.
 * <p />
 * This manager only controls notifications of when entries are evicted.
 * <p />
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
@Scope(Scopes.NAMED_CACHE)
public interface EvictionManager<K, V> {

   /**
    * @deprecated This falls back to calling {@link ExpirationManager#processExpiration()}
    * @see ExpirationManager
    */
   void processEviction();

   /**
    * @deprecated This falls back to calling {@link ExpirationManager#isEnabled()}
    * @see ExpirationManager
    * @return whether expiration is enabled or not
    */
   boolean isEnabled();

   /**
    * Handles notifications of evicted entries
    * @param evicted The entries that were just evicted
    */
   void onEntryEviction(Map<? extends K, InternalCacheEntry<? extends K, ? extends V>> evicted);
}
