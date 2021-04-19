package org.infinispan.globalstate.impl;

import static org.infinispan.globalstate.impl.GlobalConfigurationManagerImpl.CACHE_SCOPE;
import static org.infinispan.globalstate.impl.GlobalConfigurationManagerImpl.isKnownScope;
import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.globalstate.ScopedState;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;

/**
 * Listens to events on the global state cache and manages cache configuration creation / removal accordingly
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
@Listener(observation = Listener.Observation.POST)
public class GlobalConfigurationStateListener {
   private final GlobalConfigurationManagerImpl gcm;

   GlobalConfigurationStateListener(GlobalConfigurationManagerImpl gcm) {
      this.gcm = gcm;
   }

   @CacheEntryCreated
   public void handleCreate(CacheEntryCreatedEvent<ScopedState, CacheState> event) {
      String scope = event.getKey().getScope();
      if (!isKnownScope(scope))
         return;

      String name = event.getKey().getName();
      CacheState state = event.getValue();

      // Create the cache without delaying the CONFIG cache operation
      if (CACHE_SCOPE.equals(scope)) {
         gcm.createCacheLocally(name, state);
      } else {
         gcm.createTemplateLocally(name, state);
      }
   }

   @CacheEntryRemoved
   public void handleRemove(CacheEntryRemovedEvent<ScopedState, CacheState> event) {
      String scope = event.getKey().getScope();
      if (!isKnownScope(scope))
         return;

      String name = event.getKey().getName();
      CacheState state = event.getOldValue();

      // Create the cache without delaying the CONFIG cache operation
      if (CACHE_SCOPE.equals(scope)) {
         CONTAINER.debugf("Stopping cache %s because it was removed from global state", name);
         gcm.removeCacheLocally(name, state);
      } else {
         CONTAINER.debugf("Removing template %s because it was removed from global state", name);
         gcm.removeTemplateLocally(name, state);
      }
   }
}
