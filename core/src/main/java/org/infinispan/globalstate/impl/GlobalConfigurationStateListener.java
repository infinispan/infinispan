package org.infinispan.globalstate.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.concurrent.CompletionStage;

import org.infinispan.globalstate.ScopedState;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.util.concurrent.CompletableFutures;

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
   public CompletionStage<Void> createCache(CacheEntryCreatedEvent<ScopedState, Object> event) {
      if (!GlobalConfigurationManagerImpl.CACHE_SCOPE.equals(event.getKey().getScope()))
         return CompletableFutures.completedNull();

      String cacheName = event.getKey().getName();
      CacheState state = (CacheState) event.getValue();

      return gcm.createCacheLocally(cacheName, state);
   }

   @CacheEntryRemoved
   public CompletionStage<Void> removeCache(CacheEntryRemovedEvent<ScopedState, CacheState> event) {
      if (!GlobalConfigurationManagerImpl.CACHE_SCOPE.equals(event.getKey().getScope()))
         return CompletableFutures.completedNull();

      String cacheName = event.getKey().getName();
      CONTAINER.debugf("Stopping cache %s because it was removed from global state", cacheName);
      return gcm.removeCacheLocally(cacheName, event.getOldValue());
   }
}
