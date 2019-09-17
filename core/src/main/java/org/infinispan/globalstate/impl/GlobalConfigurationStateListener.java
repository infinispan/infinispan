package org.infinispan.globalstate.impl;

import java.util.concurrent.CompletionStage;

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
   public CompletionStage<Void> createCache(CacheEntryCreatedEvent<ScopedState, CacheState> event) {
      String cacheName = event.getKey().getName();
      CacheState state = event.getCache().get(event.getKey());
      return gcm.createCacheLocally(cacheName, state);
   }

   @CacheEntryRemoved
   public CompletionStage<Void> removeCache(CacheEntryRemovedEvent<ScopedState, CacheState> event) {
      String cacheName = event.getKey().getName();
      return gcm.removeCacheLocally(cacheName, event.getOldValue());
   }
}
