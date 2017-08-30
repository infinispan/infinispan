package org.infinispan.globalstate.impl;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;

/**
 * Listens to events on the global state cache and manages cache configuration creation / removal accordingly
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
@Listener(observation = Listener.Observation.POST, includeCurrentState = false)
public class GlobalConfigurationStateListener {
   private final GlobalConfigurationManagerImpl gcm;

   GlobalConfigurationStateListener(GlobalConfigurationManagerImpl gsm) {
      this.gcm = gsm;
   }

   @CacheEntryCreated
   public void createCache(CacheEntryCreatedEvent<String, String> event) {
      String cacheName = event.getKey();
      String cacheConfiguration = event.getCache().get(cacheName);
      gcm.localCreateCache(cacheName, cacheConfiguration);
   }

   @CacheEntryRemoved
   public void removeCache(CacheEntryRemovedEvent<String, String> event) {
      String cacheName = event.getKey();
      gcm.localRemoveCache(cacheName);
   }
}
