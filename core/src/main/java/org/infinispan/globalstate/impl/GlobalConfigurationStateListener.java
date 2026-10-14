package org.infinispan.globalstate.impl;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.globalstate.ScopeType;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;

/**
 * Listens to events on the global state cache and manages cache configuration creation / removal accordingly
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
@Listener(observation = Listener.Observation.BOTH)
public class GlobalConfigurationStateListener {
   private final GlobalConfigurationManagerImpl gcm;

   GlobalConfigurationStateListener(GlobalConfigurationManagerImpl gcm) {
      this.gcm = gcm;
   }

   @CacheEntryCreated
   public CompletionStage<Void> handleCreate(CacheEntryCreatedEvent<ScopedState, CacheState> event) {
      if (event.isPre()) {
         return CompletableFutures.completedNull();
      }
      ScopeType scopeType = ScopeType.fromString(event.getKey().getScope());
      if (scopeType == null)
         return CompletableFutures.completedNull();

      return gcm.createLocally(event.getKey(), event.getValue());
   }

   @CacheEntryModified
   public CompletionStage<Void> handleUpdate(CacheEntryModifiedEvent<ScopedState, CacheState> event) {
      ScopeType scopeType = ScopeType.fromString(event.getKey().getScope());
      if (scopeType == null)
         return CompletableFutures.completedNull();

      if (scopeType == ScopeType.CONTAINER) {
         CacheState state = event.getNewValue();
         if (event.isPre()) {
            return event.isOriginLocal() ? gcm.validateGlobalConfigurationUpdateLocally(state) : CompletableFutures.completedNull();
         }
         return gcm.updateLocally(event.getKey(), state);
      }

      String name = event.getKey().getName();
      CacheState state = event.getNewValue();
      if (event.isPre()) {
         return event.isOriginLocal() ? gcm.validateConfigurationUpdateLocally(name, state) : CompletableFutures.completedNull();
      }

      return gcm.updateLocally(event.getKey(), state);
   }

   @CacheEntryRemoved
   public CompletionStage<Void> handleRemove(CacheEntryRemovedEvent<ScopedState, CacheState> event) {
      // We are only interested in POST for removal
      if (event.isPre())
         return CompletableFutures.completedNull();
      ScopeType scopeType = ScopeType.fromString(event.getKey().getScope());
      if (scopeType == null)
         return CompletableFutures.completedNull();

      if (scopeType == ScopeType.CONTAINER)
         return CompletableFutures.completedNull();

      return gcm.removeLocally(event.getKey());
   }
}
