package org.infinispan.globalstate.impl;

import static org.infinispan.globalstate.impl.GlobalConfigurationManagerImpl.CACHE_SCOPE;
import static org.infinispan.globalstate.impl.GlobalConfigurationManagerImpl.isKnownScope;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Listens to events on the global state cache and manages cache configuration creation / removal accordingly
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
@Listener(observation = Listener.Observation.BOTH)
public class GlobalConfigurationStateListener {
   private static final Log log = LogFactory.getLog(GlobalConfigurationStateListener.class);
   private final GlobalConfigurationManagerImpl gcm;

   GlobalConfigurationStateListener(GlobalConfigurationManagerImpl gcm) {
      this.gcm = gcm;
   }

   @CacheEntryCreated
   public CompletionStage<Void> handleCreate(CacheEntryCreatedEvent<ScopedState, CacheState> event) {
      if (event.isPre()) {
         return CompletableFutures.completedNull();
      }
      String scope = event.getKey().getScope();
      if (!isKnownScope(scope))
         return CompletableFutures.completedNull();

      String name = event.getKey().getName();
      CacheState state = event.getValue();

      log.infof("Received scope '%s' for cache '%s' with %s", scope, name, state);

      if (CACHE_SCOPE.equals(scope)) {
         CompletionStage<Void> cs = gcm.createCacheLocally(name, state);
         // zero capacity nodes have to wait for a non-zero capacity node to start the cache.
         // prevent the cache creating to blocking the listener invocation
         return isZeroCapacityNode()
               ? CompletableFutures.completedNull()
               : cs;
      }

      return gcm.createTemplateLocally(name, state);
   }

   @CacheEntryModified
   public CompletionStage<Void> handleUpdate(CacheEntryModifiedEvent<ScopedState, CacheState> event) {
      String scope = event.getKey().getScope();
      if (!isKnownScope(scope))
         return CompletableFutures.completedNull();

      String name = event.getKey().getName();
      CacheState state = event.getNewValue();
      if (event.isPre()) {
         return event.isOriginLocal() ? gcm.validateConfigurationUpdateLocally(name, state) : CompletableFutures.completedNull();
      } else {
         return gcm.updateConfigurationLocally(name, state);
      }
   }

   @CacheEntryRemoved
   public CompletionStage<Void> handleRemove(CacheEntryRemovedEvent<ScopedState, CacheState> event) {
      // We are only interested in POST for removal
      if (event.isPre())
         return CompletableFutures.completedNull();
      String scope = event.getKey().getScope();
      if (!isKnownScope(scope))
         return CompletableFutures.completedNull();

      String name = event.getKey().getName();
      if (CACHE_SCOPE.equals(scope)) {
         CONTAINER.debugf("Stopping cache %s because it was removed from global state", name);
         return gcm.removeCacheLocally(name);
      } else {
         CONTAINER.debugf("Removing template %s because it was removed from global state", name);
         return gcm.removeTemplateLocally(name);
      }
   }

   private boolean isZeroCapacityNode() {
      return SecurityActions.getGlobalComponentRegistry(gcm.cacheManager).getGlobalConfiguration().isZeroCapacityNode();
   }
}
