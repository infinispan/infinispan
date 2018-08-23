package org.infinispan.spring.embedded.session;

import org.infinispan.AdvancedCache;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractApplicationPublisherBridge;

/**
 * A bridge between Infinispan Embedded events and Spring.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
@Listener(observation = Listener.Observation.POST)
public class EmbeddedApplicationPublishedBridge extends AbstractApplicationPublisherBridge {

   public EmbeddedApplicationPublishedBridge(SpringCache eventSource) {
      super(eventSource);
   }

   @Override
   protected void registerListener() {
      ((AdvancedCache<?, ?>) eventSource.getNativeCache()).addListener(this);
   }

   @Override
   public void unregisterListener() {
      ((AdvancedCache<?, ?>) eventSource.getNativeCache()).removeListener(this);
   }

   @CacheEntryCreated
   public void processCacheEntryCreated(CacheEntryCreatedEvent event) {
      emitSessionCreatedEvent((String) event.getKey());
   }

   @CacheEntryExpired
   public void processCacheEntryExpired(CacheEntryExpiredEvent event) {
      emitSessionExpiredEvent((String) event.getKey());
      emitSessionDestroyedEvent((String) event.getKey());
   }

   @CacheEntryRemoved
   public void processCacheEntryDestroyed(CacheEntryRemovedEvent event) {
      emitSessionDestroyedEvent((String) event.getKey());
   }
}
