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
import org.springframework.session.Session;

/**
 * A bridge between Infinispan Embedded events and Spring.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
@Listener(observation = Listener.Observation.POST, clustered = true)
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
      emitSessionCreatedEvent((Session) event.getValue());
   }

   @CacheEntryExpired
   public void processCacheEntryExpired(CacheEntryExpiredEvent event) {
      // The expired value is read from the data container when the event is raised, so it may be
      // missing if the entry was already removed by a concurrent operation.
      Session session = (Session) event.getValue();
      if (session != null) {
         emitSessionExpiredEvent(session);
      }
   }

   @CacheEntryRemoved
   public void processCacheEntryDestroyed(CacheEntryRemovedEvent event) {
      // An unconditional Cache#remove() of a key which is no longer present still raises a
      // CacheEntryRemoved event, but without an old value. This happens when the entry expires
      // between AbstractInfinispanSessionRepository#findById and #deleteById. No session was
      // destroyed in that case, so there is nothing to publish.
      Session session = (Session) event.getOldValue();
      if (session != null) {
         emitSessionDestroyedEvent(session);
      }
   }
}
