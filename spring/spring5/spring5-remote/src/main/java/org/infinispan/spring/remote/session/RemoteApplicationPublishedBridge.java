package org.infinispan.spring.remote.session;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryExpiredEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.common.session.AbstractApplicationPublisherBridge;
import org.springframework.core.task.TaskExecutor;
import org.springframework.session.MapSession;
import org.springframework.session.Session;
import org.springframework.session.events.SessionDestroyedEvent;
import org.springframework.session.events.SessionExpiredEvent;

/**
 * A bridge between Infinispan Remote events and Spring.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
@ClientListener
public class RemoteApplicationPublishedBridge extends AbstractApplicationPublisherBridge {

   private final TaskExecutor taskExecutor;

   public RemoteApplicationPublishedBridge(SpringCache eventSource, TaskExecutor taskExecutor) {
      super(eventSource);
      this.taskExecutor = taskExecutor;
   }

   @Override
   protected void registerListener() {
      ((RemoteCache<?, ?>) eventSource.getNativeCache()).addClientListener(this);
   }

   @Override
   public void unregisterListener() {
      ((RemoteCache<?, ?>) eventSource.getNativeCache()).removeClientListener(this);
   }

   @ClientCacheEntryCreated
   public void processCacheEntryCreated(ClientCacheEntryCreatedEvent event) {
      taskExecutor.execute(() -> {
               Session session = (Session) eventSource.get(event.getKey()).get();
               if (session != null) {
                  emitSessionCreatedEvent(session);
               }
            }
      );
   }

   @ClientCacheEntryExpired
   public void processCacheEntryExpired(ClientCacheEntryExpiredEvent event) {
      springEventsPublisher.ifPresent(p -> p.publishEvent(new SessionExpiredEvent(eventSource, new MapSession((String) event.getKey()))));
   }

   @ClientCacheEntryRemoved
   public void processCacheEntryDestroyed(ClientCacheEntryRemovedEvent event) {
      // We create a new session object because there is an incompatibility with the API right now
      // We should be able to get the value that has been removed to pass it to the event from infinispan
      springEventsPublisher.ifPresent(p -> p.publishEvent(new SessionDestroyedEvent(eventSource, new MapSession((String) event.getKey()))));
   }
}
