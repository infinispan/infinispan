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

/**
 * A bridge between Infinispan Remote events and Spring.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
@ClientListener
public class RemoteApplicationPublishedBridge extends AbstractApplicationPublisherBridge {

   public RemoteApplicationPublishedBridge(SpringCache eventSource) {
      super(eventSource);
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
      emitSessionCreatedEvent((String) event.getKey());
   }

   @ClientCacheEntryExpired
   public void processCacheEntryExpired(ClientCacheEntryExpiredEvent event) {
      emitSessionExpiredEvent((String) event.getKey());
   }

   @ClientCacheEntryRemoved
   public void processCacheEntryDestroyed(ClientCacheEntryRemovedEvent event) {
      emitSessionDestroyedEvent((String) event.getKey());
   }
}
