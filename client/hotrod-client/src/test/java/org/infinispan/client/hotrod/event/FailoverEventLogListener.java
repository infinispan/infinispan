package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheFailover;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FailoverEventLogListener<K> extends EventLogListener<K> {
   public BlockingQueue<ClientCacheFailoverEvent> failoverEvents =
         new ArrayBlockingQueue<ClientCacheFailoverEvent>(128);

   public FailoverEventLogListener(RemoteCache<K, ?> remote) {
      super(remote);
   }

   @Override @SuppressWarnings("unchecked")
   public <E extends ClientEvent> BlockingQueue<E> queue(ClientEvent.Type type) {
      switch (type) {
         case CLIENT_CACHE_ENTRY_CREATED: return (BlockingQueue<E>) createdEvents;
         case CLIENT_CACHE_ENTRY_MODIFIED: return (BlockingQueue<E>) modifiedEvents;
         case CLIENT_CACHE_ENTRY_REMOVED: return (BlockingQueue<E>) removedEvents;
         case CLIENT_CACHE_FAILOVER: return (BlockingQueue<E>) failoverEvents;
         default: throw new IllegalArgumentException("Unknown event type: " + type);
      }
   }

   @ClientCacheFailover
   @SuppressWarnings("unused")
   public void handleFailover(ClientCacheFailoverEvent e) {
      failoverEvents.add(e);
   }

}
