package org.infinispan.hotrod.event.impl;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.event.ClientCacheFailoverEvent;
import org.infinispan.hotrod.event.ClientEvent;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.InvalidatedNearRemoteCache;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.hotrod.impl.operations.ClientListenerOperation;

public final class ClientEventDispatcher extends EventDispatcher<ClientEvent> {

   public static final ClientCacheFailoverEvent FAILOVER_EVENT_SINGLETON = () -> ClientEvent.Type.CLIENT_CACHE_FAILOVER;

   private final ClientListenerOperation<?, ?> op;
   private final RemoteCache<?, ?> remoteCache;

   ClientEventDispatcher(ClientListenerOperation<?, ?> op, SocketAddress address, String cacheName, Runnable cleanup,
         RemoteCache<?, ?> remoteCache) {
      super(cacheName, op.listenerOptions, op.listenerId, address, cleanup);
      this.op = op;
      this.remoteCache = remoteCache;
   }

   public static ClientEventDispatcher create(ClientListenerOperation<?, ?> op, SocketAddress address, Runnable cleanup,
         RemoteCache<?, ?> remoteCache) {
      return new ClientEventDispatcher(op, address, op.getCacheName(), cleanup, remoteCache);
   }

   <K, V> CacheEntryEvent<K, V> toCacheEntryEvent(ClientEvent clientEvent) {
      return (CacheEntryEvent<K, V>) clientEvent;
   }

   @Override
   public void invokeEvent(ClientEvent clientEvent) {
      if (log.isTraceEnabled())
         log.tracef("Event %s received for listener with id=%s", clientEvent, Util.printArray(listenerId));

      switch (clientEvent.getType()) {
         case CLIENT_CACHE_ENTRY_CREATED:
            op.processor.onNext(toCacheEntryEvent(clientEvent));
            break;
         case CLIENT_CACHE_ENTRY_MODIFIED:
            //FIXME: invokeCallbacks(clientEvent, ClientCacheEntryModified.class);
            break;
         case CLIENT_CACHE_ENTRY_REMOVED:
            //FIXME:invokeCallbacks(clientEvent, ClientCacheEntryRemoved.class);
            break;
         case CLIENT_CACHE_ENTRY_EXPIRED:
            //FIXME:invokeCallbacks(clientEvent, ClientCacheEntryExpired.class);
            break;
         case CLIENT_CACHE_FAILOVER:
            //FIXME:invokemethod
            break;
      }
   }

   @Override
   public CompletableFuture<Void> executeFailover() {
      CompletableFuture<SocketAddress> future = op.copy().execute().toCompletableFuture();
      if (remoteCache instanceof InvalidatedNearRemoteCache) {
         future = future.thenApply(socketAddress -> {
            ((InvalidatedNearRemoteCache<?, ?>) remoteCache).setBloomListenerAddress(socketAddress);
            return socketAddress;
         });
      }
      return future.thenApply(ignore -> null);
   }

   @Override
   protected void invokeFailoverEvent() {
      invokeEvent(FAILOVER_EVENT_SINGLETON);
   }

   protected DataFormat getDataFormat() {
      return op.getDataFormat();
   }
}
