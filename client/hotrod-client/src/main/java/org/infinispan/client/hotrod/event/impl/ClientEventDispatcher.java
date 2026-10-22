package org.infinispan.client.hotrod.event.impl;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.annotation.ClientCacheFailover;
import org.infinispan.client.hotrod.event.ClientCacheFailoverEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;

import io.netty.channel.Channel;

public final class ClientEventDispatcher extends AbstractClientEventDispatcher implements FailoverEventDispatcher {
   private static final ClientCacheFailoverEvent FAILOVER_EVENT_SINGLETON = () -> ClientEvent.Type.CLIENT_CACHE_FAILOVER;

   private final ClientListenerOperation op;
   private volatile SocketAddress address;

   private ClientEventDispatcher(ClientListenerOperation op, SocketAddress address, Runnable cleanup,
                                 InternalRemoteCache<?, ?> remoteCache) {
      super(op.listener, op.listenerId, remoteCache, cleanup);
      this.op = op;
      this.address = address;
   }

   public static ClientEventDispatcher create(ClientListenerOperation op, SocketAddress address, Runnable cleanup,
                                              InternalRemoteCache<?, ?> remoteCache) {
      return new ClientEventDispatcher(op, address, cleanup, remoteCache);
   }

   public static ClientEventDispatcher create(ClientListenerOperation op, Runnable cleanup) {
      return create(op, null, cleanup, op.getRemoteCache());
   }

   @Override
   public synchronized void addAddress(SocketAddress address) {
      if (address != null) {
         this.address = address;
      }
   }

   @Override
   public synchronized boolean removeAddress(SocketAddress address) {
      if (address != null && Objects.equals(this.address, address)) {
         this.address = null;
         return true;
      }
      return false;
   }

   @Override
   public synchronized void replaceAddress(SocketAddress oldAddress, SocketAddress newAddress) {
      if (Objects.equals(address, oldAddress)) {
         address = newAddress;
      }
   }

   @Override
   public Set<SocketAddress> addresses() {
      SocketAddress current = address;
      return current == null ? Set.of() : Set.of(current);
   }

   public SocketAddress address() {
      return address;
   }

   @Override
   protected synchronized void clearAddresses() {
      address = null;
   }

   @Override
   public CompletableFuture<Void> executeFailover() {
      // A listener that was removed is not admitted again, so a queued reconnect can't resurrect it
      if (isTerminated()) {
         return CompletableFuture.completedFuture(null);
      }
      OperationDispatcher dispatcher = remoteCache.getDispatcher();
      ClientListenerNotifier notifier = dispatcher.getClientListenerNotifier();
      CompletableFuture<Channel> future = dispatcher.executeAddListener(op.copy())
            .toCompletableFuture();
      if (remoteCache instanceof InvalidatedNearRemoteCache<?, ?> inrc) {
         future = future.thenApply(channel -> {
            inrc.setBloomListenerAddress(channel);
            return channel;
         });
      }
      return future.thenCompose(channel -> {
         if (!isTerminated()) {
            return CompletableFuture.<Void>completedFuture(null);
         }
         // The listener was removed while the registration was in flight. Unregister it, as nobody else knows about it.
         SocketAddress address = dispatcher.unresolvedAddressForChannel(channel);
         notifier.removeClientListener(listenerId, address);
         dispatcher.removeListener(address, listenerId);
         return dispatcher.executeOnSingleAddress(remoteCache.getOperationsFactory().newRemoveClientListenerOperation(listenerId), address)
               .exceptionally(t -> null).toCompletableFuture();
      });
   }

   @Override
   public void invokeFailoverEvent() {
      invokeCallbacks(FAILOVER_EVENT_SINGLETON, ClientCacheFailover.class);
   }
}
