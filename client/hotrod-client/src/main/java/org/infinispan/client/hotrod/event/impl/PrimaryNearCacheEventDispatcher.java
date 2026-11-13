package org.infinispan.client.hotrod.event.impl;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.client.hotrod.impl.InvalidatedNearRemoteCache;
import org.infinispan.client.hotrod.impl.operations.ClientListenerOperation;

import io.netty.channel.Channel;

public final class PrimaryNearCacheEventDispatcher extends AbstractClientEventDispatcher {
   private final Set<SocketAddress> addresses = ConcurrentHashMap.newKeySet();
   private final InvalidatedNearRemoteCache<?, ?> nearCache;

   private PrimaryNearCacheEventDispatcher(ClientListenerOperation op, InvalidatedNearRemoteCache<?, ?> nearCache) {
      super(op.listener, op.listenerId, nearCache, null);
      this.nearCache = nearCache;
   }

   public static PrimaryNearCacheEventDispatcher create(ClientListenerOperation op,
                                                        InvalidatedNearRemoteCache<?, ?> nearCache) {
      return new PrimaryNearCacheEventDispatcher(op, nearCache);
   }

   @Override
   public void addAddress(SocketAddress address) {
      if (address != null) {
         addresses.add(address);
      }
   }

   @Override
   public boolean removeAddress(SocketAddress address) {
      return address != null && addresses.remove(address);
   }

   @Override
   public Set<SocketAddress> addresses() {
      return Collections.unmodifiableSet(addresses);
   }

   @Override
   protected void clearAddresses() {
      addresses.clear();
   }

   /** Fences the owner whose listener connection was closed; its segments remain uncached. */
   @Override
   void listenerChannelClosed(SocketAddress address, Channel channel) {
      super.listenerChannelClosed(address, channel);
      if (address != null) {
         nearCache.handlePrimaryListenerDisconnected(address, channel);
      }
   }
}
