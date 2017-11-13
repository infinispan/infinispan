package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelOperation;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Remove client listener operation. In order to avoid issues with concurrent
 * event consumption, removing client listener operation is sent in a separate
 * connection to the one used for event consumption, but it must go to the
 * same node where the listener has been added.
 *
 * @author Galder Zamarreño
 */
public class RemoveClientListenerOperation extends HotRodOperation<Void> implements ChannelOperation {

   private final ClientListenerNotifier listenerNotifier;
   private final Object listener;
   private byte[] listenerId;

   protected RemoveClientListenerOperation(Codec codec, ChannelFactory channelFactory,
                                           byte[] cacheName, AtomicInteger topologyId, int flags,
                                           Configuration cfg,
                                           ClientListenerNotifier listenerNotifier, Object listener) {
      super(codec, flags, cfg, cacheName, topologyId, channelFactory);
      this.listenerNotifier = listenerNotifier;
      this.listener = listener;
   }

   protected void fetchChannelAndInvoke() {
      listenerId = listenerNotifier.findListenerId(listener);
      if (listenerId != null) {
         SocketAddress address = listenerNotifier.findAddress(listenerId);
         channelFactory.fetchChannelAndInvoke(address, this);
      } else {
         complete(null);
      }
   }

   @Override
   public void invoke(Channel channel) {
      HeaderParams header = headerParams(REMOVE_CLIENT_LISTENER_REQUEST);
      scheduleRead(channel, header);
      sendArrayOperation(channel, header, listenerId);
   }

   @Override
   public void cancel(SocketAddress address, Throwable cause) {
      completeExceptionally(cause);
   }

   @Override
   public Void decodePayload(ByteBuf buf, short status) {
      if (HotRodConstants.isSuccess(status) || HotRodConstants.isNotExecuted(status))
         listenerNotifier.removeClientListener(listenerId);
      return null;
   }

   @Override
   public CompletableFuture<Void> execute() {
      fetchChannelAndInvoke();
      return this;
   }
}
