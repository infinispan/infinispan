package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.impl.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

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
                                           byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags,
                                           Configuration cfg,
                                           ClientListenerNotifier listenerNotifier, Object listener) {
      super(REMOVE_CLIENT_LISTENER_REQUEST, REMOVE_CLIENT_LISTENER_RESPONSE, codec, flags, cfg, cacheName, clientTopology, channelFactory);
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
      scheduleRead(channel);
      sendArrayOperation(channel, listenerId);
      releaseChannel(channel);
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      writeArrayOperation(buf, listenerId);
   }

   @Override
   public void cancel(SocketAddress address, Throwable cause) {
      completeExceptionally(cause);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isSuccess(status) || HotRodConstants.isNotExecuted(status)) {
         listenerNotifier.removeClientListener(listenerId);
      }
      complete(null);
   }

   @Override
   public CompletableFuture<Void> execute() {
      try {
         fetchChannelAndInvoke();
      } catch (Exception e) {
         completeExceptionally(e);
      }
      return this;
   }
}
