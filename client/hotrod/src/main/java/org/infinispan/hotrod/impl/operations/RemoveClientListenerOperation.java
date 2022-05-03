package org.infinispan.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ChannelOperation;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Remove client listener operation. In order to avoid issues with concurrent event consumption, removing client
 * listener operation is sent in a separate connection to the one used for event consumption, but it must go to the same
 * node where the listener has been added.
 */
public class RemoveClientListenerOperation extends HotRodOperation<Void> implements ChannelOperation {
   private final Object listener;
   private byte[] listenerId;

   protected RemoveClientListenerOperation(OperationContext operationContext, CacheOptions options, Object listener) {
      super(operationContext, REMOVE_CLIENT_LISTENER_REQUEST, REMOVE_CLIENT_LISTENER_RESPONSE, options);
      this.listener = listener;
   }

   protected void fetchChannelAndInvoke() {
      listenerId = operationContext.getListenerNotifier().findListenerId(listener);
      if (listenerId != null) {
         SocketAddress address = operationContext.getListenerNotifier().findAddress(listenerId);
         operationContext.getChannelFactory().fetchChannelAndInvoke(address, this);
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
   public void cancel(SocketAddress address, Throwable cause) {
      completeExceptionally(cause);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isSuccess(status) || HotRodConstants.isNotExecuted(status)) {
         operationContext.getListenerNotifier().removeClientListener(listenerId);
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
