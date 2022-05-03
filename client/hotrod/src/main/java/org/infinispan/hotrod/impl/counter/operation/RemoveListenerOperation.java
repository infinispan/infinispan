package org.infinispan.hotrod.impl.counter.operation;

import java.net.SocketAddress;
import java.util.Set;

import org.infinispan.counter.api.Handle;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A remove listener operation for {@link Handle#remove()}.
 *
 * @since 14.0
 */
public class RemoveListenerOperation extends BaseCounterOperation<Boolean> {

   private final byte[] listenerId;
   private final SocketAddress server;

   public RemoveListenerOperation(OperationContext operationContext, String counterName, byte[] listenerId, SocketAddress server) {
      super(operationContext, COUNTER_REMOVE_LISTENER_REQUEST, COUNTER_REMOVE_LISTENER_RESPONSE, counterName, false);
      this.listenerId = listenerId;
      this.server = server;
   }


   @Override
   protected void executeOperation(Channel channel) {
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, ByteBufUtil.estimateArraySize(listenerId));
      ByteBufUtil.writeArray(buf, listenerId);
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      checkStatus(status);
      if (status == NO_ERROR_STATUS) {
         decoder.removeListener(listenerId);
      }
      complete(status == NO_ERROR_STATUS);
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (server == null) {
         super.fetchChannelAndInvoke(retryCount, failedServers);
      } else {
         operationContext.getChannelFactory().fetchChannelAndInvoke(server, this);
      }
   }
}
