package org.infinispan.hotrod.impl.counter.operation;

import java.net.SocketAddress;
import java.util.Set;

import org.infinispan.counter.api.CounterListener;
import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.hotrod.impl.transport.netty.HotRodClientDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * An add listener operation for {@link StrongCounter#addListener(CounterListener)} and {@link
 * WeakCounter#addListener(CounterListener)}
 *
 * @since 14.0
 */
public class AddListenerOperation extends BaseCounterOperation<Boolean> {

   private final byte[] listenerId;
   private final SocketAddress server;
   private Channel channel;

   public AddListenerOperation(OperationContext operationContext, String counterName, byte[] listenerId, SocketAddress server) {
      super(operationContext, COUNTER_ADD_LISTENER_REQUEST, COUNTER_ADD_LISTENER_RESPONSE, counterName, false);
      this.listenerId = listenerId;
      this.server = server;
   }

   public Channel getChannel() {
      return channel;
   }

   @Override
   protected void executeOperation(Channel channel) {
      this.channel = channel;
      ByteBuf buf = getHeaderAndCounterNameBufferAndRead(channel, ByteBufUtil.estimateArraySize(listenerId));
      ByteBufUtil.writeArray(buf, listenerId);
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      checkStatus(status);
      if (status != NO_ERROR_STATUS) {
         complete(false);
      } else {
         decoder.addListener(listenerId);
         complete(true);
      }
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (server == null) {
         super.fetchChannelAndInvoke(retryCount, failedServers);
      } else {
         operationContext.getChannelFactory().fetchChannelAndInvoke(server, this);
      }
   }

   @Override
   public void releaseChannel(Channel channel) {
      if (operationContext.getCodec().allowOperationsAndEvents()) {
         //we aren't using this channel. we can release it
         super.releaseChannel(channel);
      }
   }

   public void cleanup() {
      // To prevent releasing concurrently from the channel and closing it
      channel.eventLoop().execute(() -> {
         if (log.isTraceEnabled()) {
            log.tracef("Cleanup for %s on %s", this, channel);
         }
         if (!operationContext.getCodec().allowOperationsAndEvents()) {
            if (channel.isOpen()) {
               super.releaseChannel(channel);
            }
         }
         HotRodClientDecoder decoder = channel.pipeline().get(HotRodClientDecoder.class);
         if (decoder != null) {
            decoder.removeListener(listenerId);
         }
      });
   }
}
