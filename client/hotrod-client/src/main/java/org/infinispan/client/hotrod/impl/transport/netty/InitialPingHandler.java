package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingOperation;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public class InitialPingHandler extends ActivationHandler {
   private static final Log log = LogFactory.getLog(InitialPingHandler.class);

   static final String NAME = "initial-ping-handler";

   private final OperationsFactory factory;
   private final HeaderDecoder headerDecoder;

   public InitialPingHandler(OperationsFactory factory, HeaderDecoder headerDecoder) {
      this.factory = factory;
      this.headerDecoder = headerDecoder;
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      Channel channel = ctx.channel();
      if (log.isTraceEnabled()) {
         log.tracef("Activating channel %s", channel);
      }
      ChannelRecord channelRecord = ChannelRecord.of(channel);
      PingOperation ping = factory.newPingOperation(false);
      headerDecoder.registerOperation(channel, ping);
      ByteBuf buf = channel.alloc().buffer();
      ping.writeBytes(channel, buf);
      ctx.writeAndFlush(buf);

      ping.whenComplete((result, throwable) -> {
         if (log.isTraceEnabled()) {
            log.tracef("Initial ping completed with result %s/%s", result, throwable);
         }
         if (throwable != null) {
            channelRecord.completeExceptionally(throwable);
         } else {
            channelRecord.complete(channel);
         }
      });
      ctx.pipeline().remove(this);
   }
}
