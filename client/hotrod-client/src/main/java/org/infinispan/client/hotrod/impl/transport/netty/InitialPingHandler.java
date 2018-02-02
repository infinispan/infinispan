package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.impl.operations.PingOperation;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public class InitialPingHandler extends ActivationHandler {
   private static final Log log = LogFactory.getLog(InitialPingHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   static final String NAME = "initial-ping-handler";

   private final PingOperation ping;

   public InitialPingHandler(PingOperation ping) {
      this.ping = ping;
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      Channel channel = ctx.channel();
      if (trace) {
         log.tracef("Activating channel %s", channel);
      }
      ChannelRecord channelRecord = ChannelRecord.of(channel);
      ping.invoke(channel);
      ping.whenComplete((result, throwable) -> {
         if (trace) {
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
