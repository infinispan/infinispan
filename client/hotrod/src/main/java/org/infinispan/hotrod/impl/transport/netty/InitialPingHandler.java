package org.infinispan.hotrod.impl.transport.netty;

import org.infinispan.hotrod.impl.operations.PingOperation;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public class InitialPingHandler extends ActivationHandler {
   private static final Log log = LogFactory.getLog(InitialPingHandler.class);

   static final String NAME = "initial-ping-handler";

   private final PingOperation ping;

   public InitialPingHandler(PingOperation ping) {
      this.ping = ping;
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      Channel channel = ctx.channel();
      if (log.isTraceEnabled()) {
         log.tracef("Activating channel %s", channel);
      }
      ChannelRecord channelRecord = ChannelRecord.of(channel);
      ping.invoke(channel);
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
