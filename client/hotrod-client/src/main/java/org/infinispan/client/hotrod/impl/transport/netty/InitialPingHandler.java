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

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      super.userEventTriggered(ctx, evt);    // TODO: Customise this generated block
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      super.exceptionCaught(ctx, cause);    // TODO: Customise this generated block
   }

   @Override
   public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      super.channelRegistered(ctx);    // TODO: Customise this generated block
   }

   @Override
   public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      super.channelUnregistered(ctx);    // TODO: Customise this generated block
   }

   @Override
   public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      super.channelInactive(ctx);    // TODO: Customise this generated block
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      super.channelRead(ctx, msg);    // TODO: Customise this generated block
   }

   @Override
   public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
      super.channelReadComplete(ctx);    // TODO: Customise this generated block
   }

   @Override
   public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      super.channelWritabilityChanged(ctx);    // TODO: Customise this generated block
   }
}
