package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler that is added to the end of pipeline during channel creation and handshake.
 * Its task is to complete {@link ChannelRecord}.
 */
@Sharable
class ActivationHandler extends ChannelInboundHandlerAdapter {
   public static final ActivationHandler INSTANCE = new ActivationHandler();
   private static final Log log = LogFactory.getLog(ActivationHandler.class);
   private static final boolean trace = log.isTraceEnabled();
   static final Object ACTIVATION_EVENT = new Object();

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      if (trace) {
         log.tracef("Activating channel %s", ctx.channel());
      }
      ChannelRecord.of(ctx.channel()).complete(ctx.channel());
      ctx.pipeline().remove(this);
   }

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt == ACTIVATION_EVENT) {
         channelActive(ctx);
      } else {
         ctx.fireUserEventTriggered(evt);
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      Channel channel = ctx.channel();
      if (trace) {
         log.tracef(cause, "Failed to activate channel %s", channel);
      }
      try {
         ctx.close();
      } finally {
         ChannelRecord channelRecord = ChannelRecord.of(channel);
         // With sync Hot Rod any failure to fetch a transport from pool was wrapped in TransportException
         channelRecord.completeExceptionally(new TransportException(cause, channelRecord.getUnresolvedAddress()));
      }
   }
}
