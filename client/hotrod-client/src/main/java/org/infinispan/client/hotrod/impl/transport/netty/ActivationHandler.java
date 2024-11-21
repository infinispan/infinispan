package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.impl.operations.NoCachePingOperation;
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
   static final String NAME = "activation-handler";
   private static final Log log = LogFactory.getLog(ActivationHandler.class);
   static final ActivationHandler INSTANCE = new ActivationHandler();
   static final Object ACTIVATION_EVENT = new Object();

   protected void activate(ChannelHandlerContext ctx, OperationChannel operationChannel) {
      Channel channel = ctx.channel();

      if (log.isTraceEnabled()) {
         log.tracef("Activating channel %s", channel);
      }
      NoCachePingOperation ping = new NoCachePingOperation();
      operationChannel.forceSendOperation(ping);
      ping.whenComplete((r, t) -> {
         ctx.pipeline().remove(this);
         if (t != null) {
            channel.pipeline().fireExceptionCaught(t);
         } else {
            operationChannel
                  .markAcceptingRequests();
         }
      });
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      // Always let the other handlers run first, as HeaderDecoder needs to be setup first properly
      super.channelActive(ctx);
      OperationChannel operationChannel = ctx.channel().attr(OperationChannel.OPERATION_CHANNEL_ATTRIBUTE_KEY).get();

      // The operationChannel can be null if the userEvent hasn't been triggered as that sets it
      if (operationChannel != null) {
         activate(ctx, operationChannel);
      }
   }

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
      if (evt == ACTIVATION_EVENT) {
         // If the netty channel isn't fully active, wait until it is before we do our activation
         // We determine it is fully active when the header decoder has a channel - aka its active method has been
         // invoked
         if (((HeaderDecoder) ctx.pipeline().get(HeaderDecoder.NAME)).getChannel() != null) {
            activate(ctx, ctx.channel().attr(OperationChannel.OPERATION_CHANNEL_ATTRIBUTE_KEY).get());
         }
      } else {
         ctx.fireUserEventTriggered(evt);
      }
   }
}
