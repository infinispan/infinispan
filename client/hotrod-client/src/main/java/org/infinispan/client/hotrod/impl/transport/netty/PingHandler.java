package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.impl.operations.NoCachePingOperation;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

public class PingHandler extends ChannelInboundHandlerAdapter {
   // Handler should handle the IdleStateEvent triggered by IdleStateHandler.
   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
      if (evt instanceof IdleStateEvent e) {
         if (e.state() == IdleState.READER_IDLE) {
            ctx.close();
         } else if (e.state() == IdleState.WRITER_IDLE) {
            NoCachePingOperation pingOperation = new NoCachePingOperation();
            ctx.channel()
                  .attr(OperationChannel.OPERATION_CHANNEL_ATTRIBUTE_KEY)
                  .get()
                  .forceSendOperation(pingOperation);
         }
      }
   }
}
