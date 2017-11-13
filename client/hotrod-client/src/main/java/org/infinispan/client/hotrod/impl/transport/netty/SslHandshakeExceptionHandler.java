package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.exceptions.TransportException;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

@Sharable
public class SslHandshakeExceptionHandler extends ChannelInboundHandlerAdapter {
   public static final SslHandshakeExceptionHandler INSTANCE = new SslHandshakeExceptionHandler();

   @Override
   public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof SslHandshakeCompletionEvent) {
         if (evt != SslHandshakeCompletionEvent.SUCCESS) {
            SslHandshakeCompletionEvent sslEvent = (SslHandshakeCompletionEvent) evt;
            ctx.fireExceptionCaught(new TransportException(sslEvent.cause(), ctx.channel().remoteAddress()));
         }
         ctx.pipeline().remove(this);
      } else {
         ctx.fireUserEventTriggered(evt);
      }
   }
}
