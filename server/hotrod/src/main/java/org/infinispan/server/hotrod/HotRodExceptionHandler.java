package org.infinispan.server.hotrod;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.logging.Log;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;

/**
 * Handler that will transform various exceptions into responses to send back to the client.
 *
 * @author wburns
 * @since 9.0
 */
public class HotRodExceptionHandler extends ChannelInboundHandlerAdapter {
   private final static Log log = LogFactory.getLog(HotRodExceptionHandler.class, Log.class);

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) throws Exception {
      Channel ch = ctx.channel();
      HotRodDecoder decoder = ctx.pipeline().get(HotRodDecoder.class);
      CacheDecodeContext decodeCtx = decoder.decodeCtx;

      log.debug("Exception caught", t);
      if (t instanceof DecoderException) {
         t = t.getCause();
      }
      if (t instanceof HotRodException) {
         // HotRodException is already translated to response
         ch.writeAndFlush(((HotRodException) t).response);
      } else {
         ch.writeAndFlush(decodeCtx.createExceptionResponse(t));
      }
   }
}
