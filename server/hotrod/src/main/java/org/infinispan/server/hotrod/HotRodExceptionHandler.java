package org.infinispan.server.hotrod;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderException;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.core.logging.JavaLog;

/**
 * Handler that will transform various exceptions into responses to send back to the client.
 *
 * @author wburns
 * @since 9.0
 */
public class HotRodExceptionHandler extends ChannelInboundHandlerAdapter {
   private final static JavaLog log = LogFactory.getLog(HotRodExceptionHandler.class, JavaLog.class);

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
         ch.writeAndFlush(((HotRodException) t).response(), ch.voidPromise());
      } else {
         ch.writeAndFlush(decodeCtx.createExceptionResponse(t), ch.voidPromise());
      }
   }
}
