package org.infinispan.server.hotrod;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.util.CharsetUtil;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.hotrod.logging.JavaLog;
import scala.Tuple2;

import java.security.PrivilegedActionException;

/**
 * Static helper to provide common way of writing response to channel
 *
 * @author wburns
 * @since 9.0
 */
public class ResponseWriting {
   private ResponseWriting() { }

   private final static JavaLog log = LogFactory.getLog(ContextHandler.class, JavaLog.class);

   /**
    * Writes the response to the channel
    * @param ctx
    * @param ch
    * @param response
    */
   public static void writeResponse(CacheDecodeContext ctx, Channel ch, Object response) {
      if (response != null) {
         if (ctx.isTrace()) {
            log.tracef("Write response %s", response);
         }
         if (response instanceof Response) {
            ch.writeAndFlush(response, ch.newPromise());
         } else if (response instanceof ByteBuf[]) {
            for (ByteBuf buf : (ByteBuf[]) response) {
               ch.write(buf, ch.voidPromise());
            }
            ch.flush();
         } else if (response instanceof byte[]) {
            ch.writeAndFlush(Unpooled.wrappedBuffer((byte[]) response), ch.newPromise());
         } else if (response instanceof CharSequence) {
            ch.writeAndFlush(Unpooled.copiedBuffer((CharSequence) response, CharsetUtil.UTF_8), ch.newPromise());
         } else {
            ch.writeAndFlush(response, ch.newPromise());
         }
      }
   }
}
