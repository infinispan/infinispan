package org.infinispan.server.hotrod;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.hotrod.logging.Log;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.CharsetUtil;

/**
 * Static helper to provide common way of writing response to channel
 *
 * @author wburns
 * @since 9.0
 */
public class ResponseWriting {
   private ResponseWriting() {
   }

   private final static Log log = LogFactory.getLog(ContextHandler.class, Log.class);
   private final static boolean trace = log.isTraceEnabled();

   /**
    * Writes the response to the channel
    *  @param ch
    * @param response
    */
   public static void writeResponse(Channel ch, Object response) {
      if (response != null) {
         if (trace) {
            log.tracef("Write response %s", response);
         }
         if (response instanceof Response) {
            ch.writeAndFlush(response);
         } else if (response instanceof ByteBuf[]) {
            for (ByteBuf buf : (ByteBuf[]) response) {
               ch.write(buf);
            }
            ch.flush();
         } else if (response instanceof byte[]) {
            ch.writeAndFlush(Unpooled.wrappedBuffer((byte[]) response));
         } else if (response instanceof CharSequence) {
            ch.writeAndFlush(Unpooled.copiedBuffer((CharSequence) response, CharsetUtil.UTF_8));
         } else {
            ch.writeAndFlush(response);
         }
      }
   }
}
