package org.infinispan.server.hotrod.logging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.server.hotrod.CacheDecodeContext;
import org.infinispan.server.hotrod.HotRodOperation;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * Logging handler for hotrod to log what requests have come into the server
 *
 * @author wburns
 * @since 9.0
 */
public class HotRodAccessLoggingHandler extends ChannelDuplexHandler {
   private static final JavaLog log = LogFactory.getLog(HotRodAccessLoggingHandler.class, JavaLog.class);

   LocalDateTime startTime;
   private int bytesRead = 0;

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (log.isTraceEnabled()) {
         if (startTime == null) {
            startTime = LocalDateTime.now();
         }
         bytesRead += getByteSize(msg);

         // Make sure we don't have a decode context between requests
         ctx.channel().attr(LoggingContextHandler.DECODE_CONTEXT_KEY).remove();
      }

      super.channelRead(ctx, msg);
   }

   @Override
   public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      if (startTime != null) {
         // We need the information from the context now, because we could clear the context soon after the write completes
         CacheDecodeContext cacheDecodeContext = ctx.channel().attr(LoggingContextHandler.DECODE_CONTEXT_KEY).get();

         HotRodOperation op;
         String cacheName;
         String status;
         String exception = ctx.channel().attr(LoggingContextHandler.EXCEPTION_MESSAGE_KEY).get();
         byte[] key;
         // This is only null if an exception was thrown before we could decode a proper message
         if (cacheDecodeContext != null && exception == null) {
            // Method
            op = cacheDecodeContext.header().op();
            key = cacheDecodeContext.key();
            // Cache name
            cacheName = cacheDecodeContext.header().cacheName();
            status = "OK";
         } else {
            op = ctx.channel().attr(LoggingContextHandler.OPERATION_KEY).get();
            key = null;
            cacheName = ctx.channel().attr(LoggingContextHandler.CACHE_NAME_KEY).get();
            status = exception;
         }

         // IP
         String remoteAddress = ctx.channel().remoteAddress().toString();
         // Date of first byte read
         LocalDateTime startTime = this.startTime;
         this.startTime = null;

         // Request Length
         int bytesRead = this.bytesRead;
         this.bytesRead = 0;

         // Response Length - We rely on the fact that our encoder encodes the entire response in 1 write method
         int bytesWritten = getByteSize(msg);

         super.write(ctx, msg, promise.addListener(f -> {
            // Duration
            long ms;
            if (startTime == null) {
               ms = -1;
            } else {
               ms = ChronoUnit.MILLIS.between(startTime, LocalDateTime.now());
            }
            log.tracef("%s [%s] \"%s %s\" \"%s\" %s %d %d %d ms", remoteAddress,
                    checkForNull(startTime), checkForNull(op), checkForNull(cacheName),
                    status, checkForNull(key), bytesRead, bytesWritten, ms);
         }));
      } else {
         super.write(ctx, msg, promise);
      }
   }

   String checkForNull(Object obj) {
      if (obj == null || obj instanceof String && ((String)obj).isEmpty()) {
         return "-";
      } else if (obj instanceof byte[]) {
         return Util.printArray((byte[])obj);
      } else {
         return obj.toString();
      }
   }

   int getByteSize(Object msg) {
      if (msg instanceof ByteBuf) {
         return ((ByteBuf) msg).readableBytes();
      } else if (msg instanceof ByteBufHolder) {
         return ((ByteBufHolder) msg).content().readableBytes();
      } else {
         return -1;
      }
   }
}
