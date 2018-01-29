package org.infinispan.server.hotrod.logging;

import java.net.InetSocketAddress;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;

import org.infinispan.commons.util.Util;
import org.infinispan.server.hotrod.CacheDecodeContext;
import org.infinispan.server.hotrod.HotRodOperation;
import org.infinispan.server.hotrod.HotRodVersion;
import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * Logging handler for Hot Rod to log what requests have come into the server
 *
 * @author wburns
 * @since 9.0
 */
public class HotRodAccessLoggingHandler extends ChannelDuplexHandler {
   private final static Logger log = LogFactory.getLogger("HOTROD_ACCESS_LOG");

   Temporal when;
   private int requestSize = 0;

   public static boolean isEnabled() {
      return log.isTraceEnabled();
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      if (when == null) {
         when = ZonedDateTime.now();
      }
      requestSize += getByteSize(msg);

      // Make sure we don't have a decode context between requests
      ctx.channel().attr(LoggingContextHandler.DECODE_CONTEXT_KEY).set(null);

      super.channelRead(ctx, msg);
   }

   @Override
   public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      if (when != null) {
         // We need the information from the context now, because we could clear the context soon after the write completes
         CacheDecodeContext cacheDecodeContext = ctx.channel().attr(LoggingContextHandler.DECODE_CONTEXT_KEY).get();

         HotRodOperation op;
         String cacheName;
         String status;
         String exception = ctx.channel().attr(LoggingContextHandler.EXCEPTION_MESSAGE_KEY).get();
         byte[] key;
         String who;
         HotRodVersion version;
         // This is only null if an exception was thrown before we could decode a proper message
         if (cacheDecodeContext != null && exception == null) {
            // Method
            op = cacheDecodeContext.getHeader().getOp();
            version = HotRodVersion.forVersion(cacheDecodeContext.getHeader().getVersion());
            key = cacheDecodeContext.getKey();
            // Cache name
            cacheName = cacheDecodeContext.getHeader().getCacheName();
            status = "OK";
            who = cacheDecodeContext.getPrincipalName();
         } else {
            op = ctx.channel().attr(LoggingContextHandler.OPERATION_KEY).get();
            key = null;
            cacheName = ctx.channel().attr(LoggingContextHandler.CACHE_NAME_KEY).get();
            status = exception;
            version = HotRodVersion.UNKNOWN;
            who = null;
         }

         // IP
         String remoteAddress = ((InetSocketAddress)ctx.channel().remoteAddress()).getHostString();
         // Date of first byte read
         Temporal startTime = this.when;
         this.when = null;

         // Request Length
         int requestSize = this.requestSize;
         this.requestSize = 0;

         // Response Length - We rely on the fact that our encoder encodes the entire response in 1 write method
         int responseSize = getByteSize(msg);

         super.write(ctx, msg, promise.addListener(f -> {
            // Duration
            long duration;
            if (startTime == null) {
               duration = -1;
            } else {
               duration = ChronoUnit.MILLIS.between(startTime, ZonedDateTime.now());
            }
            MDC.clear();
            MDC.put("address", remoteAddress);
            MDC.put("user", checkForNull(who));
            MDC.put("method", checkForNull(op));
            MDC.put("protocol", checkForNull(version));
            MDC.put("status", checkForNull(status));
            MDC.put("responseSize", responseSize);
            MDC.put("requestSize", requestSize);
            MDC.put("duration", duration);
            log.tracef("/%s/%s", checkForNull(cacheName), checkForNull(key));
         }));
      } else {
         super.write(ctx, msg, promise);
      }
   }

   String checkForNull(Object obj) {
      if (obj == null || obj instanceof String && ((String) obj).isEmpty()) {
         return "-";
      } else if (obj instanceof byte[]) {
         return Util.printArray((byte[]) obj);
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
