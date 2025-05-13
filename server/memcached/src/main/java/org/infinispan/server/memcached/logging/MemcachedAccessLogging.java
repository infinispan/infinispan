package org.infinispan.server.memcached.logging;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import io.netty.channel.ChannelFuture;

/**
 * Logging handler for Memcached to log what requests have come into the server
 *
 * @since 15.0
 */
public final class MemcachedAccessLogging {
   public static final Logger log = LogFactory.getLogger("MEMCACHED_ACCESS_LOG");

   public static boolean isEnabled() {
      return log.isTraceEnabled();
   }

   public static void logOK(ChannelFuture future, Header header, int responseBytes) {
      logAfterComplete(future, header, responseBytes, "OK");
   }

   public static void logException(ChannelFuture future, Header header, String exception, int responseBytes) {
      logAfterComplete(future, header, responseBytes, exception);
   }

   private static void logAfterComplete(ChannelFuture future, Header header, int responseBytes, String status) {
      String remoteAddress = ((InetSocketAddress)future.channel().remoteAddress()).getHostString();
      if (future.isDone()) {
         logAfterComplete(remoteAddress, header, responseBytes, status);
         return;
      }
      future.addListener(f -> logAfterComplete(remoteAddress, header, responseBytes, status));
   }

   private static void logAfterComplete(String remoteAddress, Header header, int responseBytes, String status) {
      // Duration
      long duration;
      if (header.requestStart == null) {
         duration = -1;
      } else {
         duration = ChronoUnit.MILLIS.between(header.requestStart, Instant.now());
      }
      MDC.clear();
      MDC.put("address", remoteAddress);
      MDC.put("user", checkForNull(header.principalName));
      MDC.put("method", checkForNull(header.getOp()));
      MDC.put("protocol", header.getProtocol());
      MDC.put("status", checkForNull(status));
      MDC.put("responseSize", responseBytes);
      MDC.put("requestSize", header.requestBytes);
      MDC.put("duration", duration);
      log.tracef("/%s", checkForNull(header.getKey()));
   }

   private static String checkForNull(Object obj) {
      if (obj == null || obj instanceof String && ((String) obj).isEmpty()) {
         return "-";
      } else if (obj instanceof byte[]) {
         return Util.printArray((byte[]) obj);
      } else {
         return obj.toString();
      }
   }
}
