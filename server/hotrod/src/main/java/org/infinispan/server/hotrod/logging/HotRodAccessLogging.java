package org.infinispan.server.hotrod.logging;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.infinispan.commons.util.Util;
import org.infinispan.server.hotrod.AccessLoggingHeader;
import org.infinispan.server.hotrod.HotRodVersion;
import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import io.netty.channel.ChannelFuture;

/**
 * Logging handler for Hot Rod to log what requests have come into the server
 *
 * @author wburns
 * @since 9.0
 */
public class HotRodAccessLogging {
   private static final Logger log = LogFactory.getLogger("HOTROD_ACCESS_LOG");

   public static boolean isEnabled() {
      return log.isTraceEnabled();
   }

   public void logOK(ChannelFuture future, AccessLoggingHeader header, int responseBytes) {
      logAfterComplete(future, header, responseBytes, "OK");
   }

   public void logException(ChannelFuture future, AccessLoggingHeader header, String exception, int responseBytes) {
      logAfterComplete(future, header, responseBytes, exception);
   }

   private void logAfterComplete(ChannelFuture future, AccessLoggingHeader header, int responseBytes, String status) {
      String remoteAddress = ((InetSocketAddress)future.channel().remoteAddress()).getHostString();
      future.addListener(f -> {
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
         MDC.put("protocol", checkForNull(HotRodVersion.forVersion(header.getVersion())));
         MDC.put("status", checkForNull(status));
         MDC.put("responseSize", responseBytes);
         MDC.put("requestSize", header.requestBytes);
         MDC.put("duration", duration);
         log.tracef("/%s/%s", checkForNull(header.getCacheName()), checkForNull(header.key));
      });
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
}
