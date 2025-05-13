package org.infinispan.server.resp.logging;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import io.netty.channel.ChannelFuture;

public class RespAccessLogger {

   public static final Logger log = LogFactory.getLogger("RESP_ACCESS_LOG");

   public static boolean isEnabled() {
      return log.isTraceEnabled();
   }

   static void success(ChannelFuture future, AccessData data) {
      log(future, data, "OK");
   }

   static void failure(ChannelFuture future, AccessData data, Throwable throwable) {
      log(future, data, throwable.getMessage());
   }

   private static void log(ChannelFuture future, AccessData data, String status) {
      String remoteAddress = ((InetSocketAddress) future.channel().remoteAddress()).getHostString();
      if (future.isDone()) {
         log(remoteAddress, data, status);
         return;
      }
      future.addListener(f -> log(remoteAddress, data, status));
   }

   private static void log(String remoteAddress, AccessData data, String status) {
      long duration;
      if (data.start() == null) {
         duration = -1;
      } else {
         duration = ChronoUnit.MILLIS.between(data.start(), Instant.now());
      }

      MDC.clear();
      MDC.put("address", remoteAddress);
      MDC.put("user", parameter(data.principalName()));
      MDC.put("method", parameter(data.operation()));
      MDC.put("protocol", "RESP");
      MDC.put("status", parameter(status));
      MDC.put("responseSize", data.responseBytes());
      MDC.put("requestSize", data.requestBytes());
      MDC.put("duration", duration);
      log.tracef("/%s", parameter(data.keys()));
   }

   private static String parameter(Object obj) {
      if (obj == null || obj instanceof String && ((String) obj).isEmpty()) {
         return "-";
      }

      if (obj instanceof byte[]) {
         return Util.printArray((byte[]) obj);
      }

      if (obj instanceof byte[][]) {
         boolean wrote = false;
         StringBuilder builder = new StringBuilder("[");
         for (byte[] b : (byte[][]) obj) {
            wrote = true;
            builder.append(Util.printArray(b)).append(",");
         }
         if (wrote) builder.deleteCharAt(builder.length() - 1);
         return builder.append("]").toString();
      }

      return obj.toString();
   }
}
