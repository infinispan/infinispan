package org.infinispan.rest.logging;

import java.net.InetSocketAddress;

import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

/**
 * Logging filter that can be used to output requests in a similar fashion to HTTPD log output
 *
 * @author wburns
 * @since 9.0
 */
public class RestAccessLoggingHandler {
   private final static Logger log = LogFactory.getLogger("REST_ACCESS_LOG");
   final static String X_REQUEST_TIME = "X-Request-Time";
   final static String X_FORWARDED_FOR = "X-Forwarded-For";
   final static String X_PRINCIPAL = "X-Principal";

   private boolean isEnabled() {
      return log.isTraceEnabled();
   }

   public void preLog(FullHttpRequest request) {
      if (isEnabled()) {
         // Set the starting time
         request.headers().add(X_REQUEST_TIME, Long.toString(System.currentTimeMillis()));
      }
   }

   public void log(ChannelHandlerContext ctx, FullHttpRequest request, FullHttpResponse response) {
      if (isEnabled()) {
         // IP
         String remoteAddress = request.headers().getAsString(X_FORWARDED_FOR);
         if (remoteAddress == null)
            remoteAddress = ((InetSocketAddress)ctx.channel().remoteAddress()).getHostString();
         // User
         String who = request.headers().get(X_PRINCIPAL);
         if (who == null)
            who = "-";
         // Date
         long now = System.currentTimeMillis();
         String requestTimeString = request.headers().get(X_REQUEST_TIME);
         long requestTime = requestTimeString != null ? Long.parseLong(requestTimeString) : now;

         // Request method | path | protocol
         String requestMethod = request.method().toString();
         String uri = request.uri();
         // Status code
         int status = response.status().code();
         // Body request size
         int requestSize = request.content().readableBytes();
         // Body response Size - usually -1 so we calculate below
         int responseSize = response.content().readableBytes();
         // Response time
         long duration = now - requestTime;
         MDC.clear();
         MDC.put("address", remoteAddress);
         MDC.put("user", who);
         MDC.put("method", requestMethod);
         MDC.put("protocol", request.protocolVersion().text());
         MDC.put("status", status);
         MDC.put("responseSize", responseSize);
         MDC.put("requestSize", requestSize);
         MDC.put("duration", duration);

         log.tracef("%s", uri);
      }
   }
}
