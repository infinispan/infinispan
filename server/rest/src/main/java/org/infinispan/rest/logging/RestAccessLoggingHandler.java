package org.infinispan.rest.logging;

import java.net.InetSocketAddress;
import java.util.Map;

import org.infinispan.util.logging.LogFactory;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;

/**
 * Logging filter that can be used to output requests in a similar fashion to HTTPD log output
 *
 * @author wburns
 * @since 9.0
 */
public class RestAccessLoggingHandler {
   private static final Logger log = LogFactory.getLogger("REST_ACCESS_LOG");
   static final String X_REQUEST_TIME = "X-Request-Time";
   static final String X_FORWARDED_FOR = "X-Forwarded-For";
   static final String X_PRINCIPAL = "X-Principal";

   private boolean isEnabled() {
      return log.isTraceEnabled();
   }

   public void preLog(FullHttpRequest request) {
      if (isEnabled()) {
         // Set the starting time
         request.headers().add(X_REQUEST_TIME, Long.toString(System.currentTimeMillis()));
      }
   }

   public void log(ChannelHandlerContext ctx, FullHttpRequest request, HttpResponse response) {
      if (isEnabled()) {
         // IP
         String remoteAddress = request.headers().getAsString(X_FORWARDED_FOR);
         if (remoteAddress == null)
            remoteAddress = ((InetSocketAddress) ctx.channel().remoteAddress()).getHostString();
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
         String contentLength = response.headers().get(HttpHeaderNames.CONTENT_LENGTH.toString());
         int responseSize = contentLength == null ? 0 : Integer.parseInt(contentLength);
         // Response time
         long duration = now - requestTime;
         MDC.clear();
         for (Map.Entry<String, String> s : request.headers().entries()) {
            MDC.put("h:" + s.getKey(), s.getValue());
         }
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
