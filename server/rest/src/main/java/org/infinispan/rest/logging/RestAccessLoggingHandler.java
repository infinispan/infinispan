package org.infinispan.rest.logging;

import java.util.concurrent.TimeUnit;

import org.infinispan.util.logging.LogFactory;

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

   private final static Log log = LogFactory.getLog(RestAccessLoggingHandler.class, Log.class);

   private final static String NANO_TIME = "NanoTime";
   private final static String X_FORWARDED_FOR = "X-Forwarded-For";

   private boolean isEnabled() {
      return log.isTraceEnabled();
   }

   public void log(ChannelHandlerContext ctx, FullHttpRequest request, FullHttpResponse response) {
      if (isEnabled()) {
         // IP
         String remoteAddress = request.headers().getAsString(X_FORWARDED_FOR);
         if (remoteAddress == null)
            remoteAddress = ctx.channel().remoteAddress().toString();
         // Date
         String timeString = request.headers().getAsString(NANO_TIME);
         long startNano;
         if (timeString != null) {
            startNano = Long.parseLong(timeString);
         } else {
            startNano = 0;
         }
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
         long responseTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);

         log.tracef("%s [%s] \"%s %s\" %s %d %d %d ms", remoteAddress, responseTime, requestMethod, uri, status, requestSize,
               responseSize, responseTime);
      }
   }
}
