package org.infinispan.rest.logging;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.util.logging.LogFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Logging filter that can be used to output requests in a similar fashion to HTTPD log output
 *
 * @author wburns
 * @since 9.0
 */
@Provider
public class RestAccessLoggingHandler implements ContainerResponseFilter, ContainerRequestFilter {
   private final static JavaLog log = LogFactory.getLog(RestAccessLoggingHandler.class, JavaLog.class);

   private final static String NANO_TIME = "NanoTime";

   @Context
   private ChannelHandlerContext context;

   @Override
   public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
      // IP
      String remoteAddress = context.channel().remoteAddress().toString();
      // Date
      long startNano = Long.parseLong(requestContext.getHeaderString(NANO_TIME));
      // Request method | path | protocol
      String requestMethod = requestContext.getMethod();
      String uri = requestContext.getUriInfo().getPath();
      // Status code
      int status = responseContext.getStatus();
      // Body request size
      int requestSize = requestContext.getLength();
      // Body response Size - usually -1 so we calculate below
      int responseSize = responseContext.getLength();
      // Netty doesn't usually set the CONTENT_LENGTH on response - check if we can
      if (responseSize == -1 && responseContext.hasEntity()) {
         Object entity = responseContext.getEntity();
         if (entity instanceof byte[]) {
            responseSize = ((byte[]) entity).length;
            responseContext.getHeaders().addFirst(HttpHeaders.CONTENT_LENGTH, responseSize);
         }
      }
      // Response time
      long responseTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);

      log.tracef("%s [%s] \"%s %s\" %s %d %d %d ms", remoteAddress, responseTime, requestMethod, uri, status, requestSize,
              responseSize, responseTime);
   }

   @Override
   public void filter(ContainerRequestContext requestContext) throws IOException {
      int requestSize = requestContext.getLength();
      if (requestSize == -1) {
         requestSize = requestContext.getEntityStream().available();
         requestContext.getHeaders().putSingle(HttpHeaders.CONTENT_LENGTH, Integer.toString(requestSize));
      }
      // Set the starting time
      requestContext.getHeaders().putSingle(NANO_TIME, Long.toString(System.nanoTime()));
   }
}
