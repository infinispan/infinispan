package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OPENMETRICS_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.OPTIONS;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.impl.RestResponseBuilder;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.smallrye.metrics.MetricsRequestHandler;

/**
 * Eclipse MicroProfile metrics resource.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
public final class MetricsResource implements ResourceHandler {

   private static final String METRICS_PATH = "/metrics";

   private final MetricsRequestHandler requestHandler = new MetricsRequestHandler();

   public MetricsResource() {
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET, OPTIONS).path(METRICS_PATH).handleWith(this::metrics)
            .invocation().methods(GET, OPTIONS).path(METRICS_PATH + "/*").handleWith(this::metrics)
            .create();
   }

   private CompletionStage<RestResponse> metrics(RestRequest restRequest) {
      try {
         List<String> accept = restRequest.headers(HttpHeaderNames.ACCEPT.toString());

         // provide defaults for missing ACCEPT header (based on http method)
         if (restRequest.method() == GET) {
            if (accept.isEmpty()) {
               // default to OpenMetrics (Prometheus) if nothing specified
               accept = Collections.singletonList(TEXT_PLAIN_TYPE);
            } else {
               // to handle OpenMetrics we need to swap it to "text/plain" so smallrye can recognize it
               accept = accept.stream()
                     .map(h -> h.startsWith(APPLICATION_OPENMETRICS_TYPE) ? TEXT_PLAIN_TYPE : h)
                     .collect(Collectors.toList());
            }
         } else if (restRequest.method() == OPTIONS) {
            if (accept.isEmpty()) {
               accept = Collections.singletonList(APPLICATION_JSON_TYPE);
            }
         }

         RestResponseBuilder<NettyRestResponse.Builder> builder = new NettyRestResponse.Builder();

         requestHandler.handleRequest(restRequest.path(), METRICS_PATH, restRequest.method().name(),
               accept.stream(), (status, message, headers) -> {
                  builder.status(status).entity(message);
                  for (String header : headers.keySet()) {
                     builder.header(header, headers.get(header));
                  }
               });

         return completedFuture(builder.build());
      } catch (Exception e) {
         RestResponseBuilder<NettyRestResponse.Builder> builder = new NettyRestResponse.Builder()
               .status(INTERNAL_SERVER_ERROR).entity(e.getMessage());
         return completedFuture(builder.build());
      }
   }
}
