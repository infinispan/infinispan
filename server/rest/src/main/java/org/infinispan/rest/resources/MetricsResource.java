package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.OPTIONS;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.infinispan.commons.util.Util;
import org.infinispan.metrics.impl.MetricsCollector;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.impl.RestResponseBuilder;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.common.TextFormat;

/**
 * Micrometer metrics resource.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
public final class MetricsResource implements ResourceHandler {

   private static final String METRICS_PATH = "/metrics";

   private final boolean auth;
   private final Executor blockingExecutor;
   private final MetricsCollector metricsCollector;
   private final InvocationHelper invocationHelper;

   public MetricsResource(boolean auth, InvocationHelper invocationHelper) {
      this.auth = auth;
      this.blockingExecutor = invocationHelper.getExecutor();
      this.metricsCollector = invocationHelper.getMetricsCollector();
      this.invocationHelper = invocationHelper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET, OPTIONS).path(METRICS_PATH).anonymous(!auth).handleWith(this::metrics)
            .invocation().methods(GET, OPTIONS).path(METRICS_PATH + "/*").anonymous(!auth).handleWith(this::metrics)
            .create();
   }

   private CompletionStage<RestResponse> metrics(RestRequest request) {
      return CompletableFuture.supplyAsync(() -> {
         RestResponseBuilder<NettyRestResponse.Builder> builder = invocationHelper.newResponse(request);

         try {
            PrometheusMeterRegistry registry = metricsCollector.registry();
            if (registry == null) {
               return builder.status(NOT_FOUND.code()).build();
            }
            String contentType = TextFormat.chooseContentType(request.getAcceptHeader());
            builder.header("Content-Type", contentType);
            builder.entity(registry.scrape(contentType));

            return builder.build();
         } catch (Exception e) {
            throw Util.unchecked(e);
         }
      }, blockingExecutor);
   }
}
