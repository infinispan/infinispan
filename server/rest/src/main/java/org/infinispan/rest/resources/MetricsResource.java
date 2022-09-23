package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.OPTIONS;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.infinispan.metrics.impl.MetricsCollector;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.impl.RestResponseBuilder;

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

   public MetricsResource(boolean auth, InvocationHelper invocationHelper) {
      this.auth = auth;
      this.blockingExecutor = invocationHelper.getExecutor();
      this.metricsCollector = invocationHelper.getMetricsCollector();
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder()
            .invocation().methods(GET, OPTIONS).path(METRICS_PATH).anonymous(!auth).handleWith(this::metrics)
            .invocation().methods(GET, OPTIONS).path(METRICS_PATH + "/*").anonymous(!auth).handleWith(this::metrics)
            .create();
   }

   private CompletionStage<RestResponse> metrics(RestRequest restRequest) {
      return CompletableFuture.supplyAsync(() -> {
         RestResponseBuilder<NettyRestResponse.Builder> builder = new NettyRestResponse.Builder();

         try {
            String contentType = TextFormat.chooseContentType(restRequest.getAcceptHeader());
            builder.header("Content-Type", contentType);
            builder.entity(metricsCollector.registry().scrape(contentType));

            return builder.build();
         } catch (Exception e) {
            RestResponseBuilder<NettyRestResponse.Builder> errorBuilder = new NettyRestResponse.Builder()
                  .status(INTERNAL_SERVER_ERROR).entity(e.getMessage());
            return errorBuilder.build();
         }
      }, blockingExecutor);
   }
}
