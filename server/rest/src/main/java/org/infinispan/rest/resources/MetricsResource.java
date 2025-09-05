package org.infinispan.rest.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.OPTIONS;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import org.infinispan.commons.util.Util;
import org.infinispan.metrics.impl.MetricsRegistry;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.framework.impl.RestResponseBuilder;

import io.prometheus.metrics.expositionformats.ExpositionFormats;

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
   private final InvocationHelper invocationHelper;

   public MetricsResource(boolean auth, InvocationHelper invocationHelper) {
      this.auth = auth;
      this.blockingExecutor = invocationHelper.getExecutor();
      this.invocationHelper = invocationHelper;
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("metrics", "REST endpoint for metrics.")
            .invocation().methods(GET, OPTIONS).path(METRICS_PATH).anonymous(!auth).handleWith(this::metrics)
            .invocation().methods(GET, OPTIONS).path(METRICS_PATH + "/*").anonymous(!auth).handleWith(this::metrics)
            .create();
   }

   private CompletionStage<RestResponse> metrics(RestRequest request) {
      return CompletableFuture.supplyAsync(() -> {
         RestResponseBuilder<NettyRestResponse.Builder> builder = invocationHelper.newResponse(request);

         MetricsRegistry metricsRegistry = invocationHelper.getMetricsRegistry();
         try {
            if (metricsRegistry.supportScrape()) {
               String contentType = ExpositionFormats.init().findWriter(request.getAcceptHeader()).getContentType();
               builder.header("Content-Type", contentType);
               builder.entity(metricsRegistry.scrape(contentType));
            } else {
               builder.status(NOT_FOUND.code());
            }

            return builder.build();
         } catch (Exception e) {
            throw Util.unchecked(e);
         }
      }, blockingExecutor);
   }
}
