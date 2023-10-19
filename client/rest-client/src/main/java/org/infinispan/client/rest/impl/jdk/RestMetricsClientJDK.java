package org.infinispan.client.rest.impl.jdk;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OPENMETRICS_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestMetricsClient;
import org.infinispan.client.rest.RestResponse;

/**
 * @since 14.0
 **/
public class RestMetricsClientJDK implements RestMetricsClient {
   private final RestRawClientJDK client;
   private final String path = "/metrics";

   RestMetricsClientJDK(RestRawClientJDK client) {
      this.client = client;
   }

   @Override
   public CompletionStage<RestResponse> metrics() {
      return metricsGet(false);
   }

   @Override
   public CompletionStage<RestResponse> metrics(boolean openMetricsFormat) {
      return metricsGet(openMetricsFormat);
   }

   @Override
   public CompletionStage<RestResponse> metricsMetadata() {
      return metricsOptions();
   }

   private CompletionStage<RestResponse> metricsGet(boolean openMetricsFormat) {
      return client.get(path, Map.of(RestClientJDK.ACCEPT, openMetricsFormat ? APPLICATION_OPENMETRICS_TYPE : TEXT_PLAIN_TYPE));
   }

   private CompletionStage<RestResponse> metricsOptions() {
      return client.options(path);
   }
}
