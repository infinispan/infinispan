package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OPENMETRICS_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestMetricsClient;
import org.infinispan.client.rest.RestResponse;

import okhttp3.Request;

public class RestMetricsClientOkHttp implements RestMetricsClient {

   private final RestClientOkHttp client;

   private final String baseMetricsURL;

   RestMetricsClientOkHttp(RestClientOkHttp client) {
      this.client = client;
      this.baseMetricsURL = String.format("%s/metrics", client.getBaseURL());
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
      Request.Builder builder = new Request.Builder()
            .addHeader("ACCEPT", openMetricsFormat ? APPLICATION_OPENMETRICS_TYPE : TEXT_PLAIN_TYPE);
      return client.execute(builder, baseMetricsURL);
   }

   private CompletionStage<RestResponse> metricsOptions() {
      Request.Builder builder = new Request.Builder()
            .method("OPTIONS", null);
      return client.execute(builder, baseMetricsURL);
   }
}
