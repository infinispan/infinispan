package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OPENMETRICS_TYPE;

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
      return metricsGet("", false);
   }

   @Override
   public CompletionStage<RestResponse> metrics(String path) {
      return metricsGet(path, false);
   }

   @Override
   public CompletionStage<RestResponse> metrics(boolean openMetricsFormat) {
      return metricsGet("", openMetricsFormat);
   }

   @Override
   public CompletionStage<RestResponse> metrics(String path, boolean openMetricsFormat) {
      return metricsGet(path, openMetricsFormat);
   }

   @Override
   public CompletionStage<RestResponse> metricsMetadata() {
      return metricsOptions("");
   }

   @Override
   public CompletionStage<RestResponse> metricsMetadata(String path) {
      return metricsOptions(path);
   }

   private CompletionStage<RestResponse> metricsGet(String path, boolean openMetricsFormat) {
      Request.Builder builder = new Request.Builder()
            .addHeader("ACCEPT", openMetricsFormat ? APPLICATION_OPENMETRICS_TYPE : APPLICATION_JSON_TYPE);
      return client.execute(builder, baseMetricsURL, path);
   }

   private CompletionStage<RestResponse> metricsOptions(String path) {
      Request.Builder builder = new Request.Builder()
            .method("OPTIONS", null);
      return client.execute(builder, baseMetricsURL, path);
   }
}
