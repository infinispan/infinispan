package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

/**
 * An experimental rest client for Micrometer metrics in native and Prometheus compatible OpenMetrics format.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
public interface RestMetricsClient {

   CompletionStage<RestResponse> metrics();

   CompletionStage<RestResponse> metrics(String path);

   CompletionStage<RestResponse> metrics(boolean openMetricsFormat);

   CompletionStage<RestResponse> metrics(String path, boolean openMetricsFormat);

   CompletionStage<RestResponse> metricsMetadata();

   CompletionStage<RestResponse> metricsMetadata(String path);
}
