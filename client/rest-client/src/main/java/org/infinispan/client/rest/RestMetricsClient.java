package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

/**
 * An experimental rest client for microprofile metrics and OpenMetrics.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
public interface RestMetricsClient {

   CompletionStage<RestResponse> metrics();

   CompletionStage<RestResponse> metrics(String path);

   CompletionStage<RestResponse> metrics(boolean openMetrics);

   CompletionStage<RestResponse> metrics(String path, boolean openMetrics);

   CompletionStage<RestResponse> metricsMetadata();

   CompletionStage<RestResponse> metricsMetadata(String path);
}
