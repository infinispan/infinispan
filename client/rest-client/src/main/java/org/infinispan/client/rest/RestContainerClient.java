package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

/**
 * @author Ryan Emerson
 * @since 13.0
 */
public interface RestContainerClient {
   /**
    * Shuts down the container stopping all caches and resources. The servers remain running with active endpoints
    * and clustering, however REST calls to container resources will result in a 503 Service Unavailable response.
    */
   CompletionStage<RestResponse> shutdown();
}
