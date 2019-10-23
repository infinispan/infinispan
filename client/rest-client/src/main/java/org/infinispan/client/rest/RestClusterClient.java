package org.infinispan.client.rest;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestClusterClient {
   /**
    * Shuts down the cluster
    */
   CompletionStage<RestResponse> stop();

   /**
    * Shuts down the specified servers
    */
   CompletionStage<RestResponse> stop(List<String> server);
}
