package org.infinispan.client.rest.impl.jdk;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestContainerClient;
import org.infinispan.client.rest.RestResponse;

/**
 * @since 15.0
 **/
public class RestContainerClientJDK implements RestContainerClient {
   private final RestRawClientJDK client;
   private final String path;

   RestContainerClientJDK(RestRawClientJDK client) {
      this.client = client;
      this.path = client.getConfiguration().contextPath() + "/v2/container";
   }

   @Override
   public CompletionStage<RestResponse> shutdown() {
      return client.post(path + "?action=shutdown");
   }
}
