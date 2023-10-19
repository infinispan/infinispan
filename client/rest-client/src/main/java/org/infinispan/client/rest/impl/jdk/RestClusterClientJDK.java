package org.infinispan.client.rest.impl.jdk;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestClusterClient;
import org.infinispan.client.rest.RestResponse;

/**
 * @since 14.0
 **/
public class RestClusterClientJDK implements RestClusterClient {
   private final RestRawClientJDK client;
   private final String path;

   RestClusterClientJDK(RestRawClientJDK client) {
      this.client = client;
      this.path = client.getConfiguration().contextPath() + "/v2/cluster";
   }

   @Override
   public CompletionStage<RestResponse> stop() {
      return stop(Collections.emptyList());
   }

   @Override
   public CompletionStage<RestResponse> stop(List<String> servers) {
      StringBuilder sb = new StringBuilder(path);
      sb.append("?action=stop");
      for (String server : servers) {
         sb.append("&server=");
         sb.append(server);
      }
      return client.post(sb.toString());
   }


   @Override
   public CompletionStage<RestResponse> distribution() {
      return client.get(path + "?action=distribution");
   }
}
