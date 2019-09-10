package org.infinispan.client.rest.impl.okhttp;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestServerClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestServerClientOkHttp implements RestServerClient {
   private final RestClientOkHttp client;
   private final String baseServerURL;

   RestServerClientOkHttp(RestClientOkHttp restClient) {
      this.client = restClient;
      this.baseServerURL = String.format("%s%s/v2/server", restClient.getBaseURL(), restClient.getConfiguration().contextPath()).replaceAll("//", "/");
   }

   @Override
   public CompletionStage<RestResponse> configuration() {
      return client.execute(baseServerURL, "config");
   }

   @Override
   public CompletionStage<RestResponse> stop() {
      return client.execute(baseServerURL, "stop");
   }

   @Override
   public CompletionStage<RestResponse> threads() {
      return client.execute(baseServerURL, "threads");
   }

   @Override
   public CompletionStage<RestResponse> info() {
      return client.execute(baseServerURL);
   }

   @Override
   public CompletionStage<RestResponse> memory() {
      return client.execute(baseServerURL, "memory");
   }

   @Override
   public CompletionStage<RestResponse> env() {
      return client.execute(baseServerURL, "env");
   }
}
