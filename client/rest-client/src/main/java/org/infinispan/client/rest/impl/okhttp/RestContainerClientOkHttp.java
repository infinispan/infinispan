package org.infinispan.client.rest.impl.okhttp;

import static org.infinispan.client.rest.impl.okhttp.RestClientOkHttp.EMPTY_BODY;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestContainerClient;
import org.infinispan.client.rest.RestResponse;

import okhttp3.Request;

/**
 * @author Ryan Emerson
 * @since 13.0
 */
public class RestContainerClientOkHttp implements RestContainerClient {
   private final RestClientOkHttp client;
   private final String baseClusterURL;

   public RestContainerClientOkHttp(RestClientOkHttp client) {
      this.client = client;
      this.baseClusterURL = String.format("%s%s/v2/container", client.getBaseURL(), client.getConfiguration().contextPath()).replaceAll("//", "/");
   }

   @Override
   public CompletionStage<RestResponse> shutdown() {
      return client.execute(
            new Request.Builder()
                  .post(EMPTY_BODY)
                  .url(baseClusterURL + "?action=shutdown")
      );
   }
}
