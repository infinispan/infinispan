package org.infinispan.client.rest.impl.okhttp;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestRawClient;
import org.infinispan.client.rest.RestResponse;

import okhttp3.FormBody;
import okhttp3.Request;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class RestRawClientOkHttp implements RestRawClient {
   private final RestClientOkHttp restClient;

   RestRawClientOkHttp(RestClientOkHttp restClient) {
      this.restClient = restClient;
   }

   @Override
   public CompletionStage<RestResponse> postForm(String url, Map<String, String> headers, Map<String, String> formParameters) {
      Request.Builder builder = new Request.Builder();
      builder.url(restClient.getBaseURL() + url);
      headers.forEach((k, v) -> builder.header(k, v));
      FormBody.Builder form = new FormBody.Builder();
      formParameters.forEach((k, v) -> form.add(k, v));
      builder.post(form.build());
      return restClient.execute(builder);
   }
}
