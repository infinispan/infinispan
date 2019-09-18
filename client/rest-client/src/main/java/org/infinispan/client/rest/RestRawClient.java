package org.infinispan.client.rest;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestRawClient {
   CompletionStage<RestResponse> postForm(String url, Map<String, String> headers, Map<String, String> formParameters);

   CompletionStage<RestResponse> putValue(String url, Map<String, String> headers, String body, String bodyMediaType);

   default CompletionStage<RestResponse> get(String url) {
      return get(url, Collections.emptyMap());
   }

   CompletionStage<RestResponse> get(String url, Map<String, String> headers);
}
