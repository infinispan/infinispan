package org.infinispan.client.rest;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestRawClient {
   CompletionStage<RestResponse> postForm(String url, Map<String, String> headers, Map<String, List<String>> formParameters);

   default CompletionStage<RestResponse> post(String url) {
      return post(url, Collections.emptyMap());
   }

   CompletionStage<RestResponse> post(String url, Map<String, String> headers);

   CompletionStage<RestResponse> putValue(String url, Map<String, String> headers, String body, String bodyMediaType);

   default CompletionStage<RestResponse> get(String url) {
      return get(url, Collections.emptyMap());
   }

   CompletionStage<RestResponse> get(String url, Map<String, String> headers);

   default CompletionStage<RestResponse> put(String url) {
      return put(url, Collections.emptyMap());
   }

   CompletionStage<RestResponse> put(String url, Map<String, String> headers);

   default CompletionStage<RestResponse> delete(String url) {
      return delete(url, Collections.emptyMap());
   }

   CompletionStage<RestResponse> delete(String url, Map<String, String> emptyMap);
}
