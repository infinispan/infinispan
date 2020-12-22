package org.infinispan.client.rest;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Client to execute arbitrary requests on the server.
 * The URL to be called is the scheme, host and port configured in the {@link org.infinispan.client.rest.configuration.RestClientConfigurationBuilder}
 * plus the 'path' that should be supplied to the methods of this class.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestRawClient {
   CompletionStage<RestResponse> postForm(String path, Map<String, String> headers, Map<String, List<String>> formParameters);

   default CompletionStage<RestResponse> post(String path) {
      return post(path, Collections.emptyMap());
   }

   CompletionStage<RestResponse> post(String path, String body, String bodyMediaType);

   CompletionStage<RestResponse> post(String path, Map<String, String> headers);

   CompletionStage<RestResponse> putValue(String path, Map<String, String> headers, String body, String bodyMediaType);

   default CompletionStage<RestResponse> get(String path) {
      return get(path, Collections.emptyMap());
   }

   CompletionStage<RestResponse> get(String path, Map<String, String> headers);

   default CompletionStage<RestResponse> put(String path) {
      return put(path, Collections.emptyMap());
   }

   CompletionStage<RestResponse> put(String path, Map<String, String> headers);

   default CompletionStage<RestResponse> delete(String path) {
      return delete(path, Collections.emptyMap());
   }

   CompletionStage<RestResponse> delete(String path, Map<String, String> headers);

   CompletionStage<RestResponse> options(String path, Map<String, String> headers);

   CompletionStage<RestResponse> head(String path, Map<String, String> headers);

   Closeable listen(String url, Map<String, String> headers, RestEventListener listener);
}
