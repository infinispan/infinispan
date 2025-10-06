package org.infinispan.client.rest;

import java.io.Closeable;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Client to execute arbitrary requests on the server.
 * The URL to be called is the scheme, host and port configured in the {@link org.infinispan.client.rest.configuration.RestClientConfigurationBuilder}
 * plus the 'path' that should be supplied to the methods of this class.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestRawClient {

   default CompletionStage<RestResponse> post(String path) {
      return post(path, Collections.emptyMap(), RestEntity.empty());
   }

   default CompletionStage<RestResponse> post(String path, Map<String, String> headers) {
      return post(path, headers, RestEntity.empty());
   }

   default CompletionStage<RestResponse> post(String path, RestEntity entity) {
      return post(path, Collections.emptyMap(), entity);
   }

   CompletionStage<RestResponse> post(String path, Map<String, String> headers, RestEntity entity);

   default CompletionStage<RestResponse> put(String path) {
      return put(path, Collections.emptyMap(), RestEntity.empty());
   }

   default CompletionStage<RestResponse> put(String path, RestEntity entity) {
      return put(path, Collections.emptyMap(), entity);
   }

   CompletionStage<RestResponse> put(String path, Map<String, String> headers, RestEntity entity);

   default CompletionStage<RestResponse> get(String path) {
      return get(path, Collections.emptyMap());
   }

   CompletionStage<RestResponse> get(String path, Map<String, String> headers);

   default CompletionStage<RestResponse> delete(String path) {
      return delete(path, Collections.emptyMap());
   }

   CompletionStage<RestResponse> get(String path, Map<String, String> headers, Supplier<HttpResponse.BodyHandler<?>> supplier);

   CompletionStage<RestResponse> delete(String path, Map<String, String> headers);

   default CompletionStage<RestResponse> options(String path) {
      return options(path, Collections.emptyMap());
   }

   CompletionStage<RestResponse> options(String path, Map<String, String> headers);

   CompletionStage<RestResponse> head(String path, Map<String, String> headers);

   default CompletionStage<RestResponse> head(String path) {
      return head(path, Collections.emptyMap());
   }

   Closeable listen(String url, Map<String, String> headers, RestEventListener listener);

   default CompletionStage<RestResponse> execute(String method, String path) {
      return execute(method, path, Map.of());
   }

   default CompletionStage<RestResponse> execute(String method, String path, Map<String, String> headers) {
      return execute(method, path, headers, RestEntity.EMPTY);
   }

   CompletionStage<RestResponse> execute(String method, String path, Map<String, String> headers, RestEntity entity);
}
