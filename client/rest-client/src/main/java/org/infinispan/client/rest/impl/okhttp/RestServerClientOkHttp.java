package org.infinispan.client.rest.impl.okhttp;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestLoggingClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestServerClient;

import okhttp3.FormBody;
import okhttp3.Request;

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
      return client.execute(baseServerURL + "?action=stop");
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

   @Override
   public CompletionStage<RestResponse> report() {
      return client.execute(baseServerURL, "report");
   }

   @Override
   public CompletionStage<RestResponse> ignoreCache(String cacheManagerName, String cacheName) {
      return ignoreCacheOp(cacheManagerName, cacheName, "POST");
   }

   @Override
   public CompletionStage<RestResponse> unIgnoreCache(String cacheManagerName, String cacheName) {
      return ignoreCacheOp(cacheManagerName, cacheName, "DELETE");
   }

   private CompletionStage<RestResponse> ignoreCacheOp(String cacheManagerName, String cacheName, String method) {
      String url = String.format("%s/ignored-caches/%s/%s", baseServerURL, cacheManagerName, cacheName);
      Request.Builder builder = new Request.Builder().url(url).method(method, new FormBody.Builder().build());
      return client.execute(builder);
   }

   @Override
   public CompletionStage<RestResponse> listIgnoredCaches(String cacheManagerName) {
      String url = String.format("%s/ignored-caches/%s", baseServerURL, cacheManagerName);
      return client.execute(new Request.Builder().url(url));
   }

   @Override
   public RestLoggingClient logging() {
      return new RestLoggingClientOkHttp(client);
   }
}
