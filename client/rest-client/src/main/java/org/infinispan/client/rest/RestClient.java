package org.infinispan.client.rest;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.impl.okhttp.RestClientOkHttp;
import org.infinispan.commons.util.Experimental;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Experimental
public interface RestClient extends Closeable {
   @Override
   void close() throws IOException;

   CompletionStage<RestResponse> post(String url, Map<String, String> headers, Map<String, String> formParameters);

   CompletionStage<RestResponse> cachePost(String cache, String key, String value);

   CompletionStage<RestResponse> cachePut(String cache, String key, String value);

   CompletionStage<RestResponse> cacheGet(String cache, String key);

   CompletionStage<RestResponse> cacheDelete(String cache, String key);

   CompletionStage<RestResponse> cacheCreateFromTemplate(String cacheName, String template);

   CompletionStage<RestResponse> serverConfig();

   CompletionStage<RestResponse> serverStop();

   CompletionStage<RestResponse> serverThreads();

   CompletionStage<RestResponse> serverInfo();

   CompletionStage<RestResponse> serverMemory();

   CompletionStage<RestResponse> serverEnv();

   CompletionStage<RestResponse> serverCacheManagers();

   static RestClient forConfiguration(RestClientConfiguration configuration) {
      return new RestClientOkHttp(configuration);
   }
}
