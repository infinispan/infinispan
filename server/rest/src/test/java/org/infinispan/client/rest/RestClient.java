package org.infinispan.client.rest;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestClient extends Closeable {
   @Override
   void close() throws IOException;

   CompletionStage<RestResponse> post(String cache, String key, String value);

   CompletionStage<RestResponse> put(String cache, String key, String value);

   CompletionStage<RestResponse> get(String cache, String key);

   CompletionStage<RestResponse> delete(String cache, String key);

   CompletionStage<RestResponse> createCacheFromTemplate(String cacheName, String template);

   CompletionStage<RestResponse> serverConfig();

   CompletionStage<RestResponse> serverStop();

   CompletionStage<RestResponse> serverThreads();

   CompletionStage<RestResponse> serverInfo();

   CompletionStage<RestResponse> serverMemory();

   CompletionStage<RestResponse> serverEnv();
}
