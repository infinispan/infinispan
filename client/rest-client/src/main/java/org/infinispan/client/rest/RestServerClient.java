package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestServerClient {
   CompletionStage<RestResponse> configuration();

   CompletionStage<RestResponse> stop();

   CompletionStage<RestResponse> threads();

   CompletionStage<RestResponse> info();

   CompletionStage<RestResponse> memory();

   CompletionStage<RestResponse> env();

   CompletionStage<RestResponse> ignoreCache(String cacheManagerName, String cacheName);

   CompletionStage<RestResponse> unIgnoreCache(String cacheManagerName, String cacheName);

   CompletionStage<RestResponse> listIgnoredCaches(String cacheManagerName);
}
