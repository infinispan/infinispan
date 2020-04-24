package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestServerClient {
   CompletionStage<RestResponse> configuration();

   /**
    * Shuts down the server we're connected to
    */
   CompletionStage<RestResponse> stop();

   /**
    * Returns thread information
    */
   CompletionStage<RestResponse> threads();

   /**
    * Returns information about the server
    */
   CompletionStage<RestResponse> info();

   /**
    * Returns memory information about the server
    */
   CompletionStage<RestResponse> memory();

   /**
    * Returns the server environment
    */
   CompletionStage<RestResponse> env();

   /**
    * Returns a report from the server
    */
   CompletionStage<RestResponse> report();

   CompletionStage<RestResponse> ignoreCache(String cacheManagerName, String cacheName);

   CompletionStage<RestResponse> unIgnoreCache(String cacheManagerName, String cacheName);

   CompletionStage<RestResponse> listIgnoredCaches(String cacheManagerName);

   RestLoggingClient logging();
}
