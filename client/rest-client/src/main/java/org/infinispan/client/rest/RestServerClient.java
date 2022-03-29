package org.infinispan.client.rest;

import java.util.List;
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
    * Performs a heap dump
    */
   CompletionStage<RestResponse> heapDump(boolean live);

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

   CompletionStage<RestResponse> connectorNames();

   CompletionStage<RestResponse> connector(String name);

   CompletionStage<RestResponse> connectorStart(String name);

   CompletionStage<RestResponse> connectorStop(String name);

   CompletionStage<RestResponse> connectorIpFilters(String name);

   CompletionStage<RestResponse> connectorIpFiltersClear(String name);

   CompletionStage<RestResponse> connectorIpFilterSet(String name, List<IpFilterRule> rules);

   CompletionStage<RestResponse> dataSourceNames();

   CompletionStage<RestResponse> dataSourceTest(String name);
}
