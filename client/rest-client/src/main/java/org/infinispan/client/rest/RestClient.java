package org.infinispan.client.rest;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.impl.okhttp.RestClientOkHttp;
import org.infinispan.commons.util.Experimental;

/**
 * An experimental client for interacting with an Infinispan REST server.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Experimental("This is not a supported API. Are you here for a perilous journey?")
public interface RestClient extends Closeable {

   @Override
   void close() throws IOException;

   /**
    * Interact with the single server
    */
   RestServerClient server();

   /**
    * Interact with the whole cluster
    */
   RestClusterClient cluster();

   /**
    * Returns a list of available cache manager names
    */
   CompletionStage<RestResponse> cacheManagers();

   /**
    * Operations on the specified cache manager
    */
   RestCacheManagerClient cacheManager(String name);

   /**
    * Operations on the server's Container
    */
   RestContainerClient container();

   /**
    * Returns a list of available caches
    */
   CompletionStage<RestResponse> caches();

   /**
    * Operations on the specified cache
    */
   RestCacheClient cache(String name);

   /**
    * Returns a list of available counters
    */
   CompletionStage<RestResponse> counters();

   /**
    * Operations on the specified counter
    */
   RestCounterClient counter(String name);

   /**
    * Operations on tasks
    */
   RestTaskClient tasks();

   /**
    * Raw HTTP operations
    */
   RestRawClient raw();

   /**
    * Server metrics
    */
   RestMetricsClient metrics();

   /**
    * Protobuf schemas
    */
   RestSchemaClient schemas();

   /**
    * Returns the configuration of this {@link RestClient}
    */
   RestClientConfiguration getConfiguration();

   /**
    * Creates a {@link RestClient} instance based on the supplied configuration
    *
    * @param configuration a {@link RestClientConfiguration}
    * @return a {@link RestClient} instance
    */
   static RestClient forConfiguration(RestClientConfiguration configuration) {
      return new RestClientOkHttp(configuration);
   }

   /**
    * Server security
    */
   RestSecurityClient security();
}
