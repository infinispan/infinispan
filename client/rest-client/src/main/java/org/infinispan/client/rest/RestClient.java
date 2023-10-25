package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.impl.jdk.RestClientJDK;
import org.infinispan.commons.util.Experimental;
import org.jboss.logging.Logger;

/**
 * An experimental client for interacting with an Infinispan REST server.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@Experimental("This is not a supported API. Are you here for a perilous journey?")
public interface RestClient extends AutoCloseable {

   Logger LOG = Logger.getLogger("org.infinispan.REST");

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
   @Deprecated
   CompletionStage<RestResponse> cacheManagers();

   /**
    * Operations on the cache manager
    */
   RestContainerClient container();

   /**
    * Returns a list of available caches
    */
   CompletionStage<RestResponse> caches();

   /**
    * Returns a list of available caches detailed
    */
   CompletionStage<RestResponse> detailedCacheList();

   /**
    * Returns a list of available caches for a role
    */
   CompletionStage<RestResponse> cachesByRole(String roleName);

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
      return new RestClientJDK(configuration);
   }

   /**
    * Server security
    */
   RestSecurityClient security();
}
