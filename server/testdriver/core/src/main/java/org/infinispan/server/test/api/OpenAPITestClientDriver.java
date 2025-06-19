package org.infinispan.server.test.api;

import org.infinispan.client.openapi.ApiException;
import org.infinispan.client.openapi.OpenAPIClient;
import org.infinispan.client.openapi.configuration.OpenAPIClientConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;

/**
 * REST operations for the testing framework
 *
 * @author Tristan Tarrant
 * @since 10
 */
public class OpenAPITestClientDriver extends AbstractTestClientDriver<OpenAPITestClientDriver> {
   public static final int TIMEOUT = Integer.getInteger("org.infinispan.test.server.http.timeout", 10);

   private OpenAPIClientConfigurationBuilder clientConfiguration = new OpenAPIClientConfigurationBuilder();
   private final TestServer testServer;
   private final TestClient testClient;
   private int port = 11222;

   public OpenAPITestClientDriver(TestServer testServer, TestClient testClient) {
      this.testServer = testServer;
      this.testClient = testClient;
   }

   /**
    * Provide a custom client configuration to connect to the server via REST
    *
    * @param clientConfiguration
    * @return the current {@link OpenAPITestClientDriver} instance with the rest client configuration override
    */
   public OpenAPITestClientDriver withClientConfiguration(OpenAPIClientConfigurationBuilder clientConfiguration) {
      this.clientConfiguration = clientConfiguration;
      return this;
   }

   public OpenAPITestClientDriver withPort(int port) {
      this.port = port;
      return this;
   }

   /**
    * Create and get a REST client.
    *
    * @return a new instance of the {@link OpenAPIClient}
    */
   public OpenAPIClient get() {
      return testClient.registerResource(testServer.newOpenAPIClient(clientConfiguration, port));
   }

   /**
    * Create and get a REST client that is connected to the Nth server of the cluster.
    *
    * @param n the index of the server
    * @return a new instance of the {@link OpenAPIClient}
    */
   public OpenAPIClient get(int n) {
      return testClient.registerResource(testServer.newOpenAPIClientForServer(clientConfiguration, port, n));
   }

   /**
    * Create a new REST client and create a cache whose name will be the test name where this method
    * is called from.
    *
    * @return new {@link RestClient} instance
    */
   public OpenAPIClient create() {
      OpenAPIClient client = get();
      String name = testClient.getMethodName(qualifiers);
      String configEntity;
      if (serverConfiguration != null) {
         configEntity = serverConfiguration.toStringConfiguration(name, MediaType.APPLICATION_JSON, true);
      } else {
         configEntity = forCacheMode(mode != null ? mode : CacheMode.DIST_SYNC).toStringConfiguration(name);
      }
      try {
         client.cache().putCache(name, configEntity);
         testClient.registerOpenAPICache(name, client);
         // If the request succeeded without authn but we were expecting to authenticate, it's an error
//         if (client.getConfiguration().security().authentication().enabled() && !response.usedAuthentication()) {
//            throw new SecurityException("Authentication expected but anonymous access succeeded");
//         }
         return client;
      } catch (ApiException e) {
         switch (e.getCode()) {
            case 400:
               throw new IllegalArgumentException("Bad request while attempting to obtain rest client: " + e.getCode());
            case 401:
            case 403:
               throw new SecurityException("Authentication error while attempting to obtain rest client = " + e.getCode());
            default:
               throw new RuntimeException(e);
         }
      }
   }

   @Override
   public OpenAPITestClientDriver self() {
      return this;
   }
}
