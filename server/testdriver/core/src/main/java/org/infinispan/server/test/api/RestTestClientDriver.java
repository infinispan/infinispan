package org.infinispan.server.test.api;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;

/**
 * REST operations for the testing framework
 *
 * @author Tristan Tarrant
 * @since 10
 */
public class RestTestClientDriver extends BaseTestClientDriver<RestTestClientDriver> {
   public static final int TIMEOUT = Integer.getInteger("org.infinispan.test.server.http.timeout", 10);

   private RestClientConfigurationBuilder clientConfiguration = new RestClientConfigurationBuilder();
   private TestServer testServer;
   private TestClient testClient;
   private int port = 11222;

   public RestTestClientDriver(TestServer testServer, TestClient testClient) {
      this.testServer = testServer;
      this.testClient = testClient;
   }

   /**
    * Provide a custom client configuration to connect to the server via REST
    *
    * @param clientConfiguration
    * @return the current {@link RestTestClientDriver} instance with the rest client configuration override
    */
   public RestTestClientDriver withClientConfiguration(RestClientConfigurationBuilder clientConfiguration) {
      this.clientConfiguration = clientConfiguration;
      return this;
   }

   public RestTestClientDriver withPort(int port) {
      this.port = port;
      return this;
   }

   /**
    * Create and get a REST client.
    *
    * @return a new instance of the {@link RestClient}
    */
   public RestClient get() {
      return testClient.registerResource(testServer.newRestClient(clientConfiguration, port));
   }

   /**
    * Create and get a REST client that is connected to the Nth server of the cluster.
    *
    * @return a new instance of the {@link RestClient}
    */
   public RestClient get(int n) {
      return testClient.registerResource(testServer.newRestClientForServer(clientConfiguration, port, n));
   }

   /**
    * Create a new REST client and create a cache whose name will be the test name where this method
    * is called from.
    *
    * @return new {@link RestClient} instance
    */
   public RestClient create() {
      RestClient restClient = get();
      String name = testClient.getMethodName(qualifier);
      CompletionStage<RestResponse> future;
      if (serverConfiguration != null) {
         RestEntity configEntity = RestEntity.create(MediaType.APPLICATION_XML, serverConfiguration.toStringConfiguration(name));
         future = restClient.cache(name).createWithConfiguration(configEntity, flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
      } else if (mode != null) {
         future = restClient.cache(name).createWithTemplate("org.infinispan." + mode.name(), flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
      } else {
         future = restClient.cache(name).createWithTemplate("org.infinispan." + CacheMode.DIST_SYNC.name(), flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
      }
      try (RestResponse response = Exceptions.unchecked(() -> future.toCompletableFuture().get(TIMEOUT, TimeUnit.SECONDS))) {
         if (response.getStatus() != 200) {
            switch (response.getStatus()) {
               case 400:
                  throw new IllegalArgumentException("Bad request while attempting to obtain rest client: " + response.getStatus());
               case 401:
               case 403:
                  throw new SecurityException("Authentication error while attempting to obtain rest client = " + response.getStatus());
               default:
                  throw new RuntimeException("Could not obtain rest client = " + response.getStatus());
            }
         } else {
            // If the request succeeded without authn but we were expecting to authenticate, it's an error
            if (restClient.getConfiguration().security().authentication().enabled() && !response.usedAuthentication()) {
               throw new SecurityException("Authentication expected but anonymous access succeeded");
            }
            return restClient;
         }
      }
   }

   @Override
   public RestTestClientDriver self() {
      return this;
   }
}
