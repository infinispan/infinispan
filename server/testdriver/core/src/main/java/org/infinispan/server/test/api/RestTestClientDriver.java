package org.infinispan.server.test.api;

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

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * REST operations for the testing framework
 *
 * @author Tristan Tarrant
 * @since 10
 */
public class RestTestClientDriver extends BaseTestClientDriver<RestTestClientDriver> {
   public static final int TIMEOUT = 10;

   private RestClientConfigurationBuilder clientConfiguration = new RestClientConfigurationBuilder();
   private TestServer testServer;
   private TestClient testClient;

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

   /**
    * Create and get a REST client.
    *
    * @return a new instance of the {@link RestClient}
    */
   public RestClient get() {
      return testClient.registerResource(testServer.newRestClient(clientConfiguration));
   }

   /**
    * Create and get a REST client that is connected to the Nth server of the cluster.
    *
    * @return a new instance of the {@link RestClient}
    */
   public RestClient get(int n) {
      return testClient.registerResource(testServer.newRestClient(clientConfiguration, n));
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
         RestEntity configEntity = RestEntity.create(MediaType.APPLICATION_XML, serverConfiguration.toXMLString(name));
         future = restClient.cache(name).createWithConfiguration(configEntity, flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
      } else if (mode != null) {
         future = restClient.cache(name).createWithTemplate("org.infinispan." + mode.name(), flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
      } else {
         future = restClient.cache(name).createWithTemplate("org.infinispan." + CacheMode.DIST_SYNC.name(), flags.toArray(new CacheContainerAdmin.AdminFlag[0]));
      }
      RestResponse response = Exceptions.unchecked(() -> future.toCompletableFuture().get(TIMEOUT, TimeUnit.SECONDS));
      response.close();
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
         return restClient;
      }
   }

   @Override
   public RestTestClientDriver self() {
      return this;
   }
}
