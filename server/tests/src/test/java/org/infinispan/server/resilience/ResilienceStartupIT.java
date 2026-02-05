package org.infinispan.server.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.client.rest.RestResponseInfo.BAD_REQUEST;
import static org.infinispan.client.rest.RestResponseInfo.OK;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;

import java.util.List;
import java.util.Optional;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationProperties;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;

public class ResilienceStartupIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(1)
               .runMode(ServerRunMode.CONTAINER)
               .property(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true")
               .build();

   @Test
   public void testRestartWithInvalidCache() {
      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get();

      // Create a cache with an invalid configuration that remains in the failed state.
      String invalidCacheName = "books";
      String invalidConfig = """
            <infinispan>
              <cache-container>
                <replicated-cache name="books">
                  <encoding media-type="application/x-java-object"/>
                  <indexing>
                    <indexed-entities>
                      <indexed-entity>Dummy</indexed-entity>
                    </indexed-entities>
                  </indexing>
                </replicated-cache>
              </cache-container>
            </infinispan>""";
      try (RestResponse response = sync(rest.cache(invalidCacheName).createWithConfiguration(RestEntity.create(APPLICATION_XML, invalidConfig)))) {
         assertThat(response.status()).isEqualTo(BAD_REQUEST);
      }

      // Invalid cache exists and is in failed state.
      assertCacheIsFailed(rest, invalidCacheName);

      // Restart the node again.
      // This will load the invalid cache from caches.xml and fail to create again.
      // The server and cache manager should NOT fail.
      SERVER.getServerDriver().stopCluster();
      SERVER.getServerDriver().restartCluster();

      // Assert that the cache manager and server are running.
      // Also validate that the cache still exists in a FAILED state.
      assertCacheIsFailed(rest, invalidCacheName);
   }

   @Test
   public void testInvalidConnectorCache() {
      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get();

      // Define an invalid configuration for the RESP cache.
      String invalidCacheName = RespServerConfiguration.DEFAULT_RESP_CACHE;
      String invalidConfig = """
            <infinispan>
              <cache-container>
                <distributed-cache name="respCache">
                  <encoding media-type="application/x-java-object"/>
                  <indexing>
                   <indexed-entities>
                     <indexed-entity>Dummy</indexed-entity>
                    </indexed-entities>
                 </indexing>
                </distributed-cache>
              </cache-container>
            </infinispan>
            """;
      // Store the configuration for the RESP cache.
      // Once a connection is established using the RESP protocol, it will fail to create the cache.
      try (RestResponse response = sync(rest.cache(invalidCacheName).createWithConfiguration(RestEntity.create(APPLICATION_XML, invalidConfig)))) {
         assertThat(response.status()).isEqualTo(BAD_REQUEST);
      }

      assertCacheIsFailed(rest, invalidCacheName);
      try (RedisClient client = RedisClient.create(SERVER.resp().connectionString(0))) {
         assertThatThrownBy(client::connect)
               .isInstanceOf(RedisConnectionException.class);
      }

      // Restart the driver and re-create the RESP connector.
      SERVER.getServerDriver().stopCluster();
      SERVER.getServerDriver().restartCluster();

      // Assert it still fails to create the RESP cache.
      try (RedisClient client = RedisClient.create(SERVER.resp().connectionString(0))) {
         assertThatThrownBy(client::connect)
               .isInstanceOf(RedisConnectionException.class);
      }

      // Assert the RESP cache is failed, and that the server and cache manager are running.
      assertCacheIsFailed(rest, invalidCacheName);
   }

   private void assertCacheIsFailed(RestClient rest, String cacheName) {
      try (RestResponse response = sync(rest.container().health())) {
         assertThat(response.status()).isEqualTo(OK);
         Json payload = Json.read(response.body());
         List<Json> statuses = payload.at("cache_health").asJsonList();
         Optional<Json> books = statuses.stream().filter(j -> j.at("cache_name").asString().equals(cacheName)).findFirst();
         assertThat(books.isPresent()).isTrue();
         assertThat(books.get().at("status").asString()).isEqualTo("FAILED");
      }
   }
}
