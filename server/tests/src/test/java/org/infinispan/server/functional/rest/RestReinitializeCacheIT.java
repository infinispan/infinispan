package org.infinispan.server.functional.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.client.rest.RestResponseInfo.BAD_REQUEST;
import static org.infinispan.client.rest.RestResponseInfo.NO_CONTENT;
import static org.infinispan.query.remote.client.ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;

import java.util.concurrent.TimeUnit;

import org.assertj.core.api.ThrowableAssert;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationProperties;
import org.infinispan.commons.test.Eventually;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.server.test.core.InfinispanServerDriver;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class RestReinitializeCacheIT {

   @RegisterExtension
   public static InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(3)
               .runMode(ServerRunMode.CONTAINER)
               .property(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true")
               .build();

   @AfterEach
   protected void afterEach() {
      InfinispanServerDriver driver = SERVER.getServerDriver();
      for (int i = 0; i < 3; i++) {
         if (driver.isRunning(i)) driver.stop(i);
         driver.restart(i);
      }
   }

   @Test
   public void testReinitializeCache() {
      var clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.maxRetries(5);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .persistence().addSoftIndexFileStore();
      builder.clustering().partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES);
      RemoteCache<Object, Object> hotRod = SERVER.hotrod()
            .withServerConfiguration(builder)
            .withClientConfiguration(clientBuilder)
            .create();

      // Insert an entry to the cache.
      hotRod.put("k", "v");
      assertThat(hotRod.get("k")).isEqualTo("v");

      InfinispanServerDriver serverDriver = SERVER.getServerDriver();

      // Create the REST client to retrieve health information and graceful shutdown.
      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get();

      // Gracefully shutdowns the cluster.
      shutdown(rest);

      // Restart only first node, there are still missing members.
      serverDriver.restart(0);

      // The operation does not succeed because cluster have missing members.
      assertCacheMissingMembers(() -> hotRod.get("k"));

      // Add another member, but still missing nodes.
      serverDriver.restart(1);
      assertCacheMissingMembers(() -> hotRod.get("k"));

      // Reinitialize the cache with the REST endpoint.
      reinitialize(rest, hotRod.getName());

      // Cache working as expected.
      assertThat(hotRod.get("k")).isEqualTo("v");
   }

   @Test
   public void testReinitializeInternalCache() {
      // Create the REST client to retrieve health information and graceful shutdown.
      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get();
      InfinispanServerDriver serverDriver = SERVER.getServerDriver();

      // Graceful shutdown the cluster.
      shutdown(rest);

      // Restart first node, there are still missing members.
      serverDriver.restart(0);

      // Internal caches are initialized even with missing members.
      // Attempts to initialize return an error.
      try (RestResponse response = sync(rest.cache(PROTOBUF_METADATA_CACHE_NAME).markTopologyStable(false))) {
         assertThat(response.status()).isEqualTo(BAD_REQUEST);
         assertThat(response.body()).isEqualTo("\"Cache '___protobuf_metadata' is internal\"");
      }
   }

   private void shutdown(RestClient rest) {
      sync(rest.cluster().stop(), 5, TimeUnit.MINUTES).close();
      InfinispanServerDriver serverDriver = SERVER.getServerDriver();
      Eventually.eventually(
            "Cluster did not shutdown within timeout",
            () -> (!serverDriver.isRunning(0) && !serverDriver.isRunning(1)),
            serverDriver.getTimeout(), 1, TimeUnit.SECONDS);
   }

   private void reinitialize(RestClient client, String cache) {
      assertStatus(NO_CONTENT, client.cache(cache).markTopologyStable(false));
   }

   private void assertCacheMissingMembers(ThrowableAssert.ThrowingCallable callable) {
      assertThatThrownBy(callable)
            .isInstanceOf(HotRodClientException.class)
            .hasMessageContaining("MissingMembersException: ISPN000689:");
   }
}
