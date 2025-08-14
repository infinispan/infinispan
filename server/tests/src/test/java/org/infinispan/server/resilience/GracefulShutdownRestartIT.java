package org.infinispan.server.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationProperties;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Eventually;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.CountdownLatchLoggingConsumer;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @since 10.0
 */
public class GracefulShutdownRestartIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
                                    .numServers(2)
                                    .runMode(ServerRunMode.CONTAINER)
                                    .property(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true")
                                    .build();

   @Test
   public void testGracefulShutdownRestart() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).persistence().addSoftIndexFileStore().segmented(false);
      RemoteCache<Object, Object> hotRod = SERVER.hotrod().withServerConfiguration(builder).create();

      populateCache(hotRod);

      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get();
      shutdownAndRestart(rest);

      assertCacheData(hotRod);
   }

   @Test
   public void testClusterReadyDuringRecovery() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).persistence().addSoftIndexFileStore().segmented(false);
      RemoteCache<Object, Object> hotRod = SERVER.hotrod().withServerConfiguration(builder).create();

      populateCache(hotRod);

      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get();

      sync(rest.cluster().stop(), 5, TimeUnit.MINUTES).close();
      ContainerInfinispanServerDriver serverDriver = (ContainerInfinispanServerDriver) SERVER.getServerDriver();
      Eventually.eventually(
            "Cluster did not shutdown within timeout",
            () -> (!serverDriver.isRunning(0) && !serverDriver.isRunning(1)),
            serverDriver.getTimeout(), 1, TimeUnit.SECONDS);

      for (int i = 0; i < serverDriver.serverCount(); i++) {
         serverDriver.restart(i);

         try (RestResponse res = sync(rest.server().ready())) {
            assertThat(res.status()).isEqualTo(200);
         }
      }

      assertCacheData(hotRod);
   }

   @Test
   public void testRebalanceAndRestart() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).persistence().addSoftIndexFileStore();
      RemoteCache<Object, Object> hotRod = SERVER.hotrod().withServerConfiguration(builder).create();

      populateCache(hotRod);

      // Create the REST client to issue admin commands.
      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get();

      // First, we disable rebalance cluster-wide.
      assertStatus(204, rest.container().disableRebalancing());

      // We get the cache details to assert it is in fact disabled.
      try (RestResponse res = sync(rest.cache(hotRod.getName()).details())) {
         Json body = Json.read(res.body());
         assertThat(body.at("rebalancing_enabled").asBoolean()).isFalse();
      }

      // Then we execute the graceful shutdown and restart the cluster.
      shutdownAndRestart(rest);

      // After the restart, rebalance is still disabled.
      try (RestResponse res = sync(rest.cache(hotRod.getName()).details())) {
         Json body = Json.read(res.body());
         assertThat(body.at("rebalancing_enabled").asBoolean()).isFalse();
      }

      // We enable rebalance again after restart.
      assertStatus(204, rest.container().enableRebalancing());

      // Assert all nodes back.
      assertHealthyCluster(rest);

      // All data still available.
      assertCacheData(hotRod);
   }

   @Test
   public void testCrashBeforeFinishedStarting() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      RemoteCache<Object, Object> hotRod = SERVER.hotrod().withServerConfiguration(builder).create();

      populateCache(hotRod);

      ContainerInfinispanServerDriver serverDriver = (ContainerInfinispanServerDriver) SERVER.getServerDriver();

      // Create the REST client to retrieve health information.
      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get();

      CountdownLatchLoggingConsumer consumer = new CountdownLatchLoggingConsumer(1, ".*respCache]\\[Scope=.*]ISPN100010.*");

      serverDriver.stop(1);
      serverDriver.restart(1, consumer);
      consumer.await(60, TimeUnit.SECONDS);

      // Stop the CM before finishing start.
      serverDriver.stop(1);

      try (RestResponse res = sync(rest.container().health())) {
         Json body = Json.read(res.body());
         Json clusterHealth = body.at("cluster_health");
         assertThat(clusterHealth.at("health_status").asString()).isEqualTo("HEALTHY");
         assertThat(clusterHealth.at("number_of_nodes").asInteger()).isEqualTo(1);
      }

      // Start again without problems.
      serverDriver.restart(1);

      // All data still available.
      assertCacheData(hotRod);

      // After the node joining again, it should be healthy.
      assertHealthyCluster(rest);
   }

   private void shutdownAndRestart(RestClient rest) {
      sync(rest.cluster().stop(), 5, TimeUnit.MINUTES).close();
      ContainerInfinispanServerDriver serverDriver = (ContainerInfinispanServerDriver) SERVER.getServerDriver();
      Eventually.eventually(
            "Cluster did not shutdown within timeout",
            () -> (!serverDriver.isRunning(0) && !serverDriver.isRunning(1)),
            serverDriver.getTimeout(), 1, TimeUnit.SECONDS);

      serverDriver.restartCluster();
   }

   private void populateCache(RemoteCache<Object, Object> hotRod) {
      for (int i = 0; i < 100; i++) {
         hotRod.put(String.format("k%03d", i), String.format("v%03d", i));
      }
   }

   private void assertCacheData(RemoteCache<Object, Object> hotRod) {
      for (int i = 0; i < 100; i++) {
         assertThat(hotRod.get(String.format("k%03d", i)))
               .isEqualTo(String.format("v%03d", i));
      }
   }

   private void assertHealthyCluster(RestClient rest) {
      try (RestResponse response = sync(rest.container().health())) {
         Json body = Json.read(response.body());
         assertThat(body.at("cluster_health")).isNotNull();

         Json health = body.at("cluster_health");
         assertThat(health.at("health_status").asString()).isEqualTo("HEALTHY");
         assertThat(health.at("number_of_nodes").asInteger()).isEqualTo(2);
      }
   }
}
