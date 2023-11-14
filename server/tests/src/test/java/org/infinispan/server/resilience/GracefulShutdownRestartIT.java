package org.infinispan.server.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
                                    .build();

   @Test
   public void testGracefulShutdownRestart() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).persistence().addSingleFileStore().segmented(false);
      RemoteCache<Object, Object> hotRod = SERVER.hotrod().withServerConfiguration(builder).create();

      for (int i = 0; i < 100; i++) {
         hotRod.put(String.format("k%03d", i), String.format("v%03d", i));
      }

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

      serverDriver.restartCluster();

      for (int i = 0; i < 100; i++) {
         assertEquals(String.format("v%03d", i), hotRod.get(String.format("k%03d", i)));
      }
   }

   @Test
   public void testCrashBeforeFinishedStarting() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      RemoteCache<Object, Object> hotRod = SERVER.hotrod().withServerConfiguration(builder).create();

      for (int i = 0; i < 100; i++) {
         hotRod.put(String.format("k%03d", i), String.format("v%03d", i));
      }

      ContainerInfinispanServerDriver serverDriver = (ContainerInfinispanServerDriver) SERVER.getServerDriver();

      // Create the REST client to retrieve health information.
      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get();

      // Locks is built-in and uses partition handling. We wait for it to be available on both nodes and then stops
      // one of the nodes. It will move availability to DEGRADED.
      CountdownLatchLoggingConsumer consumer = new CountdownLatchLoggingConsumer(1, ".*LOCKS]\\[Scope=.*]ISPN100010.*");

      serverDriver.stop(1);
      serverDriver.restart(1, consumer);
      consumer.await(60, TimeUnit.SECONDS);

      // Stop the CM before finishing start.
      serverDriver.stop(1);

      // Since the node left abruptly, the cluster should be DEGRADED.
      try (RestResponse res = sync(rest.cacheManager("default").health())) {
         Json body = Json.read(res.getBody());
         Json clusterHealth = body.at("cluster_health");
         assertThat(clusterHealth.at("health_status").asString()).isEqualTo("DEGRADED");
         assertThat(clusterHealth.at("number_of_nodes").asInteger()).isEqualTo(1);
      }

      // Start again without problems.
      serverDriver.restart(1);

      // All data still available.
      for (int i = 0; i < 100; i++) {
         assertEquals(String.format("v%03d", i), hotRod.get(String.format("k%03d", i)));
      }

      // After the node joining again, it should be healthy.
      try (RestResponse res = sync(rest.cacheManager("default").health())) {
         Json body = Json.read(res.getBody());
         Json clusterHealth = body.at("cluster_health");
         assertThat(clusterHealth.at("health_status").asString()).isEqualTo("HEALTHY");
         assertThat(clusterHealth.at("number_of_nodes").asInteger()).isEqualTo(2);
      }
   }
}
