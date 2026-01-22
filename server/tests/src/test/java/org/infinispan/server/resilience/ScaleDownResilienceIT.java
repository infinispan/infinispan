package org.infinispan.server.resilience;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponseInfo.ACCEPTED;
import static org.infinispan.client.rest.RestResponseInfo.NO_CONTENT;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationProperties;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.testing.Eventually;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.netty.handler.codec.http.HttpResponseStatus;

public class ScaleDownResilienceIT {

   @RegisterExtension
   public static final InfinispanServerExtension SERVER =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerTest.xml")
               .numServers(5)
               .runMode(ServerRunMode.CONTAINER)
               .property(INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true")
               .build();

   @Test
   public void testOrderlyScaleDown() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).persistence().addSoftIndexFileStore();
      RemoteCache<Object, Object> hotRod = SERVER.hotrod().withServerConfiguration(builder).create();

      populateCache(hotRod);
      assertCacheData(hotRod);

      shutdownAndRestart();
      assertCacheData(hotRod);
   }

   @Test
   public void testVeryShortTimeout() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).persistence().addSoftIndexFileStore();
      RemoteCache<Object, Object> hotRod = SERVER.hotrod().withServerConfiguration(builder).create();

      populateCache(hotRod);
      assertCacheData(hotRod);

      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);

      // Connect to the last server. We're avoiding a connection to the coordinator.
      try (RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get(SERVER.getServerDriver().serverCount() - 1)) {

         // The server has 1ms to execute state transfer.
         RestResponse res = sync(rest.container().leave(3, TimeUnit.MILLISECONDS), 1, TimeUnit.MINUTES);
         int status = res.status();
         assertThat(status)
               .withFailMessage(String.format("%nExpected: %d%nActual: %d%nWhen using short timeout", ACCEPTED, status))
               .isEqualTo(HttpResponseStatus.REQUEST_TIMEOUT.code());
         res.close();
      }
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

   private void shutdownAndRestart() throws Exception {
      for (int i = SERVER.getServerDriver().serverCount() - 1; i >= 0; i--) {
         RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
               .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
               .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
         try (RestClient rest = SERVER.rest().withClientConfiguration(restClientBuilder).get(i)) {
            RestResponse res = sync(rest.container().leave(30, TimeUnit.SECONDS), 1, TimeUnit.MINUTES);
            int status = res.status();
            assertThat(status)
                  .withFailMessage(String.format("%nExpected: %d%nActual: %d%nWhen stopping cache manager %d", NO_CONTENT, status, i))
                  .isEqualTo(NO_CONTENT);
            res.close();
         }

         SERVER.getServerDriver().stop(i);
      }

      ContainerInfinispanServerDriver serverDriver = (ContainerInfinispanServerDriver) SERVER.getServerDriver();
      Eventually.eventually(
            "Cluster did not shutdown within timeout",
            () -> IntStream.range(0, SERVER.getServerDriver().serverCount()).noneMatch(serverDriver::isRunning),
            serverDriver.getTimeout(), 1, TimeUnit.SECONDS);

      serverDriver.restartCluster();
   }
}
