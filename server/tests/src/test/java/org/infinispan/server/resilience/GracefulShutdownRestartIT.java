package org.infinispan.server.resilience;

import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationProperties;
import org.infinispan.commons.test.Eventually;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @since 10.0
 */
public class GracefulShutdownRestartIT {

   @ClassRule
   public static final InfinispanServerRule SERVER =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
                                    .numServers(2)
                                    .runMode(ServerRunMode.CONTAINER)
                                    .build();

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);

   @Test
   public void testGracefulShutdownRestart() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).persistence().addSingleFileStore().segmented(false);
      RemoteCache<Object, Object> hotRod = SERVER_TEST.hotrod().withServerConfiguration(builder).create();

      for (int i = 0; i < 100; i++) {
         hotRod.put(String.format("k%03d", i), String.format("v%03d", i));
      }

      RestClientConfigurationBuilder restClientBuilder = new RestClientConfigurationBuilder()
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      RestClient rest = SERVER_TEST.rest().withClientConfiguration(restClientBuilder).get();
      sync(rest.cluster().stop(), 5, TimeUnit.MINUTES);
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
}
