package org.infinispan.server.resilience;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.rest.RestClient;
import org.infinispan.commons.util.Eventually;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.test.ContainerInfinispanServerDriver;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.infinispan.server.test.ServerRunMode;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @since 10.0
 */
public class GracefulShutdownRestartIT {

   @ClassRule
   public static final InfinispanServerRule SERVER = new InfinispanServerRule(
         new InfinispanServerRuleConfigurationBuilder("configuration/ClusteredServerTest.xml").numServers(2).serverRunMode(ServerRunMode.CONTAINER));

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVER);

   @Test
   public void testGracefulShutdownRestart() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).persistence().addSingleFileStore();
      RemoteCache<Object, Object> hotRod = SERVER_TEST.hotrod().withServerConfiguration(builder).create();

      for (int i = 0; i < 100; i++) {
         hotRod.put(String.format("k%03d", i), String.format("v%03d", i));
      }

      RestClient rest = SERVER_TEST.rest().get();
      sync(rest.cluster().stop());
      ContainerInfinispanServerDriver serverDriver = (ContainerInfinispanServerDriver) SERVER.getServerDriver();
      Eventually.eventually(
            "Cluster did not shutdown within timeout",
            () -> (!serverDriver.isRunning(0) && !serverDriver.isRunning(1)),
            10, 1, TimeUnit.SECONDS);

      serverDriver.restartCluster();

      for (int i = 0; i < 100; i++) {
         assertEquals(String.format("v%03d", i), hotRod.get(String.format("k%03d", i)));
      }
   }
}
