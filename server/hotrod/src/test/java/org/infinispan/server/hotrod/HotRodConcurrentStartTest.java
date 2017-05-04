package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServerWithDelay;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.Test;

/**
 * Tests concurrent Hot Rod server startups
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodConcurrentStartTest")
public class HotRodConcurrentStartTest extends MultipleCacheManagersTest {
   private int numberOfServers = 2;
   private String cacheName = "hotRodConcurrentStart";

   @Override
   protected void createCacheManagers() {
      for (int i = 0; i < numberOfServers; i++) {
         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration());
         cacheManagers.add(cm);
         ConfigurationBuilder cfg =
               hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
         cm.defineConfiguration(cacheName, cfg.build());
      }
   }

   public void testConcurrentStartup() throws InterruptedException, ExecutionException, TimeoutException {
      int initialPort = serverPort();

      List<HotRodServer> servers = new ArrayList<>();
      try {
         List<Future<HotRodServer>> futures = new ArrayList<>();
         futures.add(fork(() -> {
            TestResourceTracker.testThreadStarted(this);
            HotRodServer server = startHotRodServerWithDelay(getCacheManagers().get(0), initialPort, 2000);
            servers.add(server);
            return server;
         }));
         for (int i = 1; i < numberOfServers; i++) {
            int finalI = i;
            futures.add(fork(() -> {
               TestResourceTracker.testThreadStarted(this);
               HotRodServer server = startHotRodServer(getCacheManagers().get(finalI), initialPort + (finalI * 10));
               servers.add(server);
               return server;
            }));
         }

         for (Future<HotRodServer> hotRodServerFuture : futures) {
            hotRodServerFuture.get(30, TimeUnit.SECONDS);
         }
      } finally {
         servers.forEach(ServerTestingUtil::killServer);
      }
   }

}
