package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.module.TestGlobalConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests concurrent Hot Rod server startups
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodConcurrentStartTest")
public class HotRodConcurrentStartTest extends MultipleCacheManagersTest {
   public static final int NUMBER_OF_SERVERS = 2;
   public static final String CACHE_NAME = "hotRodConcurrentStart";

   @Override
   protected void createCacheManagers() {
      for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
         GlobalConfigurationBuilder globalConfig = GlobalConfigurationBuilder.defaultClusteredBuilder();
         if (i == 0) {
            // Delay the start of the first server's address cache
            globalConfig.addModule(TestGlobalConfigurationBuilder.class).cacheStartingCallback(cr -> {
               if (cr.getCacheName().startsWith(HotRodServerConfiguration.TOPOLOGY_CACHE_NAME_PREFIX)) {
                  log.tracef("Delaying start of cache %s on %s", cr.getCacheName(),
                             cr.getComponent(Transport.class).getAddress());
                  TestingUtil.sleepThread(1000);
               }
            });
         }
         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(globalConfig, null);
         cacheManagers.add(cm);
         ConfigurationBuilder cfg =
               hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
         cm.defineConfiguration(CACHE_NAME, cfg.build());
      }
   }

   public void testConcurrentStartup(Method m) throws Exception {
      int initialPort = serverPort();

      List<HotRodServer> servers = new ArrayList<>();
      try {
         List<Future<HotRodServer>> futures = new ArrayList<>();
         for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
            int finalI = i;
            futures.add(fork(() -> {
               HotRodServer server = startHotRodServer(getCacheManagers().get(finalI), initialPort + (finalI * 10));
               servers.add(server);
               return server;
            }));
         }

         for (Future<HotRodServer> hotRodServerFuture : futures) {
            hotRodServerFuture.get(30, TimeUnit.SECONDS);
         }

         try (HotRodClient client = new HotRodClient(servers.get(0).getHost(), servers.get(0).getPort(),
                                                     CACHE_NAME, HotRodConstants.VERSION_30)) {
            client.assertPut(m);
         }
      } finally {
         servers.forEach(ServerTestingUtil::killServer);
      }
   }

}
