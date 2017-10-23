package org.infinispan.server.hotrod;

import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;

import java.lang.reflect.Method;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.HotRodSharedContainerTest")
public class HotRodSharedContainerTest extends MultipleCacheManagersTest {

   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private HotRodClient hotRodClient1;
   private HotRodClient hotRodClient2;
   private String cacheName = "HotRodCache";

   @Override
   protected void createCacheManagers() {
      GlobalConfigurationBuilder globalCfg = GlobalConfigurationBuilder.defaultClusteredBuilder();
      EmbeddedCacheManager cm =
            TestCacheManagerFactory.createClusteredCacheManager(globalCfg, hotRodCacheConfiguration());
      cacheManagers.add(cm);
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      cm.defineConfiguration(cacheName, builder.build());
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testTopologyConflict() {
      int basePort = serverPort();
      hotRodServer1 = startHotRodServer(cacheManagers.get(0), basePort, new HotRodServerConfigurationBuilder());
      hotRodServer2 = startHotRodServer(cacheManagers.get(0), basePort + 50, new HotRodServerConfigurationBuilder());
   }

   public void testSharedContainer(Method m) {
      int basePort = serverPort();
      hotRodServer1 =
            startHotRodServer(cacheManagers.get(0), basePort, new HotRodServerConfigurationBuilder().name("1"));
      hotRodServer2 =
            startHotRodServer(cacheManagers.get(0), basePort + 50, new HotRodServerConfigurationBuilder().name("2"));

      hotRodClient1 = new HotRodClient("127.0.0.1", hotRodServer1.getPort(), cacheName, 60, (byte) 20);

      hotRodClient2 = new HotRodClient("127.0.0.1", hotRodServer2.getPort(), cacheName, 60, (byte) 20);
      hotRodClient1.put(k(m), 0, 0, v(m));
      assertSuccess(hotRodClient2.get(k(m), 0), v(m));
   }

   @AfterMethod(alwaysRun = true)
   void killClientsAndServers() {
      killClient(hotRodClient1);
      killClient(hotRodClient2);
      killServer(hotRodServer1);
      killServer(hotRodServer2);
   }

}
