package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.HotRodServerStartStopTest", groups = "functional")
public class HotRodServerStartStopTest extends MultipleCacheManagersTest {

   private static int randomPort() {
      try {
         ServerSocket socket = new ServerSocket(0);
         return socket.getLocalPort();
      } catch (IOException e) {
         return 56001;
      }
   }

   private static final int FIRST_SERVER_PORT = randomPort();
   private HotRodServer hotRodServer1;
   private HotRodServer hotRodServer2;
   private HotRodServer hotRodServer3;

   @AfterMethod
   @Override
   protected void clearContent() {
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);

      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(manager(0), FIRST_SERVER_PORT, new HotRodServerConfigurationBuilder());
      hotRodServer2 = HotRodClientTestingUtil.startHotRodServer(manager(1));
      hotRodServer3 = HotRodClientTestingUtil.startHotRodServer(manager(2));

      waitForClusterToForm();
   }

   public void testTouchServer() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotRodServer1.getPort());
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build(), true);
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();
      remoteCache.put("k", "v");
      assertEquals("v", remoteCache.get("k"));

      CacheTopologyInfo info = remoteCache.getCacheTopologyInfo();
      Integer prevTop = info.getTopologyId();

      killServers(hotRodServer1, hotRodServer2, hotRodServer3);

      Arrays.stream(managers()).forEach(EmbeddedCacheManager::stop);
      cacheManagers.clear();

      hotRodServer1 = null;
      hotRodServer2 = null;
      hotRodServer3 = null;

      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      addClusterEnabledCacheManager(builder);

      hotRodServer1 = HotRodClientTestingUtil.startHotRodServer(manager(0), FIRST_SERVER_PORT, new HotRodServerConfigurationBuilder());

      // data was loss on restart
      assertNull(remoteCache.get("k1"));

      if (prevTop != null) {
         assertFalse("Topology is still the same: " + prevTop, prevTop.equals(remoteCache.getCacheTopologyInfo().getTopologyId()));
      }

      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer1);
      hotRodServer1 = null;
   }
}
